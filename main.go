package main

import (
	"bytes"
	"flag"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"

	log "github.com/sirupsen/logrus"
)

var (
	snapConfig SnapbackupConfig
)

// SnapbackupConfig exposes flags and general configuration settings
type SnapbackupConfig struct {
	QuickVerifyMode bool
	MaximumWorkers  int
	AWSOwnerID      string
	EC2Region       string

	SSHKnownHostsPath string
	SSHPrivateKeyFile string
	UploadHostname    string
	UploadUsername    string
	UploadDir         string
	FTPPassword       string
	UploadUsingFTP    bool
	UploadUsingLZ4    bool
}

func getAWSSession() *session.Session {
	sess := session.Must(session.NewSession())
	return sess
}

func getEC2InstanceMetadata(sess *session.Session) ec2metadata.EC2InstanceIdentityDocument {
	ec2metadataClient := ec2metadata.New(sess)
	instanceMetadata, err := ec2metadataClient.GetInstanceIdentityDocument()
	if err != nil {
		log.Fatal(err.Error())
	}
	return instanceMetadata
}

func getEC2Client(sess *session.Session) *ec2.EC2 {
	// Fetches creds from ~/.aws/credentials.
	ec2Client := ec2.New(sess, aws.NewConfig().WithRegion(snapConfig.EC2Region))
	return ec2Client
}

func snapshotWorker(snap *ec2.Snapshot, instance *ec2Instance, outputChan chan<- *ec2.Snapshot, quickVerify bool, workerNum int) {
	contextLogger := log.WithFields(log.Fields{
		"workernum": workerNum,
		"snapshot":  *snap.SnapshotId,
	})

	// Short delay before proceeding
	time.Sleep(10 * time.Second)

	if quickVerify {
		contextLogger.Info("Starting quick verification")
		instance.quickVerifySnapshot(snap, workerNum, contextLogger)
		contextLogger.Info("Finished quick verification")
	} else {
		contextLogger.Info("Starting migration")
		instance.migrateSnapshot(snap, workerNum, contextLogger)
		contextLogger.Info("Finished migration")
	}

	outputChan <- snap
}

func installSIGINTHandler() {
	// Attempt to clean up volumes if SIGINT is received.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		<-c

		log.Println("CTRL-C RECV. Attempting to cleanup. May take upto 60 seconds.")
		for _, uploadWorker := range uploadWorkers {
			if uploadWorker.finished {
				continue
			}

			// Kill any processes we have spawned, such as tar or lz
			for _, proc := range uploadWorker.processes {
				proc.Process.Kill()
			}

			time.Sleep(5)

			if uploadWorker.curPartitionMountPoint != "" {
				unmountAndDeleteTempMountPoint(uploadWorker.curPartitionMountPoint)
			}

			// Cleanup
			uploadWorker.inst.cleanupTempVolume(uploadWorker.volume, uploadWorker.snapshot, false)
		}

		os.Exit(1)
	}()
}

func parseFlags() {
	flag.StringVar(&snapConfig.UploadHostname, "hostname", "", "Address of the (s)ftp server.\nExample: abc.xyz:22")
	flag.StringVar(&snapConfig.UploadUsername, "username", "", "User to login to on the (s)ftp server.")
	flag.StringVar(&snapConfig.SSHPrivateKeyFile, "pkfile", "", "Path of the private key used to authenticate with the sftp server.\nExample: /home/user/.ssh/id_rsa")
	flag.StringVar(&snapConfig.AWSOwnerID, "ownerid", "847574330142", "Sets the AWS OwnerID that will be used to fetch the completed snapshots")
	flag.StringVar(&snapConfig.UploadDir, "dir", "", "The directory to upload the snapshots to. MUST end with a slash.")
	flag.StringVar(&snapConfig.SSHKnownHostsPath, "knownhostspath", "", "Path to the SSH known_hosts file")
	flag.StringVar(&snapConfig.EC2Region, "ec2region", "us-east-1", "The EC2 region to use.")
	flag.StringVar(&snapConfig.FTPPassword, "ftppass", "", "Password to login to the ftp server.")

	flag.BoolVar(&snapConfig.UploadUsingFTP, "ftp", false, "Specifies whether ftp is enabled (defaults to sftp)")
	flag.BoolVar(&snapConfig.UploadUsingLZ4, "lz4", false, "Use lz4 compression to upload the file")
	flag.BoolVar(&snapConfig.QuickVerifyMode, "quickverify", false, "Checks that all the snapshots backups that should exist on the remote system, do actually exist. However, it does NOT check the integrity of these remote files.")

	flag.IntVar(&snapConfig.MaximumWorkers, "maxworkers", 8, "Allows the user to specify how many concurrent uploads (MAX: 16)")

	printVersion := flag.Bool("version", false, "Print the version only")
	debugMode := flag.Bool("debug", false, "Enables verbose logging")

	flag.Parse()

	log.Println(GetVersion())
	if *printVersion {
		os.Exit(0)
	}

	if *debugMode {
		log.SetLevel(logrus.DebugLevel)
	}

	if snapConfig.UploadHostname == "" {
		log.Fatal("-hostname param required")
	}

	if snapConfig.UploadUsername == "" {
		log.Fatal("-username param required")
	}

	if snapConfig.UploadDir == "" {
		log.Fatal("-dir param required")
	}

	if snapConfig.EC2Region == "" {
		log.Fatal("-ec2region param required")
	}

	if snapConfig.UploadUsingFTP {
		if snapConfig.FTPPassword == "" {
			log.Fatal("-ftppass param required")
		}
	} else {
		if snapConfig.SSHPrivateKeyFile == "" {
			log.Fatal("-pkfile param required")
		}

		if snapConfig.SSHKnownHostsPath == "" {
			log.Fatal("-knownhostspath param required")
		}
	}

	if snapConfig.MaximumWorkers < 0 || snapConfig.MaximumWorkers > 16 {
		log.Fatal("-maxworkers out of bounds")
	}
}

func main() {
	parseFlags()

	// Open snapshot blacklist file
	hBlacklistFile, err := os.OpenFile("snapblacklist.dat", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.WithError(err).Fatal("Failed to open or create snapblacklist.dat")
	}
	defer hBlacklistFile.Close()

	// Read the list of blacklisted snapshots
	blacklistBuf := bytes.NewBuffer(nil)
	blacklistBuf.ReadFrom(hBlacklistFile)
	blacklistContents := string(blacklistBuf.Bytes())
	blacklistedSnapshots := strings.Split(blacklistContents, "\n")

	// Add a slash to end of the upload directory if one is not present
	if snapConfig.UploadDir[len(snapConfig.UploadDir)-1:] != "/" {
		snapConfig.UploadDir += "/"
	}

	// Exit the program if we haven't got all of the required binaries
	mandatoryBins := []string{"mount", "umount", "tar", "lz4", "lsblk"}
	for _, bin := range mandatoryBins {
		path, err := exec.LookPath(bin)
		if err != nil || path == "" {
			log.Fatalf("%s is not installed on the system.\n", bin)
			return
		}
	}

	// Test the remote connection
	if snapConfig.UploadUsingFTP {
		// Test FTP connection before proceeding
		ftpClient := connectToFTP()
		if ftpClient == nil {
			log.Fatal("Failed to the connect to the FTP server")
		}
		ftpClient.Quit()

		log.Debug("Test connection to FTP server was successful.")
	} else {
		// Test SSH connection before proceeding
		sshTestClient := connectViaSSH()
		if sshTestClient == nil {
			log.Fatal("Failed to the connect to the SFTP server")
		}
		sshTestClient.Close()

		log.Debug("Test connection to SSH server was successful.")
	}

	// Get an AWS Session
	sess := getAWSSession()
	instance := &ec2Instance{
		ec2Client:        getEC2Client(sess),
		instanceMetadata: getEC2InstanceMetadata(sess),
	}
	log.WithField("ec2inst", instance.instanceMetadata.InstanceID).Debug("Connected to instance")

	// Fetch the list of snapshots that are in the 'completed' state
	result := instance.getSnapshots(snapConfig.AWSOwnerID)
	var completedSnapshots []*ec2.Snapshot
	for _, snapshot := range result.Snapshots {
		if *snapshot.State == "completed" {
			completedSnapshots = append(completedSnapshots, snapshot)
		}
	}
	log.Infof("Found %d completed snapshots to process", len(completedSnapshots))

	// Handle CTRL-C
	installSIGINTHandler()

	if concurrencyMutex == nil {
		concurrencyMutex = &sync.Mutex{}
	}

	// Find out how many workers we can run concurrently
	availAttachPoints := instance.getAvailAttachmentPoints()
	if len(availAttachPoints) < snapConfig.MaximumWorkers {
		log.Warnf("Max workers restricted to %d, due to a lack of available attachment points", len(availAttachPoints))
		snapConfig.MaximumWorkers = len(availAttachPoints)
	}

	// Start spinning up snapshot workers
	currentSnapshotWorkers := 0
	workerChan := make(chan *ec2.Snapshot, len(result.Snapshots))
	for i, snapshot := range completedSnapshots {
		if currentSnapshotWorkers >= snapConfig.MaximumWorkers {
			break
		}

		// Check the snapshot is not in the blacklist
		skipSnapshot := false
		for _, scName := range blacklistedSnapshots {
			if *snapshot.SnapshotId == scName {
				log.WithField("snapshot", *snapshot.SnapshotId).Info("Skipped snapshot due to blacklist")
				skipSnapshot = true
				break
			}
		}
		if skipSnapshot {
			continue
		}

		// Spin up the worker on a goroutine
		go snapshotWorker(snapshot, instance, workerChan, snapConfig.QuickVerifyMode, i+1)
		currentSnapshotWorkers++
	}

	for i := 0; i < currentSnapshotWorkers; i++ {
		completedSnapshot := <-workerChan

		// Add the completed snapshot to the blacklist
		blacklistData := *completedSnapshot.SnapshotId + "\n"
		if _, err := hBlacklistFile.Write([]byte(blacklistData)); err != nil {
			log.WithError(err).WithField("snapshot", *completedSnapshot.SnapshotId).Error("Failed to write to blacklist file")
		}
		hBlacklistFile.Sync()

		log.WithField("snapshot", *completedSnapshot.SnapshotId).Debug("Finished handling snapshot")
	}

	log.Info("================================")
	log.Info("================================")
	log.Info("All workers finished.")
	log.Info("================================")
	log.Info("================================")

	for _, worker := range uploadWorkers {
		log.Info("================================")
		worker.log.Infof("Snapshot %s | Successful %d/%d", *worker.snapshot.SnapshotId,
			worker.numSuccessfulOps, worker.numExpectedPartitions)

		if worker.numSuccessfulOps < worker.numExpectedPartitions {
			worker.log.Warn("Errors for following partitions:")
			for _, partData := range worker.missingPartitions {
				log.Warn(partData)
			}
		}
		log.Info("================================")
	}

	log.Info("You may now manually detach all snapshot volumes from the instance.")
	log.Info("If one gets stuck while detaching, the server should be rebooted to prevent it from crashing on the next run.")
	log.Warn("NOTE: All snapshots are added to the blacklist after they have been processed, including ones where the requested operation failed.")
	log.Warn("If you wish to retry any snapshot that failed, you must first manually remove it from snapblacklist.dat")

	close(workerChan)
}

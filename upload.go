package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"golang.org/x/crypto/ssh"

	"github.com/jlaffaye/ftp"
	"github.com/pkg/sftp"
	log "github.com/sirupsen/logrus"
)

func connectViaSSH() *ssh.Client {
	var hostKey ssh.PublicKey

	key, err := ioutil.ReadFile(snapConfig.SSHPrivateKeyFile)
	if err != nil {
		log.WithError(err).Fatalf("Unable to read private key: %v", err)
		return nil
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		log.WithError(err).Fatalf("Unable to parse private key: %v", err)
		return nil
	}

	hostName := strings.Split(snapConfig.UploadHostname, ":")[0]
	hostKey, err = getHostKey(hostName)
	if err != nil {
		log.WithError(err).Fatalf("Failed to get hostkey for host:%s", hostName)
	}

	config := &ssh.ClientConfig{
		User: snapConfig.UploadUsername,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.FixedHostKey(hostKey),
	}

	client, err := ssh.Dial("tcp", snapConfig.UploadHostname, config)
	if err != nil {
		log.WithError(err).Errorf("unable to connect: %v\n", err)
		return nil
	}
	return client
}

// https://github.com/golang/crypto/blob/master/ssh/example_test.go
func getHostKey(host string) (ssh.PublicKey, error) {
	file, err := os.Open(snapConfig.SSHKnownHostsPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var hostKey ssh.PublicKey
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), " ")
		if len(fields) != 3 {
			continue
		}
		if strings.Contains(fields[0], host) {
			var err error
			hostKey, _, _, _, err = ssh.ParseAuthorizedKey(scanner.Bytes())
			if err != nil {
				return nil, fmt.Errorf(fmt.Sprintf("error parsing %q: %v", fields[2], err))
			}
			break
		}
	}

	if hostKey == nil {
		return nil, fmt.Errorf(fmt.Sprintf("no hostkey for %s", host))
	}
	return hostKey, nil
}

func writeDataToSFTP(outputFileName string, reader io.Reader) bool {
	log.Println("Connecting to sftp...")
	sshClient := connectViaSSH()
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		log.WithError(err).Error("Failed to create a new sftp client")
		return false
	}

	defer sftpClient.Close()
	defer sshClient.Close()

	filePath := snapConfig.UploadDir + outputFileName + ".lz"
	log.Infof("Uploading to file with path:%s\n", filePath)

	hFile, err := sftpClient.Create(filePath)
	type writerOnly struct{ io.Writer }
	bufferedWriter := bufio.NewWriter(writerOnly{hFile})
	numBytesWritten, err := bufferedWriter.ReadFrom(reader)
	if err != nil {
		log.WithError(err).Error("Error writing to remote server.")
		return false
	}

	log.Infof("Wrote %d bytes to file with path:%s\n", numBytesWritten, filePath)
	return true
}

func connectToFTP() *ftp.ServerConn {
	ftpServer, err := ftp.Connect(snapConfig.UploadHostname)
	if err != nil {
		log.WithError(err).Error("Failed to contact the ftp server")
		return nil
	}

	err = ftpServer.Login(snapConfig.UploadUsername, snapConfig.FTPPassword)
	if err != nil {
		log.WithError(err).Error("Failed to login to the ftp server but could make contact.")
		return nil
	}
	return ftpServer
}

func writeDataToFTP(outputFileName string, reader io.Reader) bool {
	log.Debug("Connecting to ftp...")

	ftpConn := connectToFTP()
	defer ftpConn.Quit()

	filePath := snapConfig.UploadDir + outputFileName
	if snapConfig.UploadUsingLZ4 {
		filePath += ".lz"
	}
	log.Infof("Uploading to file with path:%s\n", filePath)

	err := ftpConn.Stor(filePath, reader)
	if err != nil {
		log.WithError(err).Error("Error writing to remote server.")
		return false
	}

	log.Infof("Finished writing to FTP Server (path:%s)", filePath)
	return true
}

// uploadProcessStdOut takes a process's stdout, potentially lz4 compresses it and uploads it over (s)ftp.
// Expects procCmd to not yet be started.
func uploadProcessStdOut(outputFileName string, procCmd *exec.Cmd, uploadWorker *uploadWorkerInfo) bool {
	stdoutPipe, err := procCmd.StdoutPipe()
	if err != nil {
		log.WithError(err).Error("Failed to get the stdout pipe of process for stdout upload")
		return false
	}
	procCmd.Start()

	var uploadPipe io.Reader
	uploadPipe = stdoutPipe

	if snapConfig.UploadUsingLZ4 {
		log.Debug("Starting lz4")

		lzCmd := exec.Command("lz4", "-z", "-1")
		uploadWorker.processes = append(uploadWorker.processes, lzCmd)

		lzCmd.Stdin = stdoutPipe
		lzStdoutPipe, err := lzCmd.StdoutPipe()
		if err != nil {
			log.WithError(err).Error("Failed to get the stdout pipe for lz4")
			return false
		}
		lzCmd.Start()
		uploadPipe = lzStdoutPipe
	}

	// Currently only supports two modes
	if snapConfig.UploadUsingFTP {
		return writeDataToFTP(outputFileName, uploadPipe)
	}
	return writeDataToSFTP(outputFileName, uploadPipe)
}

func getFileUploadName(volumeID string, snapshotID string, partitionNum int, fileExtension string) string {
	return fmt.Sprintf("%s-%s-%d%s", volumeID, snapshotID, partitionNum, fileExtension)
}

func uploadPartitionsUsingTar(partitions []map[string]string, uploadWorker *uploadWorkerInfo) {
	correctDevices := filterBlockDevicesToGetFilesystems(partitions)
	if correctDevices == nil || len(correctDevices) == 0 {
		uploadWorker.log.Error("Failed to obtain any partitions to upload")
		return
	}

	uploadWorker.numExpectedPartitions = len(correctDevices)
	uploadWorker.log.Debugf("Found %d partition(s) to upload using tar\n", len(correctDevices))

	for i, partDevice := range correctDevices {
		// Ignore partitions where no FSTYPE is set but count as successful
		if partDevice["FSTYPE"] == "" {
			uploadWorker.log.Warnf("Skipping over partition %d as no FSTYPE given. Counted as successful.", i+1)
			uploadWorker.numSuccessfulOps++
			continue
		}

		partMountPoint := pickMountpointForSnapshotPartition(*uploadWorker.snapshot.SnapshotId, partDevice["NAME"])
		uploadWorker.log.WithFields(log.Fields{
			"volume":   *uploadWorker.volume.VolumeId,
			"snapshot": *uploadWorker.snapshot.SnapshotId,
			"partname": partDevice["NAME"],
			"devpath":  partDevice["DEVICEPATH"],
		}).Infof("Uploading partition %d/%d...\n", i+1, len(correctDevices))

		// Mount
		if mountTempVolume(partDevice, partMountPoint) == false {
			uploadWorker.log.Errorf("Partition %d/%d failed to mount", i+1, len(correctDevices))

			partData := fmt.Sprintf("%v", partDevice)
			uploadWorker.missingPartitions = append(uploadWorker.missingPartitions, partData)
			continue
		}

		uploadWorker.curPartitionMountPoint = partMountPoint

		// Upload
		// Create the command but do not yet execute it
		cmd := exec.Command("tar", "-c", ".")
		cmd.Dir = partMountPoint
		uploadWorker.processes = append(uploadWorker.processes, cmd)

		uploadFileName := getFileUploadName(*uploadWorker.volume.VolumeId, *uploadWorker.snapshot.SnapshotId, i+1, ".tar")
		if uploadProcessStdOut(uploadFileName, cmd, uploadWorker) == false {
			uploadWorker.log.WithFields(log.Fields{
				"volume":   *uploadWorker.volume.VolumeId,
				"snapshot": *uploadWorker.snapshot.SnapshotId,
				"partname": partDevice["NAME"],
				"devpath":  partDevice["DEVICEPATH"],
			}).Errorf("Failed to upload partition %d/%d\n", i+1, len(correctDevices))

			partData := fmt.Sprintf("%v", partDevice)
			uploadWorker.missingPartitions = append(uploadWorker.missingPartitions, partData)
		} else {
			uploadWorker.log.WithFields(log.Fields{
				"volume":   *uploadWorker.volume.VolumeId,
				"snapshot": *uploadWorker.snapshot.SnapshotId,
				"partname": partDevice["NAME"],
				"devpath":  partDevice["DEVICEPATH"],
			}).Infof("Successfully uploaded partition %d/%d\n", i+1, len(correctDevices))

			uploadWorker.numSuccessfulOps++
		}

		// Cleanup
		unmountAndDeleteTempMountPoint(partMountPoint)
		uploadWorker.curPartitionMountPoint = ""
	}
}

// findPartialFileNameOnRemote returns the full name of a file on a remote (S)FTP server based on a partial match
func findPartialFileNameOnRemote(partialName string) (string, error) {
	// Find partial file on a remote FTP server
	if snapConfig.UploadUsingFTP {
		ftpConn := connectToFTP()
		defer ftpConn.Quit()

		dirListing, err := ftpConn.List(snapConfig.UploadDir)
		if err != nil {
			return "", err
		}

		for _, remoteFile := range dirListing {
			if remoteFile.Type == ftp.EntryTypeFile {
				if strings.Contains(remoteFile.Name, partialName) {
					return remoteFile.Name, nil
				}
			}
		}

		return "", nil
	}

	// Find partial file on a remote SFTP server
	sshClient := connectViaSSH()
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		sshClient.Close()
		return "", err
	}

	defer sftpClient.Close()
	defer sshClient.Close()

	dirListing, err := sftpClient.ReadDir(snapConfig.UploadDir)
	if err != nil {
		return "", err
	}

	for _, remoteFile := range dirListing {
		if !remoteFile.IsDir() {
			if strings.Contains(remoteFile.Name(), partialName) {
				return remoteFile.Name(), nil
			}
		}
	}

	return "", nil
}

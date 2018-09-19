package main

import (
	"crypto/rand"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/service/ec2"
)

type ec2Instance struct {
	ec2Client        *ec2.EC2
	instanceMetadata ec2metadata.EC2InstanceIdentityDocument
}

// getSnapshots returns all snapshots related to an ownerID
func (inst *ec2Instance) getSnapshots(ownerID string) *ec2.DescribeSnapshotsOutput {
	snapInputData := &ec2.DescribeSnapshotsInput{
		OwnerIds: []*string{
			aws.String(ownerID),
		},
	}

	result, err := inst.ec2Client.DescribeSnapshots(snapInputData)
	if err != nil {
		log.Fatal(err.Error())
	}

	return result
}

type uploadWorkerInfo struct {
	workerNum              int
	processes              []*exec.Cmd
	finished               bool
	curPartitionMountPoint string
	inst                   *ec2Instance
	volume                 *ec2.Volume
	snapshot               *ec2.Snapshot
	missingPartitions      []string
	numExpectedPartitions  int
	numSuccessfulOps       int
	log                    *log.Entry
}

var (
	migrateTagSetting = "ec2migrate"

	// TODO: Set proper mount point. Has to end in slash.
	mountPointSetting = "/mnt/"

	concurrencyMutex      *sync.Mutex
	deviceSuffixBlacklist []string

	uploadWorkers []*uploadWorkerInfo
)

// findTempVolumeForSnap checks if a volume has already been created for the purpose of backing up this snapshot already
func (inst *ec2Instance) findTempVolumeForSnap(snap *ec2.Snapshot) *ec2.Volume {
	input := &ec2.DescribeVolumesInput{
		Filters: []*ec2.Filter{
			{
				Name: aws.String("snapshot-id"),
				Values: []*string{
					snap.SnapshotId,
				},
			},
			{
				// Tag used to check volume was created for snapbackup purposes
				Name: aws.String("tag-key"),
				Values: []*string{
					aws.String(migrateTagSetting),
				},
			},
			{
				Name: aws.String("availability-zone"),
				Values: []*string{
					aws.String(inst.instanceMetadata.AvailabilityZone),
				},
			},
		},
	}

	result, err := inst.ec2Client.DescribeVolumes(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Println(aerr.Error())
			}
		} else {
			log.Println(err.Error())
		}
		return nil
	}

	for _, vol := range result.Volumes {
		foundCorrectTag := false
		for _, tag := range vol.Tags {
			if *tag.Key == migrateTagSetting {
				foundCorrectTag = true
			}
		}

		if !foundCorrectTag {
			log.WithField("snapshot", *snap.SnapshotId).Errorf("Tried to obtain volume where no migrate tag was attached")
			return nil
		}

		if *vol.SnapshotId != *snap.SnapshotId {
			log.WithField("snapshot", *snap.SnapshotId).Errorf("Tried to obtain volume for wrong snapshot-id")
			return nil
		}

		if *vol.AvailabilityZone != inst.instanceMetadata.AvailabilityZone {
			log.WithField("snapshot", *snap.SnapshotId).Errorf("Tried to obtain volume for wrong availability-zone")
			return nil
		}

		alreadyInUse := false
		for _, attachment := range vol.Attachments {
			if *attachment.InstanceId != inst.instanceMetadata.InstanceID {
				alreadyInUse = true
			}
		}

		if !alreadyInUse {
			return vol
		}
	}

	return nil
}

func (inst *ec2Instance) createTempVolumeFromSnapshot(snapshotID string) *ec2.Volume {
	input := &ec2.CreateVolumeInput{
		SnapshotId:       aws.String(snapshotID),
		VolumeType:       aws.String("standard"), // Consider using gp2?
		AvailabilityZone: aws.String(inst.instanceMetadata.AvailabilityZone),

		TagSpecifications: []*ec2.TagSpecification{
			{
				ResourceType: aws.String("volume"),
				Tags: []*ec2.Tag{
					{
						Key:   aws.String("Name"),
						Value: aws.String("Temp vol for snapbackup"),
					},
					{
						// Set migrateTag to flag volume as used for snapbackup purposes
						Key:   aws.String(migrateTagSetting),
						Value: aws.String("in-progress"),
					},
				},
			},
		},
	}

	// Create the volume
	result, err := inst.ec2Client.CreateVolume(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Println(aerr.Error())
			}
		} else {
			log.Println(err.Error())
		}
		return nil
	}

	return result
}

func (inst *ec2Instance) detatchVolume(volume *ec2.Volume) bool {
	log.WithField("volume", *volume.VolumeId).Debug("Detatching volume")
	input := &ec2.DetachVolumeInput{
		VolumeId: volume.VolumeId,
	}

	// Detatch the volume
	_, err := inst.ec2Client.DetachVolume(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Println(aerr.Error())
			}
		} else {
			log.Println(err.Error())
		}
		return false
	}

	vol := inst.waitForVolumeState(*volume.VolumeId, []string{"available"}, 60, 10*time.Second)
	if vol == nil {
		log.WithField("volume", *volume.VolumeId).Warn("Failed to detatch volume")
		return false
	}

	log.WithField("volume", *volume.VolumeId).Info("Detached volume")
	return true
}

func (inst *ec2Instance) cleanupTempVolume(volume *ec2.Volume, snap *ec2.Snapshot, deleteTempVolume bool) bool {
	// ======= Get the current volume device suffix =======
	var volAttachement *ec2.VolumeAttachment
	var attachedDriveSuffix string
	for _, attachment := range volume.Attachments {
		if *attachment.InstanceId == inst.instanceMetadata.InstanceID {
			volAttachement = attachment
		}
	}
	if volAttachement != nil {
		attachedDriveSuffix = getDeviceSuffix(*volAttachement.Device)
	}

	if !inst.detatchVolume(volume) {
		log.Error("Failed to detatch volume")
		return false
	}

	// Try to wait for volume to properly unmount
	time.Sleep(45 * time.Second)

	// Unblacklist the suffix as we have now unmounted
	concurrencyMutex.Lock()
	log.Infof("Device suffix blacklist: %v", deviceSuffixBlacklist)
	for i, suffix := range deviceSuffixBlacklist {
		if suffix == "" || attachedDriveSuffix == "" {
			log.Error("Failed length check on suffix or attachedDriveSuffix")
			log.Errorf("|%s|", suffix)
			log.Errorf("|%s|", attachedDriveSuffix)
			continue
		}

		if suffix == attachedDriveSuffix {
			log.WithField("snapshot", *snap.SnapshotId).WithField("volume", *volume.VolumeId).Infof("Removed Suffix `%s` from blacklist", attachedDriveSuffix)
			deviceSuffixBlacklist = append(deviceSuffixBlacklist[:i], deviceSuffixBlacklist[i+1:]...)
			break
		}
	}
	concurrencyMutex.Unlock()

	if !deleteTempVolume {
		return true
	}

	log.WithFields(log.Fields{
		"volume":   *volume.VolumeId,
		"snapshot": *snap.SnapshotId,
	}).Debug("Deleting temp volume...")

	deleteInput := &ec2.DeleteVolumeInput{
		VolumeId: volume.VolumeId,
	}

	// Delete the volume
	_, err := inst.ec2Client.DeleteVolume(deleteInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Println(aerr.Error())
			}
		} else {
			log.Println(err.Error())
		}
		return false
	}

	log.WithFields(log.Fields{
		"volume":   *volume.VolumeId,
		"snapshot": *snap.SnapshotId,
	}).Info("Deleted temp volume")
	return true
}

// waitForVolumeState retries until the specified volume is in any of the specified states
func (inst *ec2Instance) waitForVolumeState(volumeID string, stateNames []string, retries int, retryInterval time.Duration) *ec2.Volume {
	for ; retries >= 0; retries-- {
		input := &ec2.DescribeVolumesInput{
			VolumeIds: []*string{
				aws.String(volumeID),
			},
		}

		result, err := inst.ec2Client.DescribeVolumes(input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					log.Println(aerr.Error())
				}
			} else {
				log.Println(err.Error())
			}
			return nil
		}

		for _, vol := range result.Volumes {
			for _, state := range stateNames {
				if *vol.State == state {
					log.WithFields(log.Fields{
						"volume":   volumeID,
						"ec2state": state,
					}).Debug("Volume is now in requested state")
					return vol
				}
			}
		}

		time.Sleep(retryInterval)
	}

	log.WithFields(log.Fields{
		"volume":   volumeID,
		"ec2state": stateNames,
	}).Error("Timed out while waiting for volume to become state")
	return nil
}

// getAvailAttachmentPoints returns a list of suffixes that can be used as a device name.
// NOTE: this function should only be called when no other go routines are being executed as it is not thread-safe.
func (inst *ec2Instance) getAvailAttachmentPoints() []string {
	input := &ec2.DescribeInstanceAttributeInput{
		Attribute:  aws.String("blockDeviceMapping"),
		InstanceId: aws.String(inst.instanceMetadata.InstanceID),
	}

	result, err := inst.ec2Client.DescribeInstanceAttribute(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Println(aerr.Error())
			}
		} else {
			log.Println(err.Error())
		}
		return nil
	}

	// Setup a map of available letters to use as the device name suffix
	availableCharMap := make(map[string]bool)
	for i := int('f'); i <= int('z'); i++ {
		curCharBlacklisted := false

		// Ignore any device suffixes that are already in use
		for _, blacklistedSuffix := range deviceSuffixBlacklist {
			if string(i) == blacklistedSuffix {
				availableCharMap[string(i)] = false
				curCharBlacklisted = true
				break
			}
		}

		if !curCharBlacklisted {
			availableCharMap[string(i)] = true
		}
	}

	// Get the list of current device names so we don't try to create a new device with the same name
	for _, devMap := range result.BlockDeviceMappings {
		deviceSuffix := getDeviceSuffix(*devMap.DeviceName)
		availableCharMap[deviceSuffix] = false

		// Add to blacklist if not already in blacklist
		curCharBlacklisted := false
		for _, blacklistedSuffix := range deviceSuffixBlacklist {
			if deviceSuffix == blacklistedSuffix {
				curCharBlacklisted = true
				break
			}
		}

		if !curCharBlacklisted {
			deviceSuffixBlacklist = append(deviceSuffixBlacklist, deviceSuffix)
		}
	}

	// Exclude all current device names
	var availableChars []string
	for key, val := range availableCharMap {
		if val == true {
			availableChars = append(availableChars, key)
		}
	}

	return availableChars
}

// pickAvailAttachmentPoint returns a point where the new volume can be mounted
func (inst *ec2Instance) pickAvailAttachmentPoint() string {
	concurrencyMutex.Lock()
	defer concurrencyMutex.Unlock()

	availableChars := inst.getAvailAttachmentPoints()

	if len(availableChars) == 0 {
		log.WithFields(log.Fields{
			"ec2inst": inst.instanceMetadata.InstanceID,
		}).Error("No available mount point")
		return ""
	}

	// Generate a random byte just in case two instances get run at the same time
	rdmByte := make([]byte, 1)
	_, err := rand.Read(rdmByte)
	if err != nil {
		log.Error("Failed to generate random byte for attachment point allocation")
		return ""
	}

	charCode := int(rdmByte[0]) % len(availableChars)
	suffixToUse := availableChars[charCode]
	deviceSuffixBlacklist = append(deviceSuffixBlacklist, suffixToUse)

	log.Infof("Current blacklist: %v", deviceSuffixBlacklist)
	return "/dev/sd" + suffixToUse
}

// waitForVolumeToAttach retries until a volume is attached to the instance
func (inst *ec2Instance) waitForVolumeToAttach(volumeID string, retries int, retryInterval time.Duration) *ec2.Volume {
	input := &ec2.DescribeVolumesInput{
		VolumeIds: []*string{
			aws.String(volumeID),
		},
	}

	// Keep retrying until we have retried the specified number of times
	for ; retries >= 0; retries-- {
		result, err := inst.ec2Client.DescribeVolumes(input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					log.Println(aerr.Error())
				}
			} else {
				log.Println(err.Error())
			}
			return nil
		}

		// Cycle through all volumes and find the correct one that is attached
		for _, volume := range result.Volumes {
			if *volume.VolumeId == volumeID {
				for _, attachment := range volume.Attachments {
					if *attachment.State == "attached" && *attachment.InstanceId == inst.instanceMetadata.InstanceID {
						log.WithFields(log.Fields{
							"volume":  volumeID,
							"ec2inst": inst.instanceMetadata.InstanceID,
						}).Debug("Volume has been attached to the instance")
						return volume
					}
				}
			}
		}

		time.Sleep(retryInterval)
	}

	log.WithFields(log.Fields{
		"volume":  volumeID,
		"ec2inst": inst.instanceMetadata.InstanceID,
	}).Error("Timed out while waiting for volume to attach. May need to manually be force detached.")
	return nil
}

// findOrAttachVolumeToInstance either gets a new attachment point or returns the volume if it is already attached
func (inst *ec2Instance) findOrAttachVolume(volume *ec2.Volume, snap *ec2.Snapshot) *ec2.Volume {
	var volAttachement *ec2.VolumeAttachment
	for _, attachment := range volume.Attachments {
		if *attachment.InstanceId == inst.instanceMetadata.InstanceID {
			volAttachement = attachment
		}
	}

	// Check to see if volume is already attached or is attaching
	if volAttachement != nil {
		if *volAttachement.State == ec2.VolumeAttachmentStateAttached {
			log.WithFields(log.Fields{
				"volume":  *volume.VolumeId,
				"ec2inst": inst.instanceMetadata.InstanceID,
			}).Debug("Volume is already attached to instance")
			return volume
		} else if *volAttachement.State == ec2.VolumeAttachmentStateAttaching {
			log.WithFields(log.Fields{
				"volume":  *volume.VolumeId,
				"ec2inst": inst.instanceMetadata.InstanceID,
			}).Debug("Volume is already in the process of attaching to instance")
			return inst.waitForVolumeToAttach(*volume.VolumeId, 60, 10*1000)
		} else {
			log.WithFields(log.Fields{
				"volume":   *volume.VolumeId,
				"ec2inst":  inst.instanceMetadata.InstanceID,
				"volstate": *volAttachement.State,
			}).Warn("Volume is in unknown state; trying to wait for it to attach...")
			return inst.waitForVolumeToAttach(*volume.VolumeId, 60, 10*1000)
		}
	}

	attachPoint := inst.pickAvailAttachmentPoint()
	if attachPoint == "" {
		log.WithFields(log.Fields{
			"volume":  *volume.VolumeId,
			"ec2inst": inst.instanceMetadata.InstanceID,
		}).Error("Failed to pick mount point when attaching volume to instance")
		return nil
	}

	log.WithFields(log.Fields{
		"volume":      *volume.VolumeId,
		"ec2inst":     inst.instanceMetadata.InstanceID,
		"attachpoint": attachPoint,
	}).Debug("Attaching volume to instance at mountpoint")

	input := &ec2.AttachVolumeInput{
		Device:     aws.String(attachPoint),
		InstanceId: aws.String(inst.instanceMetadata.InstanceID),
		VolumeId:   volume.VolumeId,
	}

	_, err := inst.ec2Client.AttachVolume(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Println(aerr.Error())
			}
		} else {
			log.Println(err.Error())
		}
		return nil
	}

	return inst.waitForVolumeToAttach(*volume.VolumeId, 60, 10*time.Second)
}

// identifyPartitionsForAttachedVolume finds all partitions for the volume we attached
func (inst *ec2Instance) identifyPartitionsForAttachedVolume(volume *ec2.Volume) []map[string]string {
	var volAttachement *ec2.VolumeAttachment
	for _, attachment := range volume.Attachments {
		if *attachment.InstanceId == inst.instanceMetadata.InstanceID {
			volAttachement = attachment
		}
	}

	if volAttachement == nil {
		log.WithFields(log.Fields{
			"volume":  *volume.VolumeId,
			"ec2inst": inst.instanceMetadata.InstanceID,
		}).Error("Volume is not yet attached to instance")
		return nil
	}

	blockDevices := listBlockDevices()
	if blockDevices == nil {
		log.Error("Failed to list the block devices")
		return nil
	}

	// TODO: Sort block devices by "NAME"
	attachedDriveSuffix := strings.Replace(*volAttachement.Device, "/dev/sd", "", 1)
	attachedDriveSuffix = strings.Replace(attachedDriveSuffix, "/dev/xvd", "", 1)
	acceptablePrefixes := []string{"sd" + attachedDriveSuffix, "xvd" + attachedDriveSuffix}

	matchingDevices := make([]map[string]string, 0)
	nvmeDetected := false
	for _, device := range blockDevices {
		for _, prefix := range acceptablePrefixes {
			if strings.Index(device["NAME"], prefix) == 0 {
				matchingDevices = append(matchingDevices, device)
			}
		}

		// Detect if there is any nvme devices
		if strings.Index(device["NAME"], "nvme") == 0 {
			nvmeDetected = true
		}
	}

	log.WithFields(log.Fields{
		"volume": *volume.VolumeId,
	}).Debugf("Found %d matching devices for volume", len(matchingDevices))

	if len(matchingDevices) == 0 && nvmeDetected {
		return identifyNVMePartitionsForAttachedVolume(*volume.VolumeId, blockDevices)
	}

	if len(matchingDevices) > 0 {
		return matchingDevices
	}

	log.WithFields(log.Fields{
		"volume": *volume.VolumeId,
	}).Error("Failed to find any matching devices for volume")
	return nil
}

func (inst *ec2Instance) waitForVolumePartitions(volume *ec2.Volume, retries int, retryInterval time.Duration) []map[string]string {
	var lastSetOfPartitions []map[string]string
	for ; retries >= 0; retries-- {
		partitions := inst.identifyPartitionsForAttachedVolume(volume)
		if partitions == nil {
			log.WithFields(log.Fields{
				"volume": *volume.VolumeId,
			}).Error("Failed to identify partitions belonging to volume")
			return nil
		}

		if len(partitions) != 0 {
			// Wait for list of partitions to settle
			if reflect.DeepEqual(lastSetOfPartitions, partitions) {
				log.WithFields(log.Fields{
					"volume": *volume.VolumeId,
				}).Debug("Partitions list has settled")
				return partitions
			}

			// Only output that partitions are not stable if we've actually read the partitions at least once
			if len(lastSetOfPartitions) > 0 {
				log.WithFields(log.Fields{
					"volume": *volume.VolumeId,
				}).Debug("DeepEqual did not match for volume, continuing to wait for partitions to settle")
			}
			lastSetOfPartitions = partitions
		}

		time.Sleep(retryInterval)
	}

	log.WithFields(log.Fields{
		"volume": *volume.VolumeId,
	}).Error("Timed out while waiting for volume to appear")
	return nil
}

func (inst *ec2Instance) uploadTempVolume(uploadWorker *uploadWorkerInfo) {
	uploadWorker.log.WithFields(log.Fields{
		"volume": *uploadWorker.volume.VolumeId,
	}).Debug("Waiting for volume partitions to be visible to the OS")

	partitions := inst.waitForVolumePartitions(uploadWorker.volume, 70, 5*1000)
	if partitions == nil {
		return
	}

	uploadPartitionsUsingTar(partitions, uploadWorker)

	uploadWorker.finished = true
}

func (inst *ec2Instance) findCreateOrAttachVol(snap *ec2.Snapshot) *ec2.Volume {
	// Find or Create Volume
	vol := inst.findTempVolumeForSnap(snap)
	volumeID := ""

	if vol == nil {
		// Create a new volume
		log.WithFields(log.Fields{
			"snapshot": *snap.SnapshotId,
		}).Debug("Creating EBS volume for snapshot")

		vol = inst.createTempVolumeFromSnapshot(*snap.SnapshotId)
		if vol == nil {
			log.WithFields(log.Fields{
				"snapshot": *snap.SnapshotId,
			}).Error("Failed to create volume from snapshot")
			return nil
		}

		volumeID = *vol.VolumeId
		// Wait for volume to become available
		vol = inst.waitForVolumeState(volumeID, []string{"available"}, 60, 10*time.Second)
		if vol == nil {
			log.WithFields(log.Fields{
				"volume":   volumeID,
				"snapshot": *snap.SnapshotId,
			}).Error("Timed out while waiting for volume to be available")
			return nil
		}
	} else {
		volumeID = *vol.VolumeId

		// Existing volume can be used
		log.WithFields(log.Fields{
			"volume":   volumeID,
			"snapshot": *snap.SnapshotId,
		}).Debug("Temp volume for snapshot already exists, using this")

		// Wait for volume to become available or in-use
		vol = inst.waitForVolumeState(volumeID, []string{"available", "in-use"}, 60, 10*time.Second)
		if vol == nil {
			log.WithFields(log.Fields{
				"volume":   volumeID,
				"snapshot": *snap.SnapshotId,
			}).Error("Timed out while waiting for volume to be available or in-use")
			return nil
		}
	}

	// Try to attach volume to instance
	vol = inst.findOrAttachVolume(vol, snap)
	if vol == nil {
		log.WithFields(log.Fields{
			"volume":   volumeID,
			"snapshot": *snap.SnapshotId,
		}).Error("Errored while waiting for volume to attach to the instance")
		return nil
	}

	return vol
}

func (inst *ec2Instance) migrateSnapshot(snap *ec2.Snapshot, workerNum int, contextLogger *log.Entry) {
	vol := inst.findCreateOrAttachVol(snap)
	if vol == nil {
		return
	}

	uploadWorker := &uploadWorkerInfo{}
	uploadWorker.snapshot = snap
	uploadWorker.volume = vol
	uploadWorker.inst = inst
	uploadWorker.workerNum = workerNum
	uploadWorker.log = contextLogger

	concurrencyMutex.Lock()
	uploadWorkers = append(uploadWorkers, uploadWorker)
	concurrencyMutex.Unlock()

	// Start the upload sequence
	inst.uploadTempVolume(uploadWorker)

	// Kill any still running processes
	for _, proc := range uploadWorker.processes {
		proc.Process.Kill()
	}

	// Cleanup -- originally removed due to EC2 instance crashing
	inst.cleanupTempVolume(vol, snap, false)
}

func (inst *ec2Instance) quickVerifySnapshot(snap *ec2.Snapshot, workerNum int, contextLogger *log.Entry) {
	vol := inst.findCreateOrAttachVol(snap)
	if vol == nil {
		return
	}

	uploadWorker := &uploadWorkerInfo{}
	uploadWorker.snapshot = snap
	uploadWorker.volume = vol
	uploadWorker.inst = inst
	uploadWorker.log = contextLogger
	uploadWorker.workerNum = workerNum

	concurrencyMutex.Lock()
	uploadWorkers = append(uploadWorkers, uploadWorker)
	concurrencyMutex.Unlock()

	uploadWorker.log.WithFields(log.Fields{
		"volume": *vol.VolumeId,
	}).Debug("Waiting for volume partitions to be visible to the OS")

	partitions := inst.waitForVolumePartitions(vol, 70, 5*1000)
	if partitions == nil {
		return
	}

	quickVerifyPartitions(partitions, uploadWorker)

	// Kill any still running processes
	for _, proc := range uploadWorker.processes {
		proc.Process.Kill()
	}

	// Cleanup -- originally removed due to EC2 instance crashing
	inst.cleanupTempVolume(vol, snap, false)
}

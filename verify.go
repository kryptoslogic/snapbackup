package main

import (
	"fmt"
)

// quickVerifyPartitions checks whether the backup files that corresponds to the partitions of this snapshot EXIST on the remote server.
// It does NOT check the integrity of the remote file if it exists.
func quickVerifyPartitions(partitions []map[string]string, uploadWorker *uploadWorkerInfo) {
	correctDevices := filterBlockDevicesToGetFilesystems(partitions)
	if correctDevices == nil || len(correctDevices) == 0 {
		uploadWorker.log.Error("Failed to obtain any partitions to verify for snap")
		return
	}

	uploadWorker.log.Infof("Found %d partition(s) to quick verify for snapshot", len(correctDevices))
	uploadWorker.numExpectedPartitions = len(correctDevices)

	for i, partDevice := range correctDevices {
		if partDevice["FSTYPE"] == "" {
			uploadWorker.log.Warnf("Skipping over partition %d as no FSTYPE given. Counted as successful.", i+1)
			uploadWorker.numSuccessfulOps++
			continue
		}

		partialUploadFileName := fmt.Sprintf("%s-%d.tar", *uploadWorker.snapshot.SnapshotId, i+1)
		fullName, err := findPartialFileNameOnRemote(partialUploadFileName)
		if err != nil {
			uploadWorker.log.WithError(err).Fatal("Error occured when trying to find uploaded file on remote")
			continue
		}

		if fullName == "" {
			uploadWorker.log.Errorf("Partition %d/%d missing on remote for snapshot", i+1, len(correctDevices))

			partData := fmt.Sprintf("%v", partDevice)
			uploadWorker.missingPartitions = append(uploadWorker.missingPartitions, partData)
			continue
		}

		uploadWorker.log.Debugf("Quick verified partition %d/%d", i+1, len(correctDevices))
		uploadWorker.numSuccessfulOps++
	}
}

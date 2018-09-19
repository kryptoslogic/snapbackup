package main

import (
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

// getSupportedNVMeTool returns the nvme tool that is installed on the system.
// Logs and returns empty string if it does not exist.
func getSupportedNVMeTool() string {
	_, err := exec.LookPath("ebsnvme-id")
	if err == nil {
		return "ebsnvme-id"
	}

	_, err = exec.LookPath("nvme")
	if err == nil {
		return "nvme"
	}

	log.Fatal("No supported NVMe tool was installed on the system. Required: ebsnvme-id or nvme\n")
	return ""
}

func supportsPKNAME() bool {
	cmd := exec.Command("lsblk", "--output", "PKNAME")
	_, err := cmd.CombinedOutput()
	if err != nil {
		return false
	}
	return true
}

// udevsettle runs udevadm settle but does not error if it fails to run
func udevsettle() {
	cmd := exec.Command("udevadm", "settle")
	cmd.CombinedOutput()
}

func isDirectoryEmpty(dir string) bool {
	handle, err := os.Open(dir)
	if err != nil {
		return false
	}

	_, err = handle.Readdirnames(1)
	handle.Close()

	if err == io.EOF {
		return true
	}
	return false
}

func killRunningProcsAccessingDevice(deviceName string) bool {
	cmd := exec.Command("fuser", "-k", "-m", deviceName)
	_, err := cmd.Output()
	if err != nil {
		return false
	}
	return true
}

func getDeviceSuffix(deviceName string) string {
	reDevName := regexp.MustCompile(`^\/dev\/(?:sd|xvd)([a-zA-Z])`)
	match := reDevName.FindString(deviceName)
	if match == "" {
		return ""
	}
	return match[len(match)-1:]
}

func mountPartition(deviceName string, fsType string, mountPoint string) bool {
	mountArgs := []string{"--source", deviceName, "--target", mountPoint, "--read-only"}

	if fsType == "xfs" {
		mountArgs = append(mountArgs, "-o", "nouuid")
	}

	cmd := exec.Command("mount", mountArgs...)
	_, err := cmd.Output()
	if err != nil {
		log.WithError(err).Errorf("Failed to mount %s at %s (with cmd: mount %s)\n", deviceName, mountPoint, strings.Join(mountArgs, " "))
		return false
	}

	return true
}

func unmount(mountPoint string) bool {
	cmd := exec.Command("umount", mountPoint)
	log.Infof("Running umount with: %v", cmd.Args)
	_, err := cmd.Output()
	if err != nil {
		log.WithError(err).Error("Unmounting failed")
		return false
	}
	return true
}

func unmountAndDeleteTempMountPoint(mountPoint string) {
	if unmount(mountPoint) == false {
		log.Errorf("Failed to un-mount %s\n", mountPoint)
		return
	}
	log.Debugf("Unmounted %s\n", mountPoint)

	err := os.Remove(mountPoint)
	if err != nil {
		log.Errorf("Failed to cleanup mount point (%s), could be non-empty\n", mountPoint)
		return
	}
	log.Debugf("Removed empty dir:%s\n", mountPoint)
}

func filterBlockDevicesToGetFilesystems(blockDevices []map[string]string) []map[string]string {
	// No partition table => only one block device: type="disk"
	// Else, multiple entries consisting of type="disk" + type="part"
	// ---- Only require type="part" in this case

	numBlockDevices := len(blockDevices)
	if numBlockDevices == 0 {
		log.Error("Failed to find any partitions for the current volume")
		return nil
	}

	if numBlockDevices == 1 {
		log.Debug("Found 1 block device with a filesystem (type='disk')")
		return blockDevices
	}

	// Multiple devices, so discard type="disk" device
	expectedPartCount := numBlockDevices - 1
	correctDevices := make([]map[string]string, 0)

	// Filter for non-disk devices
	for _, device := range blockDevices {
		if device["TYPE"] != "part" {
			continue
		}
		correctDevices = append(correctDevices, device)
	}

	if len(correctDevices) != expectedPartCount {
		log.Error("Current volume contains block devices of an unhandled type")
		return nil
	}

	log.Debugf("Found %d block devices with filesystems (type='part')\n", len(correctDevices))
	return correctDevices
}

func listBlockDevices() []map[string]string {
	flagList := "NAME,FSTYPE,MOUNTPOINT,SIZE,TYPE,LOG-SEC,PHY-SEC"
	supportsPKN := supportsPKNAME()
	if supportsPKN {
		flagList += ",PKNAME"
	}

	udevsettle()

	cmd := exec.Command("lsblk", "--bytes", "--pairs", "--output", flagList)
	stdOut, err := cmd.Output()
	if err != nil {
		log.WithError(err).Error("Failed to list block devices")
		return nil
	}
	strStdOut := string(stdOut)
	outputLines := strings.Split(strStdOut, "\n")
	paramMatch := regexp.MustCompile(`(?:^|\s)([A-Z-]+)="([^"\n]*)"`)

	blockDeviceData := make([]map[string]string, 0)
	for _, line := range outputLines {
		deviceData := make(map[string]string)
		deviceParams := paramMatch.FindAllStringSubmatch(line, -1)

		if deviceParams == nil {
			continue
		}

		for _, matchData := range deviceParams {
			if len(matchData) >= 3 {
				deviceData[matchData[1]] = matchData[2]
			}
		}

		if supportsPKN {
			if len(deviceData["PKNAME"]) > 0 && strings.Index(deviceData["NAME"], deviceData["PKNAME"]) == 0 {
				deviceData["PARTNAME"] = deviceData["PKNAME"][len(deviceData["PKNAME"]):]
			} else if deviceData["TYPE"] == "disk" {
				deviceData["PARTNAME"] = ""
			} else {
				deviceData["PARTNAME"] = deviceData["NAME"]
			}

		}

		deviceData["DEVICEPATH"] = "/dev/" + deviceData["NAME"]
		blockDeviceData = append(blockDeviceData, deviceData)
	}

	if !supportsPKN {
		for _, deviceData := range blockDeviceData {
			deviceData["PKNAME"] = ""
			deviceData["PARTNAME"] = deviceData["NAME"]

			if deviceData["TYPE"] == "part" {
				// Try and find a parent disk
				for _, parentDevice := range blockDeviceData {
					if parentDevice["TYPE"] == "disk" && strings.Index(deviceData["NAME"], parentDevice["NAME"]) == 0 {
						deviceData["PARTNAME"] = deviceData["NAME"][len(parentDevice["NAME"]):]
					}
				}
			} else if deviceData["TYPE"] == "disk" {
				deviceData["PARTNAME"] = ""
			}
		}
	}

	if len(blockDeviceData) > 0 {
		return blockDeviceData
	}

	return nil
}

func pickMountpointForSnapshotPartition(snapshotID string, partitionName string) string {
	mountPointSuffix := "-x"
	if len(partitionName) > 0 {
		mountPointSuffix += "-" + partitionName
	}
	return mountPointSetting + snapshotID + mountPointSuffix
}

func mountTempVolume(volume map[string]string, mountPoint string) bool {
	err := os.MkdirAll(mountPoint, 0600)
	if err != nil {
		log.WithError(err).Errorf("Failed to create directory:%s\n", mountPoint)
	}

	if volume["MOUNTPOINT"] == mountPoint {
		log.Errorf("%s is already mounted at %s\n", volume["DEVICEPATH"], mountPoint)
		return true
	}

	if isDirectoryEmpty(mountPoint) == false {
		log.Errorf("Failed to mount as %s is not empty!\n", mountPoint)
		return false
	}

	log.Debugf("Mounting device:%s at %s\n", volume["DEVICEPATH"], mountPoint)
	return mountPartition(volume["DEVICEPATH"], volume["FSTYPE"], mountPoint)
}

func identifyNVMePartitionsForAttachedVolume(volumeID string, blockDevices []map[string]string) []map[string]string {
	nvmeTool := getSupportedNVMeTool()

	if nvmeTool == "" {
		log.Fatal("No nvme tool installed")
		return nil
	}

	nvmeDevices := make([]map[string]string, 0)
	for _, device := range blockDevices {
		// Only handle nvme devices
		if strings.Index(device["NAME"], "nvme") != 0 {
			continue
		}

		if nvmeTool == "ebsnvme-id" {
			cmd := exec.Command(nvmeTool, "--volume", device["DEVICEPATH"])
			stdOut, err := cmd.Output()
			if err != nil {
				log.WithError(err).Errorf("Failed to run: %s\n", nvmeTool)
			}
			strStdOut := string(stdOut)

			reMatchVol := regexp.MustCompile("vol-[0-9a-zA-Z]+")
			volumeMatch := reMatchVol.FindString(strStdOut)
			if volumeMatch == volumeID {
				nvmeDevices = append(nvmeDevices, device)
			}
		} else if nvmeTool == "nvme" {
			cmd := exec.Command(nvmeTool, "id-ctrl", device["DEVICEPATH"])
			stdOut, err := cmd.Output()
			if err != nil {
				log.WithError(err).Errorf("Failed to run: %s\n", nvmeTool)
			}
			strStdOut := string(stdOut)

			reMatchVol := regexp.MustCompile("vol([0-9a-zA-Z]+)")
			volumeMatchGroups := reMatchVol.FindStringSubmatch(strStdOut)
			if len(volumeMatchGroups) >= 2 {
				expectedVolID := "vol-" + volumeMatchGroups[1]
				if expectedVolID == volumeID {
					nvmeDevices = append(nvmeDevices, device)
				}
			}
		}
	}

	log.Debugf("Found %d nvme partitions for volume:%s\n", len(nvmeDevices), volumeID)
	if len(nvmeDevices) > 0 {
		return nvmeDevices
	}

	return nil
}

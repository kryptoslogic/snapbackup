package main

import (
	"fmt"
)

var (
	// Version of the binary
	Version = "N/A"
	// BuildDate contains the date that this binary was built
	BuildDate = "N/A"
)

// GetVersion will return the version string
func GetVersion() string {
	return fmt.Sprintf("snapbackup version: %s - built on %s", Version, BuildDate)
}

// version.go — Protocol version compatibility checking.
package remote

import (
	"fmt"
	"strconv"
	"strings"
)

// ProtocolVersion is the current version of the remote dispatch protocol.
const ProtocolVersion = "1.0.0"

// MinCompatibleVersion is the minimum agent version the server accepts.
const MinCompatibleVersion = "1.0.0"

// CheckVersionCompatibility verifies an agent's version is compatible.
// Returns nil if compatible, error with upgrade message if not.
func CheckVersionCompatibility(agentVersion string) error {
	if agentVersion == "" {
		return fmt.Errorf("agent version not provided")
	}

	agentParts := parseVersion(agentVersion)
	minParts := parseVersion(MinCompatibleVersion)

	if agentParts == nil || minParts == nil {
		return fmt.Errorf("invalid version format: %s", agentVersion)
	}

	// Compare major.minor.patch
	if agentParts[0] < minParts[0] {
		return fmt.Errorf("agent version %s is incompatible (minimum: %s). Please update: pip install --upgrade ad-bot-agent",
			agentVersion, MinCompatibleVersion)
	}
	if agentParts[0] == minParts[0] && agentParts[1] < minParts[1] {
		return fmt.Errorf("agent version %s is incompatible (minimum: %s). Please update: pip install --upgrade ad-bot-agent",
			agentVersion, MinCompatibleVersion)
	}

	return nil
}

func parseVersion(v string) []int {
	parts := strings.Split(v, ".")
	if len(parts) != 3 {
		return nil
	}
	result := make([]int, 3)
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil
		}
		result[i] = n
	}
	return result
}

// Static address plan for crawler_net (T-180): every compose service except
// the one-off tools run container has an ipv4_address. Docker's dynamic
// allocator does not reserve addresses of stopped containers, so a service
// without an address reproduces the "Address already in use" race
// (incidents of 2026-10-23/24 — tasks/tasks.md, stage 28). The test keeps
// the plan invariants (docs/docker.md §8): a new long-lived service must
// get an address; addresses are unique and lie within the subnet hardcoded
// at consumers (unbound, testbedgen, config.dev.yaml, integration tests);
// .250 is the "black hole" reserved for closed-port scenarios.
package integration

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

type composeFile struct {
	Services map[string]struct {
		// a list (dynamic attach from the x-go-common anchor) or a map
		// with ipv4_address — they differ after YAML anchor resolution.
		Networks any `yaml:"networks"`
	} `yaml:"services"`
	Networks map[string]struct {
		IPAM struct {
			Config []struct {
				Subnet string `yaml:"subnet"`
			} `yaml:"config"`
		} `yaml:"ipam"`
	} `yaml:"networks"`
}

// staticIP returns the service's ipv4_address in crawler_net; an empty
// string means no address is set (dynamic allocation).
func staticIP(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	netCfg, ok := m["crawler_net"].(map[string]any)
	if !ok {
		return ""
	}
	s, _ := netCfg["ipv4_address"].(string)
	return s
}

func TestComposeStaticAddressPlan(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy/docker/compose.yaml"))
	if err != nil {
		t.Fatalf("read deploy/docker/compose.yaml: %v", err)
	}
	var f composeFile
	if err := yaml.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse compose.yaml (anchors/merge — yaml.v3): %v", err)
	}
	if len(f.Services) == 0 {
		t.Fatal("compose.yaml: no services read")
	}

	// The subnet is pinned at consumers — changing it is a separate task
	// (the list of places — docs/docker.md §7, "pool overlaps").
	subnetStr := ""
	for _, cfg := range f.Networks["crawler_net"].IPAM.Config {
		subnetStr = cfg.Subnet
	}
	_, subnet, err := net.ParseCIDR(subnetStr)
	if err != nil {
		t.Fatalf("networks.crawler_net.ipam: a subnet expected, got %q (%v)", subnetStr, err)
	}
	if subnetStr != "172.28.100.0/24" {
		t.Errorf("the crawler_net subnet = %q: a change breaks the consumers (unbound, testbedgen, config.dev.yaml, tests)", subnetStr)
	}

	blackhole := net.ParseIP("172.28.100.250")
	seen := map[string]string{}
	for name, svc := range f.Services {
		ipStr := staticIP(svc.Networks)
		if name == "tools" {
			// Plan exception (T-180): one-off runs go in parallel
			// from worktrees into the same network — a shared address
			// would conflict between them.
			if ipStr != "" {
				t.Errorf("tools: the static address %s forbidden (parallel run containers, T-180)", ipStr)
			}
			continue
		}
		if ipStr == "" {
			t.Errorf("the service %s without ipv4_address: a long-lived service must have a static address (T-180, docs/docker.md §8)", name)
			continue
		}
		if other, dup := seen[ipStr]; dup {
			t.Errorf("the address %s taken twice: %s and %s", ipStr, other, name)
		}
		seen[ipStr] = name
		ip := net.ParseIP(ipStr)
		if ip == nil {
			t.Errorf("the service %s: an invalid address %q", name, ipStr)
			continue
		}
		if !subnet.Contains(ip) {
			t.Errorf("the service %s: the address %s outside the subnet %s", name, ipStr, subnetStr)
		}
		if ip.Equal(blackhole) {
			t.Errorf("the service %s took 172.28.100.250 — the \"black hole\" of the closed-port scenarios (scenarios.yaml)", name)
		}
	}
}

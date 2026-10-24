// План статических адресов crawler_net (T-180): всем сервисам compose,
// кроме одноразового run-контейнера tools, задан ipv4_address. Динамический
// аллокатор Docker не резервирует адреса остановленных контейнеров, и сервис
// без адреса воспроизводит гонку «Address already in use» (инциденты
// 2026-10-23/24 — tasks/tasks.md, этап 28). Тест держит инварианты плана
// (docs/docker.md §8): новый долгоживущий сервис обязан получить адрес;
// адреса уникальны и лежат в подсети, захардкоженной у потребителей
// (unbound, testbedgen, config.dev.yaml, интеграционные тесты); .250 —
// «чёрная дыра» сценариев закрытых портов.
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
		// list (динамический attach из якоря x-go-common) или map
		// с ipv4_address — различаются после резолва YAML-якорей.
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

// staticIP возвращает ipv4_address сервиса в crawler_net; пустая строка —
// адрес не задан (динамический).
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
		t.Fatalf("прочитать deploy/docker/compose.yaml: %v", err)
	}
	var f composeFile
	if err := yaml.Unmarshal(raw, &f); err != nil {
		t.Fatalf("разобрать compose.yaml (якоря/merge — yaml.v3): %v", err)
	}
	if len(f.Services) == 0 {
		t.Fatal("compose.yaml: не прочитаны сервисы")
	}

	// Подсеть зафиксирована у потребителей — смена это отдельная задача
	// (список мест — docs/docker.md §7, «pool overlaps»).
	subnetStr := ""
	for _, cfg := range f.Networks["crawler_net"].IPAM.Config {
		subnetStr = cfg.Subnet
	}
	_, subnet, err := net.ParseCIDR(subnetStr)
	if err != nil {
		t.Fatalf("networks.crawler_net.ipam: ожидается подсеть, получено %q (%v)", subnetStr, err)
	}
	if subnetStr != "172.28.100.0/24" {
		t.Errorf("подсеть crawler_net = %q: смена ломает потребителей (unbound, testbedgen, config.dev.yaml, тесты)", subnetStr)
	}

	blackhole := net.ParseIP("172.28.100.250")
	seen := map[string]string{}
	for name, svc := range f.Services {
		ipStr := staticIP(svc.Networks)
		if name == "tools" {
			// Исключение плана (T-180): одноразовые run-прогоны идут
			// параллельно из worktree'ов в ту же сеть — общий адрес
			// конфликтовал бы между ними.
			if ipStr != "" {
				t.Errorf("tools: статический адрес %s запрещён (параллельные run-контейнеры, T-180)", ipStr)
			}
			continue
		}
		if ipStr == "" {
			t.Errorf("сервис %s без ipv4_address: долгоживущий сервис обязан иметь статический адрес (T-180, docs/docker.md §8)", name)
			continue
		}
		if other, dup := seen[ipStr]; dup {
			t.Errorf("адрес %s занят дважды: %s и %s", ipStr, other, name)
		}
		seen[ipStr] = name
		ip := net.ParseIP(ipStr)
		if ip == nil {
			t.Errorf("сервис %s: некорректный адрес %q", name, ipStr)
			continue
		}
		if !subnet.Contains(ip) {
			t.Errorf("сервис %s: адрес %s вне подсети %s", name, ipStr, subnetStr)
		}
		if ip.Equal(blackhole) {
			t.Errorf("сервис %s занял 172.28.100.250 — «чёрная дыра» сценариев закрытых портов (scenarios.yaml)", name)
		}
	}
}

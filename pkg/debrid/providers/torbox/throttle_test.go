package torbox

import (
	"github.com/sirrobot01/decypharr/internal/config"
	"testing"
	"time"
)

func TestThrottleConfiguration(t *testing.T) {
	backoff, cooldown, threshold, err := throttleConfig(config.Debrid{})
	if err != nil || backoff != 5*time.Minute || cooldown != time.Minute || threshold != 3 {
		t.Fatal("unsafe defaults")
	}
	for _, dc := range []config.Debrid{
		{TorboxBackoffMax: "-1s"}, {TorboxBackoffMax: "16m"}, {TorboxBackoffMax: "invalid"},
		{TorboxBreakerCooldown: "0s"}, {TorboxBreakerCooldown: "1h"}, {TorboxBreakerThreshold: -1}, {TorboxBreakerThreshold: 101},
	} {
		if _, _, _, err := throttleConfig(dc); err == nil {
			t.Fatalf("accepted invalid config: %+v", dc)
		}
	}
}

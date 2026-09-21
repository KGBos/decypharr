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
		{TorboxBackoffMax: "-1s"}, {TorboxBackoffMax: "invalid"},
		{TorboxBreakerCooldown: "0s"}, {TorboxBreakerThreshold: -1}, {TorboxBreakerThreshold: 101},
		{TorboxBackoffMax: "49h"}, {TorboxBreakerCooldown: "49h"},
	} {
		if _, _, _, err := throttleConfig(dc); err == nil {
			t.Fatalf("accepted invalid config: %+v", dc)
		}
	}
	// A full-day TorBox ban must be configurable; the previous 15m ceiling
	// rejected both of these.
	for _, dc := range []config.Debrid{
		{TorboxBackoffMax: "24h"}, {TorboxBackoffMax: "48h"}, {TorboxBreakerCooldown: "24h"}, {TorboxBreakerCooldown: "48h"},
	} {
		if _, _, _, err := throttleConfig(dc); err != nil {
			t.Fatalf("rejected long bound %+v: %v", dc, err)
		}
	}
	backoff, cooldown, _, err = throttleConfig(config.Debrid{TorboxBackoffMax: "24h", TorboxBreakerCooldown: "24h"})
	if err != nil || backoff != 24*time.Hour || cooldown != 24*time.Hour {
		t.Fatalf("long bounds round-trip: %v %s %s", err, backoff, cooldown)
	}
}

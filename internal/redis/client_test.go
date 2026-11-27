package redis

import (
	"runtime"
	"testing"
)

func TestClientConfigDefaultsKeepIdleCapacity(t *testing.T) {
	got := ClientConfig{}.withDefaults()

	if want := 10 * runtime.GOMAXPROCS(0); got.MaxActiveConns != want {
		t.Fatalf("maxActiveConns = %d, want %d", got.MaxActiveConns, want)
	}

	// A smaller idle pool closes connections under concurrency and re-dials
	// them, which churns through the ephemeral ports (docs/16-benchmark-p99.md):
	// by default the whole cap stays usable as idle capacity.
	if got.PoolSize != got.MaxActiveConns {
		t.Fatalf("poolSize = %d, want maxActiveConns (%d)", got.PoolSize, got.MaxActiveConns)
	}

	// An explicit idle pool is respected.
	got = ClientConfig{PoolSize: 3}.withDefaults()
	if got.PoolSize != 3 {
		t.Fatalf("poolSize = %d, want 3", got.PoolSize)
	}

	// The idle pool can never exceed the total number of connections.
	got = ClientConfig{PoolSize: 8, MaxActiveConns: 4}.withDefaults()
	if got.PoolSize != 8 || got.MaxActiveConns != 8 {
		t.Fatalf("poolSize/maxActiveConns = %d/%d, want 8/8", got.PoolSize, got.MaxActiveConns)
	}

	// Negative values mean "unset", as the configuration layer already rejects them.
	got = ClientConfig{PoolSize: -1, MaxActiveConns: -1}.withDefaults()
	if got.PoolSize != got.MaxActiveConns || got.MaxActiveConns <= 0 {
		t.Fatalf("poolSize/maxActiveConns = %d/%d, want equal positive values", got.PoolSize, got.MaxActiveConns)
	}
}

package coordinator

import (
	"net"
	"testing"

	"golang.org/x/xerrors"

	"github.com/gomodule/redigo/redis"
)

func buildBuiltInCoordinator(resolver func(string) ([]net.IP, error)) *builtInCoordinator {
	conn, err := redis.DialURL("redis://localhost:11111/1")
	if err != nil {
		panic(err)
	}

	_, err = conn.Do("FLUSHALL")
	if err != nil {
		panic(err)
	}

	return &builtInCoordinator{
		conn:         conn,
		nameResolver: resolver,
	}
}

func mockSuccessfulNameResolver(_ string) ([]net.IP, error) {
	return []net.IP{
		net.IPv4(192, 168, 0, 1),
		net.IPv4(192, 168, 0, 2),
	}, nil
}

func mockFailedNameResolver(_ string) ([]net.IP, error) {
	return nil, xerrors.New("failed to resolve domain name")
}

func TestBuiltInCoordinator_AllocNextGWN(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*builtInCoordinator)
		want  uint16
	}{
		{
			name:  "まだWorkerが登録されていない場合、1を返す",
			setup: func(_ *builtInCoordinator) {},
			want:  1,
		},
		{
			name: "Workerがいくつか登録されている場合、次の番号を返す",
			setup: func(coordinator *builtInCoordinator) {
				_, err := coordinator.conn.Do("SET", "gokurou_workers", 3)
				if err != nil {
					panic(err)
				}
			},
			want: 4,
		},
	}

	for _, tt := range tests {
		coordinator := buildBuiltInCoordinator(mockSuccessfulNameResolver)
		tt.setup(coordinator)

		got, err := coordinator.AllocNextGWN()
		if err != nil {
			t.Errorf("AllocNextGWN() = %v", err)
		}

		if got != tt.want {
			t.Errorf("AllocNextGWN() = %d, want = %d", got, tt.want)
		}
	}
}

func TestBuiltInCoordinator_LockByIPAddrOf(t *testing.T) {
	t.Run("ロックを獲得できる場合、trueを返す", func(t *testing.T) {
		coordinator := buildBuiltInCoordinator(mockSuccessfulNameResolver)

		locked, err := coordinator.LockByIPAddrOf("example.com")
		if err != nil {
			t.Errorf("LockByIPAddrOf() = %v", err)
		}

		if !locked {
			t.Errorf("LockByIPAddrOf() = %v, want = true", locked)
		}

		ttl, _ := redis.Uint64(coordinator.conn.Do("TTL", "l-192.168.0.1"))
		if ttl < 55 {
			t.Errorf("LockByIPAddrOf() is NOT set TTL for 192.168.0.1")
		}

		ttl, _ = redis.Uint64(coordinator.conn.Do("TTL", "l-192.168.0.2"))
		if ttl < 55 {
			t.Errorf("LockByIPAddrOf() is NOT set TTL for 192.168.0.2")
		}
	})

	t.Run("ロックを獲得できない場合、falseを返す", func(t *testing.T) {
		coordinator := buildBuiltInCoordinator(mockSuccessfulNameResolver)
		_, _ = coordinator.conn.Do("SETEX", "l-192.168.0.1", 10, 1)

		locked, err := coordinator.LockByIPAddrOf("example.com")
		if err != nil {
			t.Errorf("LockByIPAddrOf() = %v", err)
		}

		if locked {
			t.Errorf("LockByIPAddrOf() = %v, want = false", locked)
		}

		ttl, _ := redis.Uint64(coordinator.conn.Do("TTL", "l-192.168.0.1"))
		if ttl > 55 {
			t.Errorf("LockByIPAddrOf() overrides TTL for 192.168.0.1")
		}
	})

	t.Run("名前解決に失敗する場合", func(t *testing.T) {
		coordinator := buildBuiltInCoordinator(mockFailedNameResolver)
		got, err := coordinator.LockByIPAddrOf("example.com")

		if err != nil {
			t.Errorf("LockByIPAddrOf() = %v", err)
		}

		if got {
			t.Errorf("LockByIPAddrOf() = %v, want = false", got)
		}
	})
}

func TestBuiltInCoordinator_Finish(t *testing.T) {
	err := buildBuiltInCoordinator(mockSuccessfulNameResolver).Finish()
	if err != nil {
		t.Errorf("Finish() = %v", err)
	}
}

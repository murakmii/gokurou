package coordinator

import (
	"net"
	"testing"

	"github.com/gomodule/redigo/redis"
)

func NewBuiltInCoordinator() *builtInCoordinator {
	conn, err := redis.DialURL("redis://localhost:11111/2")
	if err != nil {
		panic(err)
	}

	_, err = conn.Do("FLUSHALL")
	if err != nil {
		panic(err)
	}

	return &builtInCoordinator{
		conn:         conn,
		nameResolver: mockNameResolver,
	}
}

func mockNameResolver(_ string) ([]net.IP, error) {
	return []net.IP{
		net.IPv4(192, 168, 0, 1),
		net.IPv4(192, 168, 0, 2),
	}, nil
}

func TestBuiltInCoordinator_AllocNextGWN(t *testing.T) {
	t.Run("まだWorkerが登録されていない場合、1を返す", func(t *testing.T) {
		coordinator := NewBuiltInCoordinator()

		gwn, err := coordinator.AllocNextGWN()
		if err != nil {
			t.Errorf("AllocNextGWN() = %v", err)
		}

		if gwn != 1 {
			t.Errorf("AllocNextGWN() = %d, want = 1", gwn)
		}
	})

	t.Run("Workerがいくつか登録されている場合、次の番号を返す", func(t *testing.T) {
		coordinator := NewBuiltInCoordinator()
		_, err := coordinator.conn.Do("SET", "gokurou_workers", 3)
		if err != nil {
			panic(err)
		}

		gwn, err := coordinator.AllocNextGWN()
		if err != nil {
			t.Errorf("AllocNextGWN() = %v", err)
		}

		if gwn != 4 {
			t.Errorf("AllocNextGWN() = %d, want = 4", gwn)
		}
	})
}

func TestBuiltInCoordinator_LockByIPAddrOf(t *testing.T) {
	t.Run("ロックを獲得できる場合、trueを返す", func(t *testing.T) {
		coordinator := NewBuiltInCoordinator()

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
		coordinator := NewBuiltInCoordinator()
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
}

func TestBuiltInCoordinator_Finish(t *testing.T) {
	err := NewBuiltInCoordinator().Finish()
	if err != nil {
		t.Errorf("Finish() = %v", err)
	}
}

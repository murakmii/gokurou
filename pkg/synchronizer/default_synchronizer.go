package synchronizer

import (
	"github.com/gomodule/redigo/redis"
	"github.com/murakmii/gokurou/pkg"
)

type defaultSynchronizer struct {
	conn redis.Conn
}

func NewDefaultSynchronizer() pkg.Synchronizer {
	return &defaultSynchronizer{conn: nil}
}

func (s *defaultSynchronizer) GetNextGlobalWorkerNumber() (uint16, error) {
	return 0, nil
}

func (s *defaultSynchronizer) LockByIPAddrOf(host string) (bool, error) {
	return true, nil
}

package synchronizer

import (
	"net"

	"github.com/murakmii/gokurou/pkg/gokurou"

	"github.com/gomodule/redigo/redis"
)

const (
	redisURLConfName = "REDIS_URL"
)

type defaultSynchronizer struct {
	conn         redis.Conn
	nameResolver func(host string) ([]net.IP, error)
}

func NewDefaultSynchronizer(conf *gokurou.Configuration) (gokurou.Synchronizer, error) {
	conn, err := redis.DialURL(conf.MustFetchAdvancedAsString(redisURLConfName))
	if err != nil {
		return nil, err
	}

	return &defaultSynchronizer{
		conn:         conn,
		nameResolver: net.LookupIP,
	}, nil
}

func (s *defaultSynchronizer) AllocNextGWN() (uint16, error) {
	gwn, err := redis.Uint64(s.conn.Do("INCR", "gokurou_workers"))
	if err != nil {
		_ = s.Finish()
		return 0, err
	}

	return uint16(gwn), nil
}

func (s *defaultSynchronizer) LockByIPAddrOf(host string) (bool, error) {
	ips, err := s.nameResolver(host)
	if err != nil {
		return false, err
	}

	lockKeys := make([]string, len(ips))
	for i, ip := range ips {
		lockKeys[i] = "l-" + ip.String()
	}

	mSetNXArgs := make([]interface{}, len(ips)*2)
	mSetNXArgs[0] = "MSETNX"
	for i, key := range lockKeys {
		mSetNXArgs[i*2] = key
		mSetNXArgs[i*2+1] = "1"
	}

	locked, err := redis.Uint64(s.conn.Do("MSETNX", mSetNXArgs...))
	if err != nil {
		return false, err
	}

	if locked == 0 {
		return false, nil
	}

	// EXPIREに失敗するとMSETNXで設定したキーにTTLが付かない可能性があるがしょうがない
	// TODO: 全て1つのLuaスクリプト中で実行するように
	if _, err := s.conn.Do("MULTI"); err != nil {
		return false, err
	}

	for _, key := range lockKeys {
		if err = s.conn.Send("EXPIRE", key, 60); err != nil {
			return false, err
		}
	}

	if _, err := s.conn.Do("EXEC"); err != nil {
		return false, err
	}

	return true, nil
}

func (s *defaultSynchronizer) Finish() error {
	return s.conn.Close()
}

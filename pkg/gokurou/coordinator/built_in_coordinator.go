package coordinator

import (
	"net"

	"github.com/murakmii/gokurou/pkg/gokurou"

	"github.com/gomodule/redigo/redis"
)

const (
	redisURLConfKey = "built_in.redis_url"
)

// TODO: Redis関連のエラーは何回かは許容&リトライしたい
type builtInCoordinator struct {
	conn         redis.Conn
	nameResolver func(host string) ([]net.IP, error)
}

func BuiltInCoordinatorProvider(conf *gokurou.Configuration) (gokurou.Coordinator, error) {
	conn, err := redis.DialURL(conf.MustOptionAsString(redisURLConfKey))
	if err != nil {
		return nil, err
	}

	return &builtInCoordinator{
		conn:         conn,
		nameResolver: net.LookupIP,
	}, nil
}

func (c *builtInCoordinator) AllocNextGWN() (uint16, error) {
	gwn, err := redis.Uint64(c.conn.Do("INCR", "gokurou_workers"))
	if err != nil {
		_ = c.Finish()
		return 0, err
	}

	return uint16(gwn), nil
}

func (c *builtInCoordinator) LockByIPAddrOf(host string) (bool, error) {
	ips, err := c.nameResolver(host)
	if err != nil {
		return false, nil // 名前解決に失敗した場合でも、エラーにはせず単にロック不可とするだけ
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

	locked, err := redis.Uint64(c.conn.Do("MSETNX", mSetNXArgs...))
	if err != nil {
		return false, err
	}

	if locked == 0 {
		return false, nil
	}

	// EXPIREに失敗するとMSETNXで設定したキーにTTLが付かない可能性があるがしょうがない
	// TODO: 全て1つのLuaスクリプト中で実行するように
	if _, err := c.conn.Do("MULTI"); err != nil {
		return false, err
	}

	for _, key := range lockKeys {
		if err = c.conn.Send("EXPIRE", key, 60); err != nil {
			return false, err
		}
	}

	if _, err := c.conn.Do("EXEC"); err != nil {
		return false, err
	}

	return true, nil
}

func (c *builtInCoordinator) Finish() error {
	return c.conn.Close()
}

func (c *builtInCoordinator) Reset() error {
	_, err := c.conn.Do("FLUSHALL")
	if err != nil {
		return err
	}

	return c.Finish()
}

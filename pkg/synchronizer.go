package pkg

type Synchronizer interface {
	ResourceOwner

	AllocNextGWN() (uint16, error)
	LockByIPAddrOf(host string) (bool, error)
}

package gokurou

type Coordinator interface {
	ResourceOwner

	AllocNextGWN() (uint16, error)
	LockByIPAddrOf(host string) (bool, error)
}

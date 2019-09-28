package gokurou

type Coordinator interface {
	Finisher

	AllocNextGWN() (uint16, error)
	LockByIPAddrOf(host string) (bool, error)
}

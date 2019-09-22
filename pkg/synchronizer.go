package pkg

type Synchronizer interface {
	ResourceOwner

	GetNextGlobalWorkerNumber() (uint16, error)
	LockByIPAddrOf(host string) (bool, error)
}

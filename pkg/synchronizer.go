package pkg

type Synchronizer interface {
	GetNextGlobalWorkerNumber() (uint16, error)
	LockByIPAddrOf(host string) (bool, error)
}

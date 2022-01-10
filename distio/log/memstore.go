package log

import (
	"encoding/gob"
	"time"
)

func init() {
	gob.Register(Store{})
	gob.Register(Entry{})
	gob.Register(time.Time{})
}

type (
	// Entry is the entry of the context storage Store - .Values()
	Entry struct {
		Key       string
		ValueRaw  interface{}
		immutable bool // if true then it can't change by its caller.
	}

	Store []Entry
)

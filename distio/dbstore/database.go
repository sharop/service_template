package badger

import (
	"errors"
	"github.com/dgraph-io/badger/v3"
	"go.uber.org/zap"
	inform "log"
	"os"
	"runtime"
	"sync/atomic"
)

// DefaultFileMode used as the default database's "fileMode"
// for creating the sessions directory path, opening and write the session file.
var (
	DefaultFileMode = 0755
)

// Database the badger(key-value file-based) session storage.
type Database struct {
	// Service is the underline badger database connection,
	// it's initialized at `New` or `NewFromDB`.
	// Can be used to get stats.
	Service *badger.DB

	closed uint32 // if 1 is closed.
}

// New creates and returns a new badger(key-value file-based) storage
// instance based on the "directoryPath".
// DirectoryPath should is the directory which the badger database will store the sessions,
// i.e ./sessions
//
// It will remove any old session files.
func New(directoryPath string) (*Database, error) {

	if directoryPath == "" {
		return nil, errors.New("directoryPath is missing")
	}

	lindex := directoryPath[len(directoryPath)-1]
	if lindex != os.PathSeparator && lindex != '/' {
		directoryPath += string(os.PathSeparator)
	}
	// create directories if necessary
	if err := os.MkdirAll(directoryPath, os.FileMode(DefaultFileMode)); err != nil {
		return nil, err
	}

	opts := badger.DefaultOptions(directoryPath)

	service, err := badger.Open(opts)

	if err != nil {
		inform.Printf("unable to initialize the badger-based session database: %v\n", err)
		return nil, err
	}

	return NewFromDB(service), nil
}

// NewFromDB same as `New` but accepts an already-created custom badger connection instead.
func NewFromDB(service *badger.DB) *Database {
	db := &Database{Service: service}

	runtime.SetFinalizer(db, closeDB)
	return db
}

var delim = byte('_')
var sid = "sio"

func makePrefix(sid string) []byte {
	return append([]byte(sid), delim)
}

func makeKey(key string) []byte {
	return append(makePrefix(sid), []byte(key)...)
}
func (db *Database) Set(key string, value string) error {
	valueBytes := []byte(value)
	return db.Service.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(badger.NewEntry(makeKey(key), valueBytes))
	},
	)
}

func (db *Database) Get(key string) (value interface{}) {
	err := db.Service.View(func(txn *badger.Txn) error {
		item, err := txn.Get(makeKey(key))
		if err != nil {
			return err
		}

		return item.Value(func(valueBytes []byte) error {
			value = string(append([]byte{}, valueBytes...)[:])
			return nil
		})
	})

	if err != nil && err != badger.ErrKeyNotFound {
		return nil
	}

	return
}

func (db *Database) Delete(key string) (deleted bool) {
	txn := db.Service.NewTransaction(true)
	err := txn.Delete(makeKey(key))
	if err != nil {
		return
	}
	err = txn.Commit()
	return err == nil
}

// Close shutdowns the badger connection.
func (db *Database) Close() error {
	zap.L().Info("Shuttingdown db..")

	return closeDB(db)
}

func closeDB(db *Database) error {
	if atomic.LoadUint32(&db.closed) > 0 {
		return nil
	}
	err := db.Service.Close()
	if err == nil {
		atomic.StoreUint32(&db.closed, 1)
	}
	return err
}

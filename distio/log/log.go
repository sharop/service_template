package log

//TODO: Implementing log with badger

import (
	"github.com/google/uuid"
	"github.com/sharop/service_template_dex/distio/dbstore"
	api "github.com/sharop/service_template_dex/pb/v1/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"log"
	"os"
	"sync"
)

type Log struct {
	mu     sync.RWMutex
	Dir    string
	Config Config
	db     *badger.Database
}

func NewLog(dir string, c Config) (*Log, error) {

	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

func (l *Log) setup() error {
	db, err := badger.New(l.Dir)
	if err != nil {
		log.Fatal(err)
	}

	l.db = db
	return nil
}

func (l *Log) Set(record *api.Record) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	uid, _ := uuid.NewUUID()
	err := l.db.Set(uid.String(), string(record.Value[:]))
	if err != nil {
		return "", err
	}

	return uid.String(), nil
}

func (l *Log) Get(key string) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	val := l.db.Get(key)
	if val == nil {
		return nil, nil
	}

	record := &api.Record{}
	//Type assertion http://golang.org/ref/spec#Type_assertions
	if str, ok := val.(string); ok {
		err := proto.Unmarshal([]byte(str), record)
		return record, err
	} else {
		return nil, nil
	}

}

type cError struct{}

func (e *cError) Error() string {
	return "Log Error"
}

func (l *Log) Delete(key string) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if err := l.db.Delete(key); !err {
		return &cError{}
	}
	return os.RemoveAll(l.Dir)
}
func (l *Log) Close() error {
	zap.L().Info("Shuttingdown log..")

	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.db.Close(); err != nil {
		return err
	}

	return nil
}

func (l *Log) Append(record *api.Record) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.Set(record)
	if err != nil {
		return "", err
	}

	return off, err
}

type StoreDB interface {
	// Set key and value
	Set(key string, value string) error
	// Get value by key
	Get(key string) interface{}
	// Delete removes a session key value based on its key.
	Delete(key string) (deleted bool)
}

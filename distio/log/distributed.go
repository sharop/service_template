package log

import (
	"bytes"
	"fmt"
	"github.com/hashicorp/raft"
	badger "github.com/sharop/service_template_dex/distio/dbstore"
	api "github.com/sharop/service_template_dex/pb/v1/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type DistributedLog struct {
	config Config
	log    *Log
	ldb    *badger.BadgerStore
	sdb    *badger.BadgerStore
	raft   *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (
	*DistributedLog,
	error,
) {
	l := &DistributedLog{
		config: config,
	}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

func (l *DistributedLog) setupRaft(dataDir string) error {

	//Finite-state machine that applies the commands given to raft.
	fsm := &fsm{log: l.log}

	var err error
	baseDir := filepath.Join(dataDir, "raft")
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return err
	}
	//Log store where raft store commands
	l.ldb, err = badger.NewBadgerStore(filepath.Join(baseDir, "log"))
	//ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}
	//Store where raft store cluster's configuration. (Server in the cluster, their addresses and so on)
	l.sdb, err = badger.NewBadgerStore(filepath.Join(baseDir, "stable"))
	//sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err)
	}
	retain := 3
	//Snapshot where raft stores compact snapshots of its data.
	fss, err := raft.NewFileSnapshotStore(filepath.Join(baseDir, "snapshot"), retain, os.Stderr)
	if err != nil {
		return fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err)
	}

	maxPool := 5
	timeout := 10 * time.Second
	// Raft uses to connect with the server's peers
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	l.raft, err = raft.NewRaft(
		config,
		fsm,
		l.ldb,
		l.sdb,
		fss,
		transport,
	)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(
		l.ldb,
		l.sdb,
		fss,
	)
	if err != nil {
		return err
	}
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: raft.ServerAddress(l.config.Raft.BindAddr), //transport.LocalAddr(),
			}},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}
	return err
}

// Commitlog implementation
func (l *DistributedLog) Append(record *api.Record) (string, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)
	if err != nil {
		return "", err
	}
	return res.(*api.ProduceResponse).Key, nil
}

func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (
	interface{},
	error,
) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	future := l.raft.Apply(buf.Bytes(), timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

// Commitlog implementation
func (l *DistributedLog) Read(key string) (*api.Record, error) {
	return l.log.Get(key)
}

//Implementation of JOIN handler for membership
func (l *DistributedLog) Join(id, addr string, voter bool) error {
	log.Printf("Requested to join, id: %v, add: %v ", id, addr)
	configFuture := l.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// server has already joined
				zap.L().Debug("Server has already joined.")
				return nil
			}
			// remove the existing server
			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}

	var addFuture raft.IndexFuture
	if voter {
		addFuture = l.raft.AddVoter(serverID, serverAddr, 0, 0)
	} else {
		addFuture = l.raft.AddNonvoter(serverID, serverAddr, 0, 0)
	}
	if err := addFuture.Error(); err != nil {
		return err
	}
	return nil
	return nil
}

//Implementation of LEAVE handler for membership
func (l *DistributedLog) Leave(id string) error {
	log.Printf("Say bye bye flaca id: %v ", id)
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()

}

func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("Se te acabo el tiempo chavo")
		case <-ticker.C:
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

func (l *DistributedLog) Close() error {
	zap.L().Info("Shuttingdown raft..")
	var err error
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	err = l.sdb.Close()
	if err != nil {
		return err
	}
	err = l.ldb.Close()
	if err != nil {
		return err
	}

	return l.log.Close()
}

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log  *Log
	word []string
}
type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

func (l *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return l.applyAppend(buf[1:])
	}
	return nil
}

func (l *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	offset, err := l.log.Set(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Key: offset}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{reader: "Algo"}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader string
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write([]byte(s.reader))
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("sink.Write(): %v", err)
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

func (f *fsm) Restore(r io.ReadCloser) error {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	words := strings.Split(string(b), "\n")
	copy(f.word, words)
	return nil

}

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln net.Listener
}

func NewStreamLayer(
	ln net.Listener,
) *StreamLayer {
	return &StreamLayer{
		ln: ln,
	}
}

const RaftRPC = 1

func (s *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	// identify to mux this is a raft rpc
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}

	return conn, err
}

func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if bytes.Compare([]byte{byte(RaftRPC)}, b) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	}

	return conn, nil
}

func (s *StreamLayer) Close() error {
	zap.L().Info("Closing stream layer..")

	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}

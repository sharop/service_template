package log

import "github.com/hashicorp/raft"

type Config struct {
	Raft struct {
		raft.Config
		StreamLayer *StreamLayer
		BindAddr    string
		Bootstrap   bool
		Voter       bool
	}
}

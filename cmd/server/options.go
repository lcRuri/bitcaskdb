package server

import bitcask_go "bitcask-go"

type RunOptions struct {
	StandaloneOpt bitcask_go.Options
	Addr          string
}

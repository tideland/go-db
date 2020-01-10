// Tideland Go Database Clients - Redis Client
//
// Copyright (C) 2017-2020 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis // import "tideland.dev/go/db/redis"

//--------------------
// IMPORTS
//--------------------

import (
	"time"

	"tideland.dev/go/trace/failure"
)

//--------------------
// OPTIONS
//--------------------

const (
	defaultAddress  = "127.0.0.1:6379"
	defaultSocket   = "/tmp/redis.sock"
	defaultNetwork  = "unix"
	defaultTimeout  = 30 * time.Second
	defaultIndex    = 0
	defaultPassword = ""
	defaultPoolSize = 10
	defaultLogging  = false
)

// Options is returned when calling Options() on Database to
// provide information about the database configuration.
type Options struct {
	Address  string
	Network  string
	Timeout  time.Duration
	Index    int
	Password string
	PoolSize int
	Logging  bool
}

// Option defines a function setting an option.
type Option func(d *Database) error

// TCPConnection sets the connection to use TCP/IP. The default address
// is "127.0.0.1:6379". The default timeout to connect are 30 seconds.
func TCPConnection(address string, timeout time.Duration) Option {
	return func(d *Database) error {
		if address == "" {
			address = defaultAddress
		}
		d.address = address
		d.network = "tcp"
		if timeout < 0 {
			return failure.New("invalid configuration value in field 'timeout': %v", timeout)
		} else if timeout == 0 {
			timeout = defaultTimeout
		}
		d.timeout = timeout
		return nil
	}
}

// UnixConnection sets the connection to use a Unix socket. The default
// is "/tmp/redis.sock". The default timeout to connect are 30 seconds.
func UnixConnection(socket string, timeout time.Duration) Option {
	return func(d *Database) error {
		if socket == "" {
			socket = defaultSocket
		}
		d.address = socket
		d.network = "unix"
		if timeout < 0 {
			return failure.New("invalid configuration value in field 'timeout': %v", timeout)
		} else if timeout == 0 {
			timeout = defaultTimeout
		}
		d.timeout = timeout
		return nil
	}
}

// Index selects the database and sets the authentication. The
// default database is the 0, the default password is empty.
func Index(index int, password string) Option {
	return func(d *Database) error {
		if index < 0 {
			return failure.New("invalid configuration value in field 'index': %v", index)
		}
		d.index = index
		d.password = password
		return nil
	}
}

// PoolSize sets the pool size of the database. The default is 10.
func PoolSize(poolsize int) Option {
	return func(d *Database) error {
		if poolsize < 0 {
			return failure.New("invalid configuration value in field 'pool size': %v", poolsize)
		} else if poolsize == 0 {
			poolsize = defaultPoolSize
		}
		d.poolsize = poolsize
		return nil
	}
}

// EOF

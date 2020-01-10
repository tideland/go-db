// Tideland Go Database Clients - CouchDB Client
//
// Copyright (C) 2016-2020 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package couchdb // import "tideland.dev/go/db/couchdb"

//--------------------
// IMPORTS
//--------------------

import (
	"fmt"
)

//--------------------
// OPTIONS
//--------------------

const (
	defaultHost    = "127.0.0.1:5984"
	defaultAddress = "127.0.0.1"
	defaultPort    = 5984
	defaultName    = "default"
	defaultLogging = false
)

// Options is returned when calling Options() on Database to
// provide information about the database configuration.
type Options struct {
	Host    string
	Logging bool
	Name    string
}

// Option defines a function setting an option.
type Option func(db *Database) error

// Host sets the network address and port of the CouchDB.
func Host(address string, port int) Option {
	return func(db *Database) error {
		if address == "" {
			address = defaultAddress
		}
		if port <= 0 {
			port = defaultPort
		}
		db.host = fmt.Sprintf("%s:%d", address, port)
		return nil
	}
}

// Name sets the database name to use.
func Name(name string) Option {
	return func(db *Database) error {
		if name == "" {
			name = defaultName
		}
		db.name = name
		return nil
	}
}

// Logging activates the logging.
func Logging() Option {
	return func(db *Database) error {
		db.logging = true
		return nil
	}
}

// EOF

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
	"encoding/json"

	"tideland.dev/go/trace/failure"
)

//--------------------
// UNMARSHABLE
//--------------------

// Unmarshable describes a not yet unmarshalled value that
// can be unmarshalled into a given variable. It is used to
// access key, value, or document of view result rows.
type Unmarshable struct {
	message json.RawMessage
}

// NewUnmarshableRaw creates a new Unmarshable out of
// the raw bytes.
func NewUnmarshableRaw(raw []byte) *Unmarshable {
	return NewUnmarshableJSON(json.RawMessage(raw))
}

// NewUnmarshableJSON creates a new Unmarshable out of
// a json.RawMessage
func NewUnmarshableJSON(msg json.RawMessage) *Unmarshable {
	return &Unmarshable{
		message: msg,
	}
}

// String returns the unmarshable as string.
func (u *Unmarshable) String() string {
	if u.message == nil {
		return ""
	}
	return string(u.message)
}

// Raw returns the unmarshable as raw byte slice.
func (u *Unmarshable) Raw() []byte {
	if u.message == nil {
		return nil
	}
	dest := make([]byte, len(u.message))
	copy(dest, u.message)
	return dest
}

// Unmarshal unmarshals the interface into the passed variable.
func (u *Unmarshable) Unmarshal(doc interface{}) error {
	err := json.Unmarshal(u.message, doc)
	if err != nil {
		return failure.Annotate(err, "cannot unmarshal database document")
	}
	return nil
}

// EOF

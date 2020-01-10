// Tideland Go Database Clients - CouchDB Client
//
// Copyright (C) 2016-2020 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package couchdb // import "tideland.dev/go/db/couchdb"

//--------------------
// CONSTANTS
//--------------------

// Error codes.
const (
	ErrStartupActionFailed = "ESTARTUP"
	ErrInvalidVersion      = "EINVVSN"
	ErrInvalidDocument     = "EINVDOC"
	ErrNoIdentifier        = "ENOID"
	ErrNotFound            = "ENOTFOUND"
	ErrEncoding            = "EENCODING"
	ErrDecoding            = "EDECODING"
	ErrPreparingRequest    = "EPREPARE"
	ErrPerformingRequest   = "EPERFORM"
	ErrClientRequest       = "EREQ"
	ErrReadingResponseBody = "ERESP"
	ErrUserNotFound        = "ENOUSR"
	ErrUserExists          = "EUSREXIST"

	msgStartupActionFailed = "startup action failed for version '%v'"
	msgInvalidVersion      = "CouchDB returns no or invalid version"
	msgInvalidDocument     = "document needs _id and _rev"
	msgNoIdentifier        = "document contains no identifier"
	msgNotFound            = "document with identifier '%s' not found"
	msgEncoding            = "cannot marshal into database document"
	msgDecoding            = "cannot unmarshal database document"
	msgPreparingRequest    = "cannot prepare request"
	msgPerformingRequest   = "cannot perform request"
	msgClientRequest       = "client request failed: status code %d, error '%s', reason '%s'"
	msgReadingResponseBody = "cannot read response body"
	msgUserNotFound        = "user not found"
	msgUserExists          = "user already exists"
)

// EOF

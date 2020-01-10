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
	"io/ioutil"
	"net/http"

	"tideland.dev/go/trace/failure"
)

//--------------------
// STATUS CODES
//--------------------

// Status codes after database requests.
const (
	StatusOK       = http.StatusOK
	StatusCreated  = http.StatusCreated
	StatusAccepted = http.StatusAccepted

	StatusFound = http.StatusFound

	StatusBadRequest         = http.StatusBadRequest
	StatusUnauthorized       = http.StatusUnauthorized
	StatusForbidden          = http.StatusForbidden
	StatusNotFound           = http.StatusNotFound
	StatusMethodNotAllowed   = http.StatusMethodNotAllowed
	StatusNotAcceptable      = http.StatusNotAcceptable
	StatusConflict           = http.StatusConflict
	StatusPreconditionFailed = http.StatusPreconditionFailed
	StatusTooManyRequests    = http.StatusTooManyRequests

	StatusInternalServerError = http.StatusInternalServerError
)

//--------------------
// RESULT SET
//--------------------

// ResultSet contains the server result set.
type ResultSet struct {
	statusCode  int
	body        []byte
	headers     map[string]string
	document    map[string]interface{}
	id          string
	revision    string
	deleted     bool
	errorText   string
	errorReason string
	err         error
}

// newResultSet analyzes the HTTP response and creates a the
// client ResultSet type out of it.
func newResultSet(resp *http.Response, err error) *ResultSet {
	rs := &ResultSet{
		statusCode: 200,
		err:        err,
	}
	switch {
	case err != nil && failure.Contains(err, "not found"):
		rs.statusCode = StatusNotFound
	case err != nil && failure.Contains(err, "no identifier"):
		rs.statusCode = StatusBadRequest
	case err != nil && failure.Contains(err, "perform request"):
		rs.statusCode = StatusBadRequest
	case err != nil:
		rs.statusCode = StatusInternalServerError
	case resp != nil:
		// Get status code.
		rs.statusCode = resp.StatusCode
		// Read body.
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			rs.err = failure.Annotate(err, "cannot read response body")
		}
		rs.body = body
		// Read headers.
		rs.headers = make(map[string]string)
		for key, values := range resp.Header {
			if len(values) > 0 {
				rs.headers[key] = values[0]
			}
		}
	}
	return rs
}

// IsOK checks the status code if the result is okay.
func (rs *ResultSet) IsOK() bool {
	return rs.err == nil && (rs.statusCode >= 200 && rs.statusCode <= 299)
}

// StatusCode returns the status code of the request.
func (rs *ResultSet) StatusCode() int {
	return rs.statusCode
}

// Error returns a possible error of a request.
func (rs *ResultSet) Error() error {
	if rs.IsOK() {
		return nil
	}
	if rs.err != nil {
		return rs.err
	}
	if err := rs.readDocument(); err != nil {
		return err
	}
	return failure.New(
		"client request failed: status code %d, error '%s', reason '%s'",
		rs.statusCode, rs.errorText, rs.errorReason,
	)
}

// ID returns a potentially returned document identifier.
func (rs *ResultSet) ID() string {
	if !rs.IsOK() {
		return ""
	}
	if err := rs.readDocument(); err != nil {
		return ""
	}
	return rs.id
}

// Revision returns a potentially returned document revision.
func (rs *ResultSet) Revision() string {
	if !rs.IsOK() {
		return ""
	}
	if err := rs.readDocument(); err != nil {
		return ""
	}
	return rs.revision
}

// IsDeleted returns true if a returned document is already deleted.
func (rs *ResultSet) IsDeleted() bool {
	if !rs.IsOK() {
		return false
	}
	if err := rs.readDocument(); err != nil {
		return false
	}
	return rs.deleted
}

// Document returns the received document of a client
// request and unmorshals it.
func (rs *ResultSet) Document(value interface{}) error {
	if rs.err != nil {
		return rs.err
	}
	err := json.Unmarshal(rs.body, value)
	if err != nil {
		return failure.Annotate(err, "cannot unmarshal database document")
	}
	return nil
}

// Raw returns the received raw data of a client request.
func (rs *ResultSet) Raw() ([]byte, error) {
	return rs.body, rs.err
}

// Header provides access to header variables.
func (rs *ResultSet) Header(key string) string {
	value, ok := rs.headers[key]
	if !ok {
		return ""
	}
	return value
}

// readDocument lazily loads and analyzis a generic document.
func (rs *ResultSet) readDocument() error {
	if rs.document == nil {
		rs.document = make(map[string]interface{})
		if err := rs.Document(&rs.document); err != nil {
			return err
		}
		if id, ok := rs.document["_id"]; ok {
			rs.id = id.(string)
		} else if id, ok := rs.document["id"]; ok {
			rs.id = id.(string)
		}
		if revision, ok := rs.document["_rev"]; ok {
			rs.revision = revision.(string)
		} else if revision, ok := rs.document["rev"]; ok {
			rs.revision = revision.(string)
		}
		if deleted, ok := rs.document["_deleted"]; ok {
			rs.deleted = deleted.(bool)
		}
		if errorText, ok := rs.document["error"]; ok {
			rs.errorText = errorText.(string)
		}
		if errorReason, ok := rs.document["reason"]; ok {
			rs.errorReason = errorReason.(string)
		}
	}
	return nil
}

// EOF

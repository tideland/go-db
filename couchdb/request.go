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
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"

	"tideland.dev/go/trace/failure"
	"tideland.dev/go/trace/logger"
)

//--------------------
// REQUEST
//--------------------

// Request is responsible for an individual request to a CouchDB. It has to be
// created by the dabase and can be used queued.
//
// cdb.Request().SetPath(...).SetDocument(...).Put()
type Request struct {
	db        *Database
	path      string
	doc       interface{}
	docReader io.Reader
	query     url.Values
	header    http.Header
}

// newRequest creates a new request for the given location, method, and path. If needed
// query and header can be added like newRequest().setQuery().setHeader.do().
func newRequest(db *Database) *Request {
	req := &Request{
		db:     db,
		path:   "/",
		query:  url.Values{},
		header: http.Header{},
	}
	return req
}

// SetPath sets the absolute path of the request.
func (req *Request) SetPath(parts ...string) *Request {
	req.path = "/" + strings.Join(parts, "/")
	return req
}

// SetDocument sets the document of the request.
func (req *Request) SetDocument(doc interface{}) *Request {
	req.doc = doc
	return req
}

// SetQuery sets a query parameter.
func (req *Request) SetQuery(key, value string) {
	req.query.Set(key, value)
}

// AddQuery adds a query parameter to an existing one.
func (req *Request) AddQuery(key, value string) {
	req.query.Add(key, value)
}

// SetHeader sets a header parameter.
func (req *Request) SetHeader(key, value string) {
	req.header.Set(key, value)
}

// UpdateDocument allows to modify or exchange the request document.
func (req *Request) UpdateDocument(update func(interface{}) interface{}) {
	req.doc = update(req.doc)
}

// ApplyParameters applies a list of parameters to the request.
func (req *Request) ApplyParameters(params ...Parameter) *Request {
	for _, param := range params {
		param(req)
	}
	return req
}

// Head performs a HEAD request.
func (req *Request) Head() *ResultSet {
	return req.do(http.MethodHead)
}

// Get performs a GET request.
func (req *Request) Get() *ResultSet {
	return req.do(http.MethodGet)
}

// Put performs a PUT request.
func (req *Request) Put() *ResultSet {
	return req.do(http.MethodPut)
}

// Post performs a POST request.
func (req *Request) Post() *ResultSet {
	return req.do(http.MethodPost)
}

// GetOrPost decides based on the document if it will perform
// a GET request or a POST request. The document can be set directly
// or by one of the parameters. Several of the CouchDB commands
// work this way.
func (req *Request) GetOrPost() *ResultSet {
	if req.doc != nil {
		return req.Post()
	}
	return req.Get()
}

// Delete performs a DELETE request.
func (req *Request) Delete() *ResultSet {
	return req.do(http.MethodDelete)
}

// do performs a request.
func (req *Request) do(method string) *ResultSet {
	// Prepare URL.
	u := &url.URL{
		Scheme: "http",
		Host:   req.db.host,
		Path:   req.path,
	}
	if len(req.query) > 0 {
		u.RawQuery = req.query.Encode()
	}
	// Marshal a potential document.
	if req.doc != nil {
		marshalled, err := json.Marshal(req.doc)
		if err != nil {
			return newResultSet(nil, failure.Annotate(err, "cannot marshal into database document"))
		}
		req.docReader = bytes.NewBuffer(marshalled)
	}
	// Prepare HTTP request.
	httpReq, err := http.NewRequest(method, u.String(), req.docReader)
	if err != nil {
		return newResultSet(nil, failure.Annotate(err, "cannot prepare request"))
	}
	httpReq.Close = true
	if len(req.header) > 0 {
		httpReq.Header = req.header
	}
	httpReq.Header.Add("Content-Type", "application/json")
	httpReq.Header.Add("Accept", "application/json")
	// Log if wanted.
	if req.db.logging {
		logger.Debugf("couchdb request '%s %s'", method, u)
	}
	// Perform HTTP request.
	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return newResultSet(nil, failure.Annotate(err, "cannot perform request"))
	}
	return newResultSet(httpResp, nil)
}

// EOF

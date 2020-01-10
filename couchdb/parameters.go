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
	"encoding/base64"
	"encoding/json"
	"strconv"
)

//--------------------
// CONSTANTS
//--------------------

// Fixed values for some of the view parameters.
const (
	SinceNow = "now"

	StyleMainOnly = "main_only"
	StyleAllDocs  = "all_docs"
)

//--------------------
// PARAMETERS
//--------------------

// KeyValue is used for generic query and header parameters.
type KeyValue struct {
	Key   string
	Value string
}

// Parameter is a function changing one (or if needed multile) parameter.
type Parameter func(req *Request)

// Query is generic for setting request query parameters.
func Query(kvs ...KeyValue) Parameter {
	return func(req *Request) {
		for _, kv := range kvs {
			req.AddQuery(kv.Key, kv.Value)
		}
	}
}

// Header is generic for setting request header parameters.
func Header(kvs ...KeyValue) Parameter {
	return func(req *Request) {
		for _, kv := range kvs {
			req.SetHeader(kv.Key, kv.Value)
		}
	}
}

// Revision sets the revision for the access to concrete document revisions.
func Revision(revision string) Parameter {
	return func(req *Request) {
		req.SetQuery("rev", revision)
	}
}

// Limit sets the maximum number of result rows.
func Limit(limit int) Parameter {
	return func(req *Request) {
		req.SetQuery("limit", strconv.Itoa(limit))
	}
}

// Skip sets the number to skip for view requests.
func Skip(skip int) Parameter {
	return func(req *Request) {
		if skip > 0 {
			req.SetQuery("skip", strconv.Itoa(skip))
		}
	}
}

// SkipLimit sets the number to skip and the limit for
// view requests.
func SkipLimit(skip, limit int) Parameter {
	return func(req *Request) {
		if skip > 0 {
			req.SetQuery("skip", strconv.Itoa(skip))
		}
		if limit > 0 {
			req.SetQuery("limit", strconv.Itoa(limit))
		}
	}
}

// Since sets the start of the changes gathering, can also be "now".
func Since(sequence string) Parameter {
	return func(req *Request) {
		req.SetQuery("since", sequence)
	}
}

// Descending sets the flag for a descending order of changes gathering.
func Descending() Parameter {
	return func(req *Request) {
		req.SetQuery("descending", "true")
	}
}

// Style sets how many revisions are returned. Default is
// StyleMainOnly only returning the winning document revision.
// StyleAllDocs will return all revision including possible
// conflicts.
func Style(style string) Parameter {
	return func(req *Request) {
		req.SetQuery("style", style)
	}
}

// FilterDocumentIDs sets a filtering of the changes to the
// given document identifiers.
func FilterDocumentIDs(documentIDs ...string) Parameter {
	update := func(doc interface{}) interface{} {
		if doc == nil {
			doc = &couchdbDocumentIDs{}
		}
		idsdoc, ok := doc.(*couchdbDocumentIDs)
		if ok {
			idsdoc.DocumentIDs = append(idsdoc.DocumentIDs, documentIDs...)
			return idsdoc
		}
		return doc
	}
	return func(req *Request) {
		req.SetQuery("filter", "_doc_ids")
		req.UpdateDocument(update)
	}
}

// FilterSelector sets the filter to the passed selector expression.
func FilterSelector(selector json.RawMessage) Parameter {
	update := func(doc interface{}) interface{} {
		// TODO 2020-03-31 Mue Set selector expression.
		return doc
	}
	return func(req *Request) {
		req.SetQuery("filter", "_selector")
		req.UpdateDocument(update)
	}
}

// FilterView sets the name of a view which map function acts as
// filter in case it emits at least one record.
func FilterView(view string) Parameter {
	return func(req *Request) {
		req.SetQuery("filter", "_view")
		req.SetQuery("view", view)
	}
}

// BasicAuthentication is intended for basic authentication
// against the database.
func BasicAuthentication(name, password string) Parameter {
	return func(req *Request) {
		np := []byte(name + ":" + password)
		auth := "Basic " + base64.StdEncoding.EncodeToString(np)

		req.SetHeader("Authorization", auth)
	}
}

// Keys sets a number of keys wanted for a view request.
func Keys(keys ...interface{}) Parameter {
	update := func(doc interface{}) interface{} {
		if doc == nil {
			doc = &couchdbKeys{}
		}
		kdoc, ok := doc.(*couchdbKeys)
		if ok {
			kdoc.Keys = append(kdoc.Keys, keys...)
			return kdoc
		}
		return doc
	}
	return func(req *Request) {
		req.UpdateDocument(update)
	}
}

// StringKeys sets a number of keys of type string wanted for a view request.
func StringKeys(keys ...string) Parameter {
	var ikeys []interface{}
	for _, key := range keys {
		ikeys = append(ikeys, key)
	}
	return Keys(ikeys...)
}

// StartKey sets the startkey for a view request.
func StartKey(start interface{}) Parameter {
	jstart, _ := json.Marshal(start)
	return func(req *Request) {
		req.SetQuery("startkey", string(jstart))
	}
}

// EndKey sets the endkey for a view request.
func EndKey(end interface{}) Parameter {
	jend, _ := json.Marshal(end)
	return func(req *Request) {
		req.SetQuery("endkey", string(jend))
	}
}

// StartEndKey sets the startkey and endkey for a view request.
func StartEndKey(start, end interface{}) Parameter {
	jstart, _ := json.Marshal(start)
	jend, _ := json.Marshal(end)
	return func(req *Request) {
		req.SetQuery("startkey", string(jstart))
		req.SetQuery("endkey", string(jend))
	}
}

// OneKey reduces a view result to only one emitted key.
func OneKey(key interface{}) Parameter {
	jkey, _ := json.Marshal(key)
	return func(req *Request) {
		req.SetQuery("key", string(jkey))
	}
}

// NoReduce sets the flag for usage of a reduce function to false.
func NoReduce() Parameter {
	return func(req *Request) {
		req.SetQuery("reduce", "false")
	}
}

// Group sets the flag for grouping including the level for the
// reduce function.
func Group(level int) Parameter {
	return func(req *Request) {
		req.SetQuery("group", "true")
		if level > 0 {
			req.SetQuery("group_level", strconv.Itoa(level))
		}
	}
}

// IncludeDocuments sets the flag for the including of found view documents.
func IncludeDocuments() Parameter {
	return func(req *Request) {
		req.SetQuery("include_docs", "true")
	}
}

// EOF

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
)

//--------------------
// SEARCH
//--------------------

// Search allows to formulate what documents shall be selected and the
// additional parameters.
type Search struct {
	parameters map[string]interface{}
}

// NewSearch creates a query for the search of documents.
func NewSearch(selector string) *Search {
	s := &Search{
		parameters: make(map[string]interface{}),
	}
	s.parameters["selector"] = json.RawMessage(selector)
	return s
}

// Fields sets the fields to return.
func (s *Search) Fields(fields ...string) *Search {
	s.parameters["fields"] = fields
	return s
}

// Sort sets the sorting of the result by alternates of field names
// and directions like "asc" or "desc". For examle ("name", "asc",
// "age", "desc").
func (s *Search) Sort(fieldsOrDirs ...string) *Search {
	sort := []map[string]string{}
	field := ""
	for _, fieldOrDir := range fieldsOrDirs {
		if field == "" {
			field = fieldOrDir
			continue
		}
		sort = append(sort, map[string]string{
			field: fieldOrDir,
		})
		field = ""
	}
	s.parameters["sort"] = sort
	return s
}

// Limit sets the maximum number of results returned.
func (s *Search) Limit(limit int) *Search {
	s.parameters["limit"] = limit
	return s
}

// Skip sets a number of results to skip.
func (s *Search) Skip(skip int) *Search {
	s.parameters["skip"] = skip
	return s
}

// UseIndex instructs the search to use a specific index. Name
// is allowed be empty.
func (s *Search) UseIndex(designDocument, name string) *Search {
	if name == "" {
		s.parameters["use_index"] = designDocument
	} else {
		s.parameters["use_index"] = []string{designDocument, name}
	}
	return s
}

// ReadQuorum sets the needed read quorum for the result. The default is 1,
// so that the document found un the index is returned. Higher quorums forces
// to read from more replicas. This case needs more time.
func (s *Search) ReadQuorum(quorum int) *Search {
	s.parameters["r"] = quorum
	return s
}

// Bookmark enables to specify which page of results is required. Every
// search returns an opaque string under the bookmark key that can be passed
// this way. Only works for indexes of type "text".
func (s *Search) Bookmark(bookmark string) *Search {
	s.parameters["bookmark"] = bookmark
	return s
}

// Update sets whether to update the index prior to returning the result.
// Default is true.
func (s *Search) Update(update bool) *Search {
	s.parameters["update"] = update
	return s
}

// Stable sets whether to view results from a "stable" set of shards.
func (s *Search) Stable(stable bool) *Search {
	s.parameters["stable"] = stable
	return s
}

// MarshalJSON implements json.Marshaler.
func (s *Search) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.parameters)
}

//--------------------
// FINDS
//--------------------

// FindProcessor is a function processing the content of a found document.
type FindProcessor func(document *Unmarshable) error

// Find allows to find and process documents by a given selector.
type Find struct {
	db   *Database
	find *couchdbFind
}

// newFind returns a new finds instance.
func newFind(db *Database, search *Search, params ...Parameter) (*Find, error) {
	rs := db.Request().SetPath(db.name, "_find").SetDocument(search).ApplyParameters(params...).Post()
	if !rs.IsOK() {
		return nil, rs.Error()
	}
	find := couchdbFind{}
	err := rs.Document(&find)
	if err != nil {
		return nil, err
	}
	return &Find{
		db:   db,
		find: &find,
	}, nil
}

// Len returns the number of found documents.
func (f *Find) Len() int {
	return len(f.find.Documents)
}

// Process iterates over the found documents and processes them.
func (f *Find) Process(process FindProcessor) error {
	for _, doc := range f.find.Documents {
		unmarshableDoc := NewUnmarshableJSON(doc)
		if err := process(unmarshableDoc); err != nil {
			return err
		}
	}
	return nil
}

// EOF

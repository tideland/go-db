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
// CHANGES
//--------------------

// ChangeProcessor is a function processing the content of a changed document.
type ChangeProcessor func(id, sequence string, deleted bool, revisions []string, document *Unmarshable) error

// Changes provides access to the responded changes.
type Changes struct {
	db      *Database
	changes *couchdbChanges
}

// newChanges requests the changes document and prepares the access type.
func newChanges(db *Database, params ...Parameter) (*Changes, error) {
	rs := db.Request().SetPath(db.name, "_changes").ApplyParameters(params...).GetOrPost()
	if !rs.IsOK() {
		return nil, rs.Error()
	}
	changes := couchdbChanges{}
	err := rs.Document(&changes)
	if err != nil {
		return nil, err
	}
	return &Changes{
		db:      db,
		changes: &changes,
	}, nil
}

// LastSequence returns the sequence ID of the last change.
func (c *Changes) LastSequence() string {
	return fmt.Sprintf("%v", c.changes.LastSequence)
}

// Pending returns the number of pending changes if the
// query has been limited.
func (c *Changes) Pending() int {
	return c.changes.Pending
}

// Len returns the number of changes.
func (c *Changes) Len() int {
	return len(c.changes.Results)
}

// Process iterates over the found changes and processes them.
func (c *Changes) Process(process ChangeProcessor) error {
	for _, result := range c.changes.Results {
		revisions := []string{}
		for _, change := range result.Changes {
			revisions = append(revisions, change.Revision)
		}
		seq := fmt.Sprintf("%v", result.Sequence)
		doc := NewUnmarshableJSON(result.Document)
		if err := process(result.ID, seq, result.Deleted, revisions, doc); err != nil {
			return err
		}
	}
	return nil
}

// EOF

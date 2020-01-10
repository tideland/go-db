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
// EXTERNAL DOCUMENT TYPES
//--------------------

// Status contains internal status information CouchDB returns.
type Status struct {
	OK       bool   `json:"ok"`
	ID       string `json:"id"`
	Revision string `json:"rev"`
	Error    string `json:"error"`
	Reason   string `json:"reason"`
}

// Statuses is the list of status information after a bulk writing.
type Statuses []Status

// DatabaseVersionID is used for the database version document.
const DatabaseVersionID = "database-version"

// DatabaseVersion stores the current database version with
// the document ID "database-version".
type DatabaseVersion struct {
	ID       string `json:"_id"`
	Revision string `json:"_rev,omitempty"`
	Version  string `json:"version"`
}

// User contains name and password for
// user management and authentication.
type User struct {
	DocumentID       string `json:"_id,omitempty"`
	DocumentRevision string `json:"_rev,omitempty"`

	Name     string   `json:"name"`
	Password string   `json:"password"`
	Type     string   `json:"type,omitempty"`
	Roles    []string `json:"roles,omitempty"`
}

// NamesRoles contains names and roles for
// administrators and users.
type NamesRoles struct {
	Names []string `json:"names,omitempty"`
	Roles []string `json:"roles,omitempty"`
}

// Security contains administrators and
// members for one database.
type Security struct {
	Admins  NamesRoles `json:"admins,omitempty"`
	Members NamesRoles `json:"members,omitempty"`
}

//--------------------
// INTERNAL DOCUMENT TYPES
//--------------------

// couchdbBulkDocuments contains a number of documents added at once.
type couchdbBulkDocuments struct {
	Docs     []interface{} `json:"docs"`
	NewEdits bool          `json:"new_edits,omitempty"`
}

// couchdbRows returns rows containing IDs of documents. It's
// part of a view document.
type couchdbRows struct {
	Rows []struct {
		ID string `json:"id"`
	}
}

// couchdbDocumentIDs contains document identifiers as body
// for the according changes filter.
type couchdbDocumentIDs struct {
	DocumentIDs []string `json:"doc_ids"`
}

// couchdbChangesResultChange contains the revision number of one
// change of one document.
type couchdbChangesResultChange struct {
	Revision string `json:"rev"`
}

// couchdbChangesResult contains one result of a changes feed.
type couchdbChangesResult struct {
	ID       string                       `json:"id"`
	Sequence interface{}                  `json:"seq"`
	Changes  []couchdbChangesResultChange `json:"changes"`
	Document json.RawMessage              `json:"doc,omitempty"`
	Deleted  bool                         `json:"deleted,omitempty"`
}

// couchdbChanges is a generic result of a CouchDB changes feed.
type couchdbChanges struct {
	LastSequence interface{}            `json:"last_seq"`
	Pending      int                    `json:"pending"`
	Results      []couchdbChangesResult `json:"results"`
}

// couchdbKeys sets key constraints for view requests.
type couchdbKeys struct {
	Keys []interface{} `json:"keys"`
}

// couchdbViewRow contains one row of a view result.
type couchdbViewRow struct {
	ID       string          `json:"id"`
	Key      json.RawMessage `json:"key"`
	Value    json.RawMessage `json:"value"`
	Document json.RawMessage `json:"doc"`
}

type couchdbViewRows []couchdbViewRow

// couchdbView is a generic result of a view.
type couchdbView struct {
	TotalRows int             `json:"total_rows"`
	Offset    int             `json:"offset"`
	Rows      couchdbViewRows `json:"rows"`
}

// couchdbFind is the result of a find command.
type couchdbFind struct {
	Warning   string            `json:"warning"`
	Documents []json.RawMessage `json:"docs"`
}

// couchdRoles contains the roles of a user if the
// authentication succeeded.
type couchdbRoles struct {
	OK       bool     `json:"ok"`
	Name     string   `json:"name"`
	Password string   `json:"password_sha,omitempty"`
	Salt     string   `json:"salt,omitempty"`
	Type     string   `json:"type"`
	Roles    []string `json:"roles"`
}

// EOF

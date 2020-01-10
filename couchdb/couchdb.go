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
	"reflect"
	"strings"

	"tideland.dev/go/dsa/identifier"
	"tideland.dev/go/trace/failure"
)

//--------------------
// DATABASE
//--------------------

// Database provides the access to a database.
type Database struct {
	host    string
	name    string
	logging bool
}

// Open returns a configured connection to a CouchDB server.
// Permanent parameters, e.g. for authentication, are possible.
func Open(options ...Option) (*Database, error) {
	db := &Database{
		host:    defaultHost,
		name:    defaultName,
		logging: defaultLogging,
	}
	for _, option := range options {
		if err := option(db); err != nil {
			return nil, err
		}
	}
	return db, nil
}

// Name returns the name of the configured database.
func (db *Database) Name() string {
	return db.name
}

// Manager returns the database system manager.
func (db *Database) Manager() *Manager {
	return newManager(db)
}

// Designs returns the design document manager.
func (db *Database) Designs() *Designs {
	return newDesigns(db)
}

// StartSession starts a cookie based session for the given user.
func (db *Database) StartSession(name, password string) (*Session, error) {
	user := User{
		Name:     name,
		Password: password,
	}
	rs := db.Request().SetPath("_session").SetDocument(user).Post()
	if !rs.IsOK() {
		return nil, rs.Error()
	}
	roles := couchdbRoles{}
	err := rs.Document(&roles)
	if err != nil {
		return nil, err
	}
	setCookie := rs.Header("Set-Cookie")
	authSession := ""
	for _, part := range strings.Split(setCookie, ";") {
		if strings.HasPrefix(part, "AuthSession=") {
			authSession = part
			break
		}
	}
	s := &Session{
		db:          db,
		name:        roles.Name,
		authSession: authSession,
	}
	return s, nil
}

// AllDocumentIDs returns a list of all document IDs
// of the configured database.
func (db *Database) AllDocumentIDs(params ...Parameter) ([]string, error) {
	rs := db.Request().SetPath(db.name, "_all_docs").ApplyParameters(params...).Get()
	if !rs.IsOK() {
		return nil, rs.Error()
	}
	designRows := couchdbRows{}
	err := rs.Document(&designRows)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for _, row := range designRows.Rows {
		ids = append(ids, row.ID)
	}
	return ids, nil
}

// HasDocument checks if the document with the ID exists.
func (db *Database) HasDocument(id string, params ...Parameter) (bool, error) {
	rs := db.Request().SetPath(db.name, id).ApplyParameters(params...).Head()
	if rs.IsOK() {
		return true, nil
	}
	if rs.StatusCode() == StatusNotFound {
		return false, nil
	}
	return false, rs.Error()
}

// CreateDocument creates a new document.
func (db *Database) CreateDocument(doc interface{}, params ...Parameter) *ResultSet {
	id, _, err := db.idAndRevision(doc)
	if err != nil {
		return newResultSet(nil, err)
	}
	if id == "" {
		id = identifier.NewUUID().ShortString()
	}
	return db.Request().SetPath(db.name, id).SetDocument(doc).ApplyParameters(params...).Put()
}

// ReadDocument reads the a document by ID.
func (db *Database) ReadDocument(id string, params ...Parameter) *ResultSet {
	return db.Request().SetPath(db.name, id).ApplyParameters(params...).Get()
}

// UpdateDocument update a document if exists.
func (db *Database) UpdateDocument(doc interface{}, params ...Parameter) *ResultSet {
	id, _, err := db.idAndRevision(doc)
	if err != nil {
		return newResultSet(nil, err)
	}
	if id == "" {
		return newResultSet(nil, failure.New("document contains no identifier"))
	}
	hasDoc, err := db.HasDocument(id)
	if err != nil {
		return newResultSet(nil, err)
	}
	if !hasDoc {
		return newResultSet(nil, failure.New("document with identifier '%s' not found", id))
	}
	return db.Request().SetPath(db.name, id).SetDocument(doc).ApplyParameters(params...).Put()
}

// DeleteDocument deletes a existing document.
func (db *Database) DeleteDocument(doc interface{}, params ...Parameter) *ResultSet {
	id, revision, err := db.idAndRevision(doc)
	if err != nil {
		return newResultSet(nil, err)
	}
	hasDoc, err := db.HasDocument(id)
	if err != nil {
		return newResultSet(nil, err)
	}
	if !hasDoc {
		return newResultSet(nil, failure.New("document with identifier '%s' not found", id))
	}
	params = append(params, Revision(revision))
	return db.Request().SetPath(db.name, id).ApplyParameters(params...).Delete()
}

// DeleteDocumentByID deletes an existing document simply by
// its identifier and revision.
func (db *Database) DeleteDocumentByID(id, revision string, params ...Parameter) *ResultSet {
	hasDoc, err := db.HasDocument(id)
	if err != nil {
		return newResultSet(nil, err)
	}
	if !hasDoc {
		return newResultSet(nil, failure.New("document with identifier '%s' not found", id))
	}
	params = append(params, Revision(revision))
	return db.Request().SetPath(db.name, id).ApplyParameters(params...).Delete()
}

// BulkWriteDocuments allows to create or update many
// documents en bloc.
func (db *Database) BulkWriteDocuments(docs []interface{}, params ...Parameter) (Statuses, error) {
	bulk := &couchdbBulkDocuments{
		Docs: docs,
	}
	rs := db.Request().SetPath(db.name, "_bulk_docs").SetDocument(bulk).ApplyParameters(params...).Post()
	if !rs.IsOK() {
		return nil, rs.Error()
	}
	statuses := Statuses{}
	err := rs.Document(&statuses)
	if err != nil {
		return nil, err
	}
	return statuses, nil
}

// Changes returns access to the changes of the configured database.
func (db *Database) Changes(params ...Parameter) (*Changes, error) {
	return newChanges(db, params...)
}

// View returns access to a view of the configured database.
func (db *Database) View(designID, viewID string, params ...Parameter) (*View, error) {
	return newView(db, designID, viewID, params...)
}

// Find runs a selection and returns access to the found results.
func (db *Database) Find(search *Search, params ...Parameter) (*Find, error) {
	return newFind(db, search, params...)
}

// Request returns a raw database request for this database. Can
// be used for not covered low-level commands.
func (db *Database) Request() *Request {
	return newRequest(db)
}

// idAndRevision retrieves the ID and the revision of the
// passed document.
func (db *Database) idAndRevision(doc interface{}) (string, string, error) {
	v := reflect.Indirect(reflect.ValueOf(doc))
	t := v.Type()
	k := t.Kind()
	if k != reflect.Struct {
		return "", "", failure.New("document needs _id and _rev")
	}
	var id string
	var revision string
	var found int
	for i := 0; i < t.NumField(); i++ {
		vf := v.Field(i)
		tf := t.Field(i)
		if json, ok := tf.Tag.Lookup("json"); ok {
			switch json {
			case "_id", "_id,omitempty":
				id = vf.String()
				found++
			case "_rev", "_rev,omitempty":
				revision = vf.String()
				found++
			}
		}
	}
	if found != 2 {
		return "", "", failure.New("document needs _id and _rev")
	}
	return id, revision, nil
}

// EOF

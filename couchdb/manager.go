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

	"tideland.dev/go/dsa/version"
	"tideland.dev/go/trace/failure"
)

//--------------------
// STEPS
//--------------------

// StepAction is the concrete action of a step.
type StepAction func(db *Database) error

// Step returns the version after a startup step and the action
// that shall be performed on the database. The returned action
// will only be performed, if the current if the new version is
// than the current version.
type Step func() (version.Version, StepAction)

// execute performs one step.
func (step Step) execute(db *Database) error {
	// Retrieve current database version.
	resp := db.ReadDocument(DatabaseVersionID)
	if !resp.IsOK() {
		return resp.Error()
	}
	dv := DatabaseVersion{}
	err := resp.Document(&dv)
	if err != nil {
		return err
	}
	cv, err := version.Parse(dv.Version)
	if err != nil {
		return failure.Annotate(err, "CouchDB returns no or invalid version")
	}
	// Get new version of the step and action.
	nv, action := step()
	// Check the new version.
	precedence, _ := nv.Compare(cv)
	if precedence != version.Newer {
		return nil
	}
	// Now perform the step action and update the
	// version document.
	err = action(db)
	if err != nil {
		return failure.Annotate(err, "startup action failed for version '%v'", nv)
	}
	dv.Version = nv.String()
	resp = db.UpdateDocument(&dv)
	return resp.Error()
}

// Steps is just an ordered number of steps.
type Steps []Step

// execute performs the steps.
func (steps Steps) execute(db *Database) error {
	for _, step := range steps {
		if err := step.execute(db); err != nil {
			return err
		}
	}
	return nil
}

//--------------------
// INDEX
//--------------------

// Index allows to generate an index for faster find operations.
type Index struct {
	name       string
	parameters map[string]interface{}
}

// NewIndex creates an index.
func NewIndex(name string, fields ...string) *Index {
	idx := &Index{
		name:       name,
		parameters: make(map[string]interface{}),
	}
	idx.parameters["fields"] = fields
	return idx
}

// Selector adds a selector to the index.
func (idx *Index) Selector(selector string) *Index {
	idx.parameters["selector"] = json.RawMessage(selector)
	return idx
}

// Sort sets the sorting of the index by alternates of field names
// and directions like "asc" or "desc". For examle ("name", "asc",
// "age", "desc").
func (idx *Index) Sort(fieldsOrDirs ...string) *Index {
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
	idx.parameters["sort"] = sort
	return idx
}

// Limit sets the maximum number of index documents.
func (idx *Index) Limit(limit int) *Index {
	idx.parameters["limit"] = limit
	return idx
}

// Skip sets a number of documents to skip.
func (idx *Index) Skip(skip int) *Index {
	idx.parameters["skip"] = skip
	return idx
}

// MarshalJSON implements json.Marshaler.
func (idx *Index) MarshalJSON() ([]byte, error) {
	doc := map[string]interface{}{
		"name":  idx.name,
		"index": idx.parameters,
		"type":  "json",
	}
	return json.Marshal(doc)
}

//--------------------
// MANAGER
//--------------------

// Manager bundles the methods to manage the database system
// opposite to handle documents.
type Manager struct {
	db *Database
}

// newManager creates the manager instance.
func newManager(db *Database) *Manager {
	return &Manager{
		db: db,
	}
}

// Version returns the version number of CouchDB.
func (m *Manager) Version() (version.Version, error) {
	rs := m.db.Request().Get()
	if !rs.IsOK() {
		return version.New(0, 0, 0), rs.Error()
	}
	welcome := map[string]interface{}{}
	err := rs.Document(&welcome)
	if err != nil {
		return version.New(0, 0, 0), err
	}
	vsn, ok := welcome["version"].(string)
	if !ok {
		return version.New(0, 0, 0), failure.New("CouchDB returns no or invalid version")
	}
	return version.Parse(vsn)
}

// DatabaseVersion returns the version number of the database.
func (m *Manager) DatabaseVersion() (version.Version, error) {
	rs := m.db.ReadDocument(DatabaseVersionID)
	if !rs.IsOK() {
		return version.New(0, 0, 0), failure.New("CouchDB returns no or invalid version")
	}
	dv := DatabaseVersion{}
	err := rs.Document(&dv)
	if err != nil {
		return version.New(0, 0, 0), failure.New("CouchDB returns no or invalid version")
	}
	return version.Parse(dv.Version)
}

// Init checks and creates the database if needed and performs
// the individual steps.
func (m *Manager) Init(steps ...Step) error {
	// Check database.
	ok, err := m.HasDatabase()
	if err != nil {
		return err
	}
	// Create and initialize it.
	if !ok {
		resp := m.CreateDatabase()
		if !resp.IsOK() {
			return resp.Error()
		}
		dv := DatabaseVersion{
			ID:      DatabaseVersionID,
			Version: version.New(0, 0, 0).String(),
		}
		resp = m.db.CreateDocument(&dv)
		if !resp.IsOK() {
			return resp.Error()
		}
	}
	// Execute the steps.
	return Steps(steps).execute(m.db)
}

// AllDatabaseIDs returns a list of all database IDs
// of the connected server.
func (m *Manager) AllDatabaseIDs() ([]string, error) {
	rs := m.db.Request().SetPath("_all_dbs").Get()
	if !rs.IsOK() {
		return nil, rs.Error()
	}
	ids := []string{}
	err := rs.Document(&ids)
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// HasDatabase checks if the configured database exists.
func (m *Manager) HasDatabase() (bool, error) {
	rs := m.db.Request().SetPath(m.db.name).Head()
	if rs.IsOK() {
		return true, nil
	}
	if rs.StatusCode() == StatusNotFound {
		return false, nil
	}
	return false, rs.Error()
}

// CreateDatabase creates the configured database.
func (m *Manager) CreateDatabase(params ...Parameter) *ResultSet {
	return m.db.Request().SetPath(m.db.name).ApplyParameters(params...).Put()
}

// DeleteDatabase removes the configured database.
func (m *Manager) DeleteDatabase(params ...Parameter) *ResultSet {
	return m.db.Request().SetPath(m.db.name).ApplyParameters(params...).Delete()
}

// DeleteNamedDatabase removes the given database.
func (m *Manager) DeleteNamedDatabase(name string, params ...Parameter) *ResultSet {
	return m.db.Request().SetPath(name).ApplyParameters(params...).Delete()
}

// CreateIndex creates a new index for finds.
func (m *Manager) CreateIndex(index *Index, params ...Parameter) *ResultSet {
	return m.db.Request().SetPath(m.db.name, "_index").SetDocument(index).ApplyParameters(params...).Post()
}

// HasAdministrator checks if a given administrator account exists.
func (m *Manager) HasAdministrator(nodename, name string, params ...Parameter) (bool, error) {
	rs := m.db.Request().SetPath("_node", nodename, "_config", "admins", name).ApplyParameters(params...).Get()
	if !rs.IsOK() {
		if rs.StatusCode() == StatusNotFound {
			return false, nil
		}
		return false, rs.Error()
	}
	return true, nil
}

// WriteAdministrator adds or updates an administrator to the given database.
func (m *Manager) WriteAdministrator(nodename, name, password string, params ...Parameter) error {
	rs := m.db.Request().SetPath("_node", nodename, "_config", "admins", name).SetDocument(password).ApplyParameters(params...).Put()
	if !rs.IsOK() {
		return rs.Error()
	}
	return nil
}

// DeleteAdministrator deletes an administrator from the given database.
func (m *Manager) DeleteAdministrator(nodename, name string, params ...Parameter) error {
	rs := m.db.Request().SetPath("_node", nodename, "_config", "admins", name).ApplyParameters(params...).Delete()
	if !rs.IsOK() {
		return rs.Error()
	}
	return nil
}

// ReadUser reads an existing user from the system.
func (m *Manager) ReadUser(name string, params ...Parameter) (*User, error) {
	if err := ensureUsersDatabase(m.db, params...); err != nil {
		return nil, err
	}
	rs := m.db.Request().SetPath("_users", userDocumentID(name)).ApplyParameters(params...).Get()
	if !rs.IsOK() {
		if rs.StatusCode() == StatusNotFound {
			return nil, failure.New("user not found")
		}
		return nil, rs.Error()
	}
	var user User
	err := rs.Document(&user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

// CreateUser adds a new user to the system.
func (m *Manager) CreateUser(user *User, params ...Parameter) error {
	if err := ensureUsersDatabase(m.db, params...); err != nil {
		return err
	}
	if _, err := m.ReadUser(user.Name, params...); err == nil {
		return failure.New("user already exists")
	}
	user.DocumentID = userDocumentID(user.Name)
	user.Type = "user"
	rs := m.db.Request().SetPath("_users", user.DocumentID).SetDocument(user).ApplyParameters(params...).Put()
	return rs.Error()
}

// UpdateUser updates a user in the system.
func (m *Manager) UpdateUser(user *User, params ...Parameter) error {
	if err := ensureUsersDatabase(m.db, params...); err != nil {
		return err
	}
	rs := m.db.Request().SetPath("_users", user.DocumentID).SetDocument(user).ApplyParameters(params...).Put()
	return rs.Error()
}

// DeleteUser deletes a user from the system.
func (m *Manager) DeleteUser(name string, params ...Parameter) error {
	if err := ensureUsersDatabase(m.db, params...); err != nil {
		return err
	}
	rs := m.db.Request().SetPath("_users", userDocumentID(name)).ApplyParameters(params...).Get()
	if rs.IsOK() {
		var user User
		err := rs.Document(&user)
		if err != nil {
			return err
		}
		params = append(params, Revision(user.DocumentRevision))
		rs = m.db.Request().SetPath("_users", user.DocumentID).ApplyParameters(params...).Delete()
		return rs.Error()
	}
	return nil
}

// ReadSecurity returns the security for the given database.
func (m *Manager) ReadSecurity(params ...Parameter) (*Security, error) {
	rs := m.db.Request().SetPath(m.db.name, "_security").ApplyParameters(params...).Get()
	if !rs.IsOK() {
		return nil, rs.Error()
	}
	var security Security
	err := rs.Document(&security)
	if err != nil {
		return nil, err
	}
	return &security, nil
}

// WriteSecurity writes new or changed security data to
// the given database.
func (m *Manager) WriteSecurity(security Security, params ...Parameter) error {
	rs := m.db.Request().SetPath(m.db.name, "_security").SetDocument(security).ApplyParameters(params...).Put()
	if !rs.IsOK() {
		return rs.Error()
	}
	return nil
}

//--------------------
// HELPERS
//--------------------

// ensureUsersDatabase checks if the _users database exists and
// creates it if needed.
func ensureUsersDatabase(db *Database, params ...Parameter) error {
	rs := db.Request().SetPath("_users").ApplyParameters(params...).Get()
	if rs.IsOK() {
		return nil
	}
	return db.Request().SetPath("_users").ApplyParameters(params...).Put().Error()
}

// userDocumentID builds the document ID based
// on the name.
func userDocumentID(name string) string {
	return "org.couchdb.user:" + name
}

// EOF

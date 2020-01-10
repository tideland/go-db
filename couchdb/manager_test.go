// Tideland Go Database Clients - CouchDB Client
//
// Copyright (C) 2016-2020 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package couchdb_test // import "tideland.dev/go/db/couchdb_test"

//--------------------
// IMPORTS
//--------------------

import (
	"testing"

	"tideland.dev/go/audit/asserts"
	"tideland.dev/go/db/couchdb"
	"tideland.dev/go/dsa/version"
)

//--------------------
// TESTS
//--------------------

// TestVersion tests the retrieving of the DBMS version.
func TestVersion(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)

	// Open the database to retrieve the DBMS version.
	cdb, err := couchdb.Open(couchdb.Name(testDB))
	assert.NoError(err)
	vsn, err := cdb.Manager().Version()
	assert.NoError(err)

	assert.Logf("CouchDB version %v", vsn)
}

// TestNoSteps tests creating the database with no steps.
func TestNoSteps(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)

	cdb, err := couchdb.Open(couchdb.Name(testDB))
	assert.Nil(err)
	defer func() { cdb.Manager().DeleteDatabase() }()

	err = cdb.Manager().Init()
	assert.Nil(err)

	ok, err := cdb.Manager().HasDatabase()
	assert.Nil(err)
	assert.True(ok)

	vsn, err := cdb.Manager().DatabaseVersion()
	assert.Nil(err)
	assert.Equal(vsn.String(), "0.0.0")
}

// TestSomeSteps tests creating the database with some steps.
func TestSomeSteps(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)

	cdb, err := couchdb.Open(couchdb.Name(testDB))
	assert.Nil(err)
	defer func() { cdb.Manager().DeleteDatabase() }()

	err = cdb.Manager().Init(StepA, StepB)
	assert.Nil(err)

	resp := cdb.ReadDocument(couchdb.DatabaseVersionID)
	assert.True(resp.IsOK())

	vsn, err := cdb.Manager().DatabaseVersion()
	assert.Nil(err)
	assert.Equal(vsn.String(), "0.2.0")

	ids, err := cdb.AllDocumentIDs()
	assert.Nil(err)
	assert.Length(ids, 3)
}

// TestMultipleStartups tests calling startup multiple times.
func TestMultipleStartups(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)

	cdb, err := couchdb.Open(couchdb.Name(testDB))
	assert.Nil(err)
	defer func() { cdb.Manager().DeleteDatabase() }()

	err = cdb.Manager().Init(StepA)
	assert.Nil(err)

	ids, err := cdb.AllDocumentIDs()
	assert.Nil(err)
	assert.Length(ids, 2)

	resp := cdb.ReadDocument(couchdb.DatabaseVersionID)
	assert.True(resp.IsOK())

	vsn, err := cdb.Manager().DatabaseVersion()
	assert.Nil(err)
	assert.Equal(vsn.String(), "0.1.0")

	err = cdb.Manager().Init(StepA, StepB, StepC)
	assert.Nil(err)

	resp = cdb.ReadDocument(couchdb.DatabaseVersionID)
	assert.True(resp.IsOK())

	vsn, err = cdb.Manager().DatabaseVersion()
	assert.Nil(err)
	assert.Equal(vsn.String(), "0.3.0")

	ids, err = cdb.AllDocumentIDs()
	assert.Nil(err)
	assert.Length(ids, 4)
}

// TestAllDatabaseIDs tests the retrieving of all database IDs.
func TestAllDatabaseIDs(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)

	// Open the database.
	cdb, err := couchdb.Open(couchdb.Name(testDB))
	assert.NoError(err)
	_, err = cdb.Manager().AllDatabaseIDs()
	assert.NoError(err)
}

// TestCreateDeleteDatabase tests the creation and deletion
// of a database.
func TestCreateDeleteDatabase(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)

	// Open and check existence.
	cdb, err := couchdb.Open(couchdb.Name(testDB))
	assert.NoError(err)
	has, err := cdb.Manager().HasDatabase()
	assert.NoError(err)
	assert.False(has)

	// Create and check existence,
	resp := cdb.Manager().CreateDatabase()
	assert.Nil(resp.Error())
	assert.True(resp.IsOK())
	has, err = cdb.Manager().HasDatabase()
	assert.NoError(err)
	assert.True(has)

	// Delete and check existence.
	resp = cdb.Manager().DeleteDatabase()
	assert.True(resp.IsOK())
	has, err = cdb.Manager().HasDatabase()
	assert.NoError(err)
	assert.False(has)
}

// TestAdministraotor tests the administrator related functions.
func TestAdministrator(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareDatabase(assert, "administrator")
	defer cleanup()

	// Check first admin before it exists.
	ok, err := cdb.Manager().HasAdministrator("nonode@nohost", "admin1")
	assert.NoError(err)
	assert.False(ok)

	err = cdb.Manager().WriteAdministrator("nonode@nohost", "admin1", "admin1")
	assert.NoError(err)
	defer func() {
		// Let the administator remove himself.
		session, err := cdb.StartSession("admin1", "admin1")
		assert.NoError(err)
		err = cdb.Manager().DeleteAdministrator("nonode@nohost", "admin1", session.Cookie())
		assert.NoError(err)
	}()

	// Check first admin after creation without session.
	ok, err = cdb.Manager().HasAdministrator("nonode@nohost", "admin1")
	assert.ErrorMatch(err, ".*status code 401.*")
	assert.False(ok)

	// Check first admin after creation with session.
	session, err := cdb.StartSession("admin1", "admin1")
	assert.NoError(err)
	ok, err = cdb.Manager().HasAdministrator("nonode@nohost", "admin1", session.Cookie())
	assert.NoError(err)
	assert.True(ok)

	// Now care for second administrator, first withour session,
	// then with.
	err = cdb.Manager().WriteAdministrator("nonode@nohost", "admin2", "admin2")
	assert.ErrorMatch(err, ".*status code 401.*")

	err = cdb.Manager().WriteAdministrator("nonode@nohost", "admin2", "admin2", session.Cookie())
	assert.NoError(err)

	ok, err = cdb.Manager().HasAdministrator("nonode@nohost", "admin2", session.Cookie())
	assert.NoError(err)
	assert.True(ok)

	auth := couchdb.BasicAuthentication("admin1", "admin1")
	err = cdb.Manager().DeleteAdministrator("nonode@nohost", "admin2", auth)
	assert.NoError(err)

	ok, err = cdb.Manager().HasAdministrator("nonode@nohost", "admin2", auth)
	assert.NoError(err)
	assert.False(ok)
}

// TestUser tests the user management related functions.
func TestUser(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareDeletedDatabase(assert, "user")
	defer cleanup()

	userA := &couchdb.User{
		Name:     "userA",
		Password: "userA",
		Roles:    []string{"developer"},
	}
	cdb.Manager().DeleteUser("userA")

	userB, err := cdb.Manager().ReadUser("userA")
	assert.Nil(userB)
	assert.ErrorMatch(err, ".*user not found.*")

	err = cdb.Manager().CreateUser(userA)
	assert.NoError(err)

	userB, err = cdb.Manager().ReadUser("userA")
	assert.NoError(err)
	assert.Equal(userA.Name, "userA")

	err = cdb.Manager().CreateUser(userB)
	assert.ErrorMatch(err, ".*user already exists.*")

	err = cdb.Manager().DeleteUser("userA")
	assert.NoError(err)
}

// TestSecurity tests the cdb.Manager() related functions.
func TestSecurity(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareDeletedDatabase(assert, "security")
	defer cleanup()

	// Without database and authentication.
	in := couchdb.Security{
		Admins: couchdb.NamesRoles{
			Names: []string{"admin"},
		},
	}
	err := cdb.Manager().WriteSecurity(in)
	assert.ErrorMatch(err, ".*status code 404.*")

	// Without database but with authentication.
	err = cdb.Manager().WriteAdministrator("nonode@nohost", "admin", "admin")
	assert.NoError(err)
	defer func() {
		// Let the administator remove himself.
		session, err := cdb.StartSession("admin", "admin")
		assert.NoError(err)
		err = cdb.Manager().DeleteAdministrator("nonode@nohost", "admin", session.Cookie())
		assert.NoError(err)
	}()
	session, err := cdb.StartSession("admin", "admin")
	assert.NoError(err)
	err = cdb.Manager().WriteSecurity(in, session.Cookie())
	assert.ErrorMatch(err, ".*status code 404.*")

	// With database and without authentication.
	rs := cdb.Manager().CreateDatabase()
	assert.ErrorMatch(rs.Error(), ".*status code 401.*")
	rs = cdb.Manager().CreateDatabase(session.Cookie())
	assert.True(rs.IsOK())
	defer func() {
		rs := cdb.Manager().DeleteDatabase(session.Cookie())
		assert.True(rs.IsOK())
	}()
	err = cdb.Manager().WriteSecurity(in)
	assert.ErrorMatch(err, ".*status code 500.*")

	// With database and authentication.
	err = cdb.Manager().WriteSecurity(in, session.Cookie())
	assert.NoError(err)

	// Now read the security information.
	out, err := cdb.Manager().ReadSecurity(couchdb.BasicAuthentication("admin", "admin"))
	assert.NoError(err)
	assert.Equal(out.Admins, in.Admins)
}

// TestScenario tests a scenario with administrator, user,
// amd unauthorized access to the database.
func TestScenario(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareDeletedDatabase(assert, "scenario")
	defer cleanup()

	// Create administrator.
	err := cdb.Manager().WriteAdministrator("nonode@nohost", "admin", "admin")
	assert.NoError(err)
	session, err := cdb.StartSession("admin", "admin")
	assert.NoError(err)
	defer func() {
		// Let the administator remove himself.
		err = cdb.Manager().DeleteAdministrator("nonode@nohost", "admin", session.Cookie())
		assert.NoError(err)
	}()

	// Create users.
	user := &couchdb.User{
		Name:     "user",
		Password: "user",
		Roles:    []string{"developer"},
	}
	err = cdb.Manager().CreateUser(user, session.Cookie())
	assert.NoError(err)
	user = &couchdb.User{
		Name:     "somebody",
		Password: "somebody",
		Roles:    []string{"visitors"},
	}
	err = cdb.Manager().CreateUser(user, session.Cookie())
	assert.NoError(err)
	defer func() {
		user, err := cdb.Manager().ReadUser("user", session.Cookie())
		assert.NoError(err)
		err = cdb.Manager().DeleteUser(user.Name, session.Cookie())
		assert.NoError(err)
		user, err = cdb.Manager().ReadUser("somebody", session.Cookie())
		assert.NoError(err)
		err = cdb.Manager().DeleteUser(user.Name, session.Cookie())
		assert.NoError(err)
	}()

	// Create database.
	rs := cdb.Manager().CreateDatabase(session.Cookie())
	assert.NoError(rs.Error())
	assert.True(rs.IsOK())
	defer func() {
		rs := cdb.Manager().DeleteDatabase(session.Cookie())
		assert.True(rs.IsOK())
	}()

	// Add security.
	sec := couchdb.Security{
		Admins: couchdb.NamesRoles{
			Names: []string{"admin"},
		},
		Members: couchdb.NamesRoles{
			Names: []string{"user"},
		},
	}
	err = cdb.Manager().WriteSecurity(sec, session.Cookie())
	assert.NoError(err)

	// Add document.
	doc := Worker{
		DocumentID: "foo",
		Name:       "foo",
	}
	rs = cdb.CreateDocument(doc)
	assert.False(rs.IsOK())
	rs = cdb.CreateDocument(doc, couchdb.BasicAuthentication("somebody", "somebody"))
	assert.False(rs.IsOK())
	rs = cdb.CreateDocument(doc, couchdb.BasicAuthentication("user", "user"))
	assert.True(rs.IsOK())
}

//--------------------
// STEPS
//--------------------

func StepA() (version.Version, couchdb.StepAction) {
	v := version.New(0, 1, 0)
	return v, func(db *couchdb.Database) error {
		md := Worker{
			DocumentID: "my-document-a",
			Name:       "Joe Black",
			Age:        25,
		}
		resp := db.CreateDocument(&md)
		return resp.Error()
	}
}

func StepB() (version.Version, couchdb.StepAction) {
	v := version.New(0, 2, 0)
	return v, func(db *couchdb.Database) error {
		md := Worker{
			DocumentID: "my-document-b",
			Name:       "John Doe",
			Age:        51,
		}
		resp := db.CreateDocument(&md)
		return resp.Error()
	}
}

func StepC() (version.Version, couchdb.StepAction) {
	v := version.New(0, 3, 0)
	return v, func(db *couchdb.Database) error {
		md := Worker{
			DocumentID: "my-document-c",
			Name:       "Donald Duck",
			Age:        85,
		}
		resp := db.CreateDocument(&md)
		return resp.Error()
	}
}

// EOF

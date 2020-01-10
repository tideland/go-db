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
	"tideland.dev/go/trace/failure"
)

//--------------------
// CONSTANTS
//--------------------

const (
	// testDB is the name of the database used for testing.
	testDB = "tmp-couchdb-testing"
)

//--------------------
// TESTS
//--------------------

// TestInvalidConfiguration tests opening the database with an invalid
//  configuration.
func TestInvalidConfiguration(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)

	// Open with illegal configuration is okay, only
	// usage of this will fail.
	cdb, err := couchdb.Open(couchdb.Host("some-non-existing-host", 12345))
	assert.Nil(err)

	// Deleting the database has to fail.
	resp := cdb.Manager().DeleteDatabase()
	assert.Equal(resp.StatusCode(), couchdb.StatusBadRequest)
}

// TestCreateDesignDocument tests creating new design documents.
func TestCreateDesignDocument(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareFilledDatabase(assert, "tmp-create-design")
	defer cleanup()

	// Create design document and check if it has been created.
	designIDsA, err := cdb.Designs().IDs()
	assert.Nil(err)

	design, err := cdb.Designs().Design("testing-a")
	assert.Nil(err)
	assert.Equal(design.ID(), "testing-a")
	design.SetView("index-a", "function(doc){ if (doc._id.indexOf('a') !== -1) { emit(doc._id, doc._rev);  } }", "")
	resp := design.Write()
	assert.True(resp.IsOK())

	design, err = cdb.Designs().Design("testing-b")
	assert.Nil(err)
	assert.Equal(design.ID(), "testing-b")
	design.SetView("index-b", "function(doc){ if (doc._id.indexOf('b') !== -1) { emit(doc._id, doc._rev);  } }", "")
	resp = design.Write()
	assert.True(resp.IsOK())

	designIDsB, err := cdb.Designs().IDs()
	assert.Nil(err)
	assert.Equal(len(designIDsB), len(designIDsA)+2)
}

// TestReadDesignDocument tests reading design documents.
func TestReadDesignDocument(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareFilledDatabase(assert, "tmp-read-design")
	defer cleanup()

	// Create design document and read it again.
	designA, err := cdb.Designs().Design("testing-a")
	assert.Nil(err)
	assert.Equal(designA.ID(), "testing-a")
	designA.SetView("index-a", "function(doc){ if (doc._id.indexOf('a') !== -1) { emit(doc._id, doc._rev);  } }", "")
	resp := designA.Write()
	assert.True(resp.IsOK())

	designB, err := cdb.Designs().Design("testing-a")
	assert.Nil(err)
	assert.Equal(designB.ID(), "testing-a")
}

// TestUpdateDesignDocument tests updating design documents.
func TestUpdateDesignDocument(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareFilledDatabase(assert, "tmp-update-design")
	defer cleanup()

	// Create design document and read it again.
	designA, err := cdb.Designs().Design("testing-a")
	assert.Nil(err)
	assert.Equal(designA.ID(), "testing-a")
	designA.SetView("index-a", "function(doc){ if (doc._id.indexOf('a') !== -1) { emit(doc._id, doc._rev);  } }", "")
	resp := designA.Write()
	assert.True(resp.IsOK())

	designB, err := cdb.Designs().Design("testing-a")
	assert.Nil(err)
	assert.Equal(designB.ID(), "testing-a")

	// Now update it and read it again.
	designB.SetView("index-b", "function(doc){ if (doc._id.indexOf('b') !== -1) { emit(doc._id, doc._rev);  } }", "")
	resp = designB.Write()
	assert.True(resp.IsOK())

	designC, err := cdb.Designs().Design("testing-a")
	assert.Nil(err)
	assert.Equal(designC.ID(), "testing-a")
	_, _, ok := designC.View("index-a")
	assert.True(ok)
	_, _, ok = designC.View("index-b")
	assert.True(ok)
}

// TestDeleteDesignDocument tests deleting design documents.
func TestDeleteDesignDocument(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareFilledDatabase(assert, "tmp-delete-design")
	defer cleanup()

	// Create design document and check if it has been created.
	designIDsA, err := cdb.Designs().IDs()
	assert.Nil(err)

	designA, err := cdb.Designs().Design("testing")
	assert.Nil(err)
	designA.SetView("index-a", "function(doc){ if (doc._id.indexOf('a') !== -1) { emit(doc._id, doc._rev);  } }", "")
	resp := designA.Write()
	assert.True(resp.IsOK())

	designIDsB, err := cdb.Designs().IDs()
	assert.Nil(err)
	assert.Equal(len(designIDsB), len(designIDsA)+1)

	// Read it and delete it.
	designB, err := cdb.Designs().Design("testing")
	assert.Nil(err)
	resp = designB.Delete()
	assert.True(resp.IsOK())

	designIDsC, err := cdb.Designs().IDs()
	assert.Nil(err)
	assert.Equal(len(designIDsC), len(designIDsA))
}

// TestCreateDocument tests creating new documents.
func TestCreateDocument(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareDatabase(assert, "tmp-create-document")
	defer cleanup()

	// Create document without ID.
	docA := Worker{
		Name: "foo",
		Age:  50,
	}
	resp := cdb.CreateDocument(docA)
	assert.True(resp.IsOK())
	id := resp.ID()
	assert.Match(id, "[0-9a-f]{32}")

	// Create document with ID.
	docB := Worker{
		DocumentID: "bar-12345",
		Name:       "bar",
		Age:        25,
		Active:     true,
	}
	resp = cdb.CreateDocument(docB)
	assert.True(resp.IsOK())
	id = resp.ID()
	assert.Equal(id, "bar-12345")
}

// TestReadDocument tests reading a document.
func TestReadDocument(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareDatabase(assert, "tmp-read-document")
	defer cleanup()

	// Create test document.
	docA := Worker{
		DocumentID: "foo-12345",
		Name:       "foo",
		Age:        18,
	}
	resp := cdb.CreateDocument(docA)
	assert.True(resp.IsOK())
	id := resp.ID()
	assert.Equal(id, "foo-12345")

	// Read test document.
	resp = cdb.ReadDocument(id)
	assert.True(resp.IsOK())
	docB := Worker{}
	err := resp.Document(&docB)
	assert.Nil(err)
	assert.Equal(docB.DocumentID, docA.DocumentID)
	assert.Equal(docB.Name, docA.Name)
	assert.Equal(docB.Age, docA.Age)

	// Try to read non-existent document.
	resp = cdb.ReadDocument("i-do-not-exist")
	assert.False(resp.IsOK())
	assert.ErrorMatch(resp.Error(), ".* 404,.*")
}

// TestUpdateDocument tests updating documents.
func TestUpdateDocument(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareDatabase(assert, "tmp-update-document")
	defer cleanup()

	// Create first revision.
	docA := Worker{
		DocumentID: "foo-12345",
		Name:       "foo",
		Age:        22,
	}
	resp := cdb.CreateDocument(docA)
	assert.True(resp.IsOK())
	id := resp.ID()
	revision := resp.Revision()
	assert.Equal(id, "foo-12345")

	resp = cdb.ReadDocument(id)
	assert.True(resp.IsOK())
	docB := Worker{}
	err := resp.Document(&docB)
	assert.Nil(err)

	// Update the document.
	docB.Age = 23

	resp = cdb.UpdateDocument(docB)
	assert.True(resp.IsOK())

	// Read the updated revision.
	resp = cdb.ReadDocument(id)
	assert.True(resp.IsOK())
	docC := Worker{}
	err = resp.Document(&docC)
	assert.Nil(err)
	assert.Equal(docC.DocumentID, docB.DocumentID)
	assert.Substring("2-", docC.DocumentRevision)
	assert.Equal(docC.Name, docB.Name)
	assert.Equal(docC.Age, docB.Age)

	// Read the first revision.
	resp = cdb.ReadDocument(id, couchdb.Revision(revision))
	assert.True(resp.IsOK())
	assert.Equal(resp.Revision(), revision)

	// Try to update a non-existent document.
	docD := Worker{
		DocumentID: "i-do-not-exist",
		Name:       "none",
		Age:        999,
	}
	resp = cdb.UpdateDocument(docD)
	assert.False(resp.IsOK())
	assert.Equal(resp.StatusCode(), couchdb.StatusNotFound)
	assert.True(failure.Contains(resp.Error(), "not found"))
}

// TestDeleteDocument tests deleting a document.
func TestDeleteDocument(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareDatabase(assert, "tmp-delete-document")
	defer cleanup()

	// Create test document.
	docA := Worker{
		DocumentID: "foo-12345",
		Name:       "foo",
		Age:        33,
	}
	resp := cdb.CreateDocument(docA)
	assert.True(resp.IsOK())
	id := resp.ID()
	assert.Equal(id, "foo-12345")

	// Read test document, we need it including the revision.
	resp = cdb.ReadDocument(id)
	assert.True(resp.IsOK())
	docB := Worker{}
	err := resp.Document(&docB)
	assert.Nil(err)

	// Delete the test document.
	resp = cdb.DeleteDocument(docB)
	assert.True(resp.IsOK())

	// Try to read deleted document.
	resp = cdb.ReadDocument(id)
	assert.False(resp.IsOK())
	assert.Equal(resp.StatusCode(), couchdb.StatusNotFound)

	// Try to delete it a second time.
	resp = cdb.DeleteDocument(docB)
	assert.False(resp.IsOK())
	assert.Equal(resp.StatusCode(), couchdb.StatusNotFound)
	assert.True(failure.Contains(resp.Error(), "not found"))
}

// TestDeleteDocumentByID tests deleting a document by identifier.
func TestDeleteDocumentByID(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareDatabase(assert, "tmp-delete-document-by-id")
	defer cleanup()

	// Create test document.
	docA := Worker{
		DocumentID: "foo-12345",
		Name:       "foo",
		Age:        33,
	}
	resp := cdb.CreateDocument(docA)
	assert.True(resp.IsOK())
	id := resp.ID()
	revision := resp.Revision()
	assert.Equal(id, "foo-12345")

	// Delete the test document by ID.
	resp = cdb.DeleteDocumentByID(id, revision)
	assert.True(resp.IsOK())

	// Try to read deleted document.
	resp = cdb.ReadDocument(id)
	assert.False(resp.IsOK())
	assert.Equal(resp.StatusCode(), couchdb.StatusNotFound)

	// Try to delete it a second time.
	resp = cdb.DeleteDocumentByID(id, revision)
	assert.False(resp.IsOK())
	assert.Equal(resp.StatusCode(), couchdb.StatusNotFound)
	assert.True(failure.Contains(resp.Error(), "not found"))
}

// EOF

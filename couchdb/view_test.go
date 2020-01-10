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
	"strings"
	"testing"

	"tideland.dev/go/audit/asserts"
	"tideland.dev/go/db/couchdb"
)

//--------------------
// TESTS
//--------------------

// TestView tests calling a view.
func TestView(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareFilledDatabase(assert, "views")
	defer cleanup()

	// Create design document.
	design, err := cdb.Designs().Design("testing")
	assert.Nil(err)
	design.SetView("index-a", "function(doc){ if (doc._id.indexOf('a') !== -1) { emit(doc._id, doc);  } }", "")
	design.SetView("age", "function(doc){ emit(doc.age, doc.name); }", "")
	resp := design.Write()
	assert.True(resp.IsOK())

	// Call the view for the first time.
	v, err := cdb.View("testing", "index-a")
	assert.NoError(err)
	trOld := v.TotalRows()
	assert.True(trOld > 0)

	// Add a matching document and view again.
	docA := Worker{
		DocumentID: "black-jack-4711",
		Name:       "Jack Black",
	}
	resp = cdb.CreateDocument(docA)
	assert.True(resp.IsOK())
	v, err = cdb.View("testing", "index-a")
	assert.NoError(err)
	trNew := v.TotalRows()
	assert.Equal(trNew, trOld+1)
	err = v.Process(func(id string, key, value, document *couchdb.Unmarshable) error {
		valueA := Worker{}
		err := value.Unmarshal(&valueA)
		assert.Nil(err)
		assert.True(strings.Contains(valueA.DocumentID, "a"))
		return err
	})
	assert.Nil(err)

	// Add a non-matching document and view again.
	docB := Worker{
		DocumentID: "doe-john-999",
		Name:       "John Doe",
	}
	resp = cdb.CreateDocument(docB)
	assert.True(resp.IsOK())
	v, err = cdb.View("testing", "index-a")
	assert.NoError(err)
	trFinal := v.TotalRows()
	assert.Equal(trFinal, trNew)

	// Call age view with a key.
	v, err = cdb.View("testing", "age", couchdb.OneKey(51))
	assert.NoError(err)
	assert.True(v.TotalRows() > v.ReturnedRows())
	err = v.Process(func(id string, key, value, document *couchdb.Unmarshable) error {
		var age int
		var name string
		err := key.Unmarshal(&age)
		assert.Nil(err)
		assert.Equal(age, 51)
		err = value.Unmarshal(&name)
		assert.Nil(err)
		return err
	})
	assert.Nil(err)

	// Call age view with the oldest 5 people below 50.
	v, err = cdb.View("testing", "age", couchdb.StartKey(50), couchdb.Descending(), couchdb.Limit(5))
	assert.NoError(err)
	assert.True(v.ReturnedRows() <= 5)
	err = v.Process(func(id string, key, value, document *couchdb.Unmarshable) error {
		var age int
		var name string
		err := key.Unmarshal(&age)
		assert.Nil(err)
		assert.True(age <= 50)
		err = value.Unmarshal(&name)
		assert.Nil(err)
		return err
	})
	assert.Nil(err)

	// Call age view with multiple keys (even multiple times).
	v, err = cdb.View("testing", "age", couchdb.Keys(50, 51, 52), couchdb.Keys(53, 54))
	assert.NoError(err)
	err = v.Process(func(id string, key, value, document *couchdb.Unmarshable) error {
		var age int
		var name string
		err := key.Unmarshal(&age)
		assert.Nil(err)
		assert.True(age >= 50)
		assert.True(age <= 54)
		err = value.Unmarshal(&name)
		assert.Nil(err)
		return err
	})
	assert.Nil(err)
}

// EOF

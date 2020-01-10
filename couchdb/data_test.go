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
	"time"

	"tideland.dev/go/audit/asserts"
	"tideland.dev/go/audit/generators"
	"tideland.dev/go/db/couchdb"
	"tideland.dev/go/dsa/identifier"
	"tideland.dev/go/trace/logger"
)

//--------------------
// TEST DATA
//--------------------

// Note is used for the tests.
type Note struct {
	Title string `json:"title"`
	Text  string `json:"text"`
}

// Worker is used for the tests.
type Worker struct {
	DocumentID       string `json:"_id,omitempty"`
	DocumentRevision string `json:"_rev,omitempty"`

	Name        string `json:"name"`
	Age         int    `json:"age"`
	Shifts      []int  `json:"shifts"`
	Active      bool   `json:"active"`
	LastActive  int64  `json:"last_active,omitempty"`
	Notes       []Note `json:"notes"`
	Description string `json:"description"`
}

// prepareDatabase opens the database, deletes a possible test
// database, and creates it newly.
func prepareDatabase(assert *asserts.Asserts, name string) (*couchdb.Database, func()) {
	logger.SetLevel(logger.LevelDebug)
	cdb, err := couchdb.Open(couchdb.Name(name))
	assert.Nil(err)
	cdb.Manager().DeleteDatabase()
	rs := cdb.Manager().CreateDatabase()
	assert.Nil(rs.Error())
	assert.True(rs.IsOK())
	return cdb, func() { cdb.Manager().DeleteDatabase() }
}

// prepareDeletedDatabase opens the database, checks result,
// and deletes it. Ensures a good environment including
// cleanup func.
func prepareDeletedDatabase(assert *asserts.Asserts, name string) (*couchdb.Database, func()) {
	logger.SetLevel(logger.LevelDebug)
	cdb, err := couchdb.Open(couchdb.Name(name))
	assert.Nil(err)
	cdb.Manager().DeleteDatabase()
	cdb.Manager().DeleteNamedDatabase("_users")
	return cdb, func() {
		cdb.Manager().DeleteDatabase()
		cdb.Manager().DeleteNamedDatabase("_users")
	}
}

// prepareFilledDatabase opens the database, deletes a possible test
// database, creates it newly and adds some data.
func prepareFilledDatabase(assert *asserts.Asserts, name string) (*couchdb.Database, func()) {
	return prepareSizedFilledDatabase(assert, name, 1000)
}

// prepareSizedFilledDatabase opens the database, deletes a possible test
// database, creates it newly and adds a given number of data.
func prepareSizedFilledDatabase(assert *asserts.Asserts, name string, count int) (*couchdb.Database, func()) {
	logger.SetLevel(logger.LevelDebug)
	cdb, err := couchdb.Open(couchdb.Name(name))
	assert.Nil(err)
	cdb.Manager().DeleteDatabase()
	rs := cdb.Manager().CreateDatabase()
	assert.True(rs.IsOK())

	idx := couchdb.NewIndex("worker-names", "name")
	rs = cdb.Manager().CreateIndex(idx)
	assert.True(rs.IsOK())

	docs := generateDocuments(count)
	results, err := cdb.BulkWriteDocuments(docs)
	assert.Nil(err)
	for _, result := range results {
		assert.True(result.OK)
	}

	return cdb, func() { cdb.Manager().DeleteDatabase() }
}

// generateDocuments creates a number of documents.
func generateDocuments(count int) []interface{} {
	gen := generators.New(generators.SimpleRand())
	docs := []interface{}{}
	for i := 0; i < count; i++ {
		first, middle, last := gen.Name()
		worker := Worker{
			DocumentID:  identifier.Identifier(last, first, i),
			Name:        first + " " + middle + " " + last,
			Age:         gen.Int(18, 65),
			Shifts:      []int{gen.Int(1, 3), gen.Int(1, 3), gen.Int(1, 3)},
			Active:      gen.FlipCoin(75),
			Description: gen.Sentence(),
		}
		if worker.Active {
			worker.LastActive = gen.Time(time.UTC, time.Now().Add(-24*time.Hour), 24*time.Hour).Unix()
		}
		for j := 0; j < gen.Int(3, 9); j++ {
			note := Note{
				Title: gen.Sentence(),
				Text:  gen.Paragraph(),
			}
			worker.Notes = append(worker.Notes, note)
		}
		docs = append(docs, worker)
	}
	return docs
}

// EOF

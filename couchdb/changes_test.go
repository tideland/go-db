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
)

//--------------------
// TESTS
//--------------------

// TestChanges tests retrieving changes.
func TestChanges(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	count := 1000
	cdb, cleanup := prepareSizedFilledDatabase(assert, "changes", count)
	defer cleanup()

	// Simple changes access, length is plus one due to index document.
	chgs, err := cdb.Changes()
	assert.NoError(err)
	assert.Equal(chgs.Len(), count+1)

	chgs.Process(func(id, sequence string, deleted bool, revisions []string, document *couchdb.Unmarshable) error {
		assert.Length(revisions, 1)
		return nil
	})

	lseq := chgs.LastSequence()
	chgs, err = cdb.Changes(couchdb.Since(lseq))
	assert.NoError(err)
	assert.Equal(chgs.Len(), 0)

	chgs, err = cdb.Changes(couchdb.Since(couchdb.SinceNow))
	assert.NoError(err)
	assert.Equal(chgs.Len(), 0)

	// Add some more documents and check changes.
	docs := generateDocuments(count)
	results, err := cdb.BulkWriteDocuments(docs)
	assert.Nil(err)
	for _, result := range results {
		assert.True(result.OK)
	}

	chgs, err = cdb.Changes()
	assert.NoError(err)
	assert.Equal(chgs.Len(), 2*count+1)

	chgs, err = cdb.Changes(couchdb.Since(lseq))
	assert.NoError(err)
	assert.Equal(chgs.Len(), count)
}

// EOF

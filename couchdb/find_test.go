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

// TestSimpleFind tests tests calling find with a simple search.
func TestSimpleFind(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareFilledDatabase(assert, "find-simple")
	defer cleanup()

	// Try to find some documents a simple way.
	search := couchdb.NewSearch(`{"$or": [
			{"$and": [
				{"age": {"$lt": 30}},
				{"active": {"$eq": false}}
			]},
			{"$and": [
				{"age": {"$gt": 60}},
				{"active": {"$eq": true}}
			]}
		]}`).
		Fields("name", "age", "active")

	fnds, err := cdb.Find(search)
	assert.NoError(err)

	err = fnds.Process(func(document *couchdb.Unmarshable) error {
		fields := struct {
			Name   string `json:"name"`
			Age    int    `json:"age"`
			Active bool   `json:"active"`
		}{}
		if err := document.Unmarshal(&fields); err != nil {
			return err
		}
		assert.True((fields.Age < 30 && !fields.Active) || (fields.Age > 60 && fields.Active))
		return nil
	})
	assert.Nil(err)
}

// TestLimitedFind tests retrieving a larger number but set the limit.
func TestLimitedFind(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareFilledDatabase(assert, "find-limited")
	defer cleanup()

	// Limit found documents.
	search := couchdb.NewSearch(`{"active": {"$eq": true}}`).Limit(5)

	fnds, err := cdb.Find(search)
	assert.NoError(err)
	assert.Length(fnds, 5)

	// Greater limit.
	search = couchdb.NewSearch(`{"active": {"$eq": true}}`).Limit(100)

	fnds, err = cdb.Find(search)
	assert.NoError(err)
	assert.Length(fnds, 100)
}

// TestSortedFind tests retrieving a larger number in a sorted way.
func TestSortedFind(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareFilledDatabase(assert, "find-sorted")
	defer cleanup()

	// Sorting field has to be part of selector.
	search := couchdb.NewSearch(`{"name": {"$gt": ""}, "active": {"$eq": true}}`).Sort("name", "asc").Limit(100)

	fnds, err := cdb.Find(search)
	assert.NoError(err)

	name := ""
	err = fnds.Process(func(document *couchdb.Unmarshable) error {
		fields := struct {
			Name string `json:"name"`
		}{}
		if err := document.Unmarshal(&fields); err != nil {
			return err
		}
		assert.True(fields.Name >= name)
		name = fields.Name
		return nil
	})
	assert.Nil(err)
}

// TestFindExists tests calling find with an exists selector.
func TestFindExists(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareFilledDatabase(assert, "find-exists")
	defer cleanup()

	// Try to find some documents having an existing "last_active".
	search := couchdb.NewSearch(`{"last_ective": {"$exists": true}, "age": {"$lte": 25}}`).
		Fields("name", "age", "active", "last_active")

	fnds, err := cdb.Find(search)
	assert.NoError(err)

	err = fnds.Process(func(document *couchdb.Unmarshable) error {
		fields := struct {
			Name       string `json:"name"`
			Age        int    `json:"age"`
			Active     bool   `json:"active"`
			LastActive int64  `json:"last_active"`
		}{}
		if err := document.Unmarshal(&fields); err != nil {
			return err
		}
		assert.True(fields.Age <= 25 && fields.LastActive > 0 && fields.Active)
		return nil
	})
	assert.Nil(err)

	// Now look for existing "last_active" but "active" is false. So
	// no results.
	search = couchdb.NewSearch(`{"last_ective": {"$exists": true}, "active": {"$eq": true}}`).
		Fields("name", "age", "active", "last_active")

	fnds, err = cdb.Find(search)
	assert.NoError(err)
	assert.Length(fnds, 0)
}

// TestMatches tests using element and all match operators.
func TestMatches(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	cdb, cleanup := prepareFilledDatabase(assert, "find-match")
	defer cleanup()

	// Find with at least one matching element.
	search := couchdb.NewSearch(`{"shifts": {"$elemMatch": {"$eq": 3}}}`).
		Fields("name", "shifts")

	fnds, err := cdb.Find(search)
	assert.NoError(err)

	err = fnds.Process(func(document *couchdb.Unmarshable) error {
		fields := struct {
			Name   string `json:"name"`
			Shifts []int  `json:"shifts"`
		}{}
		if err := document.Unmarshal(&fields); err != nil {
			return err
		}
		assert.Contents(3, fields.Shifts)
		return nil
	})
	assert.Nil(err)

	// Find with all matching elements (dumb query,
	// but checking combination).
	search = couchdb.NewSearch(`{"shifts": {"$allMatch": {"$gt": 1, "$lt": 3}}}`).
		Fields("name", "shifts")

	fnds, err = cdb.Find(search)
	assert.NoError(err)

	err = fnds.Process(func(document *couchdb.Unmarshable) error {
		fields := struct {
			Name   string `json:"name"`
			Shifts []int  `json:"shifts"`
		}{}
		if err := document.Unmarshal(&fields); err != nil {
			return err
		}
		assert.Equal(fields.Shifts, []int{2, 2, 2})
		return nil
	})
	assert.Nil(err)
}

// EOF

// Tideland Go Database Clients - Redis Client - Unit Tests
//
// Copyright (C) 2017-2020 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis_test

//--------------------
// IMPORTS
//--------------------

import (
	"sync"
	"testing"
	"time"

	"tideland.dev/go/audit/asserts"
	"tideland.dev/go/db/redis"
	"tideland.dev/go/trace/logger"
)

//--------------------
// TESTS
//--------------------

func TestTCPConnection(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	conn, restore := connectDatabase(t, assert, redis.TCPConnection("", 0))
	defer restore()

	result, err := conn.Do("echo", "Hello, World!")
	assert.Nil(err)
	assertEqualString(assert, result, 0, "Hello, World!")
	result, err = conn.Do("ping")
	assert.Nil(err)
	assertEqualString(assert, result, 0, "+PONG")
}

func BenchmarkTCPConnection(b *testing.B) {
	assert := asserts.NewTesting(b, asserts.FailStop)
	conn, restore := connectDatabase(nil, assert, redis.TCPConnection("", 0))
	defer restore()

	for i := 0; i < b.N; i++ {
		result, err := conn.Do("ping")
		assert.Nil(err)
		assertEqualString(assert, result, 0, "+PONG")
	}
}

func TestPipelining(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	ppl, restore := pipelineDatabase(t, assert)
	defer restore()

	for i := 0; i < 1000; i++ {
		err := ppl.Do("ping")
		assert.Nil(err)
	}

	results, err := ppl.Collect()
	assert.Nil(err)
	assert.Length(results, 1000)

	for _, result := range results {
		assertEqualString(assert, result, 0, "+PONG")
	}
}

func BenchmarkPipelining(b *testing.B) {
	assert := asserts.NewTesting(b, asserts.FailStop)
	ppl, restore := pipelineDatabase(nil, assert)
	defer restore()

	for i := 0; i < b.N; i++ {
		err := ppl.Do("ping")
		assert.Nil(err)
	}
	results, err := ppl.Collect()
	assert.Nil(err)
	assert.Length(results, b.N)

	for _, result := range results {
		assertEqualString(assert, result, 0, "+PONG")
	}
}

func TestOptions(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	db, err := redis.Open(redis.TCPConnection("", 0), redis.PoolSize(5))
	assert.Nil(err)
	defer db.Close()

	options := db.Options()
	assert.Equal(options.Address, "127.0.0.1:6379")
	assert.Equal(options.Network, "tcp")
	assert.Equal(options.Timeout, 30*time.Second)
	assert.Equal(options.Index, 0)
	assert.Equal(options.Password, "")
	assert.Equal(options.PoolSize, 5)
	assert.Equal(options.Logging, false)
}

func TestConcurrency(t *testing.T) {
	assert := asserts.NewTesting(t, asserts.FailStop)
	db, err := redis.Open(redis.TCPConnection("", 0), redis.PoolSize(100))
	assert.Nil(err)
	defer db.Close()

	var wg sync.WaitGroup
	var connErr error
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := db.Connection()
			if err != nil {
				if connErr == nil {
					connErr = err
				}
				return
			}
			defer conn.Return()
			result, err := conn.Do("ping")
			assert.NoError(err)
			assertEqualString(assert, result, 0, "+PONG")
		}()
	}
	wg.Wait()
	assert.NoError(connErr)
}

//--------------------
// TOOLS
//--------------------

func init() {
	logger.SetLevel(logger.LevelDebug)
}

const (
	// testTimeout defines the time waited to establish a connection.
	testTimeout = 100 * time.Millisecond

	// testDatabaseIndex defines the database index for the tests to not
	// get in conflict with existing databases.
	testDatabaseIndex = 0
)

// connectDatabase connects to a Redis database with the given options
// and returns a connection and a function for closing. This function
// shall be called with defer.
func connectDatabase(t *testing.T, assert *asserts.Asserts, options ...redis.Option) (*redis.Connection, func()) {
	// Open and connect database.
	options = append(options, redis.TCPConnection("", testTimeout), redis.Index(testDatabaseIndex, ""))
	db, err := redis.Open(options...)
	if !assert.Nil(err) {
		t.FailNow()
	}
	conn, err := db.Connection()
	if !assert.Nil(err) {
		t.FailNow()
	}
	// Flush all keys to get a clean testing environment.
	_, err = conn.Do("flushdb")
	assert.Nil(err)
	// Return connection and cleanup function.
	return conn, func() {
		conn.Return()
		db.Close()
	}
}

// pipelineDatabase connects to a Redis database with the given options
// and returns a pipeling and a function for closing. This function
// shall be called with a defer.
func pipelineDatabase(t *testing.T, assert *asserts.Asserts, options ...redis.Option) (*redis.Pipeline, func()) {
	// Open and connect database.
	options = append(options, redis.TCPConnection("", testTimeout), redis.Index(testDatabaseIndex, ""))
	db, err := redis.Open(options...)
	if !assert.Nil(err) {
		t.FailNow()
	}
	ppl, err := db.Pipeline()
	if !assert.Nil(err) {
		t.FailNow()
	}
	// Return pipeline and cleanup function.
	return ppl, func() {
		db.Close()
	}
}

// subscribeDatabase connects to a Redis database with the given options
// and returns a subscription and a function for closing. This function
// shall be called with a defer.
func subscribeDatabase(t *testing.T, assert *asserts.Asserts, options ...redis.Option) (*redis.Subscription, func()) {
	// Open and connect database.
	options = append(options, redis.TCPConnection("", testTimeout), redis.Index(testDatabaseIndex, ""))
	db, err := redis.Open(options...)
	if !assert.Nil(err) {
		t.FailNow()
	}
	sub, err := db.Subscription()
	if !assert.Nil(err) {
		t.FailNow()
	}
	// Return subscription and cleanup function.
	return sub, func() {
		sub.Close()
		db.Close()
	}
}

// assertEqualString checks if the result at index is value.
func assertEqualString(assert *asserts.Asserts, result *redis.ResultSet, index int, value string) {
	s, err := result.StringAt(index)
	assert.Nil(err)
	assert.Equal(s, value)
}

// EOF

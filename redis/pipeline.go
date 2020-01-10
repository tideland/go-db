// Tideland Go Database Clients - Redis Client
//
// Copyright (C) 2017-2020 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis // import "tideland.dev/go/db/redis"

//--------------------
// IMPORTS
//--------------------

import (
	"strings"

	"tideland.dev/go/trace/failure"
)

//--------------------
// CONNECTION
//--------------------

// Pipeline manages a Redis connection executing
// pipelined commands.
type Pipeline struct {
	database *Database
	resp     *resp
	counter  int
}

// newPipeline creates a new pipeline instance.
func newPipeline(db *Database) (*Pipeline, error) {
	ppl := &Pipeline{
		database: db,
	}
	err := ppl.ensureProtocol()
	if err != nil {
		return nil, err
	}
	// Perform authentication and database selection.
	if err != nil {
		return nil, err
	}
	err = ppl.resp.authenticate()
	if err != nil {
		ppl.database.pool.kill(ppl.resp)
		return nil, err
	}
	err = ppl.resp.selectDatabase()
	if err != nil {
		ppl.database.pool.kill(ppl.resp)
		return nil, err
	}
	return ppl, nil
}

// Do executes one Redis command and returns
// the result as result set.
func (ppl *Pipeline) Do(cmd string, args ...interface{}) error {
	cmd = strings.ToLower(cmd)
	if strings.Contains(cmd, "subscribe") {
		return failure.New("use subscription type for subscriptions")
	}
	err := ppl.ensureProtocol()
	if err != nil {
		return err
	}
	err = ppl.resp.sendCommand(cmd, args...)
	logCommand(cmd, args, err, ppl.database.logging)
	if err != nil {
		return err
	}
	ppl.counter++
	return err
}

// Collect collects all the result sets of the commands and returns
// the connection back into the pool.
func (ppl *Pipeline) Collect() ([]*ResultSet, error) {
	defer func() {
		ppl.resp = nil
	}()
	err := ppl.ensureProtocol()
	if err != nil {
		return nil, err
	}
	results := []*ResultSet{}
	for i := ppl.counter; i > 0; i-- {
		result, err := ppl.resp.receiveResultSet()
		if err != nil {
			ppl.database.pool.kill(ppl.resp)
			return nil, err
		}
		results = append(results, result)
	}
	ppl.database.pool.push(ppl.resp)
	return results, nil
}

// ensureProtocol retrieves a protocol from the pool if needed.
func (ppl *Pipeline) ensureProtocol() error {
	if ppl.resp == nil {
		p, err := ppl.database.pool.pullForced()
		if err != nil {
			return err
		}
		ppl.resp = p
		ppl.counter = 0
	}
	return nil
}

// EOF

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
	"sync"
	"time"

	"tideland.dev/go/together/wait"
	"tideland.dev/go/trace/failure"
)

//--------------------
// CONNECTION POOL
//--------------------

// pool manages a number of Redis resp instances.
type pool struct {
	mu        sync.Mutex
	database  *Database
	active    bool
	available map[*resp]*resp
	inUse     map[*resp]*resp
}

// newPool creates a connection pool with uninitialized
// protocol instances.
func newPool(db *Database) *pool {
	p := &pool{
		database:  db,
		active:    true,
		available: make(map[*resp]*resp),
		inUse:     make(map[*resp]*resp),
	}
	return p
}

// close deactivates the pool and closes the available connections.
// Those in use will be closed when returned.
func (p *pool) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.active {
		return failure.New("connection pool closed")
	}
	p.active = false
	var err error
	for resp := range p.available {
		cerr := resp.close()
		if err == nil {
			err = cerr
		}
	}
	p.available = nil
	return err
}

// pullForced retrieves a new created protocol.
func (p *pool) pullForced() (*resp, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.active {
		return nil, failure.New("connection pool closed")
	}
	resp, err := newResp(p.database)
	if err != nil {
		return nil, err
	}
	p.inUse[resp] = resp
	return resp, nil
}

// pullRetry retrieves a protocol out of the pool. It tries to
// do it multiple times.
func (p *pool) pullRetry() (*resp, error) {
	var r *resp
	var err error
	if werr := wait.WithTimeout(
		p.database.ctx,
		5*time.Millisecond,
		5*time.Second,
		func() (bool, error) {
			r, err = p.pull()
			if r != nil {
				return true, nil
			}
			return false, nil
		},
	); werr != nil {
		return nil, werr
	}
	return r, err
}

// pull retrieves a protocol out of the pool.
func (p *pool) pull() (*resp, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.active {
		return nil, failure.New("connection pool closed")
	}
	switch {
	case len(p.available) > 0:
		// Pulls first one from availables.
		for resp := range p.available {
			delete(p.available, resp)
			p.inUse[resp] = resp
			return resp, nil
		}
	case len(p.inUse) < p.database.poolsize:
		// Lazily open a new one.
		resp, err := newResp(p.database)
		if err != nil {
			return nil, err
		}
		p.inUse[resp] = resp
		return resp, nil
	}
	return nil, failure.New("connection pool limit (%d) reached", p.database.poolsize)
}

// push returns a protocol back into the pool.
func (p *pool) push(resp *resp) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.inUse, resp)
	if !p.active || len(p.available) >= p.database.poolsize {
		// Simply close it.
		return resp.close()
	}
	// Return to availanle ones.
	p.available[resp] = resp
	return nil
}

// kill closes the connection and removes it from the pool.
func (p *pool) kill(resp *resp) (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.inUse, resp)
	return resp.close()
}

// EOF

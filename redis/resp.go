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
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"

	"tideland.dev/go/trace/failure"
)

//--------------------
// respONSE
//--------------------

// responseKind classifies a response of Redis.
type responseKind int

const (
	receivingError responseKind = iota
	timeoutError
	statusResponse
	errorResponse
	integerResponse
	bulkResponse
	nullBulkResponse
	arrayResponse
)

var responseKindDescr = map[responseKind]string{
	receivingError:   "receiving error",
	timeoutError:     "timeout error",
	statusResponse:   "status",
	errorResponse:    "error",
	integerResponse:  "integer",
	bulkResponse:     "bulk",
	nullBulkResponse: "null-bulk",
	arrayResponse:    "array",
}

// response contains one Redis response.
type response struct {
	kind   responseKind
	length int
	data   []byte
	err    error
}

// value returns the data as value.
func (r *response) value() Value {
	return Value(r.data)
}

// String creates a string representation of the response.
func (r *response) String() string {
	descr := responseKindDescr[r.kind]
	return fmt.Sprintf("RESPONSE (Kind: %s / Length: %d / Value: %v / Error: %v)", descr, r.length, r.value(), r.err)
}

//--------------------
// REDIS SERIALIZATION PROTOCOL
//--------------------

// resp implements the Redis Serialization Protocol.
type resp struct {
	database *Database
	conn     net.Conn
	reader   *bufio.Reader
	cmd      string
}

// newResp establishes a connection to a Redis database
// based on the configuration of the passed database
// configuration.
func newResp(db *Database) (*resp, error) {
	// Dial the database and create the protocol instance.
	conn, err := net.DialTimeout(db.network, db.address, db.timeout)
	if err != nil {
		return nil, failure.Annotate(err, "cannot establish new connection")
	}
	r := &resp{
		database: db,
		conn:     conn,
		reader:   bufio.NewReader(conn),
	}
	return r, nil
}

// sendCommand sends a command and possible arguments to the server.
func (r *resp) sendCommand(cmd string, args ...interface{}) error {
	r.cmd = cmd
	lengthPart := r.buildLengthPart(args)
	cmdPart := r.buildValuePart(cmd)
	argsPart := r.buildArgumentsPart(args)

	packet := join(lengthPart, cmdPart, argsPart)
	_, err := r.conn.Write(packet)
	if err != nil {
		return failure.Annotate(err, "cannot send %s, connection is broken", r.cmd)
	}
	return nil
}

// receiveResponse retrieves a response from the server.
func (r *resp) receiveResponse() *response {
	// Receive first line.
	line, err := r.reader.ReadBytes('\n')
	if err != nil {
		rerr := failure.Annotate(err, "cannot receive after %s, connection is broken", r.cmd)
		return &response{receivingError, 0, nil, rerr}
	}
	content := line[1 : len(line)-2]
	// First byte defines kind.
	switch line[0] {
	case '+':
		// Status response.
		return &response{statusResponse, 0, line[:len(line)-2], nil}
	case '-':
		// Error response.
		return &response{errorResponse, 0, line[:len(line)-2], nil}
	case ':':
		// Integer response.
		return &response{integerResponse, 0, content, nil}
	case '$':
		// Bulk response or null bulk response.
		count, err := strconv.Atoi(string(content))
		if err != nil {
			return &response{receivingError, 0, nil, failure.Annotate(err, "server responded error")}
		}
		if count == -1 {
			// Null bulk response.
			return &response{nullBulkResponse, 0, nil, nil}
		}
		// Receive the bulk data.
		toRead := count + 2
		buffer := make([]byte, toRead)
		n, err := io.ReadFull(r.reader, buffer)
		if err != nil {
			return &response{receivingError, 0, nil, err}
		}
		if n < toRead {
			return &response{receivingError, 0, nil, failure.New("server responded error")}
		}
		return &response{bulkResponse, 0, buffer[0:count], nil}
	case '*':
		// Array reply. Check for timeout.
		length, err := strconv.Atoi(string(content))
		if err != nil {
			return &response{receivingError, 0, nil, failure.Annotate(err, "server responded error")}
		}
		if length == -1 {
			// Timeout.
			return &response{timeoutError, 0, nil, nil}
		}
		return &response{arrayResponse, length, nil, nil}
	}
	return &response{receivingError, 0, nil, failure.New("invalid server response: %q", string(line))}
}

// receiveResultSet receives all responses and converts them into a result set.
func (r *resp) receiveResultSet() (*ResultSet, error) {
	defer func() { r.cmd = "-none-" }()
	result := newResultSet()
	current := result
	for {
		response := r.receiveResponse()
		switch response.kind {
		case receivingError:
			return nil, response.err
		case timeoutError:
			return nil, failure.New("timeout waiting for response")
		case statusResponse, errorResponse, integerResponse, bulkResponse, nullBulkResponse:
			current.append(response.value())
		case arrayResponse:
			switch {
			case current == result && current.Len() == 0:
				current.length = response.length
			case !current.allReceived():
				next := newResultSet()
				next.parent = current
				current.append(next)
				current = next
				current.length = response.length
			}
		}
		// Check if all values are received.
		current = current.nextResultSet()
		if current == nil {
			return result, nil
		}
	}
}

// buildLengthPart creates the length part of a command.
func (r *resp) buildLengthPart(args []interface{}) []byte {
	length := 1
	for _, arg := range args {
		switch typedArg := arg.(type) {
		case valuer:
			length += typedArg.Len()
		case Hash:
			length += typedArg.Len() * 2
		case Hashable:
			length += typedArg.Len() * 2
		default:
			length++
		}
	}
	return join("*", length, "\r\n")
}

// buildValuePart creates one value part of a command.
func (r *resp) buildValuePart(value interface{}) []byte {
	var raw []byte
	if v, ok := value.(Value); ok {
		raw = []byte(v)
	} else {
		raw = valueToBytes(value)
	}
	return join("$", len(raw), "\r\n", raw, "\r\n")
}

// buildArgumentsPart creates the the arguments parts of a command.
func (r *resp) buildArgumentsPart(args []interface{}) []byte {
	buildValuesPart := func(vs valuer) []byte {
		tmp := []byte{}
		for _, value := range vs.Values() {
			tmp = append(tmp, r.buildValuePart(value)...)
		}
		return tmp
	}
	buildHashPart := func(h Hash) []byte {
		tmp := []byte{}
		for key, value := range h {
			tmp = append(tmp, r.buildValuePart(key)...)
			tmp = append(tmp, r.buildValuePart(value)...)
		}
		return tmp
	}
	tmp := []byte{}
	for _, arg := range args {
		var part []byte
		switch typedArg := arg.(type) {
		case valuer:
			part = buildValuesPart(typedArg)
		case Hash:
			part = buildHashPart(typedArg)
		case Hashable:
			part = buildHashPart(typedArg.GetHash())
		default:
			part = r.buildValuePart(arg)
		}
		tmp = append(tmp, part...)
	}
	return tmp
}

// authenticate authenticates against the server if configured.
func (r *resp) authenticate() error {
	if r.database.password != "" {
		err := r.sendCommand("auth", r.database.password)
		if err != nil {
			return failure.Annotate(err, "cannot authenticate")
		}
		result, err := r.receiveResultSet()
		if err != nil {
			return failure.Annotate(err, "cannot authenticate")
		}
		value, err := result.ValueAt(0)
		if err != nil {
			return failure.Annotate(err, "cannot authenticate")
		}
		if !value.IsOK() {
			return failure.New("cannot authenticate")
		}
	}
	return nil
}

// selectDatabase selects the database.
func (r *resp) selectDatabase() error {
	err := r.sendCommand("select", r.database.index)
	if err != nil {
		return failure.Annotate(err, "cannot select database")
	}
	result, err := r.receiveResultSet()
	if err != nil {
		return failure.Annotate(err, "cannot select database")
	}
	value, err := result.ValueAt(0)
	if err != nil {
		return failure.Annotate(err, "cannot select database")
	}
	if !value.IsOK() {
		return failure.New("cannot select database")
	}
	return nil
}

// close ends the connection to Redis.
func (r *resp) close() error {
	return r.conn.Close()
}

// EOF

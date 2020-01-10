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
	"fmt"
	"strconv"
	"strings"

	"tideland.dev/go/trace/failure"
	"tideland.dev/go/trace/logger"
)

//--------------------
// TOOLS
//--------------------

// valuer describes any type able to return a list of values.
type valuer interface {
	Len() int
	Values() []Value
}

// join builds a byte slice out of some parts.
func join(parts ...interface{}) []byte {
	tmp := []byte{}
	for _, part := range parts {
		switch typedPart := part.(type) {
		case []byte:
			tmp = append(tmp, typedPart...)
		case string:
			tmp = append(tmp, []byte(typedPart)...)
		case int:
			tmp = append(tmp, []byte(strconv.Itoa(typedPart))...)
		default:
			tmp = append(tmp, []byte(fmt.Sprintf("%v", typedPart))...)
		}
	}
	return tmp
}

// valueToBytes converts a value into a byte slice.
func valueToBytes(value interface{}) []byte {
	switch typedValue := value.(type) {
	case string:
		return []byte(typedValue)
	case []byte:
		return typedValue
	case []string:
		return []byte(strings.Join(typedValue, "\r\n"))
	case map[string]string:
		tmp := make([]string, len(typedValue))
		i := 0
		for k, v := range typedValue {
			tmp[i] = fmt.Sprintf("%v:%v", k, v)
			i++
		}
		return []byte(strings.Join(tmp, "\r\n"))
	case Hash:
		tmp := []byte{}
		for k, v := range typedValue {
			kb := valueToBytes(k)
			vb := valueToBytes(v)
			tmp = append(tmp, kb...)
			tmp = append(tmp, vb...)
		}
		return tmp
	}
	return []byte(fmt.Sprintf("%v", value))
}

// containsPatterns checks, if the channel contains a pattern
// to subscribe to or unsubscribe from multiple channels.
func containsPattern(channel interface{}) bool {
	ch := channel.(string)
	return strings.ContainsAny(ch, "*?[")
}

// logCommand logs a command and its execution status.
func logCommand(cmd string, args []interface{}, err error, log bool) {
	// Format the command for the log entry.
	formatArgs := func() string {
		if len(args) == 0 {
			return "(none)"
		}
		output := make([]string, len(args))
		for i, arg := range args {
			output[i] = string(valueToBytes(arg))
		}
		return strings.Join(output, " / ")
	}
	logOutput := func() string {
		format := "CMD %s ARGS %s %s"
		if err == nil {
			return fmt.Sprintf(format, cmd, formatArgs(), "OK")
		}
		return fmt.Sprintf(format, cmd, formatArgs(), "ERROR "+err.Error())
	}
	// Log positive commands only if wanted, failure always.
	if err != nil {
		if failure.Contains(err, "server responded error") || failure.Contains(err, "timeout") {
			return
		}
		logger.Errorf(logOutput())
	} else if log {
		logger.Infof(logOutput())
	}
}

// EOF

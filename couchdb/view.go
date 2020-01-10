// Tideland Go Database Clients - CouchDB Client
//
// Copyright (C) 2016-2020 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package couchdb // import "tideland.dev/go/db/couchdb"

//--------------------
// VIEW
//--------------------

// ViewProcessor is a function processing the content of a view row.
type ViewProcessor func(id string, key, value, document *Unmarshable) error

// View provides access to the responded views.
type View struct {
	db   *Database
	view *couchdbView
}

// newView requests the view document and prepares the access type.
func newView(db *Database, designID, viewID string, params ...Parameter) (*View, error) {
	rs := db.Request().SetPath(db.name, "_design", designID, "_view", viewID).ApplyParameters(params...).GetOrPost()
	if !rs.IsOK() {
		return nil, rs.Error()
	}
	view := couchdbView{}
	err := rs.Document(&view)
	if err != nil {
		return nil, err
	}
	return &View{
		db:   db,
		view: &view,
	}, nil
}

// TotalRows returns the number of view rows.
func (v *View) TotalRows() int {
	return v.view.TotalRows
}

// ReturnedRows returns the nnumber of returned view rows.
func (v *View) ReturnedRows() int {
	return len(v.view.Rows)
}

// Offset returns the starting offset of the view rows.
func (v *View) Offset() int {
	return v.view.Offset
}

// Process iterates over the found view documents and processes them.
func (v *View) Process(process ViewProcessor) error {
	for _, row := range v.view.Rows {
		key := NewUnmarshableJSON(row.Key)
		value := NewUnmarshableJSON(row.Value)
		doc := NewUnmarshableJSON(row.Document)
		if err := process(row.ID, key, value, doc); err != nil {
			return err
		}
	}
	return nil
}

// EOF

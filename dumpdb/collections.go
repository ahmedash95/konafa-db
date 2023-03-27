package dumpdb

func ensureCollection(db *DB, name string) error {
	// check if the collection exists
	if _, ok := db.meta.collections[name]; ok {
		return nil
	}

	// create the collection
	err := db.createCollection(name)
	if err != nil {
		return err
	}

	return nil
}

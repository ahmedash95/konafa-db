package konafadb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	pageSize = 0x1000 // 4KB

	version = 0x01 // database version format

	magic = 0xDDBBFFEE // magic number
)

type DB struct {
	path     string
	file     *os.File
	mx       sync.Mutex
	pageSize uint32
	meta     *MetaPage // holds information about collections in the database
}

func New(path string) (*DB, error) {

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)

	if err != nil {
		return nil, err
	}

	db := &DB{
		path:     path,
		file:     file,
		pageSize: pageSize, // TODO: make this configurable and read from the meta page
	}

	if info, err := file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		// initialize a new database
		err := db.init()
		if err != nil {
			return nil, err
		}
	}

	// read the database
	err = db.read()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) init() error {
	db.file.Seek(0, io.SeekStart)

	// write the magic number
	err := writeUint32(db.file, magic)
	if err != nil {
		return err
	}

	// create the meta page
	meta := &MetaPage{
		_type:       metapage,
		version:     version,
		rows:        0,
		collections: make(map[string]pgid),
	}

	meta.rows = uint32(len(meta.collections))

	err = writeMetaPage(db.file, meta)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) read() error {
	db.file.Seek(0, io.SeekStart)

	// read the magic number
	var magicNumber uint32
	err := binary.Read(db.file, binary.BigEndian, &magicNumber)
	if err != nil {
		return err
	}

	if magicNumber != magic {
		return fmt.Errorf("invalid magic number")
	}

	meta, err := db.readMetaPage()
	if err != nil {
		return err
	}

	db.meta = meta

	fmt.Println("meta", meta)

	return nil
}

func (db *DB) Close() {
	err := db.file.Close()
	if err != nil {
		fmt.Println("db close error", err)
	}
}

func (db *DB) Insert(table string, data map[string]interface{}) bool {
	err := ensureCollection(db, table)
	if err != nil {
		fmt.Println("ensure collection error", err)
		return false
	}

	return true
}

func (db *DB) Count(table string) int {
	return 0
}

func (db *DB) readMetaPage() (*MetaPage, error) {
	// read the meta page
	var meta MetaPage

	binary.Read(db.file, binary.BigEndian, &meta._type)
	binary.Read(db.file, binary.BigEndian, &meta.version)
	binary.Read(db.file, binary.BigEndian, &meta.rows)

	// read tables
	meta.collections = make(map[string]pgid)

	for i := 0; i < int(meta.rows); i++ {
		var name string
		var pageId pgid

		name, err := readStringUntilNull(db.file)
		if err != nil {
			return nil, err
		}

		binary.Read(db.file, binary.BigEndian, &pageId)

		meta.collections[name] = pageId
	}

	return &meta, nil
}

func (db *DB) createCollection(name string) error {
	fmt.Println("creating collection", name)
	// create a new page
	page := &Page{
		_type: leafpage,
		id:    0,
		items: make(map[string]string),
	}

	page.id = pgid(db.meta.rows + 1)
	db.meta.collections[name] = page.id
	db.meta.rows++

	// rewrite the meta page
	db.file.Seek(4, io.SeekStart)
	writeMetaPage(db.file, db.meta)

	// write the page to disk
	err := writePage(db, page)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) seekPage(id pgid) error {
	// skip the magic number + meta page = 4 bytes + 100kb
	offset := int64(4 + 100)

	_, err := db.file.Seek(offset+(int64(id)*int64(db.pageSize)), io.SeekStart)
	return err
}

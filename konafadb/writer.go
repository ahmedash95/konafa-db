package konafadb

import (
	"encoding/binary"
	"io"
)

func writeUint32(w io.Writer, n uint32) error {
	var buf [4]byte

	binary.BigEndian.PutUint32(buf[:], n)

	_, err := w.Write(buf[:])

	return err
}

func writeMetaPage(w io.Writer, meta *MetaPage) error {
	var buf [100]byte // reserve single page bytes for the meta page

	// Write the page type and version
	buf[0] = meta._type
	buf[1] = meta.version

	binary.BigEndian.PutUint32(buf[2:], meta.rows)

	// Write the table names and page IDs
	offset := 6
	for name, pageId := range meta.collections {
		// Write the table name and null terminator
		copy(buf[offset:], []byte(name))
		offset += len(name)
		buf[offset] = 0x00
		offset++

		// Write the page ID
		binary.BigEndian.PutUint32(buf[offset:], uint32(pageId))
		offset += 4
	}

	// Write the buffer to the output writer
	_, err := w.Write(buf[:offset])
	return err
}

func writePage(db *DB, page *Page) error {
	db.seekPage(page.id)

	buf := make([]byte, db.pageSize)

	// Write the page type
	buf[0] = page._type

	// Write the page ID
	binary.BigEndian.PutUint32(buf[1:], uint32(page.id))

	// Write the number of rows
	binary.BigEndian.PutUint32(buf[5:], page.rows)

	// Write the items
	offset := 9
	for key, value := range page.items {
		// Write the key and null terminator
		copy(buf[offset:], []byte(key))
		offset += len(key)
		buf[offset] = 0x00
		offset++

		// Write the value and null terminator
		copy(buf[offset:], []byte(value))
		offset += len(value)
		buf[offset] = 0x00
		offset++
	}

	// Write the buffer to the output writer
	_, err := db.file.Write(buf[:offset])
	return err
}

func readStringUntilNull(r io.Reader) (string, error) {
	var buf [1]byte

	var str string

	for {
		_, err := r.Read(buf[:])
		if err != nil {
			return "", err
		}

		if buf[0] == 0x00 {
			break
		}

		str += string(buf[0])
	}

	return str, nil
}

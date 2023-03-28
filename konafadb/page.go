package konafadb

type pgid uint64

const (
	metapage uint8 = 0x001
	leafpage uint8 = 0x002
)

type Page struct {
	_type uint8
	id    pgid
	rows  uint32
	items map[string]string
}

type MetaPage struct {
	_type       uint8
	version     uint8
	rows        uint32
	collections map[string]pgid
}

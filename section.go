package godb

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
)

const (
	SECTION_SIZE = 0x100
	PAGE_SIZE    = SECTION_SIZE << 4
)

var ErrInvalidSection = errors.New("The format of the section was invalid.")
var ErrInvalidLocation = errors.New("Attempted to read from the middle of a section. This should not happen.")
var errReadNextSection = errors.New("internal: read next section")

func makeSection(data [SECTION_SIZE]byte) (*goDBSection, error) {
	if data[0] == 0 {
		return nil, ErrInvalidLocation
	}

	section := new(goDBSection)
	section.data = make(M)
	if data[0] == 1 {
		buf := bytes.NewBuffer(data[1:])
		decoder := gob.NewDecoder(buf)
		err := decoder.Decode(&section.data)
		return section, err
	}
	section.buf = make([]byte, 0, SECTION_SIZE)
	copy(section.buf, data[1:])
	return section, errReadNextSection
}

func appendSection(section *goDBSection, data [SECTION_SIZE]byte) (*goDBSection, error) {
	section.buf = append(section.buf, data[:]...)

	if byte(len(section.buf)/SECTION_SIZE+1) == section.buf[0] {
		buf := bytes.NewBuffer(section.buf[:])
		decoder := gob.NewDecoder(buf)
		decoder.Decode(&section.data)
		section.buf = nil
		return section, nil
	}
	return section, errReadNextSection
}

func writeSection(w io.Writer, data M) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	encoder.Encode(data)

	numSections := byte((buffer.Len() + SECTION_SIZE - 2) / (SECTION_SIZE - 1))
	var section [SECTION_SIZE]byte

	section[0] = numSections
	n, err := buffer.Read(section[1:])
	if err != nil {
		return err
	}
	for i := n + 1; i < SECTION_SIZE; i++ {
		section[i] = 0
	}
	_, err = w.Write(section[:])
	if err != nil {
		return err
	}

	section[0] = 0
	for sec := byte(1); sec < numSections; sec++ {
		n, err = buffer.Read(section[1:])
		if err != nil {
			return err
		}
		for i := n + 1; i < SECTION_SIZE; i++ {
			section[i] = 0
		}
		_, err = w.Write(section[:])
		if err != nil {
			return err
		}
	}
	return nil
}

type goDBSection struct {
	buf  []byte
	data M
}

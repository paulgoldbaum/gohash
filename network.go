package gohash

import (
	"net"
	"log"
	"github.com/golang/protobuf/proto"
	"encoding/binary"
	"errors"
)

func Send(conn *net.TCPConn, request proto.Message) error {
	payload, err := proto.Marshal(request)
	if err != nil {
		log.Fatal("Error occurred during marshalling", err)
		return err
	}
	sizeBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuffer, uint32(len(payload)))
	conn.Write(sizeBuffer)
	conn.Write(payload)

	return nil
}

func Receive(conn *net.TCPConn, message proto.Message) error {

	sizeBuffer := make([]byte, 4)
	conn.Read(sizeBuffer)

	size := binary.BigEndian.Uint32(sizeBuffer)
	if size > 4096 {
		err := errors.New("Specified packet size is too large")
		log.Fatal(err)
		return err
	}

	buffer := make([]byte, size)
	conn.Read(buffer)

	err := proto.Unmarshal(buffer, message)
	if err != nil {
		log.Fatal("Error during unmarshaling", err)
		return err
	}

	return nil
}

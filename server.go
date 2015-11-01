package gohash

import (
	"net"
	"log"
	"encoding/binary"
	"github.com/golang/protobuf/proto"
)

func Init() {
	log.Println("Listening on 0.0.0.0:7777")
	address, _ := net.ResolveTCPAddr("tcp4", "0.0.0.0:7777")
	listener, _ := net.ListenTCP("tcp", address)

	for {
		connection, _ := listener.Accept()
		log.Println("Accepting connection from", connection.RemoteAddr().String())
		go handleConnection(connection)
	}
}

func handleConnection(connection net.Conn) {

	for {
		sizeBuffer := make([]byte, 4)
		connection.Read(sizeBuffer)

		size := binary.BigEndian.Uint32(sizeBuffer)
		if size > 4096 {
			log.Fatal("Specified packet size is too large")
			return
		}

		buffer := make([]byte, size)
		connection.Read(buffer)
		request := &Request{}
		err := proto.Unmarshal(buffer, request)
		if err != nil {
			log.Fatal("Error during unmarshaling", err)
			return
		}

		log.Println(request.GetType(), request.GetKey(), request.GetValue())
	}
}

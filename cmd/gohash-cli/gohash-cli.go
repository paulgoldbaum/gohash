package main

import (
	"errors"
	"net"
	"os"
	"log"
	"encoding/binary"
	"github.com/abiosoft/ishell"
	"github.com/paulgoldbaum/gohash"
	"github.com/golang/protobuf/proto"
)

func registerCommands(shell *ishell.Shell, conn *net.TCPConn) {
	shell.Register("set", func(args... string) (string, error) {
		if len(args) < 2 {
			return "", errors.New("set <key> <value>")
		}

		requestType := gohash.Request_SET
		request := &gohash.Request{
			Type: &requestType,
			Key: &args[0],
			Value: &args[1],
		}

		send(conn, request)
		return request.GetKey(), nil
	})
}

func send(conn *net.TCPConn, request proto.Message) {
	payload, err := proto.Marshal(request)
	if err != nil {
		log.Fatal("Error occurred during marshalling", err)
		os.Exit(2)
	}
	sizeBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuffer, uint32(len(payload)))
	conn.Write(sizeBuffer)
	conn.Write(payload)
}

func connect() (*net.TCPConn) {
	address, _ := net.ResolveTCPAddr("tcp4", "0.0.0.0:7777")
	conn, err := net.DialTCP("tcp", nil, address)
	if err != nil {
		log.Fatal("Error occurred during connection", err)
		os.Exit(1)
	}
	return conn
}

func main() {
	shell := ishell.NewShell()
	connection := connect()
	shell.Println("Connected")
	registerCommands(shell, connection)
	shell.Start()
}

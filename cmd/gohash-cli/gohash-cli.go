package main

import (
	"errors"
	"net"
	"os"
	"log"
	"github.com/abiosoft/ishell"
	"github.com/paulgoldbaum/gohash"
)

func main() {
	shell := ishell.NewShell()
	connection := connect()
	shell.Println("Connected")
	registerCommands(shell, connection)
	shell.Start()
}

func rpc(conn *net.TCPConn, request *gohash.Request) string {
	gohash.Send(conn, request)

	response := &gohash.Response{}
	gohash.Receive(conn, response)
	return response.GetValue()
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
		return rpc(conn, request), nil
	})
	shell.Register("get", func(args... string) (string, error) {
		if len(args) < 1 {
			return "", errors.New("get <key>")
		}

		requestType := gohash.Request_GET
		request := &gohash.Request{
			Type: &requestType,
			Key: &args[0],
		}
		return rpc(conn, request), nil
	})
	shell.Register("delete", func(args... string) (string, error) {
		if len(args) < 1 {
			return "", errors.New("delete <key>")
		}

		requestType := gohash.Request_DELETE
		request := &gohash.Request{
			Type: &requestType,
			Key: &args[0],
		}
		return rpc(conn, request), nil
	})
}

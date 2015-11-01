package gohash

import (
	"net"
	"log"
)

var m = make(map[string]*string)

func Init() {
	log.Println("Listening on 0.0.0.0:7777")
	address, _ := net.ResolveTCPAddr("tcp4", "0.0.0.0:7777")
	listener, _ := net.ListenTCP("tcp", address)

	for {
		conn, _ := listener.AcceptTCP()
		log.Println("Accepting connection from", conn.RemoteAddr().String())
		go handleConnection(conn)
	}
}

func handleConnection(conn *net.TCPConn) {
	defer conn.Close()

	for {
		request := &Request{}
		Receive(conn, request)
		handleMessage(conn, request)
	}
}

func handleMessage(conn *net.TCPConn, request *Request) {

	var result *string
	switch request.GetType() {
	case Request_SET:
		result = handleSet(request.Key, request.Value)
	case Request_GET:
		result = handleGet(request.Key)
	case Request_DELETE:
		result = handleDelete(request.Key)
	}

	sendResponse(conn, result)
}

func sendResponse(conn *net.TCPConn, data *string) {
	response := &Response{
		Value: data,
	}
	Send(conn, response)
}

func handleSet(key, value *string) *string {
	copyOfValue := string(*value)
	m[*key] = &copyOfValue
	return value
}

func handleGet(key *string) *string {
	return m[*key]
}

func handleDelete(key *string) *string {
	value := m[*key]
	delete(m, *key)
	return value
}

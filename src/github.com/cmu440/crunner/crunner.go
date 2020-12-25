package main

import (
	"fmt"
  "net"
  "strconv"
  "bufio"
)

const (
	defaultHost = "localhost"
	defaultPort = 9999
)

// To test your server implementation, you might find it helpful to implement a
// simple 'client runner' program. The program could be very simple, as long as
// it is able to connect with and send messages to your server and is able to
// read and print out the server's echoed response to standard output. Whether or
// not you add any code to this file will not affect your grade.
func main() {
  conn, err := net.Dial("tcp", defaultHost + ":" + strconv.Itoa(defaultPort))
  if err != nil {
    fmt.Printf("Dial error\n")
    return
  }

  reader := bufio.NewReader(conn)
  testPut(conn, reader, "foo", "bar")
  testGet(conn, reader, "foo")

  testPut(conn, reader, "foo", "sun")
  testGet(conn, reader, "foo")

  testPut(conn, reader, "foo", "sweet")
  testGet(conn, reader, "foo")

  testDelete(conn, reader, "foo")
  testGet(conn, reader, "foo")

  conn.Close()
}

func testPut(conn net.Conn, reader *bufio.Reader, key string, value string) {
  fmt.Fprintf(conn, "Put:%s:%s\n", key, value)
}

func testGet(conn net.Conn, reader *bufio.Reader, key string) {
  fmt.Fprintf(conn, "Get:%s\n", key)
  // TODO: this is wrong, it does not read all messages.
  bytes, _ := reader.ReadBytes('\n')
  fmt.Printf("resp: %s", string(bytes))
}

func testDelete(conn net.Conn, reader *bufio.Reader, key string) {
  fmt.Fprintf(conn, "Delete:%s\n", key)
}

package main

import (
	"os"
	"bufio"
	"io"
	"log"
	"net"
	"fmt"
	"flag"
)

func reader(finished chan bool, r io.Reader) {
	buf := make([]byte, 1024)
	n, err := r.Read(buf[:])
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Client received: ", string(buf[0:n]))
	finished <- true
}

func main() {
	socket_file := flag.String("socket-file", "/var/run/fuse-server.sock", "string")
	flag.Parse()

	c, err := net.Dial("unix", *socket_file)
	if err != nil {
		log.Fatal("Dial error", err)
	}
	defer c.Close()

	reader := bufio.NewReader(os.Stdin)
	for {
		text, _ := reader.ReadString('\n')
		c.Write([]byte(text))
		buf := make([]byte, 50)
		n, err := c.Read(buf[:])
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("Got %v from ID server\n", string(buf[:n]))
	}
}

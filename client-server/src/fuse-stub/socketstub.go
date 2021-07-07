package main

import (
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
	socket_file := flag.String("socket-file", "/var/run/fuse-client.sock", "string")
	sending_add := flag.String("send-address", "127.0.0.1:9999, 127.127.127.2:9999, 130.245.126.199:9999", "string")
	file_name := flag.String("file-name", "test6.img", "string")
	flag.Parse()
	c, err := net.Dial("unix", *socket_file)
	if err != nil {
		log.Fatal("Dial error", err)
	}
	defer c.Close()
	finished := make(chan bool)
	go reader(finished, c)
	msg := *file_name+","+*sending_add+"\n"
	_, err = c.Write([]byte(msg))
	if err != nil {
		log.Fatal("Write error:", err)
		return
	}
	fmt.Println("Client sent: ", msg)
	<- finished
}

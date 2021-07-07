package main

import (
	"flag"
	"bufio"
	"net"
	"os"
	"path"
	"io"
	"strconv"
	"log"
)

const(
	connType = "tcp"
	BUFFERSIZE = 4096
)

/*--
----we will be needing this function while sending filename
----and file size information back to client
*/
func fillString(retunString string, toLength int) string {
	for {
		lengtString := len(retunString)
		if lengtString < toLength {
			retunString = retunString + ":"
			continue
		}
		break
	}
	return retunString
}

func handleConnection(conn net.Conn, temp_dir string){
	buffer, err := bufio.NewReader(conn).ReadBytes('\n')
	
	if err != nil {
		log.Println("Client left.")
		conn.Close()
		return
	}
	message := string(buffer[:len(buffer)-1])
	log.Println("Received: ", message)
	
	fname := path.Join(temp_dir, message)
	log.Println("Got download request for: " + fname)
	
	file, err := os.Open(fname)
	var msg string
	if err != nil {
		msg = "NACK"
		log.Println(err)
		conn.Write([]byte(msg))
		return
	}
	fileInfo, err := file.Stat()
	if err != nil {
		msg = "NACK"
		log.Println(err)
		conn.Write([]byte(msg))
		return
	}
	//send ack that file exists in the server
	msg = fillString("ACK",4)
	conn.Write([]byte(msg))
	
	/*-- sending filename and filesize to client*/
	fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 20)
	//fileName := fillString(fileInfo.Name(), 64)
	log.Println("Sending filesize! "+fileSize)
	
	conn.Write([]byte(fileSize))
	//connection.Write([]byte(fileName))
	
	sendBuffer := make([]byte, BUFFERSIZE)
	for {
		_, err = file.Read(sendBuffer)
		if err == io.EOF {
			break
		}
		conn.Write(sendBuffer)
	}
	log.Println("File has been sent!")
}

func run_remote_server(listen_address string, listen_port string, temp_dir string){
	l, err := net.Listen(connType, listen_address+":"+listen_port);
	if err != nil{
		log.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer l.Close()
	
	for{
		conn, err := l.Accept()
		if err != nil{
			log.Println("Error Connecting:", err.Error())
			return
		}
		log.Println("Got connection from "+ conn.RemoteAddr().String())
		go handleConnection(conn, temp_dir)
	}
}

func main(){
	listen_add := flag.String("listen-address", "0.0.0.0", "string")
	listen_port := flag.String("listen-port", "9999", "string")
	temp_dir := flag.String("temp-dir", "/mnt/local-cache/server_tempdir","string")
	flag.Parse()
	
	log.Println("listen_add:"+ *listen_add)
	log.Println("listen_port:"+ *listen_port)
	log.Println("temp_dir:"+ *temp_dir)
	
	run_remote_server(*listen_add, *listen_port, *temp_dir)
}




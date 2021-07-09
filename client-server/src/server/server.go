package main

import (
	"flag"
	"bufio"
	"net"
	"os"
	"path"
	"io"
	"strconv"
	log "github.com/sirupsen/logrus"
)

const(
	connType = "tcp"
	BUFFERSIZE = 4096
)

func main(){
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.TraceLevel)
	listen_add := flag.String("listen-address", "0.0.0.0", "string")
	listen_port := flag.String("listen-port", "9999", "string")
	temp_dir := flag.String("temp-dir", "./tempdir","string")
	flag.Parse()

	log.WithFields(log.Fields{"thread": "server.main",}).Trace("listen_add: "+ *listen_add)
	log.WithFields(log.Fields{"thread": "server.main",}).Trace("listen_port: "+ *listen_port)
	log.WithFields(log.Fields{"thread": "server.main",}).Trace("temp_dir: "+ *temp_dir)

	run_remote_server(*listen_add, *listen_port, *temp_dir)
}

//listens at listen_address and listen_port, accepts and handles client connection
func run_remote_server(listen_address string, listen_port string, temp_dir string){
	l, err := net.Listen(connType, listen_address+":"+listen_port);
	if err != nil{
		log.WithFields(log.Fields{"thread": "server.main",}).Fatal("Error listening: ", err.Error())
		os.Exit(1)
	}
	defer l.Close()

	for{
		conn, err := l.Accept()
		if err != nil{
			log.WithFields(log.Fields{"thread": "server.main",}).Error("Error accepting connection: ", err.Error())
			continue
		}
		log.WithFields(log.Fields{"thread": "server.main",}).Trace("Got connection from "+ conn.RemoteAddr().String())
		go handleConnection(conn, temp_dir)
	}
}

//receives client's file request, checks if it is present and sends resp. ack to the client
//if file is present, firstly sends the file size and then the file
func handleConnection(conn net.Conn, temp_dir string){
	buffer, err := bufio.NewReader(conn).ReadBytes('\n')
	clientAddress := conn.RemoteAddr().String()
	if err != nil {
		log.WithFields(log.Fields{"thread": "server.handleConnection","clientAddress":clientAddress,}).Trace("Client left.")
		conn.Close()
		return
	}
	message := string(buffer[:len(buffer)-1])
	log.WithFields(log.Fields{"thread": "server.handleConnection","clientAddress":clientAddress,}).Trace("Received: ", message)
	
	fname := path.Join(temp_dir, message)
	log.WithFields(log.Fields{"thread": "server.handleConnection","filename": fname,"clientAddress":clientAddress,}).Info("Got download request for file: " + fname)
	file, err := os.Open(fname)
	var msg string
	if err != nil {
		msg = "NACK"
		log.WithFields(log.Fields{"thread": "server.handleConnection","filename": fname, "clientAddress": clientAddress,}).Info(err)
		conn.Write([]byte(msg))
		return
	}
	fileInfo, err := file.Stat()
	if err != nil {
		msg = "NACK"
		log.WithFields(log.Fields{"thread": "server.handleConnection","filename": fname, "clientAddress": clientAddress,}).Info(err)
		conn.Write([]byte(msg))
		return
	}
	msg = fillString("ACK",4)
	conn.Write([]byte(msg))
	
	fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 40)
	log.WithFields(log.Fields{"thread": "server.handleConnection","filename": fname, "fileSize": fileInfo.Size(), "clientAddress": clientAddress,}).Info("Sending filesize: "+fileSize)
	
	conn.Write([]byte(fileSize))
	
	sendBuffer := make([]byte, BUFFERSIZE)
	for {
		_, err = file.Read(sendBuffer)
		if err == io.EOF {
			break
		}
		conn.Write(sendBuffer)
	}
	log.WithFields(log.Fields{"thread": "server.handleConnection","filename": fname, "fileSize": fileInfo.Size(), "clientAddress": clientAddress,}).Info("File has been send.")
}

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

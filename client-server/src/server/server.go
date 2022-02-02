package main

import (
    "strings"
    "time"
	"flag"
	"bufio"
	"encoding/binary"
	"net"
	"os"
	"path"
	"io"
	log "github.com/sirupsen/logrus"
	"fmt"
    "sync"
)

const(
	connType = "tcp"
)

var(
    writeComplete = make(map[string]bool)
    writeCompleteLock sync.RWMutex
)

func main(){
    fmt.Printf("HELLO")
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.TraceLevel)
	listen_add := flag.String("listen-address", "0.0.0.0", "string")
	listen_port := flag.String("listen-port", "9999", "string")
	temp_dir := flag.String("temp-dir", "./tempdir","string")
    socket_file := flag.String("socket-file", "/f3/fuse-server.sock", "string")
	flag.Parse()

	log.WithFields(log.Fields{"thread": "server.main",}).Trace("HELLO")

	log.WithFields(log.Fields{"thread": "server.main",}).Trace("listen_add: "+ *listen_add)
	log.WithFields(log.Fields{"thread": "server.main",}).Trace("listen_port: "+ *listen_port)
	log.WithFields(log.Fields{"thread": "server.main",}).Trace("temp_dir: "+ *temp_dir)

    go run_local_server(*socket_file)
	run_remote_server(*listen_add, *listen_port, *temp_dir)
}

//This function establishes connection with the FUSE driver using a socket file
//Waits for the input (in the form of filename, server1:port1, server2:port2...) from the FUSE driver.
func run_local_server(socket_file string) {
    log.WithFields(log.Fields{"thread": "client.main",}).Trace("Creating socket file: " + socket_file)
    if err := os.RemoveAll(socket_file); err != nil {
        log.WithFields(log.Fields{"thread": "client.main",}).Fatal("Error while creating the socket file: ",err)
        os.Exit(1)
    }

    log.WithFields(log.Fields{"thread": "client.main",}).Trace("Establishing connection using socket file: " + socket_file)
    l, err := net.Listen("unix", socket_file)
    if err != nil {
        log.WithFields(log.Fields{"thread": "client.main",}).Fatal("Listen error: ", err)
        os.Exit(1)
    }
    defer l.Close()

    for {
        log.WithFields(log.Fields{"thread": "client.main",}).Trace("Waiting for connection.")
        conn, err := l.Accept()
        if err != nil {
            log.WithFields(log.Fields{"thread": "client.main",}).Error("Error accepting connection: ", err.Error())
            continue
        }

        go fuseConnectionHandler(conn)
    }
}

func fuseConnectionHandler(fuseConn net.Conn) {
    for {
        buffer, err := bufio.NewReader(fuseConn).ReadBytes('\n')
        //start := time.Now()
        if err != nil {
            log.WithFields(log.Fields{"thread": "client.main",}).Trace("Client left.")
            fuseConn.Close()
            return
        }
        message := string(buffer[:len(buffer)-1])
        fmt.Printf("Got message %v\n", message)
        if _, exists := writeComplete[message]; !exists {
            fmt.Printf("Adding %v to map\n%v\n", message, []byte(message))
            writeCompleteLock.Lock()
            writeComplete[message] = true
            writeCompleteLock.Unlock()
        }
    }
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

func handleMove(oldpath, newpath, temp_dir string) error {
    full_old_path := path.Join(temp_dir, oldpath)
    full_new_path := path.Join(temp_dir, newpath)

    fmt.Println(full_old_path + " -> " + full_new_path)

    if err := os.Rename(full_old_path, full_new_path); err != nil {
        fmt.Printf("Error renaming: %v\n", err)
        return err
    }

    return nil
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

    if strings.HasPrefix(message, "move") {
        arr := strings.Split(message, ",")
        if err := handleMove(arr[1], arr[2], temp_dir); err != nil {
            binary.Write(conn, binary.LittleEndian, false)
        } else {
            binary.Write(conn, binary.LittleEndian, true)
        }
        conn.Close()
        return
    }

    message = strings.Split(message, ",")[1]
	
	fname := path.Join(temp_dir, message)
	log.WithFields(log.Fields{"thread": "server.handleConnection","filename": fname,"clientAddress":clientAddress,}).Info("Got download request for file: " + fname)
	file, err := os.Open(fname)
    defer file.Close()
	if err != nil {
		binary.Write(conn, binary.LittleEndian, false)
		log.WithFields(log.Fields{"thread": "server.handleConnection","filename": fname, "clientAddress": clientAddress,}).Info(err)
		return
	} else {
	    binary.Write(conn, binary.LittleEndian, true)
    }
	
    start := time.Now()
    fmt.Printf("About to wait for %v to be done\n", message)
    writedone := false
    for !writedone {
        //w, err := io.Copy(conn, file)
        buf := make([]byte, 64*1024*1024)
        w, err := io.CopyBuffer(conn, file, buf)
        if err != nil {
            fmt.Println(err.Error())
        }
        fmt.Println("w: ", w)
        /*
        fmt.Println("message:", []byte(message))
        for k, e := range writeComplete {
            fmt.Println(k, e)
            fmt.Println(k, []byte(k))
        }*/
        writeCompleteLock.RLock()
        _, writedone = writeComplete[message]
        //fmt.Printf("1 %v\n", writedone)
        // Try alternate form?
        if !writedone {
            _, writedone = writeComplete["/"+message]
        }
        //fmt.Printf("2 %v\n", writedone)
        writeCompleteLock.RUnlock()

        // XXX testing:
        //writedone = true
        // XXX

        if !writedone {
            fmt.Printf("Still waiting for %v to be done %v %v\n", message, writeComplete, writedone)
            time.Sleep(1*time.Second)
        }
    }

    w, err := io.Copy(conn, file)
    if err != nil {
        fmt.Println(err.Error())
    }
    fmt.Println("w: ", w)
	fmt.Println("Done sending file?")
    elapsed := time.Since(start).Seconds()
    fmt.Printf("Took %v seconds (started at %v now %v)\n", elapsed, start.Unix(), time.Now().Unix())

	conn.Close()

    // Should remove "message" from map now, so we can write to it again
    // XXX NO: otherwise subsuquent transfers from other clients will get stick in the loop.
    // Instead need uds client to tell us when file was opened for writing and we'll delete it then.
    /*
    writeCompleteLock.Lock()
    delete(writeComplete, message)
    delete(writeComplete, "/"+message)
    writeCompleteLock.Unlock()
    */
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

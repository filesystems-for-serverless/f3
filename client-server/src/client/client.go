package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	connType           = "tcp"
	BUFFERSIZE         = 4096
)

type Return struct {
	filesize  uint64
	isSuccess bool
}

type Measures struct {
	download_speed float64
	requests       int64
}

var (
	routeTable       = make(map[string]Measures)
	rwm              sync.RWMutex
	thresholdSamples int64
)

func main() {

	socket_file := flag.String("socket-file", "/f3/fuse-client.sock", "string")
	tempDir := flag.String("temp-dir", "/mnt/local-cache/client_tempdir", "string")
	thresholdRequests := flag.Int64("threshold-requests", 10, "int64")
	flag.Parse()

	thresholdSamples = *thresholdRequests
	log.Println("socket_file:" + *socket_file)
	log.Println("temp_dir:" + *tempDir)

	run_local_server(*socket_file, *tempDir)
}

//This function establishes connection with the FUSE driver using a socket file
//Waits for the input (in the form of filename, server1:port1, server2:port2...) from the FUSE driver.
func run_local_server(socket_file string, tempDir string) {

	socket_file = socket_file
	log.Println("Creating socket file: " + socket_file)
	if err := os.RemoveAll(socket_file); err != nil {
		log.Fatal(err)
	}

	log.Println("Establishing connection using socket file: " + socket_file)
	l, err := net.Listen("unix", socket_file)
	if err != nil {
		log.Fatal("listen error: ", err)
	}
	defer l.Close()

	set := make(map[string]bool)

	for {
		log.Println("Waiting for connection")
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error Connecting:", err.Error())
			continue
		}

		log.Println("Got connection from " + conn.RemoteAddr().String())
		buffer, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			log.Println("Client left.")
			conn.Close()
			continue
		}
		message := string(buffer[:len(buffer)-1])
		log.Println("Received message from the FUSE: " + message)
		connectionHandler(conn, tempDir, set, message)
	}
}

//Extracts file name and list of servers from the message. First, it checks if the requested file already exist in the server or being downloaded.
//Calls getServer for the fastest/random server from the routing table.
//It retries until it receives the file from the input servers or all the servers are exhausted in which case it sents NACK to the FUSE driver
//If it founds a file in any server, it calls putServer to update the entry of routing table.
func connectionHandler(conn net.Conn, tempDir string, set map[string]bool, message string) {

	arr := strings.Split(message, ",")
	file := strings.TrimSpace(arr[0])
	servers := getUniqueServers(arr[1:])
	path := path.Join(tempDir, file)
	var msg = "ACK"

	if isExist, _ := checkFileLocally(string(path)); isExist {
		log.Println(file + " already exist in the local cache")
	} else if _, ok := set[file]; ok {
		log.Println(file + " is already in-progress")
	} else {
		log.Println(file + " not being downloaded and does not exist in local-cache, connecting with server")
		serverPool := make(map[string]bool)
		for true {
			server := getServer(servers, serverPool)
			serverPool[server] = true

			log.Println("filename: " + file + " server: " + server)
			start := time.Now()

			set[file] = true
			finished := make(chan Return)
			go downloadFile(finished, file, server, tempDir)

			ack := <-finished
			delete(set, file)
			if ack.isSuccess {
				elapsed := time.Since(start)
				go putServer(server, float64(ack.filesize)/float64(elapsed))
				log.Println("File found in server " + server)
				log.Printf("downloading took %s", elapsed)
			} else {
				log.Println("File doesn't exist in server " + server)
				if len(serverPool) != len(servers) {
					log.Println("Retrying..")
					continue
				}
				log.Println("Server list exhausted.")
				log.Println("Sending NACK to the client!!")
				msg = "NACK"
			}
			break
		}
	}
	conn.Write([]byte(msg))
}

//It establishes TCP connection with the chosen server and checks if the file name exists on that server or not
//If the file exists, it firstly receives the file size and then the file itself
func downloadFile(finished chan Return, file string, server string, tempDir string) {

	log.Println("Downloading Request for " + file + " from " + server + " to " + tempDir)
	conn, err := net.DialTimeout(connType, server, 6*time.Second)

	if err != nil {
		log.Println("Error connecting: ", err.Error())
		finished <- Return{0, false}
		return
	}

	log.Println("Sending file name to the server to check if it exists.")
	fmt.Fprintf(conn, file+"\n")
	received := make([]byte, 4)
	conn.Read(received)
	message := strings.Trim(string(received), ":")
	if strings.Compare(message, "NACK") == 0 {
		log.Println(file + " doesn't exist on server")
		finished <- Return{0, false}
		return
	} else {
		log.Println(file + " exist on the server. Receiving the file size")
	}

	fileSizeReceived := make([]byte, 20)
	conn.Read(fileSizeReceived)
	fileSize, _ := strconv.ParseInt(strings.Trim(string(fileSizeReceived), ":"), 10, 64)

	log.Println("Creating file locally")
	f, err := os.Create(path.Join(tempDir, file))
	if err != nil {
		log.Println("Error creating new file: ", err.Error())
		finished <- Return{0, false}
		return
	}
	defer f.Close()

	var receivedBytes int64
	log.Println("Received file size from the server. Started receiving file chunks from server")
	for {
		if (fileSize - receivedBytes) <= BUFFERSIZE {
			io.CopyN(f, conn, (fileSize - receivedBytes))
			conn.Read(make([]byte, (receivedBytes+BUFFERSIZE)-fileSize))
			break
		}
		io.CopyN(f, conn, BUFFERSIZE)
		receivedBytes += BUFFERSIZE
	}
	log.Println("Downloaded " + file)
	finished <- Return{binary.BigEndian.Uint64(fileSizeReceived), true}
	return
}

//It extracts one server from the input list of servers coming from FUSE driver either randomly or fastest one
func getServer(serverList []string, serverPool map[string]bool) string {
	minDwldSpd := math.MaxFloat64
	var server string
	var israndom = false
	for i, s := range serverList {
		if _, ok := serverPool[s]; ok {
			continue
		}
		measure, found := getRouteTable(s)
		if found == false {
			log.Println("Server doesn't exist in the route table")
			israndom = true
			break
		} else if measure.requests < thresholdSamples {
			log.Println("Requests is less than " + fmt.Sprint(thresholdSamples))
			israndom = true
			break
		}
		if i == 0 {
			minDwldSpd = measure.download_speed
			server = s
		} else {
			if minDwldSpd > measure.download_speed {
				minDwldSpd = measure.download_speed
				server = s
			}
		}
	}
	if israndom == true {
		log.Println("Going for random server")
		for {
			server = serverList[rand.Intn(len(serverList))]
			if _, ok := serverPool[server]; !ok {
				break
			}
		}
	}
	return server
}

//Calculates the running download average after every successfull download from the server and updates the download speed and requests
func putServer(server string, downloadSpeed float64) {
	measure, found := getRouteTable(server)
	if !found {
		log.Println("Inserting the record in the Route table first time. Download Speed: " + fmt.Sprint(downloadSpeed) + " Requests: " + fmt.Sprint(measure.requests+1))
		setRouteTable(server, Measures{downloadSpeed, 1})
		return
	}
	newDwSpd := (measure.download_speed*float64(measure.requests) + downloadSpeed) / float64(measure.requests+1)
	log.Println("Updating the record in the Route table. Running Download Avg.: " + fmt.Sprint(newDwSpd) + " Requests: " + fmt.Sprint(measure.requests+1))
	new_measure := Measures{download_speed: newDwSpd, requests: measure.requests + 1}
	setRouteTable(server, new_measure)
}

func getRouteTable(key string) (Measures, bool) {
	rwm.RLock()
	defer rwm.RUnlock()
	measure, found := routeTable[key]
	return measure, found
}

func setRouteTable(key string, value Measures) {
	rwm.Lock()
	defer rwm.Unlock()
	routeTable[key] = value
}

func checkFileLocally(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

//return a list of unique servers
func getUniqueServers(serverList []string) []string {
	set := make(map[string]bool)
	var newServerList []string
	for _, s := range serverList {
		s = strings.TrimSpace(s)
		if _, ok := set[s]; ok {
			continue
		}
		set[s] = true
		newServerList = append(newServerList, s)
	}
	return newServerList
}


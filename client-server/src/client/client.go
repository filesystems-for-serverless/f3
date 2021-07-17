package main

import (
	"bufio"
	"flag"
	"fmt"
	"encoding/binary"
	"io"
	log "github.com/sirupsen/logrus"
	"math"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	connType           = "tcp"
)

type Return struct {
	filesize  int64
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
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.TraceLevel)
	socket_file := flag.String("socket-file", "/f3/fuse-client.sock", "string")
	tempDir := flag.String("temp-dir", "/mnt/local-cache/client_tempdir", "string")
	thresholdRequests := flag.Int64("threshold-requests", 10, "int64")
	flag.Parse()

	thresholdSamples = *thresholdRequests
	log.WithFields(log.Fields{"thread": "client.main",}).Trace("socket_file:" + *socket_file)
	log.WithFields(log.Fields{"thread": "client.main",}).Trace("temp_dir:" + *tempDir)
	run_local_server(*socket_file, *tempDir)
}

//This function establishes connection with the FUSE driver using a socket file
//Waits for the input (in the form of filename, server1:port1, server2:port2...) from the FUSE driver.
func run_local_server(socket_file string, tempDir string) {

	socket_file = socket_file
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

	set := make(map[string]bool)

	for {
		log.WithFields(log.Fields{"thread": "client.main",}).Trace("Waiting for connection.")
		conn, err := l.Accept()
		if err != nil {
			log.WithFields(log.Fields{"thread": "client.main",}).Error("Error accepting connection: ", err.Error())
			continue
		}

		buffer, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			log.WithFields(log.Fields{"thread": "client.main",}).Trace("Client left.")
			conn.Close()
			continue
		}
		message := string(buffer[:len(buffer)-1])
		log.WithFields(log.Fields{"thread": "client.main",}).Trace("Received message from the FUSE: " + message)
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
		log.WithFields(log.Fields{"thread": "client.main","fileName": file,}).Info(file + " already exist in the local cache")
	} else if _, ok := set[file]; ok {
		log.WithFields(log.Fields{"thread": "client.main","fileName": file,}).Info(file + " is already in-progress")
	} else {
		serverPool := make(map[string]bool)
		for true {
			server := getServer(servers, serverPool)
			serverPool[server] = true

			log.WithFields(log.Fields{"thread": "client.main","fileName": file,"serverAddress": server,}).Info("Downloading file: " + file + " from server: " + server)
			start := time.Now()

			set[file] = true
			finished := make(chan Return)
			go downloadFile(finished, file, server, tempDir)

			ack := <-finished
			delete(set, file)
			if ack.isSuccess {
				elapsed := time.Since(start)
				elapsedSeconds := elapsed.Seconds()
				go putServer(server, float64(ack.filesize)/float64(elapsedSeconds*1024*1024))
				log.WithFields(log.Fields{"thread": "client.main","fileName": file,"serverAddress": server, "fileSize": ack.filesize, "downloadRate(mbps)": fmt.Sprintf("%.4f",float64(ack.filesize)/(elapsedSeconds*1024*1024))}).Info("Downloading took "+ fmt.Sprint(elapsed))
			} else {
				if len(serverPool) != len(servers) {
					log.WithFields(log.Fields{"thread": "client.main","fileName": file,}).Trace("Retrying.. on different server")
					continue
				}
				log.WithFields(log.Fields{"thread": "client.main","fileName": file,}).Info("Server list exhausted. File Not Found")
				msg = "NACK"
			}
			break
		}
	}
	conn.Write([]byte(msg))
}

//It establishes TCP connection with the chosen server and checks if the file name exists on that server or not
//If the file exists, it firstly receives the file size and then the file itself
func downloadFile(finished chan Return, file string, serverNode string, tempDir string) {
	server := getServerIP(serverNode)
	log.WithFields(log.Fields{"thread": "client.receiver","fileName": file, "serverIPAddress": server, "serverNode": serverNode,}).Trace("Resolved node to IP")

	conn, err := net.DialTimeout(connType, server, 6*time.Second)

	if err != nil {
		log.WithFields(log.Fields{"thread": "client.receiver","fileName": file, "serverAddress": server,}).Error("Error connecting to server: ", err.Error())
		finished <- Return{0, false}
		return
	}

	fmt.Fprintf(conn, file+"\n")
	var ack bool
	err = binary.Read(conn, binary.LittleEndian, &ack)
	if err != nil{
		log.WithFields(log.Fields{"thread": "client.receiver","fileName": file, "serverAddress": server,}).Error(err)
	}
	
	if !ack {
		log.WithFields(log.Fields{"thread": "client.receiver","fileName": file, "serverAddress": server,}).Info(file + " doesn't exist on this server")
		finished <- Return{0, false}
		return
	} else {
		log.WithFields(log.Fields{"thread": "client.receiver","fileName": file, "serverAddress": server,}).Trace(file + " exist on this server.")
	}
	
	var fileSize int64
	err = binary.Read(conn, binary.LittleEndian, &fileSize)
	if err != nil{
		log.WithFields(log.Fields{"thread": "client.receiver","fileName": file, "serverAddress": server,}).Error(err)
	}

	log.WithFields(log.Fields{"thread": "client.receiver","fileName": file, "serverAddress": server,}).Trace("Received file size: " + fmt.Sprint(fileSize))
	f, err := os.Create(path.Join(tempDir, file))
	if err != nil {
		log.WithFields(log.Fields{"thread": "client.receiver","fileName": file, "serverAddress": server,"fileSize": fileSize,}).Error("Error creating new file: ", err.Error())
		finished <- Return{0, false}
		return
	}
	defer f.Close()

	var receivedBytes int64
	receivedBytes, err = io.Copy(f, conn)
	if fileSize != receivedBytes {
		log.WithFields(log.Fields{"thread": "client.receiver","fileName": file, "serverAddress": server,"fileSize": fileSize, "receivedBytes": receivedBytes,}).Trace("UUUU")
	}
	if err != nil {
		log.WithFields(log.Fields{"thread": "client.receiver","fileName": file, "serverAddress": server,"fileSize": fileSize,}).Error("Error creating new file: ", err.Error())
	}
	log.WithFields(log.Fields{"thread": "client.receiver","fileName": file, "serverAddress": server, "fileSize": fileSize,}).Trace("File "+file +" has been downloaded.")
	finished <- Return{fileSize, true}
	return
}


func getServerIP(server string) string {
	nodePort := strings.Split(server, ":")
	node, port := nodePort[0], nodePort[1]
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	config.BearerTokenFile = "/var/run/secrets/kubernetes.io/podwatcher/token"
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	pods, err := clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{
		FieldSelector: "spec.nodeName="+node, LabelSelector: "app=csi-f3-node"})
	if err != nil {
		panic(err.Error())
	}
	if (len(pods.Items) > 1) {
		fmt.Println("!!!")
	}
	return pods.Items[0].Status.PodIP + ":" + port
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
			israndom = true
			break
		} else if measure.requests < thresholdSamples {
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
		log.WithFields(log.Fields{"thread": "client.putServer",}).Trace("Inserting the record in the Route table first time. Download Rate: " + fmt.Sprint(downloadSpeed) + " Requests: " + fmt.Sprint(measure.requests+1))
		setRouteTable(server, Measures{downloadSpeed, 1})
		return
	}
	newDwSpd := (measure.download_speed*float64(measure.requests) + downloadSpeed) / float64(measure.requests+1)
	log.WithFields(log.Fields{"thread": "client.putServer",}).Trace("Updating the record in the Route table. Running Download Avg.: " + fmt.Sprint(newDwSpd) + " Requests: " + fmt.Sprint(measure.requests+1))
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
	s, err := os.Stat(path)
	if err == nil && s.Size() > 0 {
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

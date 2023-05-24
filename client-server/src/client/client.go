package main

import (
    "errors"
    "bufio"
    "flag"
    "fmt"
    "encoding/binary"
    "io"
    log "github.com/sirupsen/logrus"
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
    BLOCKSIZE   = 4* 1024 * 1024
    READAHEAD   = 10*BLOCKSIZE
)

type File struct {
    fname       string
    pos         int64
    conn        net.Conn
    fd          *os.File
    c           chan int64
    posLock     sync.RWMutex
    dlLock      sync.Mutex
}

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
    runningInPod    bool
    randomServer    bool
    filesLock   sync.Mutex
    files       = make(map[string]*File)
    localServerAddress string
)

func main() {
    log.SetFormatter(&log.JSONFormatter{})
    log.SetLevel(log.TraceLevel)
    socket_file := flag.String("socket-file", "/f3/fuse-client.sock", "string")
    server_socket_file := flag.String("server-socket-file", "/f3/fuse-server.sock", "string")
    tempDir := flag.String("temp-dir", "/mnt/local-cache/client_tempdir", "string")
    thresholdRequests := flag.Int64("threshold-requests", 10, "int64")
    flag.StringVar(&localServerAddress, "server-address", "localhost:9999", "")
    flag.BoolVar(&runningInPod, "in-pod", true, "")
    flag.BoolVar(&randomServer, "random-server", true, "")
    flag.Parse()

    thresholdSamples = *thresholdRequests
    log.WithFields(log.Fields{"thread": "client.main",}).Trace("socket_file:" + *socket_file)
    log.WithFields(log.Fields{"thread": "client.main",}).Trace("temp_dir:" + *tempDir)
    run_local_server(*socket_file, *tempDir, *server_socket_file)
}

//This function establishes connection with the FUSE driver using a socket file
//Waits for the input (in the form of filename, server1:port1, server2:port2...) from the FUSE driver.
func run_local_server(socket_file string, tempDir string, server_socket_file string) {

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

        go fuseConnectionHandler(conn, tempDir, set, server_socket_file)
    }
}

// Caller should hold filesLock[fname]
//func openConnection(tempDir, server, fname, localPath string) error {
func openConnection(tempDir, server, fname string) error {
    serverIP := server
    if runningInPod {
        serverIP = getServerIP(server)
    }
    path := path.Join(tempDir, fname)

    conn, err := net.DialTimeout("tcp", serverIP, 1*time.Second)
    if err != nil {
        return err
    }
    //fmt.Printf("Openend connection to server %v (%v)\n", server, serverIP)

    fmt.Fprintf(conn, "download,"+fname+"\n")
    var ack bool
    if err := binary.Read(conn, binary.LittleEndian, &ack); err != nil {
        log.WithFields(log.Fields{"thread": "client.receiver","fileName": fname, "serverAddress": server,}).Error(err)
        conn.Close()
        return err
    } else if !ack {
        log.WithFields(log.Fields{"thread": "client.receiver","fileName": fname, "serverAddress": server,}).
        Info(fname + " doesn't exist on this server")
        conn.Close()
        return errors.New("File does not exist on server")
    }

    fd, err := os.OpenFile(path, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
    //fmt.Println("local path: " + localPath)
    //fd, err := os.OpenFile(localPath, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
    if err != nil {
        return err
    }

    stat, err := fd.Stat()
    if err != nil {
        return err
    }

    //fmt.Printf("Opened %v, at position %v\n", path, stat.Size())

    files[path] = &File{fname, stat.Size(), conn, fd, make(chan int64), sync.RWMutex{}, sync.Mutex{}}
    //files[localPath] = &File{fname, stat.Size(), conn, fd, make(chan int64), sync.RWMutex{}, sync.Mutex{}}

    return nil
}

func openMoveConnection(oldpath, newpath, server string) error {
    serverIP := server
    if runningInPod {
        serverIP = getServerIP(server)
    }

    conn, err := net.DialTimeout("tcp", serverIP, 1*time.Second)
    if err != nil {
        return err
    }

    fmt.Fprintf(conn, "move,"+oldpath+","+newpath+"\n")
    var ack bool
    if err := binary.Read(conn, binary.LittleEndian, &ack); err != nil {
        fmt.Printf("Got error: %v\n", err)
        conn.Close()
        return err
    } else if !ack {
        fmt.Println("Got NAK from server" + oldpath + ", " + newpath)
        conn.Close()
        return errors.New("File does not exist on server")
    }

    return nil
}

func sendMoveCommand(oldpath, newpath, servers string) {
    for _, server := range strings.Split(servers, ",") {
        fmt.Println("Informing " + server + " that " + oldpath + " -> " + newpath)
        if server == localServerAddress {
            fmt.Println("not sending to myself")
            continue
        }
        if err := openMoveConnection(oldpath, newpath, server); err != nil {
            fmt.Printf("Got error making move connection: %v", err)
        }
    }
}

//Extracts file name and list of servers from the message. First, it checks if the requested file already exist in the server or being downloaded.
//Calls getServer for the fastest/random server from the routing table.
//It retries until it receives the file from the input servers or all the servers are exhausted in which case it sents NACK to the FUSE driver
//If it founds a file in any server, it calls putServer to update the entry of routing table.
// TODO Also take a startByte arg, pass that to server so it knows where to seek to after opening file
// on its end
func fuseConnectionHandler(fuseConn net.Conn, tempDir string, set map[string]bool, server_socket_file string) {
    //for {
    buffer, err := bufio.NewReader(fuseConn).ReadBytes('\n')
    //start := time.Now()
    if err != nil {
        fmt.Println(err)
        log.WithFields(log.Fields{"thread": "client.main",}).Trace("Client left.")
        fuseConn.Close()
        return
    }
    message := string(buffer[:len(buffer)-1])

    fmt.Printf("got msg %s\n", message)
    arr := strings.Split(message, ",")
    if len(arr) < 4 {
        fmt.Println("!!! ignoring malformed message")
        log.WithFields(log.Fields{"thread": "client.main",}).Warning("Ignoring malformed message: " + message)
        fuseConn.Write([]byte("N\n"))
        return
        //continue
    }
    action := arr[0]
    if action == "move" {
        sendMoveCommand(arr[1], arr[2], arr[3])
        fmt.Fprintf(fuseConn, "A,%d\n", 0)
        return
    }

    fname := strings.TrimSpace(arr[1])
    volid := strings.TrimSpace(arr[3])
    servers := getUniqueServers(arr[4:])
    fullPath := path.Join(tempDir, volid, fname)

    serverPool := make(map[string]bool)
    server := getServer(servers, serverPool)
    fmt.Println("Server list: " + strings.Join(servers, " "))
    fmt.Println("Downloading " + fname + " from " + server)
    serverPool[server] = true

    filesLock.Lock()
    if _, exists := files[fullPath]; !exists {
        // the filename sent to the server should include the volid
        newFname := path.Join(volid, fname)
        if err := openConnection(tempDir, server, newFname); err != nil {
            fmt.Println(err.Error())
            filesLock.Unlock()
            fmt.Fprintf(fuseConn, "N\n")
            return
        }
    } else {
        fmt.Println("File either already exists or is already being downloaded: " + fullPath + "\n")
        filesLock.Unlock()
        return
    }

    f := files[fullPath]
    filesLock.Unlock()

    fmt.Println(files)
    // XXX There's no way to tell the UDS client that there was an error
    go func(f *File, fuseConn net.Conn) {
        start := time.Now()
        buf := make([]byte, 64*1024*1024)
        if w, err := io.CopyBuffer(f.fd, f.conn, buf); err != nil {
            fmt.Println(err.Error())
        } else {
            fmt.Printf("read,%v,%v,%v\n", w, start.Unix(), fullPath)
            fmt.Fprintf(fuseConn, "A,%d\n", 0)
        }
        elapsed := time.Since(start).Seconds()
        fmt.Printf("Took %v seconds (started at %v, now %v)\n", elapsed, start.Unix(), time.Now().Unix())
        fuseConn.Close()
        fmt.Printf("Closed conn\n");
        f.fd.Close()

        // Tell the server about this file
        c, err := net.Dial("unix", server_socket_file)
        if err != nil {
            fmt.Println(err.Error())
        }
        defer c.Close()

        _, err = c.Write([]byte(f.fname+"\n"))
        if err != nil {
            fmt.Println(err.Error())
        }
    }(f, fuseConn)
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
    if randomServer {
        return serverList[rand.Intn(len(serverList))]
    } else {
        return serverList[0]
    }
}

func getRouteTable(key string) (Measures, bool) {
    rwm.RLock()
    defer rwm.RUnlock()
    measure, found := routeTable[key]
    return measure, found
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

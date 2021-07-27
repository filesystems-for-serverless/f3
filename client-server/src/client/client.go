package main

import (
    //"syscall"
    //"os/signal"
    //"runtime/pprof"
    "errors"
    "strconv"
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

    "github.com/pkg/profile"

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
    filesLock   sync.Mutex
    files       = make(map[string]*File)
)

func main() {
    log.SetFormatter(&log.JSONFormatter{})
    log.SetLevel(log.TraceLevel)
    socket_file := flag.String("socket-file", "/f3/fuse-client.sock", "string")
    tempDir := flag.String("temp-dir", "/mnt/local-cache/client_tempdir", "string")
    thresholdRequests := flag.Int64("threshold-requests", 10, "int64")
    flag.BoolVar(&runningInPod, "in-pod", true, "")
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

        go fuseConnectionHandler(conn, tempDir, set)
    }
}

// Caller should hold filesLock[fname]
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

    fmt.Fprintf(conn, fname+"\n")
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
    if err != nil {
        return err
    }

    stat, err := fd.Stat()
    if err != nil {
        return err
    }

    //fmt.Printf("Opened %v, at position %v\n", path, stat.Size())

    files[path] = &File{fname, stat.Size(), conn, fd, make(chan int64), sync.RWMutex{}, sync.Mutex{}}

    return nil
}

func readahead(f *File, server string) {
    fmt.Printf("Reading ahead from %v to %v\n", f.pos, f.pos + READAHEAD + READAHEAD)
    f.posLock.RLock()
    pos := f.pos
    f.posLock.RUnlock()
    if _, err := downloadMore(f, pos + READAHEAD + READAHEAD, server); err != nil {
        fmt.Println(err.Error())
    }
}

func downloadMore(f *File, endByte int64, server string) (int64, error) {
    f.dlLock.Lock()
    defer f.dlLock.Unlock()

    // Don't need to worry about f.pos changing since we're the only
    // place it could be modified and we have the dlLock

    // pos has been updated sometime between when we first tried to get
    // dlLock and now when we actually got it - no longer need to download
    if f.pos >= endByte {
        return 0, nil
    }

    // Round up to nearest block size
    endByte += BLOCKSIZE - (endByte % BLOCKSIZE)

    fmt.Printf("Reading from %v to %v\n", f.pos, endByte)
    start := time.Now()
    var w int64
    var err error
    if w, err = io.CopyN(f.fd, f.conn, endByte - f.pos); err != nil {
        f.posLock.Lock()
        f.pos += w
        f.posLock.Unlock()
        //fmt.Printf("Only read %v bytes\n", w)
        if err == io.EOF {
            //fmt.Printf("Reached EOF, not an error\n")
            return w, nil
        }
        return w, err
    }
    elapsed := time.Since(start).Seconds()
    go putServer(server, float64(float64(w)/float64(elapsed)))

    // Lock out anyone else from reading pos
    f.posLock.Lock()
    // We can only be here if there was no err, so must have read everything
    f.pos = endByte
    f.posLock.Unlock()
    //fmt.Printf("Read up to byte %v\n", f.pos)

    return w, nil
}

//Extracts file name and list of servers from the message. First, it checks if the requested file already exist in the server or being downloaded.
//Calls getServer for the fastest/random server from the routing table.
//It retries until it receives the file from the input servers or all the servers are exhausted in which case it sents NACK to the FUSE driver
//If it founds a file in any server, it calls putServer to update the entry of routing table.
// TODO Also take a startByte arg, pass that to server so it knows where to seek to after opening file
// on its end
func fuseConnectionHandler(fuseConn net.Conn, tempDir string, set map[string]bool) {
    for {
        buffer, err := bufio.NewReader(fuseConn).ReadBytes('\n')
        start := time.Now()
        if err != nil {
            log.WithFields(log.Fields{"thread": "client.main",}).Trace("Client left.")
            fuseConn.Close()
            return
        }
        message := string(buffer[:len(buffer)-1])

        //fmt.Printf("got msg %s\n", message)
        arr := strings.Split(message, ",")
        if len(arr) < 3 {
            fmt.Println("!!! ignoring malformed message")
            log.WithFields(log.Fields{"thread": "client.main",}).Warning("Ignoring malformed message: " + message)
            fuseConn.Write([]byte("N\n"))
            continue
        }
        fname := strings.TrimSpace(arr[0])
        endByte, _ := strconv.ParseInt(arr[1], 10, 64)
        servers := getUniqueServers(arr[2:])
        path := path.Join(tempDir, fname)

        serverPool := make(map[string]bool)
        server := getServer(servers, serverPool)
        serverPool[server] = true

        filesLock.Lock()
        if _, exists := files[path]; !exists {
            if err := openConnection(tempDir, server, fname); err != nil {
                fmt.Println(err.Error())
                filesLock.Unlock()
                fmt.Fprintf(fuseConn, "N\n")
                continue
            }
        }

        f := files[path]
        filesLock.Unlock()

        f.posLock.RLock()
        pos := f.pos
        f.posLock.RUnlock()
        //fmt.Printf("%v %v\n", f.pos, endByte)
        var downloaded int64
        if pos < endByte {
            if w, err := downloadMore(f, endByte, server); err != nil {
                fmt.Println(err.Error())
                fmt.Fprintf(fuseConn, "NAK\n")
                continue
            } else {
                downloaded = w
            }
        } else {
            fmt.Printf("Already read past %v, doing nothing\n", endByte)
            downloaded = 0
        }

        f.posLock.RLock()
        pos = f.pos
        f.posLock.RUnlock()
        fmt.Fprintf(fuseConn, "A,%d\n", pos)

        elapsed := time.Since(start).Seconds()
        fmt.Printf("ELAPSEDTIME %v %v %v\n", downloaded, elapsed, float64(downloaded)/float64(elapsed))

        // Assume endByte is as far as FUSE driver has read so far, and f.pos
        // is as far as this ID client has read.  When the FUSE driver gets
        // close enough to f.pos, readahead some
        if (f.pos - endByte) < READAHEAD {
            go readahead(f, server)
        }
    }
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

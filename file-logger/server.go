package main

import (
    "os"
    "flag"
    "fmt"
    "sort"
    "strings"
    "sync"
    "strconv"
    "net/url"
    "net/http"
    "net/http/httputil"
    "encoding/json"
)

type FileInfo struct {
    Nodes []string
    Size int
}

func contains(s []string, e string) bool {
    for _, a := range s {
        if a == e {
            return true
        }
    }
    return false
}

var fileNodeMap = make(map[string]FileInfo)
var saveFile string
var mapLock sync.RWMutex
var persist bool

func preferredNodes(w http.ResponseWriter, req *http.Request) {
    var err error

    if rq, err := httputil.DumpRequest(req, true); err != nil {
        fmt.Printf("DumpRequest error %v\n", err.Error())
    } else {
        fmt.Printf("Got request:\n%v\n", string(rq))
    }

    fnames := req.PostFormValue("filenames")
    if len(fnames) == 0 {
        fmt.Printf("error: missing fnames\n")
        w.WriteHeader(400)
        fmt.Fprintf(w, "error: missing fnames\n")
        return
    }
    fnames, err = url.QueryUnescape(fnames)
    if err != nil {
        fmt.Printf("error: fnames query unescape %v\n", err.Error())
    }

    minSizeInt := 0
    minSize := req.PostFormValue("minsize")
    minSize, err = url.QueryUnescape(minSize)
    if err != nil {
        fmt.Printf("error: minSize query unescape %v\n", err.Error())
    }
    if len(minSize) > 0 {
        minSizeInt, err = strconv.Atoi(minSize)
        if err != nil {
            fmt.Printf("Error strconv minSize %v\n", err.Error())
        }
    }

    fs := strings.Split(fnames, ",")

    fmt.Printf("fs: %v\nminSize: %v\n", fs, minSizeInt)

    nodeScores := make(map[string]int)
    mapLock.RLock()
    for _, f := range fs {
        if finfo, found := fileNodeMap[f]; found {
            for _, n := range finfo.Nodes {
                if _, nodeFound := nodeScores[n]; !nodeFound {
                    nodeScores[n] = 0
                }
                nodeScores[n] += finfo.Size
            }
        }
    }
    mapLock.RUnlock()

    res := make([]string, 0, len(nodeScores))
    for n := range nodeScores {
        if nodeScores[n] > minSizeInt {
            res = append(res, n)
        }
    }
    sort.SliceStable(res, func(i, j int) bool {
        return nodeScores[res[i]] > nodeScores[res[j]]
    })

    w.WriteHeader(200)
    fmt.Fprintf(w, "%v", strings.Join(res, ","))
}

func addFile(w http.ResponseWriter, req *http.Request) {
    fname := req.PostFormValue("filename")
    server := req.PostFormValue("server")
    size := req.PostFormValue("size")
    fmt.Printf("fname: %v server: %v size: %v\n", fname, server, size)
    if len(fname) == 0 {
        fmt.Printf("error: missing fname\n")
        fmt.Fprintf(w, "error: missing fname\n")
        return
    }
    if len(server) == 0 {
        fmt.Printf("error: missing server\n")
        fmt.Fprintf(w, "error: missing server\n")
        return
    }
    if len(size) == 0 {
        fmt.Printf("error: missing size\n")
        fmt.Fprintf(w, "error: missing size\n")
        return
    }

    sizeInt, err := strconv.Atoi(size)
    if err != nil {
        fmt.Printf("Error strconv %v\n", err.Error())
        sizeInt = 0
    }

    mapLock.Lock()
    defer mapLock.Unlock()
    fmt.Printf("map: %v\nin map? %v\n", fileNodeMap, fileNodeMap[fname])
    if finfo, found := fileNodeMap[fname]; !found {
        fmt.Printf("New fname entry\n")
        finfo = FileInfo{make([]string, 0), 0}
        finfo.Nodes = append(finfo.Nodes, server)
        finfo.Size = sizeInt
        if persist {
            fileNodeMap[fname] = finfo
        }
    } else {
        fmt.Printf("Adding to existing fname entry\n")
        if !contains(finfo.Nodes, server) {
            finfo.Nodes = append(finfo.Nodes, server)
        }
        if finfo.Size != sizeInt {
            fmt.Printf("!!! got new size (old: %v, new: %v).  Using larger\n", finfo.Size, sizeInt)
            if sizeInt > finfo.Size {
                finfo.Size = sizeInt
            }
        }
        if persist {
            fileNodeMap[fname] = finfo
        }
    }

    fmt.Println(fileNodeMap)
    fmt.Fprintf(w, "ok\n")

    f, err := os.OpenFile(saveFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
    if err != nil {
        fmt.Printf("Error openfile %v\n", err.Error())
        return
    }
    defer f.Close()
    encoder := json.NewEncoder(f)
    err = encoder.Encode(fileNodeMap)
    if err != nil {
        fmt.Printf("!!! %v\n", err.Error())
    }
}

func loadSavedData() {
    f, err := os.Open(saveFile)
    if err != nil {
        fmt.Printf("Error open %v\n", err.Error())
        return
    }
    defer f.Close()

    mapLock.Lock()
    defer mapLock.Unlock()
    decoder := json.NewDecoder(f)
    decoder.Decode(&fileNodeMap)

    fmt.Printf("file node map:\n%v\n", fileNodeMap)
}

func main() {
	listenPort := flag.String("listen-port", "9999", "string")
	flag.StringVar(&saveFile, "save-file", "/tmp/node-file-data", "string")
	flag.BoolVar(&persist, "persist", true, "disable for testing")
    flag.Parse()

    if persist {
        loadSavedData()
    }

    http.HandleFunc("/addFile", addFile)
    http.HandleFunc("/preferredNodes", preferredNodes)

    if err := http.ListenAndServe(":"+*listenPort, nil); err != nil {
        fmt.Printf("error ListenAndServe %v\n", err.Error())
    }
}

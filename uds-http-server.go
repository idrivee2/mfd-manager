package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
)

type HttpServer struct {
	listener *net.Listener
	disk     string
}

func newHttpServer(diskid string) *HttpServer {
	httpServer := HttpServer{disk: diskid}
	return &httpServer
}

func (httpServer *HttpServer) Listen() error {
	// Define the Unix socket file path
	socketPath := "/tmp/uds_server" + strings.Replace(httpServer.disk, "/", "-", -1) + ".sock"

	// Remove the existing socket file, if any
	if err := os.RemoveAll(socketPath); err != nil {
		fmt.Printf("Error removing socket file: %v\n", err)
		return err
	}

	router := http.NewServeMux()
	router.HandleFunc("/ping", httpServer.PingHandler)
	router.HandleFunc("/append", httpServer.AppendHandler)

	// Create a Unix domain socket listener
	unixListener, err := net.Listen("unix", socketPath)
	if err != nil {
		fmt.Printf("Error creating UDS listener: %v\n", err)
		return err
	}

	// Set appropriate permissions for the socket file if needed
	if err := os.Chmod(socketPath, 0777); err != nil {
		fmt.Printf("Error setting socket file permissions: %v\n", err)
		return err
	}

	// Start the HTTP server using the Unix socket listener
	fmt.Printf("Starting server on Unix domain socket: %s\n", socketPath)
	if err := http.Serve(unixListener, router); err != nil {
		fmt.Printf("Error serving HTTP over UDS: %v\n", err)
		return err
	}
	httpServer.listener = &unixListener
	return nil
}

func (httpServer *HttpServer) Close() {
	os.Remove("/tmp/uds_server" + strings.Replace(httpServer.disk, "/", "-", -1) + ".sock")
}

func (httpServer *HttpServer) PingHandler(writer http.ResponseWriter, request *http.Request) {
	fmt.Fprintf(writer, "mfd manager, "+httpServer.disk)
}

func (httpServer *HttpServer) AppendHandler(writer http.ResponseWriter, request *http.Request) {
	if mfdManager != nil {
		buf := bufferPool.Get().(*bytes.Buffer)
		defer bufferPool.Put(buf)
		buffer := buf.Bytes()
		readLength := 0
		failed := false
		for {
			length, err := request.Body.Read(buffer[readLength:])
			readLength += length

			if err == io.EOF {
				break
			} else if err != nil {
				failed = true
				break
			}

		}
		if !failed {
			dataRec := string(buffer[:readLength])
			if len(dataRec) > 0 {
				base, dir := path.Split(dataRec)
				if base != "" && dir != "" {
					if mfdManager.Append(base, 0) {
						writer.WriteHeader(http.StatusOK)
						return
					}
				}
			}
		}
	}

	writer.WriteHeader(http.StatusNotAcceptable)
}

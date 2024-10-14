package main

import (
	"encoding/binary"
	"fmt"
	"github.com/johnsiilver/golib/ipc/uds"
	"log"
	"path/filepath"
)

type UdsServer struct {
	server *uds.Server
}

func newUdsServer() *UdsServer {
	udsServer := UdsServer{}
	return &udsServer
}

func (u *UdsServer) Listen() {
	socketAddr := filepath.Join("/tmp/", "e2-mfd-uds")

	cred, _, err := uds.Current()
	if err != nil {
		panic(err)
	}

	serv, err := uds.NewServer(socketAddr, cred.UID.Int(), cred.GID.Int(), 0770)
	u.server = serv
	if err != nil {
		panic(err)
	}

	fmt.Println("Listening on socket: ", socketAddr)

	for conn := range serv.Conn() {
		fmt.Println("new connection|" + conn.Cred.PID.String())
		conn := conn

		go func() {
			buffer := make([]byte, 1024)
			sendBuffer := make([]byte, 4)
			for {
				readLength := 0
				recLen := 0
				var err error
				for {
					if recLen, err = conn.Read(buffer[readLength:]); err == nil {
						if recLen+readLength == len(buffer) {
							break
						}
						readLength += recLen
					} else if err != nil {
						break
					}
				}
				if err != nil {
					log.Printf("Connection closed|" + err.Error())
					break
				}
				msgType := buffer[0]
				seqNumber := binary.LittleEndian.Uint16(buffer[1:])
				msgLen := binary.LittleEndian.Uint16(buffer[3:])
				size := binary.LittleEndian.Uint64(buffer[5:])
				sendBuffer[0] = 2
				binary.LittleEndian.PutUint16(sendBuffer[1:], seqNumber)
				sendBuffer[3] = 0
				if msgType == 1 {
					if msgLen > 0 && msgLen <= 1022 {
						if mfdManager.Append(string(buffer[13:msgLen+13]), size) {
							sendBuffer[3] = 1
						}
					}
				}
				conn.Write(sendBuffer)
			}

		}()
	}
}

func (u *UdsServer) Close() {
	u.server.Close()
}

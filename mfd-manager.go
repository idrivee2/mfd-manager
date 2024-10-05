package main

import (
	"os"
	pathutil "path"
	"strconv"
	"strings"
)

type MdfManager struct {
	appenders map[string]*MfdDataAppender
	udsServer *UdsServer
}

func NewMfdMananger() *MdfManager {
	mfdManager := MdfManager{appenders: map[string]*MfdDataAppender{}}
	for i := 1; i < 120; i++ {
		drive := "/data" + strconv.Itoa(i)
		res, err := os.Stat(drive)
		if err == nil && res.IsDir() {
			mfdDataAppender := NewMfdDataAppender(drive)
			mfdManager.appenders["data"+strconv.Itoa(i)] = mfdDataAppender
		}
	}
	mfdManager.udsServer = newUdsServer()
	return &mfdManager
}

func (m MdfManager) Append(message string) bool {
	parts := strings.Split(message, "/")
	if len(parts) > 1 {
		if appender, found := m.appenders[parts[1]]; found {
			basePath := ""
			dir := ""
			if parts[2] == "remote_data" {
				basePath = "/" + pathutil.Join(parts[:7]...)
				dir = "/" + pathutil.Join(parts[7:]...)
			} else {
				basePath = "/" + pathutil.Join(parts[:4]...)
				dir = "/" + pathutil.Join(parts[4:]...)
			}
			appender.Append(basePath, dir)
			return true
		}
	}
	return false
}

func (m MdfManager) Close() {
	m.udsServer.Close()
	for _, appender := range m.appenders {
		appender.Close()
	}
}

func (m MdfManager) Run() {
	m.udsServer.Listen()
}

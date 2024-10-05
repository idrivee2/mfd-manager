package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	pathutil "path"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Configurable constants
const (
	NumberOfRecords = 20 * 1000
	MaxFileAge      = 10 * time.Minute
	NewBasePath     = byte(1)
	NewDeleteItem   = byte(2)
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		// Allocate a new buffer if pool is empty
		return new(bytes.Buffer)
	},
}

type MfdDataAppender struct {
	file          *os.File
	fileSize      int64
	startTime     time.Time
	logChannel    chan MfdEntry
	staggingPath  string
	diskPath      string
	basePathMap   map[string]uint16
	MaxId         uint16
	IsInitialized bool
	fileName      string
}

func NewMfdDataAppender(disk string) *MfdDataAppender {
	fmt.Println("NewMfdDataAppender|" + disk)
	pathOnDisk := pathutil.Join(disk, "mfd-manager-files")
	staggingPath := pathutil.Join("/opt/e2-mfd-manager/mfd-manager-files", disk)

	appender := &MfdDataAppender{
		logChannel: make(chan MfdEntry, 10), staggingPath: staggingPath, diskPath: pathOnDisk, fileName: "active",
	}
	err := appender.rotateFile()
	fmt.Println("NewMfdDataAppender2|" + disk)
	if err != nil {
		log.Fatal("Failed to NewMfdManager|", err)
		return nil
	}

	appender.IsInitialized = true
	go appender.run()
	return appender
}

type MfdEntry struct {
	basepath string
	dir      string
}

func (l *MfdDataAppender) Append(basepath string, dir string) bool {
	if l.IsInitialized {
		l.logChannel <- MfdEntry{basepath: basepath, dir: dir}
		return true
	}
	return false
}

func (l *MfdDataAppender) run() {
	buffer := make([]byte, 1*1024)
	for {
		select {
		case mfdEntry := <-l.logChannel:

			value, isNew := l.GetBasePathId(mfdEntry.basepath)
			if isNew {
				length := uint16(len(mfdEntry.basepath))
				buffer[0] = NewBasePath
				binary.LittleEndian.PutUint16(buffer[1:], value)
				binary.LittleEndian.PutUint16(buffer[3:], length)
				copy(buffer[5:], mfdEntry.basepath)
				l.WriteToFileReliable(buffer, 5+length)
			}
			length := uint16(len(mfdEntry.dir))
			buffer[0] = NewDeleteItem
			binary.LittleEndian.PutUint16(buffer[1:], value)
			binary.LittleEndian.PutUint16(buffer[3:], length)
			copy(buffer[5:], mfdEntry.dir)
			l.WriteToFileReliable(buffer, 5+length)
			l.fileSize++
			if l.shouldRollover() {
				l.rotateFile()
			}
		}
	}
}

func (l *MfdDataAppender) WriteToFileReliable(buffer []byte, length uint16) {
	totalWritten := 0
	for {
		written, err := l.file.Write(buffer[totalWritten:length])
		if err == nil {
			return
		} else if os.IsNotExist(err) {
			l.rotateFile()
		} else if written > 0 {
			totalWritten = totalWritten + written
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Check if file should rollover based on size or age
func (l *MfdDataAppender) shouldRollover() bool {
	if l.fileSize >= NumberOfRecords {
		return true
	}
	if time.Since(l.startTime) >= MaxFileAge {
		return true
	}
	return false
}

// Rotate the log file by closing the current one and opening a new one
func (l *MfdDataAppender) rotateFile() error {
	if l.file != nil {
		l.file.Close()
	}

	err := l.creatingStagingPathReliable()
	fmt.Println("creatingStagingPathReliable|" + l.diskPath)

	if err != nil {
		return err
	}

	err = l.creatingTargetPathReliable()
	fmt.Println("creatingStagingPathReliable|" + l.diskPath)
	if err != nil {
		return err
	}

	err = l.RenameCurrentFileReliable()
	fmt.Println("creatingStagingPathReliable|" + l.diskPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	err = l.CreateFileReliable(err, filepath.Join(l.staggingPath, l.fileName+".mfd"))
	fmt.Println("creatingStagingPathReliable|" + l.diskPath)
	if err != nil {
		return err
	}

	l.basePathMap = make(map[string]uint16, 0)
	l.fileSize = 0
	l.MaxId = 0
	l.startTime = time.Now()
	return nil
}

func (l *MfdDataAppender) CreateFileReliable(err error, currentPath string) error {
	i := 0
	for {
		i++
		if l.file, err = os.OpenFile(currentPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
			break
		} else if i > 20 {
			log.Fatal("RenameReliable failed|", err)
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func (l *MfdDataAppender) RenameCurrentFileReliable() error {
	currentPath := filepath.Join(l.staggingPath, l.fileName+".mfd")
	if stat, err := os.Stat(currentPath); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	} else if stat.IsDir() {
		return &os.PathError{Path: currentPath}
	}
	newFileName := l.fileName + "-" + strconv.FormatInt(time.Now().Unix(), 16) + ".mfd"
	currentPathTemp := filepath.Join(l.staggingPath, newFileName)
	newPath := filepath.Join(l.diskPath, newFileName)

	i := 0
	for {
		i++
		if err := os.Rename(currentPath, currentPathTemp); err == nil {
			go MoveFile(currentPathTemp, newPath)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

func MoveFile(sourcePath, destPath string) {
	for {
		inputFile, err := os.Open(sourcePath)
		if os.IsNotExist(err) {
			break
		}
		if err == nil {
			destPathTmp := destPath + ".tmp"
			outputFile, err := os.Create(destPathTmp)
			if err == nil {
				_, err = io.Copy(outputFile, inputFile)
				outputFile.Close()
				if err == nil {
					err = os.Rename(destPathTmp, destPath)
					if err == nil {
						inputFile.Close()
						err = os.Remove(sourcePath)
						fmt.Fprintf(os.Stdout, "file"+sourcePath+" moved to"+destPath)
						return
					} else {
						fmt.Fprintf(os.Stderr, "failed to rename "+destPathTmp+" to"+destPath, err)
						_ = os.Remove(destPathTmp)
					}
				} else {
					fmt.Fprintf(os.Stderr, "failed to copy cotent to "+destPathTmp, err)
					_ = os.Remove(destPathTmp)
				}
			} else {
				fmt.Fprintf(os.Stderr, "failed to create "+destPathTmp, err)
			}
			inputFile.Close()
		} else {
			fmt.Fprintf(os.Stderr, "failed to open "+sourcePath, err)
		}
		fmt.Fprintf(os.Stderr, "failed to move "+sourcePath+" to "+destPath+"|retry in 10s")
		time.Sleep(10 * time.Second)
	}
}

func (l *MfdDataAppender) creatingStagingPathReliable() error {
	i := 0
	for {
		i++
		if err := os.MkdirAll(l.staggingPath, 0755); err == nil {
			break
		} else if i > 20 {
			log.Fatal("creatingStagingPathReliable failed|", err)
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

func (l *MfdDataAppender) GetBasePathId(message string) (uint16, bool) {
	if id, found := l.basePathMap[message]; found {
		return id, false
	} else {
		l.MaxId += 1
		l.basePathMap[message] = l.MaxId
		return l.MaxId, true
	}
}

func (l *MfdDataAppender) Close() {
	if l.file != nil {
		l.file.Close()
	}
}

func (l *MfdDataAppender) creatingTargetPathReliable() error {
	i := 0
	for {
		i++
		if err := os.MkdirAll(l.diskPath, 0755); err == nil {
			break
		} else if i > 20 {
			log.Fatal("creatingStagingPathReliable failed|", err)
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

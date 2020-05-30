package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
)

type MyFormatter struct{}

func (s *MyFormatter) Format(entry *log.Entry) ([]byte, error) {
	timestamp := time.Now().Local().Format("2006/01/02 15:04:05")
	fullPath := strings.Split(entry.Caller.File, "/")
	path := fullPath[len(fullPath) - 1] + ":" + strconv.Itoa(entry.Caller.Line)
	msg := fmt.Sprintf("%s %10s %20s\t %s\n",
		timestamp,
		"[" + strings.ToUpper(entry.Level.String()) + "]",
		path,
		entry.Message)
	return []byte(msg), nil
}

func init() {
	log.SetFormatter(new(MyFormatter))
	//log.SetLevel(log.ErrorLevel)
	log.SetReportCaller(true)
}

func main() {
	log.WithFields(log.Fields{
		"animal": "walrus",
	}).Info("A walrus appears")

	log.Info("hello, %s", "world")

	log.Infof("hello, %s", "world")

	log.WithFields(log.Fields{
		"hello": "world",
	}).Error("shit")

	fmt.Println(" E:\\virtualShare\\gopath3\\src\\tinykv_raft\\test\\log\\log.go:12 ")
	fmt.Println(" log.go:38 ")
}
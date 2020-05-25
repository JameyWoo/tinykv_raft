package main

import (
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.ErrorLevel)
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
}
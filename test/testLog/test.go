package main

import (
	"github.com/sirupsen/logrus"
)

func main() {

	logrus.Debug("Useful debugging information.")
	logrus.Info("Something noteworthy happened!")
	logrus.Warn("You should probably take a look at this.")
	logrus.Error("Something failed but I'm not quitting.")
	logrus.Fatal("Bye.")         //log之后会调用os.Exit(1)
	logrus.Panic("I'm bailing.") //log之后会panic()

}

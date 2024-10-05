package main

import (
	"fmt"
	"os"
)

var mfdManager *MdfManager

func main() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		done <- true
	}()

	mfdManager = NewMfdMananger()

	mfdManager.Run()

	<-done
	mfdManager.Close()

	fmt.Println("exiting")
}

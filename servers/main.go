package main

import (
	"github.com/zebra88/libs/servers"
)

func main() {
	servers.Init()
	if server.GetService("/backends/xue") == nil {
		log.Println("get service failed")
	} else {
		log.Println("get service succeed")
	}

	if GetServiceWithId("/backends/xue", "xue0") == nil {
		log.Println("get service with id failed")
	} else {
		log.Println("get service with id succeed")
	}
}

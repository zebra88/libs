package main

import (
	//	"github.com/davecgh/go-spew/spew"
	"server"
)

func main() {
	//	server.Init()
	//	spew.Dump(_default_pool)
	if server.GetService("/backends/snowflake") == nil {
		log.Println("get service failed")
	} else {
		log.Println("get service succeed")
	}

	if GetServiceWithId("/backends/snowflake", "snowflake1") == nil {
		log.Println("get service with id failed")
	} else {
		log.Println("get service with id succeed")
	}
}

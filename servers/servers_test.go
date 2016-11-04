package servers

import (
	//	"github.com/davecgh/go-spew/spew"
	"testing"
)

func TestService(t *testing.T) {
	Init()
	//	spew.Dump(_default_pool)
	if GetService("/backends/snowflake") == nil {
		t.Log("get service failed")
	} else {
		t.Log("get service succeed")
	}

	if GetServiceWithId("/backends/snowflake", "snowflake1") == nil {
		t.Log("get service with id failed")
	} else {
		t.Log("get service with id succeed")
	}
}

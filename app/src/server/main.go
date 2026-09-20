package main

import (
	"log"

	"github.com/kwhitestone/prism-fusion/core"
	_ "nucleagent-im/addons"
)

func main() {
	if err := core.RunApplication(core.ApplicationOptions{DisableDatabase: true}); err != nil {
		log.Fatal(err)
	}
}

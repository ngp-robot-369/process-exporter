package main

import (
	"fmt"
	"log"
)

func GenWhitelistConfig(apps []string) string {
	const (
		MatcherTemplate = "" +
			"\n  - exe:" +
			"\n    - %v" +
			"\n    name: \"{{.ExeBase}}:{{.PID}}\""
	)
	var (
		config = `process_names:`
	)
	for _, app := range apps {
		if app != "" {
			config += fmt.Sprintf(MatcherTemplate, app)
			if app == "nomad" {
				config += "\n    cmdline:"
				config += "\n    - .*agent.*"
			}
		}
	}
	log.Printf(config)
	return config
}

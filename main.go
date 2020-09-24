package main

import (
	"github.com/jessevdk/go-flags"
	"github.com/prometheus/common/log"
	"os"
)

func main() {
	parser := flags.NewParser(nil, flags.Default)

	_, err := parser.AddCommand("stage",
		"stage a directory for storage",
		"The stage command will add the full directory into API, split it into smaller"+
			"directories suitable for storage in Filecoin, and save the mapping of the filepath to the directory"+
			"in filecoin so that it can easily be retrieved later.",
		&Stage{})
	if err != nil {
		log.Fatal(err)
	}

	_, err = parser.AddCommand("serve",
		"start the web server",
		"The serve command will start the web serve to serve files stored by amzn. It will first "+
			"try to find the file on IPFS, if it's not there it will download it from filecoin.",
		&Serve{})
	if err != nil {
		log.Fatal(err)
	}

	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}
}

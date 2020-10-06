package main

import (
	"github.com/jessevdk/go-flags"
	"os"
)

func main() {
	parser := flags.NewParser(nil, flags.Default)

	_, err := parser.AddCommand("stage",
		"stage a directory for storage",
		"The stage command will add the full directory into API, split it into smaller"+
			"directories suitable for storage in Filecoin, and save the mapping of the filepath to the directory"+
			"in Filecoin so that it can easily be retrieved later.",
		&Stage{})
	if err != nil {
		log.Fatal(err)
	}

	_, err = parser.AddCommand("store",
		"store a staged directory in Filecoin",
		"The store command will store the provided directory in filecoin. You must have previously staged" +
		"the directory using the stage command. You will need to pass in the root CID for the directory into this command.",
		&Store{})
	if err != nil {
		log.Fatal(err)
	}

	_, err = parser.AddCommand("serve",
		"start the web server",
		"The serve command will start the web serve to serve files stored by amzn. It will first "+
			"try to find the file on IPFS, if it's not there it will download it from Filecoin.",
		&Serve{})
	if err != nil {
		log.Fatal(err)
	}

	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}
}

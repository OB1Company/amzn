package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"net/http"
	"strconv"
	"time"
)

type Serve struct {
	IpfGateway string `short:"g" long:"gateway" description:"The hostname:port of the IPFS Gateway." default:"127.0.0.1:8080"`
	DbAPI      string `long:"db" default:"localhost:27017"`
	Port       int    `short:"p" long:"port" default:"8000"`

	db *mongo.Collection
}

func (x *Serve) Execute(args []string) error {
	dbClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", x.DbAPI)))
	if err != nil {
		return err
	}
	defer dbClient.Disconnect(context.Background())

	collection := dbClient.Database("filemapdb").Collection("files")
	x.db = collection

	http.HandleFunc("/ipfs/", x.handle)

	if err := http.ListenAndServe(":"+strconv.Itoa(x.Port), nil); err != nil {
		return err
	}

	return nil
}

func (x *Serve) handle(w http.ResponseWriter, r *http.Request) {
	client := http.Client{
		Timeout: time.Second * 30,
	}
	resp, err := client.Get(fmt.Sprintf("http://%s%s", x.IpfGateway, r.URL.Path))
	if err == nil {
		io.Copy(w, resp.Body)
		return
	}

	var (
		obj    Object
		filter = bson.D{{"path", &r.URL.Path}}
	)

	err = x.db.FindOne(context.Background(), filter).Decode(&obj)
	if err != nil {
		// TODO: return some kind of not found page
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Write([]byte("We are fetching this file from filecoin. Please check back later."))
}

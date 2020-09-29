package main

import (
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ob1company/amzn/static"
	"github.com/op/go-logging"
	powergate "github.com/textileio/powergate/api/client"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

var log = logging.MustGetLogger("amzn")

type Serve struct {
	IpfGateway       string `short:"g" long:"gateway" description:"The hostname:port of the IPFS Gateway." default:"127.0.0.1:8080"`
	IPFSReverseProxy string `long:"ipfsreverseproxy" description:"An IPFS reverse proxy address if needed." default:"127.0.0.1:6002"`
	DbAPI            string `long:"db" default:"localhost:27017"`
	Port             int    `short:"p" long:"port" default:"8000"`
	PowergateAPI     string `short:"a" long:"powergateapi" description:"The hostname:port of the Powergate API." default:"127.0.0.1:5002"`
	PowergateToken   string `long:"powergatetoken" description:"An authentication token for powergate if needed." default:""`

	inflightFilecoinRequests map[string]bool
	mtx                      sync.RWMutex
	db                       *mongo.Collection
	powergateClient          *powergate.Client
}

func (x *Serve) Execute(args []string) error {
	dbClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", x.DbAPI)))
	if err != nil {
		return err
	}
	defer dbClient.Disconnect(context.Background())

	collection := dbClient.Database("filemapdb").Collection("files")
	x.db = collection

	x.inflightFilecoinRequests = make(map[string]bool)
	x.mtx = sync.RWMutex{}

	powergateClient, err := powergate.NewClient(x.PowergateAPI)
	if err != nil {
		return err
	}
	defer powergateClient.Close()
	x.powergateClient = powergateClient

	http.HandleFunc("/ipfs/", x.handle)

	log.Infof("Http server running on :%d", x.Port)

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
		w.WriteHeader(http.StatusNotFound)

		notFoundPage, err := static.Asset("notfound.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(notFoundPage)
		return
	}
	fetchingPage, err := static.Asset("fetching.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(fetchingPage)

	x.mtx.RLock()
	inFlight := x.inflightFilecoinRequests[obj.BucketID]
	x.mtx.RUnlock()

	if !inFlight {
		x.mtx.Lock()
		x.inflightFilecoinRequests[obj.BucketID] = true
		x.mtx.Unlock()

		go x.fetchBucketFromFilecoin(obj.BucketID)
	}
}

func (x *Serve) fetchBucketFromFilecoin(bucket string) {
	defer func() {
		x.mtx.Lock()
		delete(x.inflightFilecoinRequests, bucket)
		x.mtx.Unlock()
	}()

	id, err := cid.Decode(bucket)
	if err != nil {
		log.Errorf("Error decoding bucket CID: %s", err)
		return
	}
	r := rand.Int63()
	tmpDir := path.Join(os.TempDir(), fmt.Sprintf("amzn-%d", r))
	if err := os.Mkdir(tmpDir, os.ModePerm); err != nil {
		log.Errorf("Error creating temp directory: %s", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	if err := x.powergateClient.FFS.GetFolder(context.Background(), x.IPFSReverseProxy, id, tmpDir); err != nil {
		log.Errorf("Error downloading bucket from powergate: %s", err)
		return
	}


}

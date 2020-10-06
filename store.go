package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/ipfs/go-cid"
	powergate "github.com/textileio/powergate/api/client"
	"github.com/textileio/powergate/ffs"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"os/signal"
)

type Store struct {
	IPFSReverseProxy string `long:"ipfsreverseproxy" description:"An IPFS reverse proxy address if needed." default:"127.0.0.1:6002"`
	PowergateAPI     string `short:"p" long:"powergateapi" description:"The hostname:port of the Powergate API." default:"127.0.0.1:5002"`
	PowergateToken   string `long:"powergatetoken" description:"An authentication token for powergate if needed." default:""`
	DbAPI            string `long:"db" default:"localhost:27017"`
	Cid              string `short:"c" long:"cid" description:"The CID of a previously staged directly that you want to store in filecoin."`
}

func (x *Store) Execute(args []string) error {
	client, err := powergate.NewClient(x.PowergateAPI)
	if err != nil {
		return err
	}
	defer client.Close()

	dbClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", x.DbAPI)))
	if err != nil {
		return err
	}
	defer dbClient.Disconnect(context.Background())

	collection := dbClient.Database("filemapdb").Collection("files")

	var (
		dir    Dir
		events = make(chan powergate.JobEvent)
	)
	filter := bson.D{{"rootcid", x.Cid}}

	if err := collection.FindOne(context.TODO(), filter).Decode(&dir); err != nil {
		return err
	}

	if len(dir.Buckets) == 0 {
		return errors.New("no buckets found for CID")
	}

	for _, b := range dir.Buckets {
		id, err := cid.Decode(b)
		if err != nil {
			return err
		}

		ctx := context.WithValue(context.Background(), powergate.AuthKey, x.PowergateToken)
		jobID, err := client.FFS.PushStorageConfig(ctx, id, powergate.WithOverride(true))
		if err != nil {
			return err
		}

		dir.Jobs[jobID.String()] = ffs.Job{
			ID:  jobID,
			Cid: id,
		}

		update := bson.M{
			"$set": bson.M{
				"Jobs": dir.Jobs,
			},
		}
		fmt.Println(dir.Jobs)

		if _, err := collection.UpdateOne(context.TODO(), filter, update); err != nil {
			return err
		}
		if err := client.FFS.WatchJobs(context.TODO(), events, jobID); err != nil {
			return err
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	for {
		select {
		case e := <-events:
			dir.Jobs[e.Job.ID.String()] = e.Job
			update := bson.D{
				{"$inc", bson.D{
					{"Jobs", dir.Jobs},
				}},
			}
			if _, err := collection.UpdateOne(context.TODO(), filter, update); err != nil {
				return err
			}
			log.Info("Job %s: Cid %s: Status Update: %s", e.Job.ID.String(), e.Job.Cid.String(), e.Job.Status)

			// TODO: handle failure and retry.
		case <-c:
			close(events)
			os.Exit(1)
		}
	}
}

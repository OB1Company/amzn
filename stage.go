package main

import (
	"context"
	"fmt"
	shell "github.com/ipfs/go-ipfs-api"
	powergate "github.com/textileio/powergate/api/client"
	"github.com/textileio/powergate/ffs"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

type Stage struct {
	IpfsAPI          string `short:"a" long:"ipfsapi" description:"The hostname:port of the IPFS API." default:"127.0.0.1:5001"`
	IPFSReverseProxy string `long:"ipfsreverseproxy" description:"An IPFS reverse proxy address if needed." default:"127.0.0.1:6002"`
	PowergateAPI     string `short:"p" long:"powergateapi" description:"The hostname:port of the Powergate API." default:"127.0.0.1:5002"`
	PowergateToken   string `long:"powergatetoken" description:"An authentication token for powergate if needed." default:""`
	DbAPI            string `long:"db" default:"localhost:27017"`
	DirPath          string `short:"d" long:"directory path" description:"The path to the directory to stage."`
	BucketSize       uint64 `short:"b" long:"bucketsize" description:"The size of each bucket stored in filecoin." default:"1000000000"`
}

type Object struct {
	Path     string
	Cid      string
	Size     int64
	IsDir    bool
	BucketID string
}

type Dir struct {
	RootCID string
	Buckets []string
	Jobs    map[string]ffs.Job
}

func (x *Stage) Execute(args []string) error {
	sh := shell.NewShell(x.IpfsAPI)

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

	x.DirPath = strings.TrimSuffix(x.DirPath, "/")

	fmt.Print("Adding to IPFS...")
	rootCid, err := sh.AddDir(x.DirPath)
	if err != nil {
		return err
	}
	fmt.Print("done\n")
	fmt.Printf("IPFS Root Cid: %s\n\n", rootCid)

	files := make(map[Object]struct{})
	if err := enumerateFiles(x.DirPath, "/ipfs/"+rootCid, "", rootCid, sh, files); err != nil {
		return err
	}

	buckets := make([][]Object, 1)
	idx, bucketSize := 0, int64(0)
	for f := range files {
		if f.IsDir {
			buckets[0] = append(buckets[0], f)
			bucketSize += f.Size
		}
	}

	for f := range files {
		if f.IsDir {
			continue
		}

		if bucketSize+f.Size > int64(x.BucketSize) {
			buckets = append(buckets, []Object{})
			idx++
			bucketSize = 0
		}
		bucketSize += f.Size
		buckets[idx] = append(buckets[idx], f)
	}

	var bucketCids []string
	fmt.Print("Staging in powergate...")
	for i, bucket := range buckets {
		tmp := path.Join(os.TempDir(), fmt.Sprintf("amzn-bucket%d", i))
		if err := os.Mkdir(tmp, os.ModePerm); err != nil {
			return err
		}

		for _, f := range bucket {
			if f.IsDir {
				blk, err := sh.BlockGet(f.Path)
				if err != nil {
					return err
				}
				if err := ioutil.WriteFile(path.Join(tmp, f.Cid), blk, os.ModePerm); err != nil {
					return err
				}
				continue
			}

			pth := x.DirPath + strings.TrimPrefix(f.Path, "/ipfs/"+rootCid)

			in, err := os.Open(pth)
			if err != nil {
				return err
			}
			out, err := os.Create(path.Join(tmp, f.Cid))
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, in); err != nil {
				return err
			}
			in.Close()
			out.Close()
		}

		ctx := context.WithValue(context.Background(), powergate.AuthKey, x.PowergateToken)
		outCid, err := client.FFS.StageFolder(ctx, x.IPFSReverseProxy, tmp)
		if err != nil {
			return err
		}
		if err := os.RemoveAll(tmp); err != nil {
			return err
		}

		for x := range buckets[i] {
			buckets[i][x].BucketID = outCid.String()
			_, err = collection.InsertOne(context.Background(), buckets[i][x])
			if err != nil {
				return err
			}
		}
		bucketCids = append(bucketCids, outCid.String())
	}
	fmt.Print("done\n")
	fmt.Println("Filecoin Bucket Cids:")
	for _, id := range bucketCids {
		fmt.Println(id)
	}

	_, err = collection.InsertOne(context.Background(), Dir{
		Buckets: bucketCids,
		RootCID: rootCid,
		Jobs:    make(map[string]ffs.Job),
	})
	if err != nil {
		return err
	}

	return nil
}

func enumerateFiles(osPathPrefix, ipfsPathPrefix, pth, id string, sh *shell.Shell, objs map[Object]struct{}) error {
	stat, err := os.Stat(path.Join(osPathPrefix, pth))
	if err != nil {
		return err
	}
	links, err := sh.List(id)
	if err != nil {
		return err
	}
	for _, link := range links {
		if link.Name != "" {
			if err := enumerateFiles(osPathPrefix, ipfsPathPrefix, path.Join(pth, link.Name), link.Hash, sh, objs); err != nil {
				return err
			}
		}
	}

	objs[Object{
		Cid:   id,
		Path:  path.Join(ipfsPathPrefix, pth),
		Size:  stat.Size(),
		IsDir: stat.IsDir(),
	}] = struct{}{}
	return nil
}

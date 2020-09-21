package main

import (
	"context"
	"fmt"
	shell "github.com/ipfs/go-ipfs-api"
	powergate "github.com/textileio/powergate/api/client"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

const maxBucketSize = 1000000000

type Stage struct {
	IpfsAPI          string `short:"a" long:"ipfsapi" description:"The hostname:port of the IPFS API." default:"127.0.0.1:5001"`
	IPFSReverseProxy string `long:"ipfsreverseproxy" description:"An IPFS reverse proxy address if needed." default:"127.0.0.1:6002"`
	PowergateAPI     string `short:"p" long:"powergateapi" description:"The hostname:port of the Powergate API." default:"127.0.0.1:5002"`
	PowergateToken string `long:"powergatetoken" description:"An authentication token for powergate if needed." default:""`
	DbAPI            string `long:"db" default:"localhost:27017"`
	DirPath          string `short:"d" long:"directory path" description:"The path to the directory to stage."`
}

type Object struct {
	Path     string
	Cid      string
	Size     int64
	IsDir    bool
	BucketID string
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

	buckets := make([][]Object, 2)
	idx, bucketSize := 1, int64(0)
	for f := range files {
		if f.IsDir {
			buckets[0] = append(buckets[0], f)
			continue
		}

		if bucketSize+f.Size > maxBucketSize {
			buckets = append(buckets, []Object{})
			idx++
			bucketSize = 0
		}
		bucketSize += f.Size
		buckets[idx] = append(buckets[idx], f)
	}

	tmp0 := path.Join(os.TempDir(), fmt.Sprintf("amzn-bucket%d", 0))
	if err := os.Mkdir(tmp0, os.ModePerm); err != nil {
		return err
	}
	for _, f := range buckets[0] {
		blk, err := sh.BlockGet(f.Path)
		if err != nil {
			return err
		}
		if err := ioutil.WriteFile(path.Join(tmp0, f.Cid), blk, os.ModePerm); err != nil {
			return err
		}
	}

	var bucketCids []string
	ctx := context.WithValue(context.Background(), powergate.AuthKey, x.PowergateToken)
	outCid, err := client.FFS.StageFolder(ctx, x.IPFSReverseProxy, tmp0)
	if err != nil {
		return err
	}
	bucketCids = append(bucketCids, outCid.String())
	if err := os.RemoveAll(tmp0); err != nil {
		return err
	}

	for i := range buckets[0] {
		buckets[0][i].BucketID = outCid.String()
		_, err = collection.InsertOne(context.Background(), buckets[0][i])
		if err != nil {
			return err
		}
	}

	fmt.Print("Staging in powergate...")
	for i, bucket := range buckets[1:] {
		tmp := path.Join(os.TempDir(), fmt.Sprintf("amzn-bucket%d", i))
		if err := os.Mkdir(tmp, os.ModePerm); err != nil {
			return err
		}

		for _, f := range bucket {
			pth := x.DirPath + strings.TrimPrefix(f.Path, "/ipfs/"+rootCid)

			in, err := os.Open(pth)
			if err != nil {
				return err
			}
			out, err := os.Create(path.Join(tmp, f.Cid))
			if err != nil {
				return err
			}
			if _, err := io.Copy(in, out); err != nil {
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

		for x := range buckets[i+1] {
			buckets[i+1][x].BucketID = outCid.String()
			_, err = collection.InsertOne(context.Background(), buckets[i+1][x])
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

	return nil
}

func enumerateFiles(osPathPrefix, ipfsPathPrefix, pth, id string, sh *shell.Shell, objs map[Object]struct{}) error {
	stat, err := os.Stat(path.Join(osPathPrefix, pth))
	if err != nil {
		return err
	}
	obj, err := sh.ObjectGet(id)
	if err != nil {
		return err
	}
	for _, link := range obj.Links {
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

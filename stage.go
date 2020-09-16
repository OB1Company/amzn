package main

import (
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	shell "github.com/ipfs/go-ipfs-api"
	powergate "github.com/textileio/powergate/api/client"
	"go.mongodb.org/mongo-driver/bson"
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
	PowergateAuthKey string `long:"powergateauthkey" description:"An authentication key for powergate if needed." default:""`
	DbAPI            string `long:"db" default:"localhost:27017"`
	DirPath          string `short:"d" long:"directory path" description:"The path to the directory to stage."`
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

	db := dbClient.Database("filemapdb")
	collection := db.Collection("files")

	x.DirPath = strings.TrimSuffix(x.DirPath, "/")
	cid, err := sh.AddDir(x.DirPath)
	if err != nil {
		return err
	}

	files := make(map[ipfsObject]struct{})
	if err := enumerateFiles("/ipfs/"+cid, cid, sh, files); err != nil {
		return err
	}

	buckets := make([][]ipfsObject, 1)
	idx, bucketSize := 0, 0
	for f := range files {
		if bucketSize+f.size > maxBucketSize {
			buckets = append(buckets, []ipfsObject{})
			idx++
			bucketSize = 0
		}
		bucketSize += f.size
		buckets[idx] = append(buckets[idx], f)
	}

	for i, bucket := range buckets {
		tmp := path.Join(os.TempDir(), fmt.Sprintf("amzn-bucket%d", i))
		if err := os.Mkdir(tmp, os.ModePerm); err != nil {
			return err
		}

		for _, f := range bucket {
			if f.cid.String() == cid {
				blk, err := sh.BlockGet(f.path)
				if err != nil {
					return err
				}
				if err := ioutil.WriteFile(path.Join(tmp, f.cid.String()), blk, os.ModePerm); err != nil {
					return err
				}
				continue
			}
			pth := x.DirPath + strings.TrimPrefix(f.path, "/ipfs/"+cid)
			info, err := os.Stat(pth)
			if err != nil {
				return err
			}
			if info.IsDir() {
				blk, err := sh.BlockGet(f.path)
				if err != nil {
					return err
				}
				if err := ioutil.WriteFile(path.Join(tmp, f.cid.String()), blk, os.ModePerm); err != nil {
					return err
				}
			} else {
				in, err := os.Open(pth)
				if err != nil {
					return err
				}
				out, err := os.Create(path.Join(tmp, f.cid.String()))
				if err != nil {
					return err
				}
				if _, err := io.Copy(in, out); err != nil {
					return err
				}
				in.Close()
				out.Close()
			}
			m := bson.M{"path": f.path, "bucket": i, "root": cid, "cid": f.cid.String()}
			_, err = collection.InsertOne(context.Background(), m)
			if err != nil {
				return err
			}
		}

		ctx := context.WithValue(context.Background(), powergate.AuthKey, x.PowergateAuthKey)
		outCid, err := client.FFS.StageFolder(ctx, x.IPFSReverseProxy, tmp)
		if err != nil {
			return err
		}
		fmt.Println(outCid)
		if err := os.RemoveAll(tmp); err != nil {
			return err
		}

		m := bson.M{"rootID": cid, "bucketID": outCid.String(), "bucketIdx": i}
		_, err = collection.InsertOne(context.Background(), m)
		if err != nil {
			return err
		}
	}

	return nil
}

type ipfsObject struct {
	path string
	cid  cid.Cid
	size int
}

func enumerateFiles(pth, id string, sh *shell.Shell, objs map[ipfsObject]struct{}) error {
	d, err := cid.Decode(id)
	if err != nil {
		return err
	}
	stat, err := sh.ObjectStat(id)
	if err != nil {
		return err
	}
	dataSize := stat.DataSize
	obj, err := sh.ObjectGet(id)
	if err != nil {
		return err
	}
	for _, link := range obj.Links {
		if link.Name != "" {
			if err := enumerateFiles(path.Join(pth, link.Name), link.Hash, sh, objs); err != nil {
				return err
			}
		} else {
			stat, err := sh.ObjectStat(link.Hash)
			if err != nil {
				return err
			}
			dataSize += stat.DataSize
		}
	}

	objs[ipfsObject{
		cid:  d,
		path: pth,
		size: dataSize,
	}] = struct{}{}
	return nil
}

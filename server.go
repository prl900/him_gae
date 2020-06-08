package main

import "bufio"
import "compress/bzip2"
import "context"
import "fmt"
import "io"
import "time"
import "log"
import "net/http"
import "os"

//import "strings"

import "github.com/jlaffaye/ftp"

import "google.golang.org/api/iterator"

import "cloud.google.com/go/storage"

//import "cloud.google.com/go/datastore"
//import "cloud.google.com/go/pubsub"

const (
	jaxaFTP = "ftp.ptree.jaxa.jp:21"
)

type BandRes struct {
	Band int
	Res  int
}

func update(w http.ResponseWriter, r *http.Request) {
	t := time.Now().UTC()
	t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), (t.Minute()/10)*10, 0, 0, time.UTC)

	for i := 0; i < 4; i++ {
		for _, br := range []BandRes{{1, 10}, {2, 10}, {3, 5}, {4, 10}} {
			for _, sec := range []int{7, 8, 9} {
				if err := getFile(t, br.Band, br.Res, sec); err != nil {
					log.Printf("Error retrieving file: %v", err)
				}
			}
		}
		t = t.Add(-time.Minute * 10)
	}
	if err := cleanBucket(); err != nil {
		http.Error(w, fmt.Sprintf("Error cleaning bucket: %v", err), 400)
	}

}

func getFile(t time.Time, band, res, sec int) error {
	fName := fmt.Sprintf("HS_H08_%s_B%02d_FLDK_R%02d_S%02d10.DAT.bz2", t.Format("20060102_1504"), band, res, sec)

	// Check file already exists in Cloud Storage
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	bkt := client.Bucket(os.Getenv("BUCKET_NAME"))
	obj := bkt.Object("himawari8/" + fName[:len(fName)-4])

	_, err = obj.Attrs(ctx)
	if err != storage.ErrObjectNotExist {
		return err
	}

	// Create connection to FTP server
	c, err := ftp.Dial(jaxaFTP, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		return err
	}

	if err := c.Login(os.Getenv("USER"), os.Getenv("PSWD")); err != nil {
		return err
	}

	dirName := fmt.Sprintf("/jma/hsd/%s/%s/%s/", t.Format("200601"), t.Format("02"), t.Format("15"))
	r, err := c.Retr(dirName + fName)
	if err != nil {
		return err
	}
	defer r.Close()

	// create a reader
	bufr := bufio.NewReader(r)
	// create a bzip2.reader, using the reader we just created
	bzr := bzip2.NewReader(bufr)

	w := obj.NewWriter(ctx)

	buf := make([]byte, 4096)
	for {
		// read a chunk
		n, err := bzr.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		// write a chunk
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
	}

	// Close Object writer
	if err := w.Close(); err != nil {
		return err
	}

	/*
	   if err := publish(fName[:len(fName)-4]); err != nil {
	           return err
	   }
	*/

	// Close FTP connection
	if err := c.Quit(); err != nil {
		return err
	}
	return nil
}

func cleanBucket() error {
	// Check file already exists in Cloud Storage
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	bkt := client.Bucket(os.Getenv("BUCKET_NAME"))
	it := bkt.Objects(ctx, nil)
	for {
		objAttrs, err := it.Next()
		if err != nil && err != iterator.Done {
			return err
		}
		if err == iterator.Done {
			break
		}
		if time.Now().Add(-1 * time.Hour).After(objAttrs.Created) {
			if err := bkt.Object(objAttrs.Name).Delete(ctx); err != nil {
				return err
			}
		}
	}

	return nil
}

func main() {
	//http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/update", update)
	log.Fatal(http.ListenAndServe(":"+os.Getenv("PORT"), nil))
}

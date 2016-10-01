package main

import (
	"encoding/csv"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

var (
	input         = flag.String("input", "", "images.csv 文件地址")
	output        = flag.String("output", "", "输出目录名")
	skipHeader    = flag.Bool("skip_header", true, "是否跳过 images.csv 文件的第一行 header")
	maxDownloader = flag.Int("max_downloaders", 100, "最大并发下载数")
	filter        = flag.String("filter", "0", "仅下载以此字符串为前缀的 file id")

	downloadedUrls uint64
)

type FetchInfo struct {
	Filename string
	Folder   string
	Url      string
}

func main() {
	flag.Parse()

	file, err := os.Open(*input)
	if err != nil {
		log.Fatalf("无法打开文件：%s", err)
	}
	defer file.Close()

	r := csv.NewReader(file)

	fetchChannel := make(chan FetchInfo, *maxDownloader)
	for i := 0; i < *maxDownloader; i++ {
		go fetcher(fetchChannel)
	}

	count := uint64(0)
	headerSkipped := false
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if count == 0 && *skipHeader && !headerSkipped {
			headerSkipped = true
			continue
		}

		if len(record) < 2 {
			log.Fatal("CSV 中列数小于 2 @ count = %d", count)
		}
		filename := record[0]
		url := record[1]

		if len(filename) < 2 {
			log.Fatal("file ID 长度小于 2")
		}
		folder := filename[:2]
		if strings.HasPrefix(filename, *filter) {
			continue
		}

		count++

		fetchChannel <- FetchInfo{
			Folder:   folder,
			Filename: filename,
			Url:      url,
		}
		if count%1000 == 0 {
			log.Printf("已经下载 %d 文件", count)
		}
	}

	// 等待全部下载完成
	for downloadedUrls < count {
		time.Sleep(10 * time.Second)
	}
}

func fetcher(ch chan FetchInfo) {
	for {
		info := <-ch
		atomic.AddUint64(&downloadedUrls, 1)

		folder := *output + "/" + info.Folder
		if _, err := os.Stat(folder); os.IsNotExist(err) {
			os.Mkdir(folder, 0777)
		}

		filename := folder + "/" + info.Filename
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			content, err := HTTPDownload(info.Url)
			if err != nil {
				log.Printf("文件下载错误 %s: %s", info.Url, err)
				atomic.AddUint64(&downloadedUrls, 1)
				continue
			}

			if err := WriteFile(filename, content); err != nil {
				log.Printf("文件写错误 %s: %s", info.Filename, err)
			}
			atomic.AddUint64(&downloadedUrls, 1)
		}
	}
}

func HTTPDownload(uri string) ([]byte, error) {
	res, err := http.Get(uri)
	if err != nil {
		return []byte{}, err
	}
	defer res.Body.Close()
	d, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return []byte{}, err
	}
	return d, err
}

func WriteFile(dst string, d []byte) error {
	err := ioutil.WriteFile(dst, d, 0444)
	return err
}

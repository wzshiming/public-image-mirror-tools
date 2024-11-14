package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/wzshiming/accesslog"
	accesslog_aliyun_oss "github.com/wzshiming/accesslog-aliyun-oss"
	"github.com/wzshiming/accesslog/tocsv"
	csv_sqlite "github.com/wzshiming/csv-sqlite"
	"github.com/wzshiming/geario"
	"golang.org/x/sync/errgroup"
)

const getSQL = `
SELECT
    RemoteIP AS ip,
    count(*) AS request_count,
    sum(BodySentBytes) AS got_bytes,
    RequestPath AS path
FROM csv
GROUP BY
    ip, path
HAVING
    (
        request_count > 2 AND
    	got_bytes > 2 * 1024 * 1024 * 1024
    ) OR (
        request_count > 8 AND
    	got_bytes > 1024 * 1024 * 1024
    ) OR (
        request_count > 32 AND
    	got_bytes > 100 * 1024 * 1024
    )
ORDER BY
    ip DESC
LIMIT 1000
`

var (
	cache           = "./cache"
	endpoint        string
	bucket          string
	accessKeyID     string
	accessKeySecret string

	startTime string
	endTime   string

	fields = []string{
		"RemoteIP",
		"RequestPath",
		"BodySentBytes",
	}
	condition = `self.RemoteIP != "10.10.0.10" && self.Operation == "GetObject"`
)

var DataFormat = "2006-01-02-15"

func init() {
	now := time.Now()
	endTime = now.Add(-time.Hour).Format(DataFormat)
	startTime = now.AddDate(0, 0, -1).Format(DataFormat)

	pflag.StringVar(&cache, "cache", cache, "cache")
	pflag.StringVar(&endpoint, "endpoint", "", "endpoint")
	pflag.StringVar(&bucket, "bucket", "", "bucket")
	pflag.StringVar(&accessKeyID, "access-key-id", "", "access key id")
	pflag.StringVar(&accessKeySecret, "access-key-secret", "", "access key secret")
	pflag.Parse()
}

type info struct {
	RequestCount int
	GotBytes     geario.B
	List         []string
}

type recorder struct {
	list map[string]*info
}

func newRecorder() *recorder {
	return &recorder{
		list: map[string]*info{},
	}
}

func (r *recorder) String() string {
	keys := make([]string, 0, len(r.list))
	for k := range r.list {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := bytes.NewBuffer(nil)

	w := csv.NewWriter(out)
	for _, k := range keys {
		v := r.list[k]
		w.Write([]string{
			k,
			fmt.Sprintf("(重复的请求过多) Duplicate Request Count %d, Bytes %s, targets %v, https://github.com/DaoCloud/public-image-mirror/issues/34109", v.RequestCount, v.GotBytes, cleanList(v.List)),
		})
	}
	w.Flush()
	return out.String()
}

func cleanList(s []string) []string {
	sort.Strings(s)
	for i := range s {
		list := strings.Split(s[i], "/")
		if len(list) > 2 {
			l := list[len(list)-2]
			if len(l) > 8 {
				l = l[:8] + "..."
			}
			s[i] = l
		}
	}
	return s
}

func (r *recorder) Write(record []string) error {
	if record[0] == "ip" {
		return nil
	}

	i := r.list[record[0]]
	if i == nil {
		i = &info{}
	}

	requestCount, err := strconv.Atoi(record[1])
	if err != nil {
		return err
	}

	i.RequestCount += requestCount

	gotBytes, err := strconv.Atoi(record[2])
	if err != nil {
		return err
	}

	i.GotBytes += geario.B(gotBytes)

	i.List = append(i.List, record[3])

	r.list[record[0]] = i
	return nil
}

func main() {
	ctx := context.Background()
	r, w := io.Pipe()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer w.Close()
		return run(
			fields,
			condition,
			startTime,
			endTime,
			endpoint,
			bucket,
			accessKeyID,
			accessKeySecret,
			cache,
			w,
		)
	})

	tmp := fmt.Sprintf("%s.%d.db", endTime, time.Now().Unix())

	g.Go(func() error {
		return csv_sqlite.CSV2DB(ctx, r, tmp, "csv")
	})
	err := g.Wait()
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmp)

	record := newRecorder()

	err = csv_sqlite.FromDB(context.Background(), tmp, record, getSQL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(record.String())

}

const dataFormat = "2006-01-02-15"

func run(
	fields []string,
	condition string,
	startTime string,
	endTime string,
	endpoint string,
	bucketName string,
	accessKeyID string,
	accessKeySecret string,
	cache string,
	output io.Writer,
) error {
	end, err := time.Parse(dataFormat, endTime)
	if err != nil {
		return err
	}

	start, err := time.Parse(dataFormat, startTime)
	if err != nil {
		return err
	}

	cacheHTTPClient := accesslog_aliyun_oss.NewCacheHTTPClient(nil, cache)

	client, err := accesslog_aliyun_oss.NewOSSClient(
		endpoint,
		accessKeyID,
		accessKeySecret,
		accesslog_aliyun_oss.HTTPClient(cacheHTTPClient),
	)
	if err != nil {
		return err
	}

	ch := make(chan accesslog_aliyun_oss.AccessLogFormatted, 128)

	go func() {
		defer close(ch)
		for i := start; i.Before(end); i = i.Add(time.Hour) {
			date := i.Format(dataFormat) + "-"
			err := accesslog_aliyun_oss.ProcessAccessLogWithClient(client, bucketName, date, func(entry accesslog.Entry[accesslog_aliyun_oss.AccessLog], err error) error {
				if err != nil {
					return err
				}
				f, err := entry.Entry().Formatted()
				if err != nil {
					return err
				}
				ch <- f
				return nil
			})
			if err != nil {
				log.Fatal("Error", err)
			}
		}
	}()

	return tocsv.ProcessToCSV[accesslog_aliyun_oss.AccessLogFormatted](output, condition, fields, ch)
}

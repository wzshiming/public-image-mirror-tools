package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/wzshiming/accesslog"
	accesslog_aliyun_oss "github.com/wzshiming/accesslog-aliyun-oss"
	"github.com/wzshiming/accesslog/tocsv"
	"github.com/wzshiming/accesslog/unsafeutils"
	csv_sqlite "github.com/wzshiming/csv-sqlite"
	"github.com/wzshiming/geario"
	"golang.org/x/sync/errgroup"
)

var (
	now             = time.Now()
	cache           = "./cache"
	endpoint        string
	bucket          string
	accessKeyID     string
	accessKeySecret string

	startTime string
	endTime   string

	condition = `self.RemoteIP != "10.10.0.10" && self.Operation == "GetObject"`
)

var DataFormat = "2006-01-02-15"

func init() {

	endTime = now.Format(DataFormat)
	startTime = now.Add(-8*time.Hour).AddDate(0, 0, -1).Format(DataFormat)

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

	tmp := fmt.Sprintf("%s.%d.db", endTime, now.Unix())

	g.Go(func() error {
		return csv_sqlite.CSV2DB(ctx, r, tmp, "csv")
	})
	err := g.Wait()
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmp)

	{
		record := newIpAndImageRecorder()
		err = csv_sqlite.FromDB(context.Background(), tmp, record, getIPAndImageSQL)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(record.String())
	}

	{
		record := newIpAndPathRecorder()
		err = csv_sqlite.FromDB(context.Background(), tmp, record, getIPAndPathSQL)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(record.String())
	}

	{
		record := newIpRecorder()
		err = csv_sqlite.FromDB(context.Background(), tmp, record, getIPSQL)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(record.String())
	}

	{
		record := newAbnormalIpRecorder()
		err = csv_sqlite.FromDB(context.Background(), tmp, record, getAbnormalIPSQL)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(record.String())
	}
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

	ch := make(chan accessLogFormatted, 128)

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

				out := accessLogFormatted{
					Operation:     f.Operation,
					RemoteIP:      f.RemoteIP,
					RequestPath:   f.RequestPath,
					BodySentBytes: f.BodySentBytes,
				}
				q, err := url.ParseQuery(f.RequestQuery)
				if err == nil && len(q) != 0 {
					referer := q["referer"]
					if len(referer) != 0 {
						r := strings.Split(referer[0], ":")
						if len(r) == 2 {
							out.PullRemoteIP = r[0]
							out.Image = r[1]
						}
					}
				}

				ch <- out
				return nil
			})
			if err != nil {
				log.Fatal("Error", err)
			}
		}
	}()

	return tocsv.ProcessToCSV[accessLogFormatted](output, condition, fields, ch)
}

type accessLogFormatted struct {
	Operation     string
	RemoteIP      string
	PullRemoteIP  string
	RequestPath   string
	Image         string
	BodySentBytes string
}

func (e accessLogFormatted) Values(fields []string) []string {
	accessLogEntryFieldsIndexMapping := unsafeutils.FieldsOffset[accessLogFormatted]()
	out := make([]string, len(fields))
	for i, f := range fields {
		offset, ok := accessLogEntryFieldsIndexMapping[f]
		if !ok {
			continue
		}
		out[i] = unsafeutils.GetWithOffset[string](&e, offset)
	}
	return out
}

var (
	fields = []string{
		"RemoteIP",
		"PullRemoteIP",
		"RequestPath",
		"Image",
		"BodySentBytes",
	}
)

package main

import (
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
)

var (
	cache           = "./cache"
	endpoint        string
	bucket          string
	accessKeyID     string
	accessKeySecret string

	startTime string
	endTime   string

	fields    = accesslog_aliyun_oss.AccessLogFormatted{}.Fields()
	condition string
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
	pflag.StringVar(&startTime, "start-time", startTime, "start time")
	pflag.StringVar(&endTime, "end-time", endTime, "end time")
	pflag.StringSliceVar(&fields, "field", fields, "fields")
	pflag.StringVar(&condition, "condition", condition, "condition")
	pflag.Parse()
}

func main() {
	err := run(
		fields,
		condition,
		startTime,
		endTime,
		endpoint,
		bucket,
		accessKeyID,
		accessKeySecret,
		cache,
	)
	if err != nil {
		log.Fatal(err)
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
					RemoteIP:                            f.RemoteIP,
					Time:                                f.Time,
					RequestMethod:                       f.RequestMethod,
					RequestScheme:                       f.RequestScheme,
					RequestHost:                         f.RequestHost,
					RequestPath:                         f.RequestPath,
					RequestQuery:                        f.RequestQuery,
					RequestProto:                        f.RequestProto,
					Status:                              f.Status,
					BodySentBytes:                       f.BodySentBytes,
					RequestTime:                         f.RequestTime,
					Referer:                             f.Referer,
					UserAgent:                           f.UserAgent,
					HostName:                            f.HostName,
					RequestID:                           f.RequestID,
					LoggingFlag:                         f.LoggingFlag,
					RequesterAliyunID:                   f.RequesterAliyunID,
					Operation:                           f.Operation,
					BucketName:                          f.BucketName,
					ObjectName:                          f.ObjectName,
					ObjectSize:                          f.ObjectSize,
					ServerCostTime:                      f.ServerCostTime,
					ErrorCode:                           f.ErrorCode,
					RequestLength:                       f.RequestLength,
					UserID:                              f.UserID,
					DeltaDataSize:                       f.DeltaDataSize,
					SyncRequest:                         f.SyncRequest,
					StorageClass:                        f.StorageClass,
					TargetStorageClass:                  f.TargetStorageClass,
					TransmissionAccelerationAccessPoint: f.TransmissionAccelerationAccessPoint,
					AccessKeyID:                         f.AccessKeyID,
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

	return tocsv.ProcessToCSV[accessLogFormatted](os.Stdout, condition, fields, ch)
}

type accessLogFormatted struct {
	RemoteIP                            string
	Time                                string
	RequestMethod                       string
	RequestScheme                       string
	RequestHost                         string
	RequestPath                         string
	RequestQuery                        string
	RequestProto                        string
	Status                              string
	BodySentBytes                       string
	RequestTime                         string
	Referer                             string
	UserAgent                           string
	HostName                            string
	RequestID                           string
	LoggingFlag                         string
	RequesterAliyunID                   string
	Operation                           string
	BucketName                          string
	ObjectName                          string
	ObjectSize                          string
	ServerCostTime                      string
	ErrorCode                           string
	RequestLength                       string
	UserID                              string
	DeltaDataSize                       string
	SyncRequest                         string
	StorageClass                        string
	TargetStorageClass                  string
	TransmissionAccelerationAccessPoint string
	AccessKeyID                         string

	PullRemoteIP string
	Image        string
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

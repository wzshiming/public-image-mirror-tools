package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"sort"
	"strconv"

	"github.com/wzshiming/geario"
)

const getIPAndImageSQL = `
SELECT
    RemoteIP AS ip,
    count(*) AS request_count,
    sum(BodySentBytes) AS got_bytes,
    Image AS image
FROM csv
WHERE
    image != ""
GROUP BY
    ip, image
HAVING
    (
        request_count > 50 AND
    	got_bytes > 10 * 1024 * 1024
    )
`

type ipAndImageRecorder struct {
	list map[string]*info
}

func newIpAndImageRecorder() *ipAndImageRecorder {
	return &ipAndImageRecorder{
		list: map[string]*info{},
	}
}

func (r *ipAndImageRecorder) String() string {
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
			fmt.Sprintf("(近 24 小时, 重复拉取同一镜像的不同 tags) Duplicate Request Count %d, Bytes %s, targets %v, https://github.com/DaoCloud/public-image-mirror/issues/34109", v.RequestCount, v.GotBytes, v.List),
		})
	}
	w.Flush()
	return out.String()
}

func (r *ipAndImageRecorder) Write(record []string) error {
	if record[0] == "ip" {
		return nil
	}

	i := r.list[record[0]]
	if i == nil {
		i = &info{}
		r.list[record[0]] = i
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

	return nil
}

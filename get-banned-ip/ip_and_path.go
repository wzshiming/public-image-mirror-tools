package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/wzshiming/geario"
)

const getIPAndPathSQL = `
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
        request_count > 1 AND
    	got_bytes > 100 * 1024 * 1024
    ) OR (
        request_count > 2 AND
    	got_bytes > 10 * 1024 * 1024
    )
`

type ipAndPathRecorder struct {
	list map[string]*info
}

func newIpAndPathRecorder() *ipAndPathRecorder {
	return &ipAndPathRecorder{
		list: map[string]*info{},
	}
}

func (r *ipAndPathRecorder) String() string {
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
			fmt.Sprintf("(近 24 小时, 重复拉取同一镜像, 请缓存镜像) Duplicate Request Count %d, Bytes %s, targets %v, https://github.com/DaoCloud/public-image-mirror/issues/34109", v.RequestCount, v.GotBytes, cleanList(v.List)),
		})
	}
	w.Flush()
	return out.String()
}

func (r *ipAndPathRecorder) Write(record []string) error {
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

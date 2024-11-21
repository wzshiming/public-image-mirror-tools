package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"sort"
	"strconv"

	"github.com/wzshiming/geario"
)

const getIPSQL = `
SELECT
    RemoteIP AS ip,
    count(*) AS request_count,
    sum(BodySentBytes) AS got_bytes
FROM csv
GROUP BY
    ip
HAVING
    (
        got_bytes > 4 * 1024 * 1024 * 1024
    ) OR (
        request_count > 1024 AND
    	got_bytes > 10 * 1024 * 1024
    )
`

type ipRecorder struct {
	list map[string]*info
}

func newIpRecorder() *ipRecorder {
	return &ipRecorder{
		list: map[string]*info{},
	}
}

func (r *ipRecorder) String() string {
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
			fmt.Sprintf("(近 24 小时, 拉取次数或流量过多) Request Count %d, Bytes %s, https://github.com/DaoCloud/public-image-mirror/issues/34109", v.RequestCount, v.GotBytes),
		})

	}
	w.Flush()
	return out.String()
}

func (r *ipRecorder) Write(record []string) error {
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

	r.list[record[0]] = i
	return nil
}

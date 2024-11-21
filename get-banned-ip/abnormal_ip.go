package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"slices"
	"sort"
	"strconv"

	"github.com/wzshiming/geario"
)

const getAbnormalIPSQL = `
SELECT
    RemoteIP AS ip,
    PullRemoteIP AS pull_ip,
    count(*) AS request_count,
	sum(BodySentBytes) AS got_bytes
FROM csv
WHERE
    pull_ip != "" AND pull_ip != ip
GROUP BY
    ip, pull_ip
`

type abnormalIpRecorder struct {
	list map[string]*info
}

func newAbnormalIpRecorder() *abnormalIpRecorder {
	return &abnormalIpRecorder{
		list: map[string]*info{},
	}
}

func (r *abnormalIpRecorder) String() string {
	keys := make([]string, 0, len(r.list))
	for k := range r.list {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := bytes.NewBuffer(nil)

	w := csv.NewWriter(out)
	for _, k := range keys {
		v := r.list[k]

		list := append([]string{k}, v.List...)
		sort.Strings(list)

		w.Write([]string{
			k,
			fmt.Sprintf("(禁用关联 IP 组) Request Count %d, Bytes %s, ip %s, https://github.com/DaoCloud/public-image-mirror/issues/34109", v.RequestCount, v.GotBytes, list),
		})

	}
	w.Flush()
	return out.String()
}

func (r *abnormalIpRecorder) Write(record []string) error {
	if record[0] == "ip" {
		return nil
	}

	requestCount, err := strconv.Atoi(record[2])
	if err != nil {
		return err
	}

	gotBytes, err := strconv.Atoi(record[2])
	if err != nil {
		return err
	}

	i := r.list[record[0]]
	if i == nil {
		i = &info{}
	}
	i.RequestCount += requestCount
	i.GotBytes += geario.B(gotBytes)

	if !slices.Contains(i.List, record[1]) {
		i.List = append(i.List, record[1])
	}
	r.list[record[0]] = i

	for _, ip := range i.List {
		i := r.list[ip]
		if i == nil {
			i = &info{}
		}
		i.RequestCount += requestCount
		i.GotBytes += geario.B(gotBytes)

		if !slices.Contains(i.List, record[0]) {
			i.List = append(i.List, record[0])
		}
		r.list[ip] = i
	}

	return nil
}

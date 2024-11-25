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
	ipGroup []*info
	list    map[string]int
}

func newAbnormalIpRecorder() *abnormalIpRecorder {
	return &abnormalIpRecorder{
		list: map[string]int{},
	}
}

func (r *abnormalIpRecorder) SQL() string {
	return getAbnormalIPSQL
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
		index := r.list[k]

		v := r.ipGroup[index]

		w.Write([]string{
			k,
			fmt.Sprintf("(禁用关联 IP 组) Request Count %d, Bytes %s, ip %s, https://github.com/DaoCloud/public-image-mirror/issues/34109", v.RequestCount, v.GotBytes, v.List),
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

	gotBytes, err := strconv.Atoi(record[3])
	if err != nil {
		return err
	}

	var i *info
	index, ok := r.list[record[0]]
	if !ok {
		i = &info{
			List: []string{record[0]},
		}
		r.ipGroup = append(r.ipGroup, i)

		index = len(r.ipGroup) - 1
	} else {
		i = r.ipGroup[index]
	}
	i.RequestCount += requestCount
	i.GotBytes += geario.B(gotBytes)

	if !slices.Contains(i.List, record[1]) {
		i.List = append(i.List, record[1])
		sort.Strings(i.List)
	}

	for _, ip := range i.List {
		vindex, ok := r.list[ip]
		if ok {
			if vindex != index {
				r.ipGroup[index].RequestCount += r.ipGroup[vindex].RequestCount
				r.ipGroup[index].GotBytes += r.ipGroup[vindex].GotBytes

				for _, vip := range r.ipGroup[vindex].List {
					if !slices.Contains(r.ipGroup[index].List, vip) {
						r.ipGroup[index].List = append(r.ipGroup[index].List, vip)
						sort.Strings(r.ipGroup[index].List)
					}
					r.list[vip] = index
				}
				r.ipGroup[vindex] = nil
			}
		} else {
			r.list[ip] = index
		}
	}

	return nil
}

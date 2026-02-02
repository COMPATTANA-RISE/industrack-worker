package mqtt

import (
	"strconv"
	"time"
)

// parseTime supports RFC3339 string, Unix seconds, or Unix milliseconds.
func parseTime(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err == nil {
		return &t, nil
	}
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, err
	}
	// 13+ digits = milliseconds (e.g. 1770014810462)
	if val > 1e12 || val < -1e12 {
		sec := val / 1000
		nsec := (val % 1000) * 1e6
		if nsec < 0 {
			nsec += 1e9
		}
		ts := time.Unix(sec, nsec)
		return &ts, nil
	}
	ts := time.Unix(val, 0)
	return &ts, nil
}

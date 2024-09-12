package util

import "strings"

func NormalizeString(s string) string {
	s = strings.ReplaceAll(s, " ", "_")
	return strings.ToLower(s)
}

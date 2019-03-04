package promtable

import "strings"

var escapeReplacer = strings.NewReplacer(
	"#", "%23",
	",", "%2c",
	"=", "%3d",
)

var unescapeReplacer = strings.NewReplacer(
	"%23", "#",
	"%2c", ",",
	"%3d", "=",
)

// EscapeLabelValue -
// # -> %23
// , -> %2C
func EscapeLabelValue(v string) string {
	return escapeReplacer.Replace(v)
}

// UnescapeLabelValue -
func UnescapeLabelValue(v string) string {
	return unescapeReplacer.Replace(v)
}

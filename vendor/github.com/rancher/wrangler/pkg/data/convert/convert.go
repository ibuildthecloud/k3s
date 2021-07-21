package convert

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func Singular(value interface{}) interface{} {
	if slice, ok := value.([]string); ok {
		if len(slice) == 0 {
			return nil
		}
		return slice[0]
	}
	if slice, ok := value.([]interface{}); ok {
		if len(slice) == 0 {
			return nil
		}
		return slice[0]
	}
	return value
}

func ToStringNoTrim(value interface{}) string {
	if t, ok := value.(time.Time); ok {
		return t.Format(time.RFC3339)
	}
	single := Singular(value)
	if single == nil {
		return ""
	}
	return fmt.Sprint(single)
}

func ToString(value interface{}) string {
	return strings.TrimSpace(ToStringNoTrim(value))
}

func ToTimestamp(value interface{}) (int64, error) {
	str := ToString(value)
	if str == "" {
		return 0, errors.New("invalid date")
	}
	t, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return 0, err
	}
	return t.UnixNano() / 1000000, nil
}

func ToBool(value interface{}) bool {
	value = Singular(value)

	b, ok := value.(bool)
	if ok {
		return b
	}

	str := strings.ToLower(ToString(value))
	return str == "true" || str == "t" || str == "yes" || str == "y"
}

func ToNumber(value interface{}) (int64, error) {
	value = Singular(value)

	i, ok := value.(int64)
	if ok {
		return i, nil
	}
	f, ok := value.(float64)
	if ok {
		return int64(f), nil
	}
	if n, ok := value.(json.Number); ok {
		i, err := n.Int64()
		if err == nil {
			return i, nil
		}
		f, err := n.Float64()
		return int64(f), err
	}
	return strconv.ParseInt(ToString(value), 10, 64)
}

func ToFloat(value interface{}) (float64, error) {
	value = Singular(value)

	f64, ok := value.(float64)
	if ok {
		return f64, nil
	}

	f32, ok := value.(float32)
	if ok {
		return float64(f32), nil
	}

	if n, ok := value.(json.Number); ok {
		i, err := n.Int64()
		if err == nil {
			return float64(i), nil
		}
		f, err := n.Float64()
		return float64(f), err
	}
	return strconv.ParseFloat(ToString(value), 64)
}

func Capitalize(s string) string {
	if len(s) <= 1 {
		return strings.ToUpper(s)
	}

	return strings.ToUpper(s[:1]) + s[1:]
}

func Uncapitalize(s string) string {
	if len(s) <= 1 {
		return strings.ToLower(s)
	}

	return strings.ToLower(s[:1]) + s[1:]
}

func LowerTitle(input string) string {
	runes := []rune(input)
	for i := 0; i < len(runes); i++ {
		if unicode.IsUpper(runes[i]) &&
			(i == 0 ||
				i == len(runes)-1 ||
				unicode.IsUpper(runes[i+1])) {
			runes[i] = unicode.ToLower(runes[i])
		} else {
			break
		}
	}

	return string(runes)
}

func IsEmptyValue(v interface{}) bool {
	if v == nil || v == "" || v == 0 || v == false {
		return true
	}
	if m, ok := v.(map[string]interface{}); ok {
		return len(m) == 0
	}
	if s, ok := v.([]interface{}); ok {
		return len(s) == 0
	}
	return false
}

func ToMapInterface(obj interface{}) map[string]interface{} {
	v, _ := obj.(map[string]interface{})
	return v
}

func ToInterfaceSlice(obj interface{}) []interface{} {
	if v, ok := obj.([]interface{}); ok {
		return v
	}
	return nil
}

func ToMapSlice(obj interface{}) []map[string]interface{} {
	if v, ok := obj.([]map[string]interface{}); ok {
		return v
	}
	vs, _ := obj.([]interface{})
	var result []map[string]interface{}
	for _, item := range vs {
		if v, ok := item.(map[string]interface{}); ok {
			result = append(result, v)
		} else {
			return nil
		}
	}

	return result
}

func ToStringSlice(data interface{}) []string {
	if v, ok := data.([]string); ok {
		return v
	}
	if v, ok := data.([]interface{}); ok {
		var result []string
		for _, item := range v {
			result = append(result, ToString(item))
		}
		return result
	}
	if v, ok := data.(string); ok {
		return []string{v}
	}
	return nil
}

func ToObj(data interface{}, into interface{}) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, into)
}

func EncodeToMap(obj interface{}) (map[string]interface{}, error) {
	if m, ok := obj.(map[string]interface{}); ok {
		return m, nil
	}

	if unstr, ok := obj.(*unstructured.Unstructured); ok {
		return unstr.Object, nil
	}

	b, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	result := map[string]interface{}{}
	dec := json.NewDecoder(bytes.NewBuffer(b))
	dec.UseNumber()
	return result, dec.Decode(&result)
}

func ToJSONKey(str string) string {
	parts := strings.Split(str, "_")
	for i := 1; i < len(parts); i++ {
		parts[i] = strings.Title(parts[i])
	}

	return strings.Join(parts, "")
}

func ToYAMLKey(str string) string {
	var result []rune
	cap := false

	for i, r := range []rune(str) {
		if i == 0 {
			if unicode.IsUpper(r) {
				cap = true
			}
			result = append(result, unicode.ToLower(r))
			continue
		}

		if unicode.IsUpper(r) {
			if cap {
				result = append(result, unicode.ToLower(r))
			} else {
				result = append(result, '_', unicode.ToLower(r))
			}
		} else {
			cap = false
			result = append(result, r)
		}
	}

	return string(result)
}

func ToArgKey(str string) string {
	var (
		result []rune
		input  = []rune(str)
	)
	cap := false

	for i := 0; i < len(input); i++ {
		r := input[i]
		if i == 0 {
			if unicode.IsUpper(r) {
				cap = true
			}
			result = append(result, unicode.ToLower(r))
			continue
		}

		if unicode.IsUpper(r) {
			if cap {
				result = append(result, unicode.ToLower(r))
			} else if len(input) > i+2 &&
				unicode.IsUpper(input[i]) &&
				unicode.IsUpper(input[i+1]) &&
				unicode.IsUpper(input[i+2]) {
				result = append(result, '-',
					unicode.ToLower(input[i]),
					unicode.ToLower(input[i+1]),
					unicode.ToLower(input[i+2]))
				i += 2
			} else {
				result = append(result, '-', unicode.ToLower(r))
			}
		} else {
			cap = false
			result = append(result, r)
		}
	}

	return "--" + string(result)
}

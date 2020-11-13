package segmentwriter

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func setDefault(p interface{}) error {
	if reflect.TypeOf(p).Kind() != reflect.Ptr {
		return fmt.Errorf("Not a pointer")
	}

	v := reflect.ValueOf(p).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		if defaultVal := t.Field(i).Tag.Get("default"); defaultVal != "-" && defaultVal != "" {
			if err := setField(v.Field(i), defaultVal); err != nil {
				return err
			}

		}
	}
	return nil
}

func setField(field reflect.Value, defaultVal string) error {

	if !field.CanSet() {
		return fmt.Errorf("can't set value")
	}

	switch field.Kind() {

	case reflect.Int:
		if val, err := strconv.ParseInt(defaultVal, 10, 64); err == nil {
			field.Set(reflect.ValueOf(int(val)).Convert(field.Type()))
		}
	case reflect.Float32:
		if val, err := strconv.ParseFloat(defaultVal, 32); err == nil {
			field.Set(reflect.ValueOf(float32(val)).Convert(field.Type()))
		}
	case reflect.Float64:
		if val, err := strconv.ParseFloat(defaultVal, 64); err == nil {
			field.Set(reflect.ValueOf(float64(val)).Convert(field.Type()))
		}
	case reflect.String:
		field.Set(reflect.ValueOf(defaultVal).Convert(field.Type()))
	}

	return nil
}

func format(p interface{}) string {
	if reflect.TypeOf(p).Kind() != reflect.Ptr {
		return ""
	}
	var builder strings.Builder
	v := reflect.ValueOf(p).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		vField := v.Field(i)
		tField := t.Field(i)
		width := 0
		widthTag := tField.Tag.Get("width")
		if widthTag != "" {
			width, _ = strconv.Atoi(widthTag)
		}
		vValue := vField.String()
		switch vField.Kind() {

		case reflect.Int:
			vValue = strconv.FormatInt(vField.Int(), 10)
		case reflect.Float32:
			vValue = fmt.Sprintf("%f", vField.Float())
		case reflect.Float64:
			vValue = fmt.Sprintf("%f", vField.Float())
		}

		if width > 0 {
			if len(vValue) > width {
				builder.WriteString(vValue[:width])
			} else {
				builder.WriteString(fmt.Sprintf("%-"+widthTag+"v", vValue))
			}

		}
	}

	return builder.String()
}

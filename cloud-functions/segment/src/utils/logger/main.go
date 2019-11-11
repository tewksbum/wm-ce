package logger

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

// Fatal logs error level messages, returns the error so it can be chained
func Fatal(err error) error {
	return errors.New(Log("Fatal", "%s", err.Error()))
}

// Err logs error level messages, returns the error so it can be chained
func Err(err error) error {
	return errors.New(Log("Error", "%s", err.Error()))
}

// ErrStr logs error level messages, returns the error so it can be chained
func ErrStr(err string) error {
	return errors.New(Log("Error", "%s", err))
}

// ErrFmt logs error level messages, returns the error so it can be chained
func ErrFmt(fmtstr string, err error) error {
	return errors.New(Log("Error", "%s", fmt.Sprintf(fmtstr, err.Error())))
}

// ErrFmtStr logs error level messages, returns the error so it can be chained
func ErrFmtStr(fmtstr string, args ...interface{}) error {
	return errors.New(Log("Error", "%s", fmt.Sprintf(fmtstr, args...)))
}

// Info logs info level messages
func Info(msg ...string) {
	Log("Info", "%s", strings.Join(msg, " "))
}

// InfoFmt logs info level formatted messages
func InfoFmt(fmtstr string, args ...interface{}) {
	Log("Info", fmtstr, args...)
}

// Log main logging func, returns the formatted message string
func Log(level string, msg string, args ...interface{}) string {
	strmsg := fmt.Sprintf(msg, args...)
	logmsg := fmt.Sprintf("%s ["+level+"]: %s", time.Now().UTC().String(), strmsg)
	log.Println(logmsg)
	return strmsg
}

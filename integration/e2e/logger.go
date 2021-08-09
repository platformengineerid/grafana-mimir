// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/e2e/logger.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package e2e

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
)

// Global logger to use in integration tests. We use a global logger to simplify
// writing integration tests and avoiding having to pass the logger instance
// every time.
var logger log.Logger

func init() {
	logger = NewLogger(os.Stdout)
}

type Logger struct {
	w io.Writer
}

func NewLogger(w io.Writer) *Logger {
	return &Logger{
		w: w,
	}
}

func (l *Logger) Log(keyvals ...interface{}) error {
	log := strings.Builder{}
	log.WriteString(time.Now().Format("15:04:05"))

	for _, v := range keyvals {
		log.WriteString(" " + fmt.Sprint(v))
	}

	log.WriteString("\n")

	_, err := l.w.Write([]byte(log.String()))
	return err
}

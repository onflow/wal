// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

type logger struct {
	t   *testing.T
	buf bytes.Buffer
}

func (l *logger) Write(p []byte) (n int, err error) {
	n, err = l.buf.Write(p)
	if err != nil {
		return n, err
	}

	if p[len(p)-1] == '\n' {
		lines := strings.Split(l.buf.String(), "\n")
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if trimmed != "" {
				l.t.Log(trimmed)
			}
		}
		l.buf.Reset()
	}

	return
}

// NewLogger returns a zerolog compatible logger
func NewLogger(t *testing.T) zerolog.Logger {

	consoleWriter := zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.Out = &logger{t: t}
	})

	return zerolog.New(consoleWriter)
}

/*
 * Copyright (C) 2019 Yunify, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this work except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file, or at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package utils

import (
	"context"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"runtime/debug"
)

func GoRecover(f func()) {
	go func() {
		defer Recover()
		f()
	}()
}

// ForeverLoop run forever until received value from stop chan
func Loop(ctx context.Context, f func()) {
	for {
		select {
		default:
			func() {
				defer func() {
					if p := recover(); p != nil {
						debug.PrintStack()
					}
				}()
				f()
			}()
		case <-ctx.Done():
			return
		}
	}
}

// ForeverLoop run forever until received value from stop chan
func ForeverLoop(f func(), stop chan int) {
	for {
		select {
		default:
			func() {
				defer func() {
					if p := recover(); p != nil {
						debug.PrintStack()
					}
				}()
				f()
			}()
		case <-stop:
			return
		}
	}
}

//Recover use in the following case:
// (1) call another module's func to avoid not panic itself, like update in observer and processor in utils.Routine
// (2) when routine panic, but the process not run over , like GoRecover
func Recover() {
	if p := recover(); p != nil {
		log.Error("recover ",
			logf.Any("panic", p),
			logf.String("stack", string(debug.Stack())))
	}
}

func RecoverDo(f func(string)) {
	if p := recover(); p != nil {
		stackInfo := string(debug.Stack())
		log.Error("recover ",
			logf.Any("panic", p),
			logf.String("stack", stackInfo))
		f(stackInfo)
	}
}

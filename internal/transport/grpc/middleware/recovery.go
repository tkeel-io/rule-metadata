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

package middleware

import (
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-metadata/internal/utils"
	"runtime"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	MAXSTACKSIZE = 4096
)

func Recovery(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, MAXSTACKSIZE)
			stack = stack[:runtime.Stack(stack, false)]
			utils.Logger.For(ctx).Error("panic grpc invoke",
				logf.String("FullMethod", info.FullMethod),
				logf.Any("stack", stack),
				logf.Any("err", r))

			err = status.Errorf(codes.Internal, "panic error: %v", r)
		}
	}()

	return handler(ctx, req)
}

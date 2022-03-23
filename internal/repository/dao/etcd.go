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

package dao

import (
	"context"
	"time"

	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-metadata/internal/conf"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// NewEtcdDB create new etcd db
func NewEtcdDB(c *conf.EtcdConfig) (*clientv3.Client, error) {

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   c.Endpoints,
		DialTimeout: time.Duration(c.DialTimeout),
	})

	if err != nil {
		return nil, err
	}

	return client, nil
}

func (d *Dao) GetLastRevision(ctx context.Context) (int64, error) {
	n, err := d.etcd.MemberList(ctx)
	if err != nil {
		d.logger.For(ctx).Error("etcd member list error",
			logf.Error(err))
	}

	rev := int64(0)
	for _, node := range n.Members {
		for _, url := range node.ClientURLs {
			resp, err := d.etcd.Status(ctx, url)
			if err != nil {
				continue
			}
			if resp.Header.Revision == 0 {
				d.logger.For(ctx).Fatal("zero revision")
			}
			if rev == 0 || rev > resp.Header.Revision {
				rev = resp.Header.Revision
			}
		}
		d.logger.For(ctx).Info("etcd members",
			logf.Any("node", node))
	}
	return rev, nil
}

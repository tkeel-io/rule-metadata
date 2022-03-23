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

package repository

import (
	"context"
	"errors"
	"time"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
)

var (
	ErrRuleActionEmpty   = errors.New("rule's action is empty")
	ErrDeleteRunningRule = errors.New("delete running rule")
)

func (rp *Repository) NewRule(ctx context.Context, rule *metapb.RuleQL) (err error) {
	if len(rule.Actions) == 0 {
		return ErrRuleActionEmpty
	}

	has, err := rp.dao.HasRule(ctx, rule)
	if has {
		return ErrDeleteRunningRule
	}
	if !has || err == nil {
		err = rp.dao.PutRule(ctx, rule)
	}
	return
}

func (rp *Repository) DelRule(ctx context.Context, rule *metapb.RuleQL) (err error) {
	err = rp.dao.DelRule(ctx, rule)
	return
}

func (rp *Repository) GetRuleRevision(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
	defer cancel()
	return rp.dao.GetRuleRevision(ctx)
}

func (rp *Repository) RuleRange(ctx context.Context, rev int64) (<-chan []*metapb.RuleValue, chan error) {
	respchan := make(chan []*metapb.RuleValue, 1024)
	errchan := make(chan error, 1)

	// check
	go func(rp *Repository) {
		defer close(respchan)
		defer close(errchan)

		if rp.file != nil {
			rp.file.RangeRule(ctx, respchan, errchan) // TODO remove
		}

		rp.dao.RangeRule(ctx, rev, respchan, errchan)
	}(rp)

	return respchan, errchan
}

func (rp *Repository) RuleWatch(ctx context.Context, rev int64) (<-chan []*metapb.RuleEvent, chan error) {
	respchan := make(chan []*metapb.RuleEvent, 1024)
	errchan := make(chan error, 1)

	go func(rp *Repository) {
		defer close(respchan)
		defer close(errchan)

		rp.dao.WatchRule(ctx, rev, respchan, errchan)
	}(rp)

	return respchan, errchan
}

func (rp *Repository) CountRule(ctx context.Context) int {
	var count int = 0
	respchan := make(chan []*metapb.RuleValue, 1024)
	errchan := make(chan error, 1)

	ctx, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
	defer cancel()
	rev, _ := rp.dao.GetRuleRevision(ctx)
	// check
	go func(rp *Repository) {
		defer close(respchan)
		defer close(errchan)

		if rp.file != nil {
			rp.file.RangeRule(ctx, respchan, errchan) // TODO remove
		}

		rp.dao.RangeRule(ctx, rev, respchan, errchan)
	}(rp)

	for res := range respchan {
		count += len(res)
	}

	return count
}

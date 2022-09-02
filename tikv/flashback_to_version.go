// Copyright 2022 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"context"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
)

const (
	CONCURRENCY                  = 32
	flashbackToVersionMaxBackoff = 100000
)

// FlashbackToVersion will flashback the keys in [startKey, endKey) to the given version.
func (s *KVStore) FlashbackToVersion(
	ctx context.Context,
	version uint64,
	startKey []byte, endKey []byte,
	concurrency int,
) (err error) {
	return rangetask.NewRangeTaskRunner(
		"flashback-to-version-runner",
		s,
		concurrency,
		func(ctx context.Context, r kv.KeyRange) (rangetask.TaskStat, error) {
			return s.flashbackToVersion(ctx, version, r)
		},
	).RunOnRange(ctx, startKey, endKey)
}

func (t *KVStore) flashbackToVersion(
	ctx context.Context,
	version uint64,
	r kv.KeyRange,
) (rangetask.TaskStat, error) {
	startKey, rangeEndKey := r.StartKey, r.EndKey
	var taskStat rangetask.TaskStat
	for {
		select {
		case <-ctx.Done():
			return taskStat, errors.WithStack(ctx.Err())
		default:
		}

		if len(rangeEndKey) > 0 && bytes.Compare(startKey, rangeEndKey) >= 0 {
			break
		}

		bo := retry.NewBackofferWithVars(ctx, flashbackToVersionMaxBackoff, nil)
		loc, err := t.GetRegionCache().LocateKey(bo, startKey)
		if err != nil {
			return taskStat, err
		}

		endKey := loc.EndKey
		isLast := len(endKey) == 0 || (len(rangeEndKey) > 0 && bytes.Compare(endKey, rangeEndKey) >= 0)
		// If it is the last region
		if isLast {
			endKey = rangeEndKey
		}

		req := tikvrpc.NewRequest(tikvrpc.CmdFlashbackToVersion, &kvrpcpb.FlashbackToVersionRequest{
			Version:  version,
			StartKey: startKey,
			EndKey:   endKey,
		})

		resp, err := t.SendReq(bo, req, loc.Region, client.ReadTimeoutMedium)
		if err != nil {
			return taskStat, err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return taskStat, err
		}
		if regionErr != nil {
			err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return taskStat, err
			}
			continue
		}
		if resp.Resp == nil {
			return taskStat, errors.WithStack(tikverr.ErrBodyMissing)
		}
		flashbackToVersionResp := resp.Resp.(*kvrpcpb.FlashbackToVersionResponse)
		if err := flashbackToVersionResp.GetError(); err != "" {
			return taskStat, errors.Errorf("unexpected flashback to version err: %v", err)
		}
		taskStat.CompletedRegions++
		if isLast {
			break
		}
		startKey = endKey
	}
	return taskStat, nil
}

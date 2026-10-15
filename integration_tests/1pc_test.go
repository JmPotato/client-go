// Copyright 2021 TiKV Authors
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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/1pc_test.go
//

// Copyright 2020 PingCAP, Inc.
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

package tikv_test

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

func TestOnePC(t *testing.T) {
	suite.Run(t, new(testOnePCSuite))
}

type testOnePCSuite struct {
	testAsyncCommitCommon
	bo *tikv.Backoffer
}

func (s *testOnePCSuite) SetupTest() {
	s.testAsyncCommitCommon.setUpTest()
	s.bo = tikv.NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testOnePCSuite) TearDownTest() {
	s.testAsyncCommitCommon.tearDownTest()
}

func (s *testOnePCSuite) key(name string) []byte {
	return encodeKey("~1pc", name)
}

func (s *testOnePCSuite) Test1PC() {
	k1 := s.key("k1")
	v1 := []byte("v1")

	txn := s.begin1PC()
	err := txn.Set(k1, v1)
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	s.True(txn.GetCommitter().IsOnePC())
	s.Equal(txn.GetCommitter().GetOnePCCommitTS(), txn.GetCommitter().GetCommitTS())
	s.Greater(txn.GetCommitter().GetOnePCCommitTS(), txn.StartTS())
	// ttlManager is not used for 1PC.
	s.True(txn.GetCommitter().IsTTLUninitialized())

	// 1PC doesn't work if 1PC option is not set

	k2 := s.key("k2")
	v2 := []byte("v2")

	txn = s.begin()
	err = txn.Set(k2, v2)
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	s.False(txn.GetCommitter().IsOnePC())
	s.Equal(txn.GetCommitter().GetOnePCCommitTS(), uint64(0))
	s.Greater(txn.GetCommitter().GetCommitTS(), txn.StartTS())

	// Test multiple keys
	k3 := s.key("k3")
	v3 := []byte("v3")
	k4 := s.key("k4")
	v4 := []byte("v4")
	k5 := s.key("k5")
	v5 := []byte("v5")

	txn = s.begin1PC()
	err = txn.Set(k3, v3)
	s.Nil(err)
	err = txn.Set(k4, v4)
	s.Nil(err)
	err = txn.Set(k5, v5)
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	s.True(txn.GetCommitter().IsOnePC())
	s.Equal(txn.GetCommitter().GetOnePCCommitTS(), txn.GetCommitter().GetCommitTS())
	s.Greater(txn.GetCommitter().GetOnePCCommitTS(), txn.StartTS())
	// Check keys are committed with the same version
	s.mustGetFromSnapshot(txn.CommitTS(), k3, v3)
	s.mustGetFromSnapshot(txn.CommitTS(), k4, v4)
	s.mustGetFromSnapshot(txn.CommitTS(), k5, v5)
	s.mustGetNoneFromSnapshot(txn.CommitTS()-1, k3)
	s.mustGetNoneFromSnapshot(txn.CommitTS()-1, k4)
	s.mustGetNoneFromSnapshot(txn.CommitTS()-1, k5)

	// Overwriting in MVCC
	v5New := []byte("v5new")
	txn = s.begin1PC()
	err = txn.Set(k5, v5New)
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	s.True(txn.GetCommitter().IsOnePC())
	s.Equal(txn.GetCommitter().GetOnePCCommitTS(), txn.GetCommitter().GetCommitTS())
	s.Greater(txn.GetCommitter().GetOnePCCommitTS(), txn.StartTS())
	s.mustGetFromSnapshot(txn.CommitTS(), k5, v5New)
	s.mustGetFromSnapshot(txn.CommitTS()-1, k5, v5)

	// Check all keys
	keys := [][]byte{k1, k2, k3, k4, k5}
	values := [][]byte{v1, v2, v3, v4, v5New}
	ver, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.Nil(err)
	snap := s.store.GetSnapshot(ver)
	for i, k := range keys {
		v, err := snap.Get(context.Background(), k)
		s.Nil(err)
		s.Equal(v.Value, values[i])
	}
}

func (s *testOnePCSuite) Test1PCIsolation() {
	k := s.key("isolation_k")
	v1 := []byte("v1")

	txn := s.begin1PC()
	txn.Set(k, v1)
	err := txn.Commit(context.Background())
	s.Nil(err)

	v2 := []byte("v2")
	txn = s.begin1PC()
	txn.Set(k, v2)

	// Make `txn`'s commitTs more likely to be less than `txn2`'s startTs if there's bug in commitTs
	// calculation.
	for i := 0; i < 10; i++ {
		_, err := s.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		s.Nil(err)
	}

	txn2 := s.begin1PC()
	s.mustGetFromTxn(txn2, k, v1)

	err = txn.Commit(context.Background())
	s.True(txn.GetCommitter().IsOnePC())
	s.Nil(err)

	s.mustGetFromTxn(txn2, k, v1)
	s.Nil(txn2.Rollback())

	s.mustGetFromSnapshot(txn.CommitTS(), k, v2)
	s.mustGetFromSnapshot(txn.CommitTS()-1, k, v1)
}

func (s *testOnePCSuite) Test1PCDisallowMultiRegion() {
	// This test doesn't support tikv mode.
	if *withTiKV {
		return
	}

	txn := s.begin1PC()

	keyNames := []string{"k0", "k1", "k2", "k3"}
	keys := [][]byte{s.key(keyNames[0]), s.key(keyNames[1]), s.key(keyNames[2]), s.key(keyNames[3])}
	values := []string{"v0", "v1", "v2", "v3"}

	onePC := onePCDecision(metrics.OnePCResultOnePC, "")
	regrouped := onePCDecision(metrics.OnePCResultFallback, metrics.OnePCReasonMultiRegion)
	err := txn.Set(keys[0], []byte(values[0]))
	s.Nil(err)
	err = txn.Set(keys[3], []byte(values[3]))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	s.Equal(onePC+1, onePCDecision(metrics.OnePCResultOnePC, ""))

	// 1PC doesn't work if it affects multiple regions.
	loc, err := s.store.GetRegionCache().LocateKey(s.bo, keys[2])
	s.Nil(err)
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(loc.Region.GetID(), newRegionID, keys[2], []uint64{newPeerID}, newPeerID)

	txn = s.begin1PC()
	err = txn.Set(keys[1], []byte(values[1]))
	s.Nil(err)
	err = txn.Set(keys[2], []byte(values[2]))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	s.False(txn.GetCommitter().IsOnePC())
	s.Equal(txn.GetCommitter().GetOnePCCommitTS(), uint64(0))
	s.Greater(txn.GetCommitter().GetCommitTS(), txn.StartTS())
	// The Region cache still had the merged Region, so a 1PC request was sent
	// before the epoch error made the client regroup into two Regions.
	s.Equal(regrouped+1, onePCDecision(metrics.OnePCResultFallback, metrics.OnePCReasonMultiRegion))

	// With the Region cache up to date, the split is seen when grouping and no
	// 1PC request is sent.
	firstGrouping := onePCDecision(metrics.OnePCResultNotAttempted, metrics.OnePCReasonMultiRegion)
	txn = s.begin1PC()
	s.Nil(txn.Set(keys[1], []byte(values[1])))
	s.Nil(txn.Set(keys[2], []byte(values[2])))
	s.Nil(txn.Commit(context.Background()))
	s.False(txn.GetCommitter().IsOnePC())
	s.Equal(firstGrouping+1, onePCDecision(metrics.OnePCResultNotAttempted, metrics.OnePCReasonMultiRegion))

	ver, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.Nil(err)
	snap := s.store.GetSnapshot(ver)
	for i, k := range keys {
		v, err := snap.Get(context.Background(), k)
		s.Nil(err)
		s.Equal(v.Value, []byte(values[i]))
	}
}

// It's just a simple validation of linearizability.
// Extra tests are needed to test this feature with the control of the TiKV cluster.
func (s *testOnePCSuite) Test1PCLinearizability() {
	t1 := s.begin()
	t2 := s.begin()
	err := t1.Set(s.key("linear_a"), []byte("a1"))
	s.Nil(err)
	err = t2.Set(s.key("linear_b"), []byte("b1"))
	s.Nil(err)
	// t2 commits earlier than t1
	err = t2.Commit(context.Background())
	s.Nil(err)
	err = t1.Commit(context.Background())
	s.Nil(err)
	commitTS1 := t1.GetCommitter().GetCommitTS()
	commitTS2 := t2.GetCommitter().GetCommitTS()
	s.Less(commitTS2, commitTS1)
}

func (s *testOnePCSuite) Test1PCWithMultiDC() {
	// It requires setting placement rules to run with TiKV
	if *withTiKV {
		return
	}

	localTxn := s.begin1PC()
	err := localTxn.Set(s.key("multi_dc_a"), []byte("a1"))
	localTxn.SetScope("bj")
	s.Nil(err)
	err = localTxn.Commit(context.Background())
	s.Nil(err)
	s.False(localTxn.GetCommitter().IsOnePC())

	globalTxn := s.begin1PC()
	err = globalTxn.Set(s.key("multi_dc_b"), []byte("b1"))
	globalTxn.SetScope(oracle.GlobalTxnScope)
	s.Nil(err)
	err = globalTxn.Commit(context.Background())
	s.Nil(err)
	s.True(globalTxn.GetCommitter().IsOnePC())
}

func onePCDecision(result, reason string) float64 {
	return testutil.ToFloat64(metrics.TiKVOnePCDecisionCounter.WithLabelValues(result, reason, metrics.LblGeneral, metrics.OnePCOutcomeOK))
}

func (s *testOnePCSuite) TestTxnCommitCounter() {
	initial := metrics.GetTxnCommitCounter()
	disabled := onePCDecision(metrics.OnePCResultNotAttempted, metrics.OnePCReasonDisabled)

	// 2PC
	txn := s.begin()
	err := txn.Set(s.key("counter_k"), []byte("v"))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	curr := metrics.GetTxnCommitCounter()
	diff := curr.Sub(initial)
	s.Equal(diff.TwoPC, int64(1))
	s.Equal(diff.AsyncCommit, int64(0))
	s.Equal(diff.OnePC, int64(0))
	s.Equal(disabled+1, onePCDecision(metrics.OnePCResultNotAttempted, metrics.OnePCReasonDisabled))

	// AsyncCommit
	txn = s.beginAsyncCommit()
	err = txn.Set(s.key("counter_k1"), []byte("v1"))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	curr = metrics.GetTxnCommitCounter()
	diff = curr.Sub(initial)
	s.Equal(diff.TwoPC, int64(1))
	s.Equal(diff.AsyncCommit, int64(1))
	s.Equal(diff.OnePC, int64(0))

	// 1PC
	txn = s.begin1PC()
	err = txn.Set(s.key("counter_k2"), []byte("v2"))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	curr = metrics.GetTxnCommitCounter()
	diff = curr.Sub(initial)
	s.Equal(diff.TwoPC, int64(1))
	s.Equal(diff.AsyncCommit, int64(1))
	s.Equal(diff.OnePC, int64(1))
}

func (s *testOnePCSuite) TestFailWithUndeterminedResult() {
	if *withTiKV {
		s.T().Skip("not supported in real TiKV")
	}
	txn := s.begin1PC()
	s.Nil(txn.Set(s.key("undetermined_key"), []byte("value")))
	s.Nil(failpoint.Enable(
		"tikvclient/rpcPrewriteResult",
		// it will make the prewrite in 1pc fail.
		`1*return("undeterminedResult")->return("")`,
	))
	failed := testutil.ToFloat64(metrics.TiKVOnePCDecisionCounter.WithLabelValues(
		metrics.OnePCResultOnePC, "", metrics.LblGeneral, metrics.OnePCOutcomeUnknown))
	err := txn.Commit(context.Background())
	s.NotNil(err)
	s.True(tikverr.IsErrorUndetermined(err))
	// An undetermined commit is not counted as a successful 1PC transaction.
	s.Equal(failed+1, testutil.ToFloat64(metrics.TiKVOnePCDecisionCounter.WithLabelValues(
		metrics.OnePCResultOnePC, "", metrics.LblGeneral, metrics.OnePCOutcomeUnknown)))
}

func (s *testOnePCSuite) Test1PCDecisionBatchSizeAndError() {
	if *withTiKV {
		s.T().Skip("depends on mock store failpoints")
	}
	// A single-Region write set larger than one prewrite batch never sends a
	// 1PC request.
	batchSize := onePCDecision(metrics.OnePCResultNotAttempted, metrics.OnePCReasonBatchSize)
	txn := s.begin1PC()
	big := make([]byte, kv.TxnCommitBatchSize.Load())
	s.Nil(txn.Set(s.key("batch_k1"), big))
	s.Nil(txn.Set(s.key("batch_k2"), big))
	s.Nil(txn.Commit(context.Background()))
	s.False(txn.GetCommitter().IsOnePC())
	s.Equal(batchSize+1, onePCDecision(metrics.OnePCResultNotAttempted, metrics.OnePCReasonBatchSize))

	// A failed 1PC commit is counted with outcome=error, not as a success.
	decision := func() float64 {
		return testutil.ToFloat64(metrics.TiKVOnePCDecisionCounter.WithLabelValues(
			metrics.OnePCResultOnePC, "", metrics.LblGeneral, metrics.OnePCOutcomeError))
	}
	failed, ok := decision(), onePCDecision(metrics.OnePCResultOnePC, "")
	txn = s.begin1PC()
	s.Nil(txn.Set(s.key("error_k"), []byte("v1")))
	// A later transaction commits the same key first, so the 1PC prewrite
	// meets a write conflict.
	other := s.begin()
	s.Nil(other.Set(s.key("error_k"), []byte("v2")))
	s.Nil(other.Commit(context.Background()))
	s.NotNil(txn.Commit(context.Background()))
	s.Equal(failed+1, decision())
	s.Equal(ok, onePCDecision(metrics.OnePCResultOnePC, ""))
}

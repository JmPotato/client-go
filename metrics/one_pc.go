// Copyright 2026 TiKV Authors
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

package metrics

// LblOutcome is the label of TiKVOnePCDecisionCounter that tells whether the
// commit succeeded.
const LblOutcome = "outcome"

// Outcomes of TiKVOnePCDecisionCounter: the commit succeeded (ok), failed
// (error), or its result is undetermined (unknown).
const (
	OnePCOutcomeOK      = "ok"
	OnePCOutcomeError   = "error"
	OnePCOutcomeUnknown = "unknown"
)

// Results of TiKVOnePCDecisionCounter.
const (
	OnePCResultOnePC        = "one_pc"
	OnePCResultFallback     = "fallback"
	OnePCResultNotAttempted = "not_attempted"
)

// Reasons of TiKVOnePCDecisionCounter. These describe decisions, not RPCs:
// a commit counted as one_pc with outcome=error may have failed before any
// request was sent. A not_attempted commit built no 1PC request and carries
// the reason: an eligibility check failed, or the write
// set spans several Regions (multi_region) or prewrite batches (batch_size)
// when it was first grouped. A fallback commit built a 1PC request and then
// gave it up: the server turned it down (server_fallback), or the mutations
// were regrouped into several Regions or batches after a retry.
const (
	OnePCReasonDisabled           = "disabled"
	OnePCReasonSharedLock         = "shared_lock"
	OnePCReasonLocalScope         = "local_scope"
	OnePCReasonCommitTSUpperBound = "commit_ts_upper_bound"
	OnePCReasonBinlog             = "binlog"
	OnePCReasonPipelined          = "pipelined"
	OnePCReasonMultiRegion        = "multi_region"
	OnePCReasonBatchSize          = "batch_size"
	OnePCReasonServerFallback     = "server_fallback"
)

//
// Created by m34ferna on 04/06/24.
//

#include "PriorityPrecedenceTracker.h"
#include "enjima/runtime/scheduling/policy/AdaptivePriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/InputSizeBasedPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/LatencyOptimizedPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/SPLatencyOptimizedPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/SimpleLatencyPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/SimpleThroughputPriorityCalculator.h"

namespace enjima::runtime {

    template<>
    PriorityPrecedenceTracker<NonPreemptiveMode, InputSizeBasedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, maxIdleThresholdMs),
          preemptModeCalc_(&metricsMap_, &nextSchedulingUpdateAtMicros_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, InputSizeBasedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, maxIdleThresholdMs)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveMode, AdaptivePriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, profilerPtr->GetProcessCpuGauge(),
                                             profilerPtr->GetNumAvailableCpusGauge()->GetVal(), maxIdleThresholdMs),
          preemptModeCalc_(&metricsMap_, &nextSchedulingUpdateAtMicros_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, AdaptivePriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, profilerPtr->GetProcessCpuGauge(),
                                             profilerPtr->GetNumAvailableCpusGauge()->GetVal(), maxIdleThresholdMs)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveMode, LatencyOptimizedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, maxIdleThresholdMs),
          preemptModeCalc_(&metricsMap_, &nextSchedulingUpdateAtMicros_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveSimpleLatencyMode,
            SimpleLatencyPriorityCalculator>::PriorityPrecedenceTracker(metrics::Profiler* profilerPtr,
            uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, maxIdleThresholdMs)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, LatencyOptimizedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, maxIdleThresholdMs)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveMode, SPLatencyOptimizedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, maxIdleThresholdMs),
          preemptModeCalc_(&metricsMap_, &nextSchedulingUpdateAtMicros_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, SPLatencyOptimizedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, maxIdleThresholdMs)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveMode, LeastRecentOperatorPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(maxIdleThresholdMs),
          preemptModeCalc_(&metricsMap_, &nextSchedulingUpdateAtMicros_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, LeastRecentOperatorPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(maxIdleThresholdMs)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, ThroughputOptimizedPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, maxIdleThresholdMs)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveThroughputOptimizedMode,
            ThroughputOptimizedPriorityCalculator>::PriorityPrecedenceTracker(metrics::Profiler* profilerPtr,
            uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, maxIdleThresholdMs), preemptModeCalc_(&metricsMap_)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveMode, SimpleThroughputPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(maxIdleThresholdMs),
          preemptModeCalc_(&metricsMap_, &nextSchedulingUpdateAtMicros_)
    {
        Init();
    }


    template<>
    PriorityPrecedenceTracker<PreemptiveMode, SimpleThroughputPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(maxIdleThresholdMs)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<PreemptiveMode, RoundRobinPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, maxIdleThresholdMs)
    {
        Init();
    }

    template<>
    PriorityPrecedenceTracker<NonPreemptiveSimpleLatencyMode, FCFSPriorityCalculator>::PriorityPrecedenceTracker(
            metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs)
        : profilerPtr_(profilerPtr), schedulingPolicy_(&metricsMap_, maxIdleThresholdMs)
    {
        Init();
    }

    template<>
    void PriorityPrecedenceTracker<PreemptiveMode, LeastRecentOperatorPriorityCalculator>::TrackPipeline(
            core::StreamingPipeline* pStreamingPipeline)
    {
        // These methods are called only when de-/registering operators. So it is OK to have a lock here
        std::lock_guard<std::mutex> lockGuard(modificationMutex_);
        for (const auto& pStreamOp: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            InitOpMetrics(pStreamOp);
            auto* opCtxtPtr = new (schedMemoryCursor_) OperatorContext{pStreamOp, OperatorContext::kInactive};
            schedMemoryCursor_ = static_cast<OperatorContext*>(schedMemoryCursor_) + 1;
#if ENJIMA_METRICS_LEVEL >= 3
            auto gapCounter = profilerPtr_->GetOrCreateCounter(
                    std::string(pStreamOp->GetOperatorName()).append(metrics::kGapCounterSuffix));
            auto gapTimeCounter = profilerPtr_->GetOrCreateCounter(
                    std::string(pStreamOp->GetOperatorName()).append(metrics::kGapTimeCounterSuffix));
            opCtxtPtr->SetScheduleGapCounters(gapCounter, gapTimeCounter);
#endif
            auto* decisionCtxtPtr =
                    new (schedMemoryCursor_) SchedulingDecisionContext{pStreamOp, get<4>(metricsMap_.at(pStreamOp))};
            schedMemoryCursor_ = static_cast<SchedulingDecisionContext*>(schedMemoryCursor_) + 1;
            auto* schedCtxtPtr = new (schedMemoryCursor_) SchedulingContext{opCtxtPtr, decisionCtxtPtr};
            schedMemoryCursor_ = static_cast<SchedulingContext*>(schedMemoryCursor_) + 1;
            currentOpCtxtPtrs_.emplace_back(opCtxtPtr);
            schedCtxtMap_.emplace(opCtxtPtr, schedCtxtPtr);
            std::vector<operators::StreamingOperator*> downstreamOps =
                    pStreamingPipeline->GetAllDownstreamOps(pStreamOp);
            downstreamOpsByOpPtrMap_.emplace(pStreamOp, downstreamOps);
            opPipelineMap_.emplace(opCtxtPtr, pStreamingPipeline);
        }
        currentPipelines_.emplace_back(pStreamingPipeline);
    }

    template<>
    void PriorityPrecedenceTracker<NonPreemptiveMode, LeastRecentOperatorPriorityCalculator>::TrackPipeline(
            core::StreamingPipeline* pStreamingPipeline)
    {
        // These methods are called only when de-/registering operators. So it is OK to have a lock here
        std::lock_guard<std::mutex> lockGuard(modificationMutex_);
        for (const auto& pStreamOp: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            InitOpMetrics(pStreamOp);
            auto* opCtxtPtr = new (schedMemoryCursor_) OperatorContext{pStreamOp, OperatorContext::kInactive};
            schedMemoryCursor_ = static_cast<OperatorContext*>(schedMemoryCursor_) + 1;
#if ENJIMA_METRICS_LEVEL >= 3
            auto gapCounter = profilerPtr_->GetOrCreateCounter(
                    std::string(pStreamOp->GetOperatorName()).append(metrics::kGapCounterSuffix));
            auto gapTimeCounter = profilerPtr_->GetOrCreateCounter(
                    std::string(pStreamOp->GetOperatorName()).append(metrics::kGapTimeCounterSuffix));
            opCtxtPtr->SetScheduleGapCounters(gapCounter, gapTimeCounter);
#endif
            auto* decisionCtxtPtr =
                    new (schedMemoryCursor_) SchedulingDecisionContext{pStreamOp, get<4>(metricsMap_.at(pStreamOp))};
            schedMemoryCursor_ = static_cast<SchedulingDecisionContext*>(schedMemoryCursor_) + 1;
            auto* schedCtxtPtr = new (schedMemoryCursor_) SchedulingContext{opCtxtPtr, decisionCtxtPtr};
            schedMemoryCursor_ = static_cast<SchedulingContext*>(schedMemoryCursor_) + 1;
            currentOpCtxtPtrs_.emplace_back(opCtxtPtr);
            schedCtxtMap_.emplace(opCtxtPtr, schedCtxtPtr);
            std::vector<operators::StreamingOperator*> downstreamOps =
                    pStreamingPipeline->GetAllDownstreamOps(pStreamOp);
            downstreamOpsByOpPtrMap_.emplace(pStreamOp, downstreamOps);
            opPipelineMap_.emplace(opCtxtPtr, pStreamingPipeline);
        }
        currentPipelines_.emplace_back(pStreamingPipeline);
    }

    template<>
    bool PriorityPrecedenceTracker<PreemptiveMode, LeastRecentOperatorPriorityCalculator>::UnTrackPipeline(
            core::StreamingPipeline* pStreamingPipeline)
    {
        // These methods are called only when de-/registering operators. So it is OK to have a lock here
        std::lock_guard<std::mutex> lockGuard(modificationMutex_);
        schedulingQueue_.EraseAll(pStreamingPipeline->GetOperatorsInTopologicalOrder());
        return OperatorPrecedenceTracker::UnTrackPipelineWithoutLocking(pStreamingPipeline);
    }

    template<>
    bool PriorityPrecedenceTracker<NonPreemptiveMode, LeastRecentOperatorPriorityCalculator>::UnTrackPipeline(
            core::StreamingPipeline* pStreamingPipeline)
    {
        // These methods are called only when de-/registering operators. So it is OK to have a lock here
        std::lock_guard<std::mutex> lockGuard(modificationMutex_);
        schedulingQueue_.EraseAll(pStreamingPipeline->GetOperatorsInTopologicalOrder());
        return OperatorPrecedenceTracker::UnTrackPipelineWithoutLocking(pStreamingPipeline);
    }

    template<>
    void PriorityPrecedenceTracker<NonPreemptiveThroughputOptimizedMode,
            ThroughputOptimizedPriorityCalculator>::DoPostRunUpdates(const SchedulingDecisionContext*
                                                                             prevDecisionCtxtPtr,
            SchedulingContext* prevSchedCtxtPtr, OperatorContext* prevOpCtxtPtr)
    {
        prevOpCtxtPtr->SetLastOperatorStatus(prevDecisionCtxtPtr->GetLastOperatorStatus());
        schedulingPolicy_.NotifyScheduleCompletion(prevOpCtxtPtr);
        SetPriorityPostRun(prevSchedCtxtPtr, prevOpCtxtPtr);
    }

    template<>
    void PriorityPrecedenceTracker<PreemptiveMode, RoundRobinPriorityCalculator>::DoPostRunUpdates(
            const SchedulingDecisionContext* prevDecisionCtxtPtr, SchedulingContext* prevSchedCtxtPtr,
            OperatorContext* prevOpCtxtPtr)
    {
        prevOpCtxtPtr->SetLastOperatorStatus(prevDecisionCtxtPtr->GetLastOperatorStatus());
        schedulingPolicy_.NotifyScheduleCompletion(prevOpCtxtPtr);
        SetPriorityPostRun(prevSchedCtxtPtr, prevOpCtxtPtr);
    }

    template<>
    void PriorityPrecedenceTracker<NonPreemptiveSimpleLatencyMode, FCFSPriorityCalculator>::DoPostRunUpdates(
            const SchedulingDecisionContext* prevDecisionCtxtPtr, SchedulingContext* prevSchedCtxtPtr,
            OperatorContext* prevOpCtxtPtr)
    {
        prevOpCtxtPtr->SetLastOperatorStatus(prevDecisionCtxtPtr->GetLastOperatorStatus());
        schedulingPolicy_.NotifyScheduleCompletion(prevOpCtxtPtr);
        SetPriorityPostRun(prevSchedCtxtPtr, prevOpCtxtPtr);
    }
}// namespace enjima::runtime

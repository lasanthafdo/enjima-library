//
// Created by m34ferna on 14/05/24.
//

#ifndef ENJIMA_PRIORITY_PRECEDENCE_TRACKER_H
#define ENJIMA_PRIORITY_PRECEDENCE_TRACKER_H

#include "OperatorPrecedenceTracker.h"
#include "SchedulingContext.h"
#include "SchedulingDecisionContext.h"
#include "SchedulingQueue.h"
#include "SchedulingTypes.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/runtime/AlreadyExistsException.h"
#include "enjima/runtime/InitializationException.h"
#include "enjima/runtime/scheduling/policy/FCFSPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/LeastRecentOperatorPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/NonPreemptiveMode.h"
#include "enjima/runtime/scheduling/policy/NonPreemptiveSimpleLatencyMode.h"
#include "enjima/runtime/scheduling/policy/NonPreemptiveThroughputOptimizedMode.h"
#include "enjima/runtime/scheduling/policy/PreemptiveMode.h"
#include "enjima/runtime/scheduling/policy/RoundRobinPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/SchedulingPolicy.h"
#include "enjima/runtime/scheduling/policy/SchedulingPreemptMode.h"
#include "enjima/runtime/scheduling/policy/ThroughputOptimizedPriorityCalculator.h"

#include <unordered_set>

#if ENJIMA_METRICS_LEVEL >= 3
#include "enjima/metrics/MetricNames.h"
#include "enjima/runtime/scheduling/policy/LeastRecentOperatorPriorityCalculator.h"
#endif

namespace enjima::runtime {

    template<PreemptModeType PreemptT, PriorityCalcType CalcT>
    class PriorityPrecedenceTracker : public OperatorPrecedenceTracker<SchedulingContext*> {
    public:
        PriorityPrecedenceTracker(metrics::Profiler* profilerPtr, uint64_t maxIdleThresholdMs);

        SchedulingContext* GetNextInPrecedence(SchedulingContext*& prevSchedCtxtPtr) override;
        void TrackPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        bool UnTrackPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        void UpdatePriority(uint64_t nextSchedulingUpdateAtMicros);
        uint64_t GetSchedulingEpoch() const;
        bool IsInitialized();

    private:
        void Init();
        void InitOpMetrics(operators::StreamingOperator* opPtr);
        void UpdateCostMetricsForOperator(SchedulingDecisionContext* prevDecisionCtxtPtr);
        void DoPostRunUpdates(const SchedulingDecisionContext* prevDecisionCtxtPtr, SchedulingContext* prevSchedCtxtPtr,
                OperatorContext* prevOpCtxtPtr);
        void SetPriorityPostRun(SchedulingContext* prevSchedCtxtPtr, const OperatorContext* prevOpCtxtPtr);
        uint64_t GetNumPendingEvents(operators::StreamingOperator* opPtr) const;

        metrics::Profiler* profilerPtr_;
        SchedulingQueue schedulingQueue_;
        MetricsMapT metricsMap_;
        std::atomic<uint64_t> nextSchedulingUpdateAtMicros_{0};
        CalcT schedulingPolicy_;
        PreemptT preemptModeCalc_;
        std::atomic<uint64_t> schedulingEpoch_{0};
        std::atomic<bool> anyOpWaitingToCancel_{false};
        std::atomic<bool> initialized_{false};

#if ENJIMA_METRICS_LEVEL >= 3
        metrics::DoubleAverageGauge* vecLockTimeAvgGauge_{nullptr};
        metrics::DoubleAverageGauge* totCalcTimeAvgGauge_{nullptr};
        metrics::DoubleAverageGauge* priorityUpdateTimeAvgGauge_{nullptr};
#endif
    };

    template<>
    void PriorityPrecedenceTracker<PreemptiveMode, LeastRecentOperatorPriorityCalculator>::TrackPipeline(
            core::StreamingPipeline* pStreamingPipeline);
    template<>
    void PriorityPrecedenceTracker<NonPreemptiveMode, LeastRecentOperatorPriorityCalculator>::TrackPipeline(
            core::StreamingPipeline* pStreamingPipeline);
    template<>
    bool PriorityPrecedenceTracker<PreemptiveMode, LeastRecentOperatorPriorityCalculator>::UnTrackPipeline(
            core::StreamingPipeline* pStreamingPipeline);
    template<>
    bool PriorityPrecedenceTracker<NonPreemptiveMode, LeastRecentOperatorPriorityCalculator>::UnTrackPipeline(
            core::StreamingPipeline* pStreamingPipeline);

    template<>
    void PriorityPrecedenceTracker<NonPreemptiveThroughputOptimizedMode,
            ThroughputOptimizedPriorityCalculator>::DoPostRunUpdates(const SchedulingDecisionContext*
                                                                             prevDecisionCtxtPtr,
            SchedulingContext* prevSchedCtxtPtr, OperatorContext* prevOpCtxtPtr);

    template<>
    void PriorityPrecedenceTracker<PreemptiveMode, RoundRobinPriorityCalculator>::DoPostRunUpdates(
            const SchedulingDecisionContext* prevDecisionCtxtPtr, SchedulingContext* prevSchedCtxtPtr,
            OperatorContext* prevOpCtxtPtr);

    template<>
    void PriorityPrecedenceTracker<NonPreemptiveSimpleLatencyMode, FCFSPriorityCalculator>::DoPostRunUpdates(
            const SchedulingDecisionContext* prevDecisionCtxtPtr, SchedulingContext* prevSchedCtxtPtr,
            OperatorContext* prevOpCtxtPtr);

}// namespace enjima::runtime

#include "PriorityPrecedenceTracker.tpp"

#endif//ENJIMA_PRIORITY_PRECEDENCE_TRACKER_H

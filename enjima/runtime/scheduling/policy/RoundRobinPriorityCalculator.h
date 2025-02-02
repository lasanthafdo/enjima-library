//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_ROUND_ROBIN_PRIORITY_CALCULATOR_H
#define ENJIMA_ROUND_ROBIN_PRIORITY_CALCULATOR_H

#include "MetricsBasedPriorityCalculator.h"
#include "enjima/runtime/scheduling/policy/SchedulingPolicy.h"

namespace enjima::runtime {

    class RoundRobinPriorityCalculator : public MetricsBasedPriorityCalculator {
    public:
        RoundRobinPriorityCalculator(MetricsMapT* metricsMapPtr, uint64_t maxThresholdMs);
        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
        void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        void NotifyScheduleCompletion(const OperatorContext* opCtxtPtr);
        [[nodiscard]] bool IsEligibleForScheduling(uint64_t numPendingEvents, uint64_t lastScheduledAtMs,
                uint8_t lastOperatorStatus) const override;

    private:
        int64_t GetUpdatedPipelineSchedulingCycle();

        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, core::StreamingPipeline*> pipelineByOpPtrMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, std::atomic<int64_t>> schedCycleForOp_;
        std::atomic<int64_t> currentSchedCycle_;
        ConcurrentUnorderedMapTBB<const core::StreamingPipeline*, std::atomic<bool>> pipelineActivatedMap_;
    };

}// namespace enjima::runtime


#endif//ENJIMA_ROUND_ROBIN_PRIORITY_CALCULATOR_H

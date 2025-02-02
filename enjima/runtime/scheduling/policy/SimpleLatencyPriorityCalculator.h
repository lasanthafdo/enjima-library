//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_SIMPLE_LATENCY_PRIORITY_CALCULATOR_H
#define ENJIMA_SIMPLE_LATENCY_PRIORITY_CALCULATOR_H

#include "MetricsBasedPriorityCalculator.h"

namespace enjima::runtime {

    class SimpleLatencyPriorityCalculator : public MetricsBasedPriorityCalculator {
    public:
        SimpleLatencyPriorityCalculator(MetricsMapT* metricsMapPtr, uint64_t maxThresholdMs);
        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
        void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        [[nodiscard]] bool IsEligibleForScheduling(uint64_t numPendingEvents, uint64_t lastScheduledAtMs,
                uint8_t lastOperatorStatus) const override;

    private:
        std::vector<core::StreamingPipeline*> trackedPipelines_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> outputSelectivityMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> cumulativeOutputCostMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, std::atomic<float>> outputCostMap_;
    };

}// namespace enjima::runtime

#endif//ENJIMA_SIMPLE_LATENCY_PRIORITY_CALCULATOR_H

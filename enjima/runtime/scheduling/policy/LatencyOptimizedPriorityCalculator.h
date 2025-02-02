//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_LATENCY_OPTIMIZED_PRIORITY_CALCULATOR_H
#define ENJIMA_LATENCY_OPTIMIZED_PRIORITY_CALCULATOR_H

#include "MetricsBasedPriorityCalculator.h"

namespace enjima::runtime {

    class LatencyOptimizedPriorityCalculator : public MetricsBasedPriorityCalculator {
    public:
        LatencyOptimizedPriorityCalculator(MetricsMapT* metricsMapPtr, uint64_t maxThresholdMs);
        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
        void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;

    private:
        std::vector<core::StreamingPipeline*> trackedPipelines_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> outputSelectivityMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> cumulativeOutputCostMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, std::atomic<float>> outputCostMap_;
    };

}// namespace enjima::runtime

#endif//ENJIMA_LATENCY_OPTIMIZED_PRIORITY_CALCULATOR_H

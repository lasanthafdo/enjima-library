//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_ADAPTIVE_PRIORITY_CALCULATOR_H
#define ENJIMA_ADAPTIVE_PRIORITY_CALCULATOR_H

#include "MetricsBasedPriorityCalculator.h"
#include "SchedulingPolicy.h"

namespace enjima::runtime {

    class AdaptivePriorityCalculator : public MetricsBasedPriorityCalculator {
    public:
        AdaptivePriorityCalculator(MetricsMapT* metricsMapPtr, metrics::CpuGauge* cpuGaugePtr,
                uint32_t numAvailableCpus, uint64_t maxThresholdMs);
        float CalculatePriority(const OperatorContext* opCtxtPtr) override;
        void UpdateState() override;
        void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;
        void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) override;

    private:
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, core::StreamingPipeline*> pipelineByOpPtrMap_;
        std::vector<core::StreamingPipeline*> trackedPipelines_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> outputSelectivityMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> cumulativeOutputCostMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, float> outputCostMap_;
        ConcurrentUnorderedMapTBB<const core::StreamingPipeline*, float> maxOutputCostMap_;
        ConcurrentUnorderedMapTBB<const operators::StreamingOperator*, size_t> numDownstreamOpsMap_;
        ConcurrentUnorderedMapTBB<const core::StreamingPipeline*, size_t> maxNumDownstreamOpsMap_;
        metrics::CpuGauge* cpuGaugePtr_;
        float cpuUsageMultiplier_;
    };

}// namespace enjima::runtime


#endif//ENJIMA_ADAPTIVE_PRIORITY_CALCULATOR_H

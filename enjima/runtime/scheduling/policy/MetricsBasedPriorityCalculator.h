//
// Created by m34ferna on 20/07/24.
//

#ifndef ENJIMA_METRICS_BASED_PRIORITY_CALCULATOR_H
#define ENJIMA_METRICS_BASED_PRIORITY_CALCULATOR_H

#include "SchedulingPolicy.h"

namespace enjima::runtime {

    class MetricsBasedPriorityCalculator : public SchedulingPolicy {
    public:
        explicit MetricsBasedPriorityCalculator(MetricsMapT* metricsMapPtr, uint64_t maxThresholdMs)
            : SchedulingPolicy(maxThresholdMs), metricsMapPtr_(metricsMapPtr)
        {
        }

        virtual void InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) = 0;
        virtual void DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline) = 0;

    protected:
        MetricsMapT* metricsMapPtr_;
    };

}// namespace enjima::runtime


#endif//ENJIMA_METRICS_BASED_PRIORITY_CALCULATOR_H

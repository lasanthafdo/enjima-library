//
// Created by m34ferna on 26/06/24.
//

#include "NonPreemptiveThroughputOptimizedMode.h"

namespace enjima::runtime {

    NonPreemptiveThroughputOptimizedMode::NonPreemptiveThroughputOptimizedMode(MetricsMapT* metricsMapPtr)
        : metricsMapPtr_(metricsMapPtr)
    {
    }

    long NonPreemptiveThroughputOptimizedMode::CalculateNumEventsToProcess(operators::StreamingOperator*& opPtr) const
    {
        if (opPtr != nullptr) {
            return std::numeric_limits<long>::max();
        }
        return 0;
    }
}// namespace enjima::runtime

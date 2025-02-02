//
// Created by m34ferna on 21/07/24.
//

#include "SchedulingPolicy.h"
#include "enjima/operators/StreamingOperator.h"
#include "enjima/runtime/RuntimeUtil.h"

namespace enjima::runtime {
    static const uint64_t kInputSizeThreshold = 1000;

    SchedulingPolicy::SchedulingPolicy(uint64_t maxThresholdMs) : kMaxIdleThresholdMs_(maxThresholdMs) {}

    bool SchedulingPolicy::IsEligibleForScheduling(uint64_t numPendingEvents, uint64_t lastScheduledAtMs,
            uint8_t lastOperatorStatus) const
    {
        return static_cast<bool>(lastOperatorStatus & operators::StreamingOperator::kCanOutput) &&
               (numPendingEvents > kInputSizeThreshold ||
                       (numPendingEvents > 0 &&
                               (runtime::GetSystemTimeMillis() - lastScheduledAtMs) > kMaxIdleThresholdMs_));
    }
}// namespace enjima::runtime
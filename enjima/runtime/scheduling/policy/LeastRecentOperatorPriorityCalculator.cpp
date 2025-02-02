//
// Created by m34ferna on 07/06/24.
//

#include "LeastRecentOperatorPriorityCalculator.h"
#include "enjima/runtime/scheduling/OperatorContext.h"

namespace enjima::runtime {

    LeastRecentOperatorPriorityCalculator::LeastRecentOperatorPriorityCalculator(uint64_t maxThresholdMs)
        : SchedulingPolicy(maxThresholdMs)
    {
    }

    float LeastRecentOperatorPriorityCalculator::CalculatePriority(const OperatorContext* opCtxtPtr)
    {
        return static_cast<float>(runtime::GetSystemTimeMillis() - opCtxtPtr->GetLastScheduledInMillis());
    }

    void LeastRecentOperatorPriorityCalculator::UpdateState() {}
}// namespace enjima::runtime

//
// Created by m34ferna on 07/06/24.
//

#ifndef ENJIMA_PRIORITY_CALCULATOR_H
#define ENJIMA_PRIORITY_CALCULATOR_H

#include "enjima/common/TypeAliases.h"
#include "enjima/core/CoreInternals.fwd.h"
#include "enjima/metrics/types/CpuGauge.h"
#include "enjima/operators/OperatorsInternals.fwd.h"
#include "enjima/runtime/scheduling/InternalSchedulingTypes.fwd.h"
#include "enjima/runtime/scheduling/SchedulingTypes.h"

#include <cstddef>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace enjima::runtime {

    class SchedulingPolicy {
    public:
        explicit SchedulingPolicy(uint64_t maxThresholdMs);
        virtual float CalculatePriority(const OperatorContext* opCtxtPtr) = 0;
        virtual void UpdateState() = 0;
        [[nodiscard]] virtual bool IsEligibleForScheduling(uint64_t numPendingEvents, uint64_t lastScheduledAtMs,
                uint8_t lastOperatorStatus) const;

    protected:
        constexpr static const float kMaxPriorityFloat = std::numeric_limits<float>::max();

    private:
        const uint64_t kMaxIdleThresholdMs_;
    };

    template<typename T>
    concept PriorityCalcType = std::is_base_of_v<SchedulingPolicy, T>;
}// namespace enjima::runtime

#endif//ENJIMA_PRIORITY_CALCULATOR_H

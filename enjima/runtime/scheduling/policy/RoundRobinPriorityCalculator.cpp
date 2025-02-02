//
// Created by m34ferna on 07/06/24.
//

#include "RoundRobinPriorityCalculator.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/runtime/IllegalArgumentException.h"
#include "enjima/runtime/scheduling/OperatorContext.h"

namespace enjima::runtime {

    RoundRobinPriorityCalculator::RoundRobinPriorityCalculator(MetricsMapT* metricsMapPtr, uint64_t maxThresholdMs)
        : MetricsBasedPriorityCalculator(metricsMapPtr, maxThresholdMs)
    {
    }

    float RoundRobinPriorityCalculator::CalculatePriority(const OperatorContext* opCtxtPtr)
    {
        auto opPtr = opCtxtPtr->GetOperatorPtr();
        auto currentOpPtrPipeline = pipelineByOpPtrMap_.at(opPtr);
        if (pipelineActivatedMap_.at(currentOpPtrPipeline)) {
            auto currentSchedCycle = GetUpdatedPipelineSchedulingCycle();
            auto opCyclesLeft = currentSchedCycle - schedCycleForOp_.at(opPtr).load(std::memory_order::acquire);
            // We can add 1 to the opCyclesLeft so that there is a bit of leeway, and workers are allowed to schedule one round ahead.
            // This prevents some workers starving while other workers complete the round for straggler operators. However,
            // for strict round-robin, we do not incorporate this optimization
            auto priority = std::max(static_cast<float>(opCyclesLeft), 0.0f);
            return priority;
        }
        else {
            return 0.0f;
        }
    }

    int64_t RoundRobinPriorityCalculator::GetUpdatedPipelineSchedulingCycle()
    {
        auto currentSchedCycle = currentSchedCycle_.load(std::memory_order::acquire);
        auto pipelineCycleComplete = std::all_of(schedCycleForOp_.cbegin(), schedCycleForOp_.cend(),
                [&currentSchedCycle, this](auto& opCounterPair) {
                    auto currentOpPtr = opCounterPair.first;
                    auto currentOpPtrPipeline = pipelineByOpPtrMap_.at(currentOpPtr);
                    if (pipelineActivatedMap_.at(currentOpPtrPipeline)) {
                        auto& schedCycleForOp = opCounterPair.second;
                        return schedCycleForOp.load(std::memory_order::acquire) >= currentSchedCycle;
                    }
                    else {
                        return true;
                    }
                });
        if (pipelineCycleComplete) {
            currentSchedCycle_.fetch_add(1, std::memory_order::acq_rel);
        }
        return currentSchedCycle_.load(std::memory_order::acquire);
    }

    void RoundRobinPriorityCalculator::InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        auto currentSchedCycle = currentSchedCycle_.load(std::memory_order::acquire);
        for (const auto& opPtr: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            schedCycleForOp_.emplace(opPtr, currentSchedCycle);
            pipelineByOpPtrMap_.emplace(opPtr, pStreamingPipeline);
        }
        pipelineActivatedMap_.emplace(pStreamingPipeline, true);
    }

    void RoundRobinPriorityCalculator::UpdateState() {}

    void RoundRobinPriorityCalculator::NotifyScheduleCompletion(const OperatorContext* opCtxtPtr)
    {
        const auto* opPtr = opCtxtPtr->GetOperatorPtr();
        schedCycleForOp_.at(opPtr).fetch_add(1, std::memory_order::acq_rel);
    }

    void RoundRobinPriorityCalculator::DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        if (pStreamingPipeline == nullptr) {
            throw IllegalArgumentException{"Pipeline passed for metrics deactivation is null!"};
        }
        pipelineActivatedMap_.at(pStreamingPipeline).store(false, std::memory_order::release);
    }

    bool RoundRobinPriorityCalculator::IsEligibleForScheduling([[maybe_unused]] uint64_t numPendingEvents,
            [[maybe_unused]] uint64_t lastScheduledAtMs, [[maybe_unused]] uint8_t lastOperatorStatus) const
    {
        return true;
    }
}// namespace enjima::runtime

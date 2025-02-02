//
// Created by m34ferna on 07/06/24.
//

#include "AdaptivePriorityCalculator.h"
#include "enjima/core/StreamingPipeline.h"
#include "enjima/runtime/scheduling/OperatorContext.h"

#include <ranges>

namespace enjima::runtime {

    AdaptivePriorityCalculator::AdaptivePriorityCalculator(MetricsMapT* metricsMapPtr, metrics::CpuGauge* cpuGaugePtr,
            uint32_t numAvailableCpus, uint64_t maxThresholdMs)
        : MetricsBasedPriorityCalculator(metricsMapPtr, maxThresholdMs), cpuGaugePtr_(cpuGaugePtr)
    {
        cpuUsageMultiplier_ =
                static_cast<float>(std::thread::hardware_concurrency()) / static_cast<float>(numAvailableCpus);
    }

    float AdaptivePriorityCalculator::CalculatePriority(const OperatorContext* opCtxtPtr)
    {
        auto opPtr = opCtxtPtr->GetOperatorPtr();
        if (opPtr->IsSourceOperator()) {
            return kMaxPriorityFloat;
        }
        else {
            auto pipelinePtr = pipelineByOpPtrMap_.at(opPtr);
            auto maxOutputCost = maxOutputCostMap_.at(pipelinePtr);
            auto cpuUsage = std::min(1.0f, cpuGaugePtr_->GetVal() * cpuUsageMultiplier_);
            auto priority = maxOutputCost * (1 - cpuUsage) / outputCostMap_.at(opPtr) +
                            maxOutputCost * static_cast<float>(numDownstreamOpsMap_.at(opPtr)) * cpuUsage /
                                    static_cast<float>(maxNumDownstreamOpsMap_.at(pipelinePtr));
            // auto priority =
            //         cpuUsage > 0.9 ? static_cast<float>(numDownstreamOpsMap_.at(opPtr)) : 1.0f / outputCostMap_.at(opPtr);
            return priority;
        }
    }

    void AdaptivePriorityCalculator::InitializeMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        size_t maxNumDownstreamOps = 0;
        for (const auto& opPtr: pStreamingPipeline->GetOperatorsInTopologicalOrder()) {
            outputSelectivityMap_.emplace(opPtr, 0.0f);
            cumulativeOutputCostMap_.emplace(opPtr, 0.0f);
            outputCostMap_.emplace(opPtr, 0.0f);
            auto numDownstreamOps = pStreamingPipeline->GetAllDownstreamOps(opPtr).size();
            numDownstreamOpsMap_.emplace(opPtr, numDownstreamOps);
            if (maxNumDownstreamOps < numDownstreamOps) {
                maxNumDownstreamOps = numDownstreamOps;
            }
            pipelineByOpPtrMap_.emplace(opPtr, pStreamingPipeline);
        }
        maxNumDownstreamOpsMap_.emplace(pStreamingPipeline, maxNumDownstreamOps);
        maxOutputCostMap_.emplace(pStreamingPipeline, 0.0f);
        trackedPipelines_.emplace_back(pStreamingPipeline);
    }

    void AdaptivePriorityCalculator::DeactivateMetricsForPipeline(core::StreamingPipeline* pStreamingPipeline)
    {
        std::erase(trackedPipelines_, pStreamingPipeline);
        maxNumDownstreamOpsMap_.unsafe_erase(pStreamingPipeline);
        maxOutputCostMap_.unsafe_erase(pStreamingPipeline);
    }

    void AdaptivePriorityCalculator::UpdateState()
    {
        for (const auto* pipelinePtr: trackedPipelines_) {
            maxOutputCostMap_[pipelinePtr] = 0.0f;
            auto opPtrVec = pipelinePtr->GetOperatorsInTopologicalOrder();
            for (auto* opPtr: std::ranges::views::reverse(opPtrVec)) {
                auto outputSelectivity = static_cast<float>(get<3>(metricsMapPtr_->at(opPtr))->GetVal());
                auto downstreamOpVec = pipelinePtr->GetDownstreamOperators(opPtr->GetOperatorId());
                if (!downstreamOpVec.empty()) {
                    auto downstreamOutputSel = outputSelectivityMap_.at(downstreamOpVec[0]);
                    for (auto i = downstreamOpVec.size(); i > 1; i--) {
                        downstreamOutputSel =
                                std::max(downstreamOutputSel, outputSelectivityMap_.at(downstreamOpVec[i - 1]));
                    }
                    outputSelectivity *= downstreamOutputSel;
                }
                outputSelectivityMap_[opPtr] = outputSelectivity;
                auto costGauge = get<2>(metricsMapPtr_->at(opPtr));
                auto outputCost = static_cast<float>(costGauge->GetVal()) / outputSelectivity;
                auto downstreamCumulativeOutputCost = 0.0f;
                for (auto i = downstreamOpVec.size(); i > 0; i--) {
                    downstreamCumulativeOutputCost += cumulativeOutputCostMap_.at(downstreamOpVec[i - 1]);
                }
                outputCost += downstreamCumulativeOutputCost;
                outputCostMap_[opPtr] = outputCost;
                cumulativeOutputCostMap_[opPtr] = outputCost + downstreamCumulativeOutputCost;
                if (outputCost > maxOutputCostMap_.at(pipelinePtr)) {
                    maxOutputCostMap_[pipelinePtr] = outputCost;
                }
            }
        }
    }
}// namespace enjima::runtime

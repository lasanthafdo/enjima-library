//
// Created by m34ferna on 26/03/24.
//

#include "PeriodicMetricsLogger.h"
#include "Profiler.h"
#include "enjima/runtime/RuntimeUtil.h"
#include "spdlog/spdlog.h"
#include <iostream>


namespace enjima::metrics {
    static const char* const kEventCountLogger = "enjima_event_count_logger";
    static const char* const kThroughputLogger = "enjima_throughput_logger";
    static const char* const kLatencyLogger = "enjima_latency_logger";
    static const char* const kOpMetricsLogger = "enjima_op_metrics_logger";
    static const char* const kSysMetricsLogger = "enjima_system_metrics_logger";

    PeriodicMetricsLogger::PeriodicMetricsLogger(uint64_t loggingPeriodSecs, metrics::Profiler* profilerPtr,
            bool systemMetricsEnabled, std::string systemIDString)
        : systemIDString_(std::move(systemIDString)), loggingPeriodMs_(loggingPeriodSecs * 1000),
          pProfiler_(profilerPtr), systemMetricsEnabled_(systemMetricsEnabled)
    {
        nextLoggingAtMs_ = enjima::runtime::GetSystemTimeMillis() + loggingPeriodMs_;
    }

    PeriodicMetricsLogger::~PeriodicMetricsLogger()
    {
        spdlog::drop(kEventCountLogger);
        spdlog::drop(kThroughputLogger);
        spdlog::drop(kLatencyLogger);
        spdlog::drop(kOpMetricsLogger);
        if (systemMetricsEnabled_) {
            spdlog::drop(kLatencyLogger);
        }
    }

    void PeriodicMetricsLogger::Start()
    {
        pCounterLogger_ = CreateMetricLogger("event_count.csv", kEventCountLogger, systemIDString_);
        pThroughputLogger_ = CreateMetricLogger("throughput.csv", kThroughputLogger, systemIDString_);
        pLatencyLogger_ = CreateMetricLogger("latency.csv", kLatencyLogger, systemIDString_);
        pOpMetricsLogger_ = CreateMetricLogger("operator_metrics.csv", kOpMetricsLogger, systemIDString_);
        if (systemMetricsEnabled_) {
            pSysMetricsLogger_ = CreateMetricLogger("system_metrics.csv", kSysMetricsLogger, systemIDString_);
        }
        running_ = true;
        loggerFuture_ = std::async(std::launch::async, &PeriodicMetricsLogger::Run, this);
    }

    std::shared_ptr<spdlog::logger> PeriodicMetricsLogger::CreateMetricLogger(const std::string& metricFilename,
            const std::string& loggerName, const std::string& systemIDString)
    {
        try {
            auto outCountFileSink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("metrics/" + metricFilename);
            outCountFileSink->set_pattern("%t," + systemIDString + ",%v");
            return std::make_shared<spdlog::logger>(loggerName, outCountFileSink);
        }
        catch (const spdlog::spdlog_ex& ex) {
            spdlog::error("Metric log initialization failed for {}: {}", loggerName, ex.what());
        }
        return nullptr;
    }

    void PeriodicMetricsLogger::Run()
    {
        spdlog::info("Started periodic metric logging thread for system with ID {}.", systemIDString_);
        pthread_setname_np(pthread_self(), std::string("logger_1").c_str());
        threadId_.store(gettid(), std::memory_order::release);
        while (running_.load(std::memory_order::acquire)) {
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            if ((currentTime + 1) < nextLoggingAtMs_) {
                auto sleepTime = nextLoggingAtMs_ - currentTime - 1;
                {
                    std::unique_lock<std::mutex> lock(cvMutex_);
                    cv_.wait_for(lock, std::chrono::milliseconds(sleepTime),
                            [&] { return !running_.load(std::memory_order::acquire); });
                }
                if (!running_.load(std::memory_order::acquire)) {
                    break;
                }
            }
            LogMetrics();
            nextLoggingAtMs_ = enjima::runtime::GetSystemTimeMillis() + loggingPeriodMs_;
        }
        FlushMetricsToFile();
        spdlog::info("Stopped periodic metric logging thread");
    }

    void PeriodicMetricsLogger::Shutdown()
    {
        {
            std::lock_guard<std::mutex> lockGuard{cvMutex_};
            running_.store(false, std::memory_order::release);
        }
        cv_.notify_all();
        loggerFuture_.get();
    }

    void PeriodicMetricsLogger::LogMetrics()
    {
        auto currentTime = enjima::runtime::GetSystemTimeMillis();
        auto counterMap = pProfiler_->GetCounterMap();
        for (const auto& counterPair: counterMap) {
            pCounterLogger_->info("{},{},{}", currentTime, counterPair.first,
                    dynamic_cast<Counter<uint64_t>*>(counterPair.second)->GetCount());
        }

        auto tpGaugeMap = pProfiler_->GetThroughputGaugeMap();
        for (const auto& tpGaugePair: tpGaugeMap) {
            pThroughputLogger_->info("{},{},{:.2f}", currentTime, tpGaugePair.first, tpGaugePair.second->GetVal());
        }

        auto latHistogramMap = pProfiler_->GetHistogramMap();
        for (const auto& latHistPair: latHistogramMap) {
            pLatencyLogger_->info("{},{},{:.2f},{}", currentTime, latHistPair.first, latHistPair.second->GetAverage(),
                    latHistPair.second->GetPercentile(95));
#if ENJIMA_METRICS_LEVEL >= 3
            auto instanceCount = 0;
            for (auto metricVecPtr: latHistPair.second->GetMetricVectorPtrs()) {
                instanceCount++;
                auto opCount = 0;
                auto prevVal = metricVecPtr->at(0);
                for (auto additionalVal: *metricVecPtr) {
                    opCount++;
                    pLatencyLogger_->info("{},{},{},{}", currentTime,
                            std::string(latHistPair.first).append("_additional_").append(std::to_string(opCount)),
                            std::string(std::to_string(instanceCount)).append("-").append(std::to_string(opCount)),
                            additionalVal - prevVal);
                    prevVal = additionalVal;
                }
            }
            latHistPair.second->ClearMetricVectorPtrs();
#endif
        }

        auto floatAvgGaugeMap = pProfiler_->GetDoubleAverageGaugeMap();
        for (const auto& floatAvgGaugePair: floatAvgGaugeMap) {
            pOpMetricsLogger_->info("{},{},{}", currentTime, floatAvgGaugePair.first,
                    floatAvgGaugePair.second->GetVal());
        }

        auto pendingEventsGaugeMap = pProfiler_->GetPendingEventsGaugeMap();
        for (const auto& pendingEventsGaugePair: pendingEventsGaugeMap) {
            pOpMetricsLogger_->info("{},{},{}", currentTime, pendingEventsGaugePair.first,
                    pendingEventsGaugePair.second->GetVal());
        }

        auto costGaugeMap = pProfiler_->GetCostGaugeMap();
        for (const auto& costGaugePair: costGaugeMap) {
            pOpMetricsLogger_->info("{},{},{:.4f}", currentTime, costGaugePair.first, costGaugePair.second->GetVal());
        }

        auto selectivityGaugeMap = pProfiler_->GetSelectivityGaugeMap();
        for (const auto& selectivityGaugePair: selectivityGaugeMap) {
            pOpMetricsLogger_->info("{},{},{:.4f}", currentTime, selectivityGaugePair.first,
                    selectivityGaugePair.second->GetVal());
        }

        auto uLongGaugeMap = pProfiler_->GetUnsignedLongGaugeMap();
        for (const auto& uLongGaugePair: uLongGaugeMap) {
            pOpMetricsLogger_->info("{},{},{}", currentTime, uLongGaugePair.first, uLongGaugePair.second->GetVal());
        }

        if (systemMetricsEnabled_) {
            pProfiler_->UpdateSystemMetrics();
            auto sysGaugeLMap = pProfiler_->GetSystemGaugeLongMap();
            for (const auto& sysGaugePair: sysGaugeLMap) {
                pSysMetricsLogger_->info("{},{},{},{}", currentTime, sysGaugePair.first,
                        sysGaugePair.second->GetProcessId(), sysGaugePair.second->GetVal());
            }
            auto sysGaugeULMap = pProfiler_->GetSystemGaugeUnsignedLongMap();
            for (const auto& sysGaugePair: sysGaugeULMap) {
                pSysMetricsLogger_->info("{},{},{},{}", currentTime, sysGaugePair.first,
                        sysGaugePair.second->GetProcessId(), sysGaugePair.second->GetVal());
            }
            auto sysGaugeDMap = pProfiler_->GetSystemGaugeDoubleMap();
            for (const auto& sysGaugePair: sysGaugeDMap) {
                pSysMetricsLogger_->info("{},{},{},{:.2f}", currentTime, sysGaugePair.first,
                        sysGaugePair.second->GetProcessId(), sysGaugePair.second->GetVal());
            }
        }
    }

    void PeriodicMetricsLogger::FlushMetricsToFile()
    {
        pCounterLogger_->flush();
        pThroughputLogger_->flush();
        pLatencyLogger_->flush();
        pOpMetricsLogger_->flush();
        if (systemMetricsEnabled_) {
            pSysMetricsLogger_->flush();
        }
    }

    pid_t PeriodicMetricsLogger::GetThreadId() const
    {
        return threadId_.load(std::memory_order::acquire);
    }
}// namespace enjima::metrics

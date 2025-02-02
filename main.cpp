#include "enjima/operators/SingleInputOperator.h"
#include "enjima/runtime/ExecutionEngine.h"
#include "enjima/runtime/StreamingJob.h"
#include "yaml-cpp/yaml.h"
#include <iostream>

struct DoubleInputOp : enjima::operators::SingleInputOperator<uint64_t, uint64_t> {
public:
    DoubleInputOp(enjima::operators::OperatorID opId, const std::string& opName,
            enjima::memory::MemoryManager* memoryManager)
        : enjima::operators::SingleInputOperator<uint64_t, uint64_t>(opId, opName)
    {
        this->Initialize(nullptr, memoryManager, nullptr);
    }

    uint8_t ProcessQueue() override
    {
        return 0;
    }

private:
    void ProcessEvent(uint64_t timestamp, uint64_t inputEvent, enjima::core::OutputCollector* collector) override
    {
        auto outEvent = inputEvent * 2;
        collector->CollectWithTimestamp<uint64_t>(timestamp, outEvent);
    }

    void ProcessBatch(void* inputBuffer, uint32_t numRecordsToRead, void* outputBuffer,
            enjima::core::OutputCollector* collector) override
    {
        auto numEmitted = 0u;
        auto nextOutRecordPtr = static_cast<enjima::core::Record<uint64_t>*>(outputBuffer);
        for (uint32_t i = 0; i < numRecordsToRead; i++) {
            enjima::core::Record<uint64_t>* inputRecord = static_cast<enjima::core::Record<uint64_t>*>(inputBuffer) + i;
            new (nextOutRecordPtr)
                    enjima::core::Record<uint64_t>(inputRecord->GetTimestamp(), inputRecord->GetData() * 2);
            nextOutRecordPtr += 1;
            numEmitted++;
        }
        collector->CollectBatch<uint64_t>(outputBuffer, numEmitted);
    }
};

int main()
{
    std::cout << "Hello, World!" << std::endl;
    auto config = YAML::LoadFile(std::string("config.yaml"));
    auto testConfig = config["test"];
    std::cout << testConfig.as<std::string>() << std::endl;

    auto* execution_engine = new enjima::runtime::ExecutionEngine();
    execution_engine->Start();

    enjima::runtime::StreamingJob streaming_job;
    DoubleInputOp single_in_op{0, "double_in_op", nullptr};
    enjima::core::JobID job_id = execution_engine->Submit(streaming_job);
    std::this_thread::sleep_for(std::chrono::seconds(10));
    execution_engine->Cancel(job_id);

    execution_engine->Shutdown();
    delete execution_engine;

    return 0;
}

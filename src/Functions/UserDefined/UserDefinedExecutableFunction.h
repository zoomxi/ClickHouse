#pragma once

#include <string>

#include <DataTypes/IDataType.h>
#include <Processors/Sources/ShellCommandSource.h>
#include <Interpreters/IExternalLoadable.h>
#include <Common/ExecutableProcessPool.h>


namespace DB
{

struct UserDefinedExecutableFunctionArgument
{
    DataTypePtr type;
    String name;
};

struct UserDefinedExecutableFunctionParameter
{
    String name;
    DataTypePtr type;
};

struct UserDefinedExecutableFunctionConfiguration
{
    std::string name;
    std::string command;
    std::vector<std::string> command_arguments;
    std::vector<UserDefinedExecutableFunctionArgument> arguments;
    std::vector<UserDefinedExecutableFunctionParameter> parameters;
    DataTypePtr result_type;
    String result_name;
    bool is_deterministic;
    
    // AI Function configuration
    String transport;  // "pipe" or "shared_memory"
    size_t shared_memory_size;       // Shared memory size in bytes
    size_t connection_timeout_ms;    // Subprocess startup connection timeout
    size_t operation_timeout_ms;     // UDS control message send/receive timeout
    size_t health_check_interval_seconds;  // Health check interval
    size_t ping_timeout_ms;          // PING/PONG timeout
    bool need_ai_model;              // Whether to load AI parameters at execution time
    String data_format;              // Data format for AI model parameters
};

class UserDefinedExecutableFunction final : public IExternalLoadable
{
public:

    // Constructor for pipe mode
    UserDefinedExecutableFunction(
        const UserDefinedExecutableFunctionConfiguration & configuration_,
        std::shared_ptr<ShellCommandSourceCoordinator> coordinator_,
        const ExternalLoadableLifetime & lifetime_);

    // Constructor for shared_memory mode
    UserDefinedExecutableFunction(
        const UserDefinedExecutableFunctionConfiguration & configuration_,
        std::shared_ptr<ExecutableProcessPool> process_pool_,
        const ExternalLoadableLifetime & lifetime_);

    const ExternalLoadableLifetime & getLifetime() const override
    {
        return lifetime;
    }

    std::string getLoadableName() const override
    {
        return configuration.name;
    }

    bool supportUpdates() const override
    {
        return true;
    }

    bool isModified() const override
    {
        return true;
    }

    std::shared_ptr<IExternalLoadable> clone() const override
    {
        if (process_pool)
            return std::make_shared<UserDefinedExecutableFunction>(configuration, process_pool, lifetime);

        return std::make_shared<UserDefinedExecutableFunction>(configuration, coordinator, lifetime);
    }

    const UserDefinedExecutableFunctionConfiguration & getConfiguration() const
    {
        return configuration;
    }

    std::shared_ptr<ShellCommandSourceCoordinator> getCoordinator() const
    {
        return coordinator;
    }

    std::shared_ptr<ExecutableProcessPool> getProcessPool() const
    {
        return process_pool;
    }

    std::shared_ptr<UserDefinedExecutableFunction> shared_from_this()
    {
        return std::static_pointer_cast<UserDefinedExecutableFunction>(IExternalLoadable::shared_from_this());
    }

    std::shared_ptr<const UserDefinedExecutableFunction> shared_from_this() const
    {
        return std::static_pointer_cast<const UserDefinedExecutableFunction>(IExternalLoadable::shared_from_this());
    }

private:
    UserDefinedExecutableFunctionConfiguration configuration;
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator;  // For pipe mode
    std::shared_ptr<ExecutableProcessPool> process_pool;         // For shared_memory mode
    ExternalLoadableLifetime lifetime;
};

}

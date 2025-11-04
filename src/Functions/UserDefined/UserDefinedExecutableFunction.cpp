#include <Functions/UserDefined/UserDefinedExecutableFunction.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <Processors/Sources/ShellCommandSource.h>


namespace DB
{

// Constructor for pipe mode
UserDefinedExecutableFunction::UserDefinedExecutableFunction(
    const UserDefinedExecutableFunctionConfiguration & configuration_,
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator_,
    const ExternalLoadableLifetime & lifetime_)
    : configuration(configuration_)
    , coordinator(std::move(coordinator_))
    , process_pool(nullptr)
    , lifetime(lifetime_)
{
}

// Constructor for shared_memory mode
UserDefinedExecutableFunction::UserDefinedExecutableFunction(
    const UserDefinedExecutableFunctionConfiguration & configuration_,
    std::shared_ptr<ExecutableProcessPool> process_pool_,
    const ExternalLoadableLifetime & lifetime_)
    : configuration(configuration_)
    , coordinator(nullptr)
    , process_pool(std::move(process_pool_))
    , lifetime(lifetime_)
{
}

}

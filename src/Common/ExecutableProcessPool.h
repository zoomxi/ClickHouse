#pragma once

#include <base/BorrowedObjectPool.h>
#include <Common/SharedMemoryCommand.h>
#include <Common/logger_useful.h>

#include <memory>
#include <string>
#include <atomic>
#include <sys/mman.h>

namespace DB
{
/**
 * @brief Shared memory region with RAII semantics
 */
class SharedMemoryRegion
{
public:
    SharedMemoryRegion() = default;
    
    ~SharedMemoryRegion()
    {
        detach();
    }
    
    // Move-only
    SharedMemoryRegion(SharedMemoryRegion && other) noexcept
        : ptr(other.ptr)
        , size(other.size)
        , fd(other.fd)
        , name(std::move(other.name))
        , is_creator(other.is_creator)
    {
        other.ptr = nullptr;
        other.size = 0;
        other.fd = -1;
        other.is_creator = false;
    }
    
    SharedMemoryRegion & operator=(SharedMemoryRegion && other) noexcept
    {
        if (this != &other)
        {
            detach();
            ptr = other.ptr;
            size = other.size;
            fd = other.fd;
            name = std::move(other.name);
            is_creator = other.is_creator;
            
            other.ptr = nullptr;
            other.size = 0;
            other.fd = -1;
            other.is_creator = false;
        }
        return *this;
    }
    
    // Disable copy
    SharedMemoryRegion(const SharedMemoryRegion &) = delete;
    SharedMemoryRegion & operator=(const SharedMemoryRegion &) = delete;
    
    /// Create new shared memory region
    void create(const std::string & name_, size_t size_);
    
    /// Detach and cleanup
    void detach();
    
    /// Resize shared memory (creates new region and copies data)
    void resize(size_t new_size);
    
    /// Check if valid
    bool isValid() const { return ptr != nullptr && ptr != MAP_FAILED && fd != -1; }
    
    /// Getters
    void * getPtr() const { return ptr; }
    size_t getSize() const { return size; }
    const std::string & getName() const { return name; }
    
private:
    void * ptr = nullptr;
    size_t size = 0;
    int fd = -1;
    std::string name;
    bool is_creator = false;
    
    static LoggerPtr getLogger();
};

/// Generate unique shared memory name
std::string generateSharedMemoryName(const std::string & prefix = "clickhouse_shared_memory");


/**
 * @brief UDF Process Instance
 * 
 * Represents a single UDF subprocess with its dedicated:
 * - SharedMemoryCommand connection
 * - SharedMemoryRegion for data exchange
 * - Parameter hash (for dynamic argument matching)
 * 
 * This is the unit that gets borrowed from the pool.
 */
struct ExecutableProcessInstance
{
    /// UDS connection to subprocess
    std::unique_ptr<SharedMemoryCommand> connection;
    
    /// Dedicated shared memory for this subprocess
    SharedMemoryRegion shared_memory;
    
    /// Subprocess PID (for debugging)
    pid_t pid = -1;
    
    /// Hash of dynamic arguments used to create this process
    /// Used to ensure processes with different parameters are not reused
    size_t argument_hash = 0;
    
    ExecutableProcessInstance() = default;
    
    // Move-only
    ExecutableProcessInstance(ExecutableProcessInstance &&) = default;
    ExecutableProcessInstance & operator=(ExecutableProcessInstance &&) = default;
    
    ExecutableProcessInstance(const ExecutableProcessInstance &) = delete;
    ExecutableProcessInstance & operator=(const ExecutableProcessInstance &) = delete;
    
    /// Check if valid (connection and shared memory both ready)
    bool isValid() const
    {
        return connection != nullptr && shared_memory.isValid();
    }
    
    /// Execute a request: write input block, read result block
    Block execute(const Block & input, const Block & header);
};


/**
 * @brief UDF Process Pool
 * 
 * Manages a pool of UDF subprocess instances using BorrowedObjectPool.
 * 
 * Each instance includes:
 * - SharedMemoryCommand connection to subprocess
 * - Dedicated SharedMemoryRegion for data exchange
 * 
 * Usage:
 * @code
 *   ExecutableProcessPool pool(config);
 *   
 *   // Borrow a process instance (includes connection + shared memory)
 *   ExecutableProcessInstance instance;
 *   pool.borrowProcess(instance);
 *   
 *   // Execute query
 *   Block result = instance.execute(input, header);
 *   
 *   // Return to pool
 *   pool.returnProcess(std::move(instance));
 * @endcode
 */
class ExecutableProcessPool
{
public:
    struct Config
    {
        /// Number of UDF subprocess instances in pool
        size_t pool_size = 10;
        
        /// SharedMemoryCommand configuration (command, arguments, timeouts, etc.)
        SharedMemoryCommand::Config uds_config;
        
        /// Initial shared memory size for each subprocess
        size_t initial_shared_memory_size = 16 * 1024 * 1024;  // 16MB
        
        /// Maximum shared memory size
        size_t max_shared_memory_size = 1024 * 1024 * 1024;    // 1GB
        
        /// Timeout for borrowing process (milliseconds)
        size_t borrow_timeout_ms = 5000;
        
        /// Enable health checking
        bool enable_health_check = true;
        
        /// Health check interval (0 = check on every borrow)
        size_t health_check_interval_ms = 30000;
        
        /// Enable argument matching (for need_ai_model=true)
        /// When enabled, processes with mismatched arguments will be evicted
        bool enable_argument_matching = false;
    };
    
    explicit ExecutableProcessPool(Config config_);
    ~ExecutableProcessPool();
    
    /**
     * @brief Borrow a UDF process instance from pool
     * 
     * Blocks until an instance is available.
     * The instance includes both UDS connection and dedicated shared memory.
     * 
     * @param dest Destination to receive the process instance
     * @param dynamic_arguments Optional dynamic arguments to pass when creating new process
     * @throws Exception if health check fails or timeout
     */
    void borrowProcess(ExecutableProcessInstance & dest, const std::vector<std::string> & dynamic_arguments = {});
    
    /**
     * @brief Try to borrow with timeout
     * 
     * @param dest Destination to receive the process instance
     * @param dynamic_arguments Optional dynamic arguments to pass when creating new process
     * @return true if borrowed successfully, false if timeout
     */
    bool tryBorrowProcess(ExecutableProcessInstance & dest, const std::vector<std::string> & dynamic_arguments = {});
    
    /**
     * @brief Return a process instance to the pool
     * 
     * @param instance Process instance to return
     */
    void returnProcess(ExecutableProcessInstance && instance);
    
    /// Get pool statistics
    struct Stats
    {
        size_t pool_size;
        size_t allocated_processes;
        size_t borrowed_processes;
        size_t health_check_failures;
        size_t total_requests;
        size_t total_errors;
    };
    
    Stats getStats() const;
    
private:
    Config config;
    BorrowedObjectPool<ExecutableProcessInstance> pool;
    
    /// Statistics
    mutable std::atomic<size_t> health_check_failures{0};
    mutable std::atomic<size_t> total_requests{0};
    mutable std::atomic<size_t> total_errors{0};
    mutable std::atomic<uint64_t> last_health_check_ms{0};
    
    /// Create a new UDF process instance (connection + shared memory)
    ExecutableProcessInstance createProcessInstance(const std::vector<std::string> & dynamic_arguments = {});
    
    /// Check if process instance is healthy
    bool isProcessHealthy(ExecutableProcessInstance & instance);
    
    /// Ensure shared memory is large enough
    void ensureSharedMemorySize(ExecutableProcessInstance & instance, size_t required_size);
    
    LoggerPtr logger;
};


/**
 * @brief RAII wrapper for borrowed UDF process instance
 * 
 * Automatically returns process to pool on destruction.
 */
class PooledExecutableProcess
{
public:
    PooledExecutableProcess(ExecutableProcessPool & pool_, ExecutableProcessInstance instance_)
        : pool(&pool_)
        , instance(std::move(instance_))
    {
    }
    
    ~PooledExecutableProcess()
    {
        if (pool)
        {
            pool->returnProcess(std::move(instance));
        }
    }
    
    // Move-only
    PooledExecutableProcess(PooledExecutableProcess && other) noexcept
        : pool(other.pool)
        , instance(std::move(other.instance))
    {
        other.pool = nullptr;
    }
    
    PooledExecutableProcess & operator=(PooledExecutableProcess && other) noexcept
    {
        if (this != &other)
        {
            if (pool)
                pool->returnProcess(std::move(instance));
            
            pool = other.pool;
            instance = std::move(other.instance);
            other.pool = nullptr;
        }
        return *this;
    }
    
    // Disable copy
    PooledExecutableProcess(const PooledExecutableProcess &) = delete;
    PooledExecutableProcess & operator=(const PooledExecutableProcess &) = delete;
    
    /// Execute query
    Block execute(const Block & input, const Block & header)
    {
        return instance.execute(input, header);
    }
    
    /// Access the instance
    ExecutableProcessInstance & get() { return instance; }
    const ExecutableProcessInstance & get() const { return instance; }
    
    ExecutableProcessInstance * operator->() { return &instance; }
    const ExecutableProcessInstance * operator->() const { return &instance; }
    
private:
    ExecutableProcessPool * pool;
    ExecutableProcessInstance instance;
};

} // namespace DB

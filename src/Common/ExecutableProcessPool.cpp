#include <Common/ExecutableProcessPool.h>
#include <base/errnoToString.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/ReadBufferFromMemory.h>
#include <Core/Block.h>

#include <fmt/format.h>
#include <cstring>
#include <cerrno>
#include <signal.h>
#include <thread>
#include <chrono>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

// ============================================================================
// SharedMemoryRegion Implementation
// ============================================================================

LoggerPtr SharedMemoryRegion::getLogger()
{
    return ::getLogger("SharedMemoryRegion");
}

void SharedMemoryRegion::create(const std::string & name_, size_t size_)
{
    name = name_;
    size = size_;
    is_creator = true;
    
    auto logger = getLogger();
    LOG_DEBUG(logger, "Creating shared memory: name={}, size={} bytes", name, size);
    
    // Create shared memory object
    int flags = O_CREAT | O_RDWR | O_EXCL;
    fd = shm_open(name.c_str(), flags, 0600);
    
    if (fd == -1)
    {
        if (errno == EEXIST)
        {
            LOG_WARNING(logger, "Shared memory '{}' already exists, unlinking and recreating", name);
            shm_unlink(name.c_str());
            fd = shm_open(name.c_str(), flags, 0600);
        }
        
        if (fd == -1)
        {
            throw ErrnoException(
                ErrorCodes::CANNOT_OPEN_FILE,
                "Cannot create shared memory '{}': {}",
                name, errnoToString());
        }
    }
    
    // Set size
    if (ftruncate(fd, static_cast<off_t>(size)) == -1)
    {
        int saved_errno = errno;
        close(fd);
        shm_unlink(name.c_str());
        throw ErrnoException(
            ErrorCodes::CANNOT_OPEN_FILE,
            "Cannot set shared memory size to {}: {}",
            size, errnoToString(saved_errno));
    }
    
    // Map to process address space
    ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    
    if (ptr == MAP_FAILED)
    {
        int saved_errno = errno;
        close(fd);
        shm_unlink(name.c_str());
        throw ErrnoException(
            ErrorCodes::CANNOT_OPEN_FILE,
            "Cannot mmap shared memory: {}",
            errnoToString(saved_errno));
    }
    
    LOG_INFO(logger, "Created shared memory: name={}, size={}, addr={}", name, size, ptr);
}

void SharedMemoryRegion::detach()
{
    auto logger = getLogger();
    
    if (ptr && ptr != MAP_FAILED)
    {
        LOG_DEBUG(logger, "Unmapping shared memory: name={}, addr={}", name, ptr);
        munmap(ptr, size);
        ptr = nullptr;
    }
    
    if (fd != -1)
    {
        close(fd);
        fd = -1;
    }
    
    if (!name.empty() && is_creator)
    {
        LOG_DEBUG(logger, "Unlinking shared memory: {}", name);
        shm_unlink(name.c_str());
    }
    
    name.clear();
    size = 0;
    is_creator = false;
}

void SharedMemoryRegion::resize(size_t new_size)
{
    if (!is_creator)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot resize shared memory that was not created by this instance");
    }
    
    auto logger = getLogger();
    LOG_DEBUG(logger, "Resizing shared memory from {} to {} bytes", size, new_size);
    
    std::string old_name = name;
    void * old_ptr = ptr;
    size_t old_size = size;
    int old_fd = fd;
    
    // Create new region with new name
    std::string new_name = generateSharedMemoryName();
    
    name.clear();
    ptr = nullptr;
    fd = -1;
    size = 0;
    
    create(new_name, new_size);
    
    // Copy data from old region
    if (old_ptr && old_ptr != MAP_FAILED && old_size > 0)
    {
        size_t copy_size = std::min(old_size, new_size);
        memcpy(ptr, old_ptr, copy_size);
        LOG_DEBUG(logger, "Copied {} bytes from old region to new region", copy_size);
        
        // Cleanup old region
        munmap(old_ptr, old_size);
        close(old_fd);
        shm_unlink(old_name.c_str());
    }
    
    LOG_INFO(logger, "Resized shared memory from {} to {} bytes", old_size, new_size);
}

std::string generateSharedMemoryName(const std::string & prefix)
{
    static std::atomic<uint64_t> counter{0};
    auto count = counter.fetch_add(1);
    
    // macOS has 31 char limit for shm names (PSHMNAMLEN)
    // Format: /prefix_counter (e.g., /ch_19421_0)
    return fmt::format("/{}{}", prefix, count);
}


// ============================================================================
// ExecutableProcessInstance Implementation
// ============================================================================

Block ExecutableProcessInstance::execute(const Block & input, const Block & header)
{
    if (!isValid())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ExecutableProcessInstance is not valid");
    }
    
    auto logger = ::getLogger("ExecutableProcessInstance");
    
    size_t row_count = input.rows();
    LOG_DEBUG(logger, "Executing UDF: pid={}, rows={}, columns={}", 
              pid, row_count, input.columns());
    
    // 1. Serialize input block to shared memory using Native format
    std::vector<char> temp_buffer;
    WriteBufferFromVector<std::vector<char>> temp_write_buffer(temp_buffer);
    
    auto input_header = std::make_shared<Block>(input.cloneEmpty());
    NativeWriter writer(
        temp_write_buffer,
        0,
        input_header,
        std::nullopt,
        false
    );
    
    writer.write(input);
    writer.flush();
    temp_write_buffer.finalize();
    
    size_t bytes_written = temp_buffer.size();
    
    // Check if shared memory is large enough
    if (bytes_written > shared_memory.getSize())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Serialized block size {} exceeds shared memory size {}",
            bytes_written, shared_memory.getSize());
    }
    
    // Copy to shared memory
    memcpy(shared_memory.getPtr(), temp_buffer.data(), bytes_written);
    
    // Calculate checksum
    uint64_t checksum = SharedMemoryCommand::ControlMessage::calculateChecksum(
        shared_memory.getPtr(), bytes_written);
    
    LOG_DEBUG(logger, "Serialized {} bytes to shared memory '{}', checksum={:016x}",
              bytes_written, shared_memory.getName(), checksum);
    
    // 2. Send DATA_READY message
    SharedMemoryCommand::ControlMessage msg;
    msg.type = SharedMemoryCommand::MessageType::DATA_READY;
    msg.protocol_version = SharedMemoryCommand::ControlMessage::CURRENT_PROTOCOL_VERSION;
    msg.native_version = SharedMemoryCommand::ControlMessage::getCurrentNativeVersion();
    msg.data_size = bytes_written;
    msg.row_count = row_count;
    msg.checksum = checksum;
    strncpy(msg.shm_name, shared_memory.getName().c_str(), sizeof(msg.shm_name) - 1);
    strncpy(msg.format_name, "Native", sizeof(msg.format_name) - 1);
    
    connection->sendControlMessage(msg);
    
    // 3. Wait for DATA_CONSUMED
    SharedMemoryCommand::ControlMessage consumed_msg = connection->receiveControlMessage();
    
    if (consumed_msg.type != SharedMemoryCommand::MessageType::DATA_CONSUMED)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected DATA_CONSUMED, got {}",
            static_cast<int>(consumed_msg.type));
    }
    
    LOG_DEBUG(logger, "Input data consumed by subprocess");
    
    // 4. Wait for RESULT_READY
    SharedMemoryCommand::ControlMessage result_msg = connection->receiveControlMessage();
    
    if (result_msg.type != SharedMemoryCommand::MessageType::RESULT_READY)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected RESULT_READY, got {}",
            static_cast<int>(result_msg.type));
    }
    
    LOG_DEBUG(logger, "Result ready: {} bytes, {} rows, checksum={:016x}",
              result_msg.data_size, result_msg.row_count, result_msg.checksum);
    
    // Verify protocol version
    if (!result_msg.isProtocolCompatible())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Incompatible protocol version: subprocess={:08x}, current={:08x}",
            result_msg.protocol_version,
            SharedMemoryCommand::ControlMessage::CURRENT_PROTOCOL_VERSION);
    }
    
    // Verify checksum
    if (!result_msg.verifyChecksum(shared_memory.getPtr()))
    {
        LOG_ERROR(logger, "Checksum mismatch: expected={:016x}, calculated={:016x}",
                  result_msg.checksum,
                  SharedMemoryCommand::ControlMessage::calculateChecksum(
                      shared_memory.getPtr(), result_msg.data_size));
        
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Checksum mismatch: expected={:016x}, calculated={:016x}",
            result_msg.checksum,
            SharedMemoryCommand::ControlMessage::calculateChecksum(
                shared_memory.getPtr(), result_msg.data_size));
    }
    
    LOG_DEBUG(logger, "Checksum verified successfully");
    
    Block result;
    
    try
    {
        // 5. Deserialize result from shared memory
        ReadBufferFromMemory read_buffer(
            reinterpret_cast<const char*>(shared_memory.getPtr()),
            result_msg.data_size
        );
        
        NativeReader reader(read_buffer, header, 0, std::nullopt);
        
        // Read all blocks from Native format (may contain multiple blocks if rows > DEFAULT_BLOCK_SIZE)
        Blocks blocks;
        while (true)
        {
            Block block = reader.read();
            if (!block.rows())
                break;
            blocks.push_back(std::move(block));
        }
        
        // Concatenate all blocks into a single result block
        if (blocks.empty())
        {
            result = header.cloneEmpty();
        }
        else if (blocks.size() == 1)
        {
            result = std::move(blocks[0]);
        }
        else
        {
            // Concatenate multiple blocks
            result = concatenateBlocks(blocks);
        }
        
        LOG_DEBUG(logger, "Deserialized result: {} rows, {} columns (from {} block(s))",
                  result.rows(), result.columns(), blocks.size());
    }
    catch (const Exception & e)
    {
        LOG_ERROR(logger, "Failed to deserialize result from shared memory: {}", e.what());
        
        // Still send RESULT_CONSUMED to avoid hanging
        SharedMemoryCommand::ControlMessage ack_msg;
        ack_msg.type = SharedMemoryCommand::MessageType::RESULT_CONSUMED;
        connection->sendControlMessage(ack_msg);
        
        throw;
    }
    
    // 6. Send RESULT_CONSUMED
    SharedMemoryCommand::ControlMessage ack_msg;
    ack_msg.type = SharedMemoryCommand::MessageType::RESULT_CONSUMED;
    connection->sendControlMessage(ack_msg);
    
    return result;
}


// ============================================================================
// ExecutableProcessPool Implementation
// ============================================================================

ExecutableProcessPool::ExecutableProcessPool(Config config_)
    : config(std::move(config_))
    , pool(config.pool_size)
    , logger(::getLogger("ExecutableProcessPool"))
{
    LOG_INFO(logger, "Initializing UDF process pool: pool_size={}, command={}",
             config.pool_size, config.uds_config.command);
}

ExecutableProcessPool::~ExecutableProcessPool()
{
    LOG_INFO(logger, "Destroying UDF process pool");
}

ExecutableProcessInstance ExecutableProcessPool::createProcessInstance(const std::vector<std::string> & dynamic_arguments)
{
    auto logger_local = logger;
    LOG_DEBUG(logger_local, "Creating new UDF process instance");
    
    ExecutableProcessInstance instance;
    
    // 1. Calculate argument hash for this process
    std::hash<std::string> hasher;
    size_t arg_hash = 0;
    for (const auto & arg : dynamic_arguments)
    {
        arg_hash ^= hasher(arg) + 0x9e3779b9 + (arg_hash << 6) + (arg_hash >> 2);
    }
    instance.argument_hash = arg_hash;
    
    // 2. Create UDS connection (spawns subprocess) with dynamic arguments
    auto uds_config = config.uds_config;
    
    // Merge dynamic arguments with static config arguments
    if (!dynamic_arguments.empty())
    {
        LOG_DEBUG(logger_local, "Adding {} dynamic arguments to process (hash={:016x})", 
                  dynamic_arguments.size(), arg_hash);
        uds_config.arguments.insert(uds_config.arguments.end(), 
                                    dynamic_arguments.begin(), 
                                    dynamic_arguments.end());
    }
    
    instance.connection = SharedMemoryCommand::execute(uds_config);
    instance.pid = instance.connection->getPid();
    
    LOG_INFO(logger_local, "Spawned UDF subprocess: pid={}, arg_hash={:016x}", instance.pid, arg_hash);
    
    // 3. Create dedicated shared memory for this subprocess
    // Use short prefix to avoid macOS 31-char name limit
    std::string shm_name = generateSharedMemoryName(
        fmt::format("ch_{}_", instance.pid));
    
    instance.shared_memory.create(shm_name, config.initial_shared_memory_size);
    
    LOG_INFO(logger_local, "Created shared memory for subprocess pid={}: name={}, size={}",
             instance.pid, shm_name, config.initial_shared_memory_size);
    
    return instance;
}

bool ExecutableProcessPool::isProcessHealthy(ExecutableProcessInstance & instance)
{
    if (!config.enable_health_check)
        return true;
    
    // Always perform a lightweight process liveness check first
    // This catches cases where process was killed externally (e.g., kill -9)
    // Check if process is still alive using kill(pid, 0)
    if (kill(instance.pid, 0) != 0)
    {
        LOG_WARNING(logger, "Health check failed for pid={}: process not alive (errno={})",
                   instance.pid, errno);
        health_check_failures.fetch_add(1);
        return false;
    }
    
    // Check if we need to do full PING/PONG health check based on interval
    if (config.health_check_interval_ms > 0)
    {
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        
        auto last_check = last_health_check_ms.load();
        
        if (now_ms - static_cast<int64_t>(last_check) < static_cast<int64_t>(config.health_check_interval_ms))
        {
            // Skip expensive PING/PONG check, but process liveness check already passed
            return true;
        }
        
        last_health_check_ms.store(now_ms);
    }
    
    // Perform full PING/PONG health check
    try
    {
        // Send PING
        SharedMemoryCommand::ControlMessage ping_msg;
        ping_msg.type = SharedMemoryCommand::MessageType::PING;
        instance.connection->sendControlMessage(ping_msg);
        
        // Wait for PONG
        auto pong_msg = instance.connection->receiveControlMessage();
        
        if (pong_msg.type != SharedMemoryCommand::MessageType::PONG)
        {
            LOG_WARNING(logger, "Health check failed for pid={}: expected PONG, got {}",
                       instance.pid, static_cast<int>(pong_msg.type));
            health_check_failures.fetch_add(1);
            return false;
        }
        
        LOG_DEBUG(logger, "Health check passed for pid={}", instance.pid);
        return true;
    }
    catch (const Exception & e)
    {
        LOG_WARNING(logger, "Health check failed for pid={}: {}", instance.pid, e.what());
        health_check_failures.fetch_add(1);
        return false;
    }
}

void ExecutableProcessPool::ensureSharedMemorySize(ExecutableProcessInstance & instance, size_t required_size)
{
    if (required_size > config.max_shared_memory_size)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Required shared memory size {} exceeds maximum {}",
            required_size, config.max_shared_memory_size);
    }
    
    if (instance.shared_memory.getSize() < required_size)
    {
        LOG_DEBUG(logger, "Resizing shared memory for pid={} from {} to {}",
                  instance.pid, instance.shared_memory.getSize(), required_size);
        
        // Round up to next power of 2 for better reuse
        size_t new_size = instance.shared_memory.getSize();
        while (new_size < required_size)
            new_size *= 2;
        
        new_size = std::min(new_size, config.max_shared_memory_size);
        
        instance.shared_memory.resize(new_size);
    }
}

void ExecutableProcessPool::borrowProcess(ExecutableProcessInstance & dest, const std::vector<std::string> & dynamic_arguments)
{
    LOG_DEBUG(logger, "Borrowing UDF process from pool");
    
    total_requests.fetch_add(1);
    
    // Calculate hash for requested arguments (if argument matching is enabled)
    size_t requested_hash = 0;
    if (config.enable_argument_matching)
    {
        std::hash<std::string> hasher;
        for (const auto & arg : dynamic_arguments)
        {
            requested_hash ^= hasher(arg) + 0x9e3779b9 + (requested_hash << 6) + (requested_hash >> 2);
        }
    }
    
    // Try to borrow a process - it might have different arguments
    pool.borrowObject(dest, [this, &dynamic_arguments]() { return createProcessInstance(dynamic_arguments); });
    
    // Check if borrowed process has matching arguments (only if argument matching is enabled)
    if (config.enable_argument_matching && dest.argument_hash != 0 && dest.argument_hash != requested_hash)
    {
        LOG_WARNING(logger, 
            "Borrowed process pid={} has mismatched arguments (process_hash={:016x}, requested_hash={:016x}). "
            "Evicting old process and creating new one.",
            dest.pid, dest.argument_hash, requested_hash);
        
        total_errors.fetch_add(1);
        
        // Evict the mismatched process (don't return to pool)
        // The process will be destroyed when dest goes out of scope at the end
        ExecutableProcessInstance evicted_process = std::move(dest);
        
        // Check if pool is full
        if (pool.isFull())
        {
            // Pool is full, we need to evict another process to make room
            // Try to borrow one more process and evict it
            ExecutableProcessInstance to_evict;
            bool borrowed = pool.tryBorrowObject(
                to_evict,
                []() -> ExecutableProcessInstance { 
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Should not create new process when pool is full"); 
                },
                0  // No timeout, just try once
            );
            
            if (borrowed)
            {
                LOG_INFO(logger, "Evicting process pid={} (hash={:016x}) to make room for new process with hash={:016x}",
                         to_evict.pid, to_evict.argument_hash, requested_hash);
                // to_evict will be destroyed, not returned to pool
            }
        }
        
        // Create a new process with correct arguments
        dest = createProcessInstance(dynamic_arguments);
        
        // Return the new process to pool for future reuse
        // We need to temporarily mark it as borrowed, then return it
        pool.returnObject(std::move(dest));
        
        // Now borrow it again for current use
        pool.borrowObject(dest, [this, &dynamic_arguments]() { return createProcessInstance(dynamic_arguments); });
        
        LOG_DEBUG(logger, "Created and borrowed new UDF process: pid={}, arg_hash={:016x}", dest.pid, dest.argument_hash);
        return;
    }
    
    // Health check
    if (!isProcessHealthy(dest))
    {
        LOG_WARNING(logger, "Borrowed process pid={} failed health check, creating new one",
                   dest.pid);
        total_errors.fetch_add(1);
        
        // Evict unhealthy process and create new one
        ExecutableProcessInstance evicted_process = std::move(dest);
        dest = createProcessInstance(dynamic_arguments);
        
        // Return new process to pool, then borrow again
        pool.returnObject(std::move(dest));
        pool.borrowObject(dest, [this, &dynamic_arguments]() { return createProcessInstance(dynamic_arguments); });
    }
    
    LOG_DEBUG(logger, "Borrowed UDF process: pid={}, arg_hash={:016x}", dest.pid, dest.argument_hash);
}

bool ExecutableProcessPool::tryBorrowProcess(ExecutableProcessInstance & dest, const std::vector<std::string> & dynamic_arguments)
{
    LOG_DEBUG(logger, "Trying to borrow UDF process with timeout={}ms",
              config.borrow_timeout_ms);
    
    total_requests.fetch_add(1);
    
    // Calculate hash for requested arguments (if argument matching is enabled)
    size_t requested_hash = 0;
    if (config.enable_argument_matching)
    {
        std::hash<std::string> hasher;
        for (const auto & arg : dynamic_arguments)
        {
            requested_hash ^= hasher(arg) + 0x9e3779b9 + (requested_hash << 6) + (requested_hash >> 2);
        }
    }
    
    // Try to borrow with timeout
    bool success = pool.tryBorrowObject(
        dest,
        [this, &dynamic_arguments]() { return createProcessInstance(dynamic_arguments); },
        config.borrow_timeout_ms);
    
    if (!success)
    {
        LOG_WARNING(logger, "Failed to borrow process: timeout after {}ms",
                   config.borrow_timeout_ms);
        total_errors.fetch_add(1);
        return false;
    }
    
    // Check if borrowed process has matching arguments (only if argument matching is enabled)
    if (config.enable_argument_matching && dest.argument_hash != 0 && dest.argument_hash != requested_hash)
    {
        LOG_WARNING(logger, 
            "Borrowed process pid={} has mismatched arguments (process_hash={:016x}, requested_hash={:016x}). "
            "Evicting old process and creating new one.",
            dest.pid, dest.argument_hash, requested_hash);
        
        total_errors.fetch_add(1);
        
        // Evict the mismatched process
        ExecutableProcessInstance evicted_process = std::move(dest);
        
        // Check if pool is full
        if (pool.isFull())
        {
            // Pool is full, evict another process to make room
            ExecutableProcessInstance to_evict;
            bool borrowed = pool.tryBorrowObject(
                to_evict,
                []() -> ExecutableProcessInstance { 
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Should not create new process when pool is full"); 
                },
                0
            );
            
            if (borrowed)
            {
                LOG_INFO(logger, "Evicting process pid={} (hash={:016x}) to make room for new process with hash={:016x}",
                         to_evict.pid, to_evict.argument_hash, requested_hash);
            }
        }
        
        // Create a new process with correct arguments
        dest = createProcessInstance(dynamic_arguments);
        
        // Return to pool for future reuse, then borrow again
        pool.returnObject(std::move(dest));
        pool.borrowObject(dest, [this, &dynamic_arguments]() { return createProcessInstance(dynamic_arguments); });
        
        LOG_DEBUG(logger, "Created and borrowed new UDF process: pid={}, arg_hash={:016x}", dest.pid, dest.argument_hash);
        return true;
    }
    
    // Health check
    if (!isProcessHealthy(dest))
    {
        LOG_WARNING(logger, "Borrowed process pid={} failed health check, creating new one",
                   dest.pid);
        
        ExecutableProcessInstance evicted_process = std::move(dest);
        dest = createProcessInstance(dynamic_arguments);
        
        // Return to pool for future reuse, then borrow again
        pool.returnObject(std::move(dest));
        pool.borrowObject(dest, [this, &dynamic_arguments]() { return createProcessInstance(dynamic_arguments); });
    }
    
    LOG_DEBUG(logger, "Borrowed UDF process: pid={}, arg_hash={:016x}", dest.pid, dest.argument_hash);
    return true;
}

void ExecutableProcessPool::returnProcess(ExecutableProcessInstance && instance)
{
    if (!instance.isValid())
    {
        LOG_WARNING(logger, "Attempting to return invalid process instance");
        total_errors.fetch_add(1);
        return;
    }
    
    LOG_DEBUG(logger, "Returning UDF process to pool: pid={}", instance.pid);
    
    // Return to pool
    pool.returnObject(std::move(instance));
}

ExecutableProcessPool::Stats ExecutableProcessPool::getStats() const
{
    return Stats{
        .pool_size = config.pool_size,
        .allocated_processes = pool.allocatedObjectsSize(),
        .borrowed_processes = pool.borrowedObjectsSize(),
        .health_check_failures = health_check_failures.load(),
        .total_requests = total_requests.load(),
        .total_errors = total_errors.load()
    };
}

} // namespace DB

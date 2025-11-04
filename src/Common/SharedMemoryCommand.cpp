#include <Common/SharedMemoryCommand.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/errnoToString.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/ReadBufferFromMemory.h>

#include <sys/socket.h>
#include <sys/un.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <cstring>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CREATE_SOCKET;
    extern const int CANNOT_CONNECT_SOCKET;
    extern const int CANNOT_FORK;
    extern const int CANNOT_WAITPID;
    extern const int TIMEOUT_EXCEEDED;
    extern const int CHILD_WAS_NOT_EXITED_NORMALLY;
    extern const int LOGICAL_ERROR;
    extern const int SHARED_MEMORY_COMMUNICATION_ERROR;
}

// ============================================================================
// SharedMemoryRegion Implementation
// ============================================================================

SharedMemoryCommand::SharedMemoryRegion::~SharedMemoryRegion()
{
    detach();
}

void SharedMemoryCommand::SharedMemoryRegion::create(const std::string & name_, size_t size_)
{
    name = name_;
    size = size_;
    
    auto logger = SharedMemoryCommand::getLogger();
    LOG_DEBUG(logger, "Creating shared memory: name={}, size={} bytes", name, size);
    
    // Create shared memory object (POSIX)
    int flags = O_CREAT | O_RDWR | O_EXCL;
    fd = shm_open(name.c_str(), flags, 0600);
    
    if (fd == -1)
    {
        throw ErrnoException(
            ErrorCodes::CANNOT_OPEN_FILE,
            "Cannot create shared memory object '{}': {}",
            name, errnoToString());
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

void SharedMemoryCommand::SharedMemoryRegion::attach(const std::string & name_, size_t size_)
{
    name = name_;
    size = size_;
    
    auto logger = SharedMemoryCommand::getLogger();
    LOG_DEBUG(logger, "Attaching to shared memory: name={}", name);
    
    // Open existing shared memory
    fd = shm_open(name.c_str(), O_RDWR, 0);
    if (fd == -1)
    {
        throw ErrnoException(
            ErrorCodes::CANNOT_OPEN_FILE,
            "Cannot open shared memory '{}': {}",
            name, errnoToString());
    }
    
    // Verify size
    struct stat st;
    if (fstat(fd, &st) == -1)
    {
        int saved_errno = errno;
        close(fd);
        throw ErrnoException(
            ErrorCodes::CANNOT_OPEN_FILE,
            "Cannot stat shared memory: {}",
            errnoToString(saved_errno));
    }
    
    if (static_cast<size_t>(st.st_size) < size)
    {
        close(fd);
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Shared memory size mismatch: expected >= {}, got {}",
            size, st.st_size);
    }
    
    // Map
    ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED)
    {
        int saved_errno = errno;
        close(fd);
        throw ErrnoException(
            ErrorCodes::CANNOT_OPEN_FILE,
            "Cannot mmap shared memory: {}",
            errnoToString(saved_errno));
    }
    
    LOG_INFO(logger, "Attached to shared memory: name={}, size={}, addr={}", name, size, ptr);
}

void SharedMemoryCommand::SharedMemoryRegion::detach()
{
    auto logger = SharedMemoryCommand::getLogger();
    
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
    
    if (!name.empty())
    {
        LOG_DEBUG(logger, "Unlinking shared memory: {}", name);
        shm_unlink(name.c_str());
        name.clear();
    }
    
    size = 0;
}

void SharedMemoryCommand::SharedMemoryRegion::resize(size_t new_size)
{
    auto logger = SharedMemoryCommand::getLogger();
    LOG_DEBUG(logger, "Resizing shared memory from {} to {} bytes", size, new_size);
    
    std::string old_name = name;
    void * old_ptr = ptr;
    size_t old_size = size;
    
    // Create new region
    std::string new_name = SharedMemoryCommand::generateShmName();
    create(new_name, new_size);
    
    // Copy data from old region if it exists
    if (old_ptr && old_size > 0)
    {
        size_t copy_size = std::min(old_size, new_size);
        memcpy(ptr, old_ptr, copy_size);
        
        // Cleanup old region
        munmap(old_ptr, old_size);
        shm_unlink(old_name.c_str());
    }
}

// ============================================================================
// ControlMessage Implementation
// ============================================================================

SharedMemoryCommand::ControlMessage::ControlMessage()
    : type(MessageType::PING)
    , protocol_version(CURRENT_PROTOCOL_VERSION)
    , native_version(getCurrentNativeVersion())
    , data_size(0)
    , row_count(0)
    , checksum(0)
    , sequence_num(0)
{
    memset(shm_name, 0, sizeof(shm_name));
    memset(format_name, 0, sizeof(format_name));
    memset(error_msg, 0, sizeof(error_msg));
}

void SharedMemoryCommand::ControlMessage::serialize(char * buffer) const
{
    size_t offset = 0;
    
    // Type (1 byte)
    buffer[offset] = static_cast<uint8_t>(type);
    offset += 1;
    
    // protocol_version (4 bytes)
    memcpy(buffer + offset, &protocol_version, sizeof(protocol_version));
    offset += sizeof(protocol_version);
    
    // native_version (4 bytes)
    memcpy(buffer + offset, &native_version, sizeof(native_version));
    offset += sizeof(native_version);
    
    // data_size (8 bytes)
    memcpy(buffer + offset, &data_size, sizeof(data_size));
    offset += sizeof(data_size);
    
    // row_count (8 bytes)
    memcpy(buffer + offset, &row_count, sizeof(row_count));
    offset += sizeof(row_count);
    
    // checksum (8 bytes)
    memcpy(buffer + offset, &checksum, sizeof(checksum));
    offset += sizeof(checksum);
    
    // sequence_num (8 bytes)
    memcpy(buffer + offset, &sequence_num, sizeof(sequence_num));
    offset += sizeof(sequence_num);
    
    // shm_name (64 bytes)
    memcpy(buffer + offset, shm_name, 64);
    offset += 64;
    
    // format_name (16 bytes)
    memcpy(buffer + offset, format_name, 16);
    offset += 16;
    
    // error_msg (256 bytes)
    memcpy(buffer + offset, error_msg, 256);
}

SharedMemoryCommand::ControlMessage SharedMemoryCommand::ControlMessage::deserialize(const char * buffer)
{
    ControlMessage msg;
    size_t offset = 0;
    
    // Type
    msg.type = static_cast<MessageType>(buffer[offset]);
    offset += 1;
    
    // protocol_version
    memcpy(&msg.protocol_version, buffer + offset, sizeof(msg.protocol_version));
    offset += sizeof(msg.protocol_version);
    
    // native_version
    memcpy(&msg.native_version, buffer + offset, sizeof(msg.native_version));
    offset += sizeof(msg.native_version);
    
    // data_size
    memcpy(&msg.data_size, buffer + offset, sizeof(msg.data_size));
    offset += sizeof(msg.data_size);
    
    // row_count
    memcpy(&msg.row_count, buffer + offset, sizeof(msg.row_count));
    offset += sizeof(msg.row_count);
    
    // checksum
    memcpy(&msg.checksum, buffer + offset, sizeof(msg.checksum));
    offset += sizeof(msg.checksum);
    
    // sequence_num
    memcpy(&msg.sequence_num, buffer + offset, sizeof(msg.sequence_num));
    offset += sizeof(msg.sequence_num);
    
    // shm_name
    memcpy(msg.shm_name, buffer + offset, 64);
    offset += 64;
    
    // format_name
    memcpy(msg.format_name, buffer + offset, 16);
    offset += 16;
    
    // error_msg
    memcpy(msg.error_msg, buffer + offset, 256);
    
    return msg;
}

uint32_t SharedMemoryCommand::ControlMessage::getCurrentNativeVersion()
{
    // Native format version is tied to ClickHouse version
    // Format: MAJOR << 16 | MINOR << 8 | PATCH
    // For now, return a stable version number
    // TODO: Extract from VERSION_MAJOR/VERSION_MINOR macros
    return 0x00180000; // Version 24.0.0
}

bool SharedMemoryCommand::ControlMessage::isProtocolCompatible() const
{
    // Check protocol version compatibility
    if (protocol_version < MIN_SUPPORTED_PROTOCOL_VERSION)
    {
        return false;
    }
    
    // Major version must match
    uint16_t our_major = (CURRENT_PROTOCOL_VERSION >> 16) & 0xFFFF;
    uint16_t their_major = (protocol_version >> 16) & 0xFFFF;
    
    return our_major == their_major;
}

uint64_t SharedMemoryCommand::ControlMessage::calculateChecksum(const void * data, size_t size)
{
    // CRC64-ECMA checksum implementation
    static const uint64_t CRC64_POLY = 0x42F0E1EBA9EA3693ULL;
    static uint64_t crc_table[256];
    static bool table_initialized = false;
    
    // Initialize CRC table (once)
    if (!table_initialized)
    {
        for (size_t i = 0; i < 256; i++)
        {
            uint64_t crc = i;
            for (size_t j = 0; j < 8; j++)
            {
                if (crc & 1)
                    crc = (crc >> 1) ^ CRC64_POLY;
                else
                    crc >>= 1;
            }
            crc_table[i] = crc;
        }
        table_initialized = true;
    }
    
    // Calculate CRC64
    uint64_t crc = 0xFFFFFFFFFFFFFFFFULL;
    const uint8_t * bytes = static_cast<const uint8_t *>(data);
    
    for (size_t i = 0; i < size; i++)
    {
        uint8_t index = static_cast<uint8_t>(crc) ^ bytes[i];
        crc = (crc >> 8) ^ crc_table[index];
    }
    
    return crc ^ 0xFFFFFFFFFFFFFFFFULL;
}

bool SharedMemoryCommand::ControlMessage::verifyChecksum(const void * data) const
{
    if (data_size == 0)
        return true; // No data to verify
    
    uint64_t calculated = calculateChecksum(data, data_size);
    return calculated == checksum;
}

// ============================================================================
// SharedMemoryCommand Implementation
// ============================================================================

LoggerPtr SharedMemoryCommand::getLogger()
{
    return ::getLogger("SharedMemoryCommand");
}

std::string SharedMemoryCommand::generateUDSPath()
{
    // Use /tmp for portability
    char template_path[] = "/tmp/clickhouse_uds_XXXXXX";
    
    int tmp_fd = mkstemp(template_path);
    if (tmp_fd == -1)
    {
        throw ErrnoException(
            ErrorCodes::CANNOT_OPEN_FILE,
            "Cannot create temporary file for UDS path: {}",
            errnoToString());
    }
    
    close(tmp_fd);
    unlink(template_path); // Remove file, keep path
    
    return std::string(template_path);
}

std::string SharedMemoryCommand::generateShmName()
{
    static std::atomic<uint64_t> counter{0};
    return fmt::format("/clickhouse_shm_{}_{}", getpid(), counter.fetch_add(1));
}

SharedMemoryCommand::SharedMemoryCommand(pid_t pid_, int uds_fd_, const Config & config_)
    : pid(pid_)
    , uds_fd(uds_fd_)
    , config(config_)
{
}

SharedMemoryCommand::~SharedMemoryCommand()
{
    auto logger = getLogger();
    
    if (!wait_called && config.terminate_in_destructor)
    {
        try
        {
            LOG_DEBUG(logger, "Destructor: terminating child process {}", pid);
            
            // Try graceful shutdown first
            try
            {
                ControlMessage msg;
                msg.type = MessageType::SHUTDOWN;
                sendControlMessage(msg);
            }
            catch (...) {}
            
            // Wait a bit
            sleep(config.wait_before_termination_seconds);
            
            // Force kill if still alive
            kill(pid, config.termination_signal);
            waitpid(pid, nullptr, 0);
        }
        catch (...)
        {
            tryLogCurrentException(logger);
        }
    }
    
    if (uds_fd != -1)
    {
        close(uds_fd);
        uds_fd = -1;
    }
    
    if (!config.uds_path.empty())
        unlink(config.uds_path.c_str());
}

int SharedMemoryCommand::createUDSSocket(const std::string & path)
{
    auto logger = getLogger();
    LOG_DEBUG(logger, "Creating UDS socket at: {}", path);
    
    int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd == -1)
    {
        throw ErrnoException(
            ErrorCodes::CANNOT_CREATE_SOCKET,
            "Cannot create UDS socket: {}",
            errnoToString());
    }
    
    // Set socket options
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    
    // Bind
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    
    if (path.length() >= sizeof(addr.sun_path))
    {
        close(server_fd);
        throw Exception(
            ErrorCodes::CANNOT_CREATE_SOCKET,
            "UDS path too long: {} (max {})",
            path.length(), sizeof(addr.sun_path) - 1);
    }
    
    strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    
    // Remove existing socket file
    unlink(path.c_str());
    
    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == -1)
    {
        int saved_errno = errno;
        close(server_fd);
        throw ErrnoException(
            ErrorCodes::CANNOT_CREATE_SOCKET,
            "Cannot bind UDS socket to {}: {}",
            path, errnoToString(saved_errno));
    }
    
    if (listen(server_fd, 1) == -1)
    {
        int saved_errno = errno;
        close(server_fd);
        unlink(path.c_str());
        throw ErrnoException(
            ErrorCodes::CANNOT_CREATE_SOCKET,
            "Cannot listen on UDS socket: {}",
            errnoToString(saved_errno));
    }
    
    LOG_DEBUG(logger, "UDS socket created and listening");
    return server_fd;
}

int SharedMemoryCommand::acceptConnection(int server_fd, size_t timeout_ms)
{
    auto logger = getLogger();
    LOG_DEBUG(logger, "Waiting for connection (timeout: {} ms)...", timeout_ms);
    
    // Use poll for timeout
    struct pollfd pfd;
    pfd.fd = server_fd;
    pfd.events = POLLIN;
    
    int poll_ret = poll(&pfd, 1, static_cast<int>(timeout_ms));
    
    if (poll_ret == -1)
    {
        throw ErrnoException(
            ErrorCodes::CANNOT_CONNECT_SOCKET,
            "poll() failed on UDS socket: {}",
            errnoToString());
    }
    
    if (poll_ret == 0)
    {
        throw Exception(
            ErrorCodes::TIMEOUT_EXCEEDED,
            "Timeout waiting for child process to connect ({} ms)",
            timeout_ms);
    }
    
    int client_fd = accept(server_fd, nullptr, nullptr);
    if (client_fd == -1)
    {
        throw ErrnoException(
            ErrorCodes::CANNOT_CONNECT_SOCKET,
            "Cannot accept connection: {}",
            errnoToString());
    }
    
    LOG_INFO(logger, "Child process connected via UDS (fd={})", client_fd);
    return client_fd;
}

std::unique_ptr<SharedMemoryCommand> SharedMemoryCommand::execute(const Config & config)
{
    auto logger = getLogger();
    
    LOG_INFO(logger, "Executing command with UDS: {}", config.command);
    
    // Generate UDS path
    std::string uds_path = config.uds_path.empty() 
        ? generateUDSPath() 
        : config.uds_path;
    
    // Create socket
    int server_fd = createUDSSocket(uds_path);
    
    // Fork child process
    pid_t pid = fork();
    
    if (pid == -1)
    {
        int saved_errno = errno;
        close(server_fd);
        unlink(uds_path.c_str());
        throw ErrnoException(ErrorCodes::CANNOT_FORK, "Cannot fork: {}", errnoToString(saved_errno));
    }
    
    if (pid == 0)
    {
        // ===== Child process =====
        close(server_fd);
        
        // Prepare arguments
        std::vector<char*> argv;
        argv.push_back(const_cast<char*>(config.command.c_str()));
        
        for (const auto & arg : config.arguments)
            argv.push_back(const_cast<char*>(arg.c_str()));
        
        // Add --uds-path argument
        std::string uds_arg = "--uds-path=" + uds_path;
        argv.push_back(const_cast<char*>(uds_arg.c_str()));
        
        argv.push_back(nullptr);
        
        // Execute
        execv(config.command.c_str(), argv.data());
        
        // If we get here, exec failed
        _exit(255);
    }
    
    // ===== Parent process =====
    LOG_INFO(logger, "Spawned child process: pid={}", pid);
    
    // Wait for child to connect
    int client_fd = -1;
    try
    {
        client_fd = acceptConnection(server_fd, config.connection_timeout_ms);
        close(server_fd); // No longer needed
    }
    catch (...)
    {
        close(server_fd);
        unlink(uds_path.c_str());
        kill(pid, SIGKILL);
        waitpid(pid, nullptr, 0);
        throw;
    }
    
    // Create SharedMemoryCommand instance
    auto config_copy = config;
    config_copy.uds_path = uds_path;
    
    auto cmd = std::unique_ptr<SharedMemoryCommand>(new SharedMemoryCommand(pid, client_fd, config_copy));
    
    LOG_INFO(logger, "SharedMemoryCommand initialized successfully");
    return cmd;
}

void SharedMemoryCommand::sendControlMessage(const ControlMessage & msg)
{
    auto logger = getLogger();
    
    char buffer[ControlMessage::getSerializedSize()];
    msg.serialize(buffer);
    
    uint64_t seq = message_seq_num.fetch_add(1);
    LOG_DEBUG(logger, "Sending message #{}: type={}, data_size={}, rows={}", 
             seq, static_cast<int>(msg.type), msg.data_size, msg.row_count);
    
    ssize_t sent = send(uds_fd, buffer, sizeof(buffer), 0);
    
    if (sent != static_cast<ssize_t>(sizeof(buffer)))
    {
        throw Exception(
            ErrorCodes::SHARED_MEMORY_COMMUNICATION_ERROR,
            "Failed to send control message: sent {} of {} bytes",
            sent, sizeof(buffer));
    }
}

SharedMemoryCommand::ControlMessage SharedMemoryCommand::receiveControlMessage()
{
    auto logger = getLogger();
    
    // Use poll for timeout
    struct pollfd pfd;
    pfd.fd = uds_fd;
    pfd.events = POLLIN;
    
    int poll_ret = poll(&pfd, 1, static_cast<int>(config.operation_timeout_ms));
    
    if (poll_ret == -1)
    {
        throw ErrnoException(
            ErrorCodes::SHARED_MEMORY_COMMUNICATION_ERROR,
            "poll() failed while receiving message: {}",
            errnoToString());
    }
    
    if (poll_ret == 0)
    {
        throw Exception(
            ErrorCodes::TIMEOUT_EXCEEDED,
            "Timeout receiving control message ({} ms)",
            config.operation_timeout_ms);
    }
    
    char buffer[ControlMessage::getSerializedSize()];
    ssize_t received = recv(uds_fd, buffer, sizeof(buffer), 0);
    
    if (received == 0)
    {
        throw Exception(
            ErrorCodes::SHARED_MEMORY_COMMUNICATION_ERROR,
            "Connection closed by child process");
    }
    
    if (received != static_cast<ssize_t>(sizeof(buffer)))
    {
        throw Exception(
            ErrorCodes::SHARED_MEMORY_COMMUNICATION_ERROR,
            "Incomplete control message: received {} of {} bytes",
            received, sizeof(buffer));
    }
    
    ControlMessage msg = ControlMessage::deserialize(buffer);
    
    LOG_DEBUG(logger, "Received message: type={}, data_size={}, rows={}", 
             static_cast<int>(msg.type), msg.data_size, msg.row_count);
    
    if (msg.type == MessageType::ERROR)
    {
        throw Exception(
            ErrorCodes::SHARED_MEMORY_COMMUNICATION_ERROR,
            "Child process error: {}",
            std::string(msg.error_msg));
    }
    
    return msg;
}

void SharedMemoryCommand::writeBlock(const Block & block)
{
    auto logger = getLogger();
    
    size_t row_count = block.rows();
    LOG_DEBUG(logger, "Writing block: {} rows, {} columns", row_count, block.columns());
    
    // Estimate required size (conservative)
    size_t estimated_size = block.bytes() * 2;
    
    // Create or resize shared memory if needed
    if (!input_shm.ptr)
    {
        size_t shm_size = std::max(estimated_size, config.initial_shared_memory_size);
        shm_size = std::min(shm_size, config.max_shared_memory_size);
        
        input_shm.create(generateShmName(), shm_size);
    }
    else if (input_shm.size < estimated_size)
    {
        size_t new_size = std::min(estimated_size * 2, config.max_shared_memory_size);
        
        if (new_size > input_shm.size)
        {
            LOG_DEBUG(logger, "Resizing input shared memory: {} -> {} bytes", 
                     input_shm.size, new_size);
            input_shm.resize(new_size);
        }
    }
    
    // Serialize block to shared memory using Native format
    // First serialize to a temporary vector to get the size
    std::vector<char> temp_buffer;
    WriteBufferFromVector<std::vector<char>> temp_write_buffer(temp_buffer);
    
    auto header = std::make_shared<Block>(block.cloneEmpty());
    NativeWriter writer(
        temp_write_buffer,
        0, /* client_revision */
        header,
        std::nullopt, /* format_settings */
        false /* remove_low_cardinality */
    );
    
    size_t bytes_written = writer.write(block);
    writer.flush();
    temp_write_buffer.finalize();
    
    // Copy to shared memory
    if (temp_buffer.size() > input_shm.size)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Serialized block size {} exceeds shared memory size {}",
            temp_buffer.size(), input_shm.size);
    }
    
    memcpy(input_shm.ptr, temp_buffer.data(), temp_buffer.size());
    bytes_written = temp_buffer.size();
    
    // Calculate checksum
    uint64_t checksum = ControlMessage::calculateChecksum(input_shm.ptr, bytes_written);
    
    LOG_INFO(logger, "Serialized {} bytes ({} rows) to shared memory '{}', checksum={:016x}",
             bytes_written, row_count, input_shm.name, checksum);
    
    // Send DATA_READY message
    ControlMessage msg;
    msg.type = MessageType::DATA_READY;
    msg.protocol_version = ControlMessage::CURRENT_PROTOCOL_VERSION;
    msg.native_version = ControlMessage::getCurrentNativeVersion();
    msg.data_size = bytes_written;
    msg.row_count = row_count;
    msg.checksum = checksum;
    msg.sequence_num = message_seq_num.load();
    strncpy(msg.shm_name, input_shm.name.c_str(), sizeof(msg.shm_name) - 1);
    strncpy(msg.format_name, "Native", sizeof(msg.format_name) - 1);
    
    sendControlMessage(msg);
    
    // Wait for DATA_CONSUMED acknowledgment
    ControlMessage response = receiveControlMessage();
    
    if (response.type != MessageType::DATA_CONSUMED)
    {
        throw Exception(
            ErrorCodes::SHARED_MEMORY_COMMUNICATION_ERROR,
            "Unexpected response from child: expected DATA_CONSUMED, got {}",
            static_cast<int>(response.type));
    }
    
    LOG_DEBUG(logger, "Block written and consumed successfully");
}

Block SharedMemoryCommand::readBlock(const Block & header)
{
    auto logger = getLogger();
    
    LOG_DEBUG(logger, "Waiting for result block...");
    
    // Wait for RESULT_READY message
    ControlMessage msg = receiveControlMessage();
    
    if (msg.type != MessageType::RESULT_READY)
    {
        throw Exception(
            ErrorCodes::SHARED_MEMORY_COMMUNICATION_ERROR,
            "Unexpected message type: expected RESULT_READY, got {}",
            static_cast<int>(msg.type));
    }
    
    LOG_INFO(logger, "Result ready: {} bytes, {} rows in '{}'",
             msg.data_size, msg.row_count, msg.shm_name);
    
    // Validate protocol version
    if (!msg.isProtocolCompatible())
    {
        throw Exception(
            ErrorCodes::SHARED_MEMORY_COMMUNICATION_ERROR,
            "Incompatible protocol version: child={:08x}, current={:08x}",
            msg.protocol_version, ControlMessage::CURRENT_PROTOCOL_VERSION);
    }
    
    // Attach to output shared memory if first time or name changed
    if (!output_shm.ptr || output_shm.name != std::string(msg.shm_name))
    {
        if (output_shm.ptr)
            output_shm.detach();
        
        output_shm.attach(std::string(msg.shm_name), msg.data_size);
    }
    
    // Verify checksum
    if (!msg.verifyChecksum(output_shm.ptr))
    {
        throw Exception(
            ErrorCodes::SHARED_MEMORY_COMMUNICATION_ERROR,
            "Checksum mismatch: expected={:016x}, calculated={:016x}",
            msg.checksum,
            ControlMessage::calculateChecksum(output_shm.ptr, msg.data_size));
    }
    
    LOG_DEBUG(logger, "Checksum verified successfully: {:016x}", msg.checksum);
    
    // Read block from shared memory
    ReadBufferFromMemory read_buffer(
        reinterpret_cast<const char*>(output_shm.ptr),
        msg.data_size
    );
    
    NativeReader reader(
        read_buffer,
        header,
        0, /* server_revision */
        std::nullopt /* format_settings */
    );
    Block result = reader.read();
    
    LOG_INFO(logger, "Deserialized block: {} rows, {} columns",
             result.rows(), result.columns());
    
    // Send RESULT_CONSUMED acknowledgment
    ControlMessage response;
    response.type = MessageType::RESULT_CONSUMED;
    sendControlMessage(response);
    
    return result;
}

void SharedMemoryCommand::wait()
{
    auto logger = getLogger();
    
    if (wait_called)
        return;
    
    wait_called = true;
    
    LOG_DEBUG(logger, "Waiting for child process {} to terminate", pid);
    
    // Send shutdown signal
    try
    {
        ControlMessage msg;
        msg.type = MessageType::SHUTDOWN;
        sendControlMessage(msg);
    }
    catch (...)
    {
        LOG_WARNING(logger, "Failed to send SHUTDOWN message, process may already be dead");
    }
    
    // Wait for process
    int status;
    if (waitpid(pid, &status, 0) == -1)
    {
        throw ErrnoException(
            ErrorCodes::CANNOT_WAITPID,
            "Cannot waitpid for process {}: {}",
            pid, errnoToString());
    }
    
    if (WIFEXITED(status))
    {
        int exit_code = WEXITSTATUS(status);
        LOG_INFO(logger, "Child process {} exited with code {}", pid, exit_code);
        
        if (exit_code != 0)
        {
            throw Exception(
                ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY,
                "Child process exited with non-zero code: {}",
                exit_code);
        }
    }
    else if (WIFSIGNALED(status))
    {
        int signal = WTERMSIG(status);
        throw Exception(
            ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY,
            "Child process terminated by signal: {}",
            signal);
    }
}

}

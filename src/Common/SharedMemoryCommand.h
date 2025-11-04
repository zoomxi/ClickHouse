#pragma once

#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Core/Block.h>

namespace DB
{

class NativeReader;
class NativeWriter;

/**
 * SharedMemoryCommand - Unix Domain Socket + Shared Memory based IPC
 * 
 * Provides high-performance communication between ClickHouse and external processes
 * using Unix Domain Sockets for control messages and shared memory for data transfer.
 * 
 * Protocol:
 * 1. ClickHouse creates UDS socket and spawns child process with UDS path
 * 2. Child connects to UDS socket
 * 3. Data exchange via shared memory, synchronized via UDS control messages
 * 4. Native format used for zero-copy serialization
 */
class SharedMemoryCommand final
{
public:
    ~SharedMemoryCommand();

    struct Config
    {
        /// Command executable path
        std::string command;
        
        /// Command arguments
        std::vector<std::string> arguments;
        
        /// Unix Domain Socket path (auto-generated if empty)
        std::string uds_path;
        
        /// Shared memory size in bytes (auto-calculated based on first block if 0)
        size_t initial_shared_memory_size = 16 * 1024 * 1024; // 16MB default
        
        /// Maximum shared memory size (prevents unlimited growth)
        size_t max_shared_memory_size = 1024 * 1024 * 1024; // 1GB default
        
        /// Timeout for child process to connect (milliseconds)
        size_t connection_timeout_ms = 10000;
        
        /// Timeout for send/receive operations (milliseconds)
        size_t operation_timeout_ms = 60000;
        
        /// Enable fallback to pipe if UDS initialization fails
        bool enable_pipe_fallback = true;
        
        /// Terminate process in destructor
        bool terminate_in_destructor = true;
        
        /// Signal to send when terminating
        int termination_signal = SIGTERM;
        
        /// Seconds to wait before sending termination signal
        size_t wait_before_termination_seconds = 5;
    };

    struct SharedMemoryRegion
    {
        void * ptr = nullptr;
        size_t size = 0;
        int fd = -1;
        std::string name;
        
        ~SharedMemoryRegion();
        
        /// Create new shared memory region
        void create(const std::string & name, size_t size);
        
        /// Attach to existing shared memory region
        void attach(const std::string & name, size_t size);
        
        /// Detach and cleanup
        void detach();
        
        /// Resize shared memory (creates new region and copies data if needed)
        void resize(size_t new_size);
    };

    /// Control message types for UDS protocol
    enum class MessageType : uint8_t
    {
        DATA_READY = 1,      // ClickHouse -> Child: input data ready in shared memory
        DATA_CONSUMED = 2,   // Child -> ClickHouse: input data consumed
        RESULT_READY = 3,    // Child -> ClickHouse: result ready in shared memory
        RESULT_CONSUMED = 4, // ClickHouse -> Child: result consumed
        ERROR = 5,           // Either direction: error occurred
        SHUTDOWN = 6,        // ClickHouse -> Child: graceful shutdown
        PING = 7,            // Either direction: keepalive
        PONG = 8             // Response to PING
    };

    /// Control message structure (sent over UDS)
    struct ControlMessage
    {
        MessageType type;
        uint32_t protocol_version;  // Protocol version for compatibility check
        uint32_t native_version;    // ClickHouse Native format version
        uint64_t data_size;         // Size of data in shared memory
        uint64_t row_count;         // Number of rows
        uint64_t checksum;          // CRC64 checksum of data in shared memory
        uint64_t sequence_num;      // Message sequence number for ordering
        char shm_name[64];          // Shared memory region name
        char format_name[16];       // Data format (e.g., "Native")
        char error_msg[256];        // Error message if type == ERROR
        
        ControlMessage();
        
        // Protocol version: MAJOR.MINOR
        static constexpr uint32_t CURRENT_PROTOCOL_VERSION = 0x00010000; // 1.0
        static constexpr uint32_t MIN_SUPPORTED_PROTOCOL_VERSION = 0x00010000;
        
        // Native format version (from ClickHouse version)
        static uint32_t getCurrentNativeVersion();
        
        static constexpr size_t getSerializedSize()
        {
            return sizeof(MessageType)      // 1 byte
                 + sizeof(uint32_t) * 2     // protocol_version + native_version: 8 bytes
                 + sizeof(uint64_t) * 4     // data_size + row_count + checksum + sequence_num: 32 bytes
                 + 64                       // shm_name: 64 bytes
                 + 16                       // format_name: 16 bytes
                 + 256;                     // error_msg: 256 bytes
                 // Total: 377 bytes
        }
        
        void serialize(char * buffer) const;
        static ControlMessage deserialize(const char * buffer);
        
        /// Validate protocol compatibility
        bool isProtocolCompatible() const;
        
        /// Calculate checksum of data in shared memory
        static uint64_t calculateChecksum(const void * data, size_t size);
        
        /// Verify checksum matches data
        bool verifyChecksum(const void * data) const;
    };

    /// Create and execute command with UDS communication
    static std::unique_ptr<SharedMemoryCommand> execute(const Config & config);

    /// Get process ID
    pid_t getPid() const { return pid; }
    
    /// Check if command is using UDS mode (vs fallback pipe mode)
    bool isUDSMode() const { return uds_mode; }

    /// Write block to child process via shared memory
    void writeBlock(const Block & block);

    /// Read block from child process via shared memory
    Block readBlock(const Block & header);

    /// Wait for process termination
    void wait();
    
    /// Check if wait was called
    bool isWaitCalled() const { return wait_called; }

    /// Send control message via UDS
    void sendControlMessage(const ControlMessage & msg);

    /// Receive control message via UDS (with timeout)
    ControlMessage receiveControlMessage();

private:
    SharedMemoryCommand(pid_t pid_, int uds_fd_, const Config & config_);

    /// Create Unix Domain Socket and bind
    static int createUDSSocket(const std::string & path);
    
    /// Accept connection from child process (with timeout)
    static int acceptConnection(int server_fd, size_t timeout_ms);

    /// Generate unique UDS socket path
    static std::string generateUDSPath();

    /// Generate unique shared memory name
    static std::string generateShmName();
    
    /// Logger
    static LoggerPtr getLogger();

    pid_t pid;
    int uds_fd;  // Unix Domain Socket file descriptor
    Config config;
    
    SharedMemoryRegion input_shm;   // ClickHouse writes, child reads
    SharedMemoryRegion output_shm;  // Child writes, ClickHouse reads
    
    bool uds_mode = true;  // True if using UDS, false if fallback to pipe
    bool wait_called = false;
    
    /// Sequence number for debugging
    std::atomic<uint64_t> message_seq_num{0};
};

}

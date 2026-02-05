/**
 * Copyright (C) 2026 YaoBase
 * 
 * Reordered log queue for incremental restore
 * Ensures logs are applied in strict log_id order even when rewritten out of order
 */

#ifndef YAOBASE_COMMON_OB_REORDERED_LOG_QUEUE_H_
#define YAOBASE_COMMON_OB_REORDERED_LOG_QUEUE_H_

#include "common/ob_define.h"
#include "yysys/ob_mutex.h"
#include "yysys/ob_cond.h"
#include <map>

namespace yaobase {
namespace common {

/**
 * Rewritten log entry
 * Contains rewritten commit log data ready for replay
 */
struct ObRewrittenLogEntry {
  int64_t log_id_;                // Original log ID (for ordering)
  int64_t log_seq_;               // Original log_seq
  int64_t timestamp_;             // Log timestamp
  int32_t cmd_;                   // Command type (OB_UPS_MUTATOR, etc.)
  char* rewritten_data_;          // Rewritten log data
  int64_t rewritten_data_len_;    // Rewritten data length
  int64_t tenant_id_;             // Destination tenant ID

  ObRewrittenLogEntry()
    : log_id_(0), log_seq_(0), timestamp_(0), cmd_(0),
      rewritten_data_(nullptr), rewritten_data_len_(0), tenant_id_(0) {}

  ~ObRewrittenLogEntry();

  // Deep copy
  int deep_copy(const ObRewrittenLogEntry& other);
  
  // Release allocated memory
  void reset();

private:
  DISALLOW_COPY_AND_ASSIGN(ObRewrittenLogEntry);
};

/**
 * Queue status information
 */
struct ObReorderedQueueStatus {
  int64_t next_expected_log_id_;  // Next expected log_id
  int64_t queue_size_;            // Current queue size
  int64_t max_log_id_in_queue_;   // Maximum log_id in queue
  int64_t hole_count_;            // Number of gaps in sequence

  ObReorderedQueueStatus()
    : next_expected_log_id_(0), queue_size_(0),
      max_log_id_in_queue_(0), hole_count_(0) {}
};

/**
 * Reordered log queue
 * Guarantees logs are popped in strict log_id order
 * 
 * Design:
 * - Uses std::map internally for ordered storage
 * - Pop blocks until next sequential log is available
 * - Detects and reports gaps in log sequence
 * - Thread-safe with mutex protection
 */
class ObReorderedLogQueue {
public:
  ObReorderedLogQueue();
  ~ObReorderedLogQueue();

  /**
   * Initialize the queue
   * @param max_size Maximum queue size (for backpressure)
   * @return OB_SUCCESS on success
   */
  int init(int64_t max_size = 5000);
  
  void destroy();
  bool is_inited() const { return is_inited_; }

  /**
   * Push a rewritten log entry (may be out of order)
   * @param entry Log entry to push
   * @return OB_SUCCESS on success, OB_SIZE_OVERFLOW if queue full
   */
  int push(const ObRewrittenLogEntry& entry);

  /**
   * Pop the next sequential log entry (blocking)
   * Blocks until next expected log_id is available or timeout
   * 
   * @param entry Output log entry
   * @param timeout_us Timeout in microseconds
   * @return OB_SUCCESS on success, OB_TIMEOUT on timeout
   */
  int pop(ObRewrittenLogEntry& entry, int64_t timeout_us);

  /**
   * Try to pop next sequential log (non-blocking)
   * @param entry Output log entry
   * @return OB_SUCCESS if available, OB_ENTRY_NOT_EXIST if not
   */
  int try_pop(ObRewrittenLogEntry& entry);

  /**
   * Check if queue has continuous log available
   * @return true if next expected log is in queue
   */
  bool has_continuous_log() const;

  /**
   * Get queue status
   * @param status Output status structure
   * @return OB_SUCCESS on success
   */
  int get_status(ObReorderedQueueStatus& status) const;

  /**
   * Set the starting log_id (before restore begins)
   * @param log_id Starting log ID
   */
  void set_next_expected_log_id(int64_t log_id);

  /**
   * Get current queue size
   */
  int64_t size() const;

  /**
   * Check if queue is empty
   */
  bool empty() const;

private:
  bool is_inited_;
  int64_t max_size_;
  
  // Next expected log_id for sequential pop
  int64_t next_expected_log_id_;
  
  // Ordered map: log_id -> log entry
  std::map<int64_t, ObRewrittenLogEntry> log_map_;
  
  // Synchronization
  mutable yysys::CThreadMutex mutex_;
  yysys::CThreadCond has_continuous_log_cond_;
  
  DISALLOW_COPY_AND_ASSIGN(ObReorderedLogQueue);
};

}  // namespace common
}  // namespace yaobase

#endif  // YAOBASE_COMMON_OB_REORDERED_LOG_QUEUE_H_

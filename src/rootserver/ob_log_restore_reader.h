/**
 * Copyright (C) 2026 YaoBase
 * 
 * Log restore reader
 * Reads archived commit logs from backup storage for incremental restore
 */

#ifndef YAOBASE_ROOTSERVER_OB_LOG_RESTORE_READER_H_
#define YAOBASE_ROOTSERVER_OB_LOG_RESTORE_READER_H_

#include "common/ob_define.h"
#include "common/ob_log_entry.h"
#include "common/ob_queue_thread.h"
#include "yysys/ob_mutex.h"

namespace yaobase {
namespace rootserver {

/**
 * Log restore reader
 * Reads archived logs from backup storage and pushes to processing queue
 */
class ObLogRestoreReader {
public:
  ObLogRestoreReader();
  ~ObLogRestoreReader();

  int init(const char* archive_path, int64_t start_log_id, int64_t end_timestamp);
  void destroy();
  bool is_inited() const { return is_inited_; }

  /**
   * Read next log entry from archive
   * 
   * @param log_entry Output log entry
   * @param timeout_us Timeout in microseconds
   * @return OB_SUCCESS on success, OB_ITER_END when no more logs, OB_TIMEOUT on timeout
   */
  int get_next_log_entry(common::ObLogEntry& log_entry, int64_t timeout_us);

  /**
   * Get current read position
   */
  int64_t get_current_log_id() const { return current_log_id_; }

  /**
   * Check if reader has reached end
   */
  bool is_end() const;

private:
  /**
   * Open archive file for reading
   * 
   * @param log_id Starting log ID
   * @return OB_SUCCESS on success
   */
  int open_archive_file(int64_t log_id);

  /**
   * Read one log entry from current archive file
   */
  int read_log_entry_from_file(common::ObLogEntry& log_entry);

  /**
   * Move to next archive file
   */
  int switch_to_next_file();

private:
  bool is_inited_;
  char archive_path_[OB_MAX_URI_LENGTH];
  int64_t start_log_id_;
  int64_t end_timestamp_;
  int64_t current_log_id_;
  int64_t current_file_id_;
  
  // File handle for current archive file
  int fd_;
  
  // Read buffer
  char* read_buffer_;
  int64_t buffer_size_;
  int64_t buffer_pos_;
  int64_t buffer_data_len_;
  
  mutable yysys::CThreadMutex mutex_;

  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreReader);
};

}  // namespace rootserver
}  // namespace yaobase

#endif  // YAOBASE_ROOTSERVER_OB_LOG_RESTORE_READER_H_

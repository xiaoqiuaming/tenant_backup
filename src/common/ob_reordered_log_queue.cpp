/**
 * Copyright (C) 2026 YaoBase
 * 
 * Reordered log queue implementation
 */

#include "common/ob_reordered_log_queue.h"
#include "common/ob_malloc.h"
#include "yysys/ob_log.h"
#include "yysys/ob_timeutil.h"

namespace yaobase {
namespace common {

//==============================================================================
// ObRewrittenLogEntry Implementation
//==============================================================================

ObRewrittenLogEntry::~ObRewrittenLogEntry() {
  reset();
}

int ObRewrittenLogEntry::deep_copy(const ObRewrittenLogEntry& other) {
  int ret = OB_SUCCESS;
  
  reset();  // Clean up existing data
  
  log_id_ = other.log_id_;
  log_seq_ = other.log_seq_;
  timestamp_ = other.timestamp_;
  cmd_ = other.cmd_;
  tenant_id_ = other.tenant_id_;
  
  if (other.rewritten_data_len_ > 0 && nullptr != other.rewritten_data_) {
    rewritten_data_ = static_cast<char*>(ob_malloc(other.rewritten_data_len_, ObModIds::OB_TENANT_BACKUP));
    if (nullptr == rewritten_data_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      YYSYS_LOG(WARN, "allocate memory for rewritten data failed", K(ret), K(other.rewritten_data_len_));
    } else {
      memcpy(rewritten_data_, other.rewritten_data_, other.rewritten_data_len_);
      rewritten_data_len_ = other.rewritten_data_len_;
    }
  }
  
  return ret;
}

void ObRewrittenLogEntry::reset() {
  if (nullptr != rewritten_data_) {
    ob_free(rewritten_data_);
    rewritten_data_ = nullptr;
  }
  
  log_id_ = 0;
  log_seq_ = 0;
  timestamp_ = 0;
  cmd_ = 0;
  rewritten_data_len_ = 0;
  tenant_id_ = 0;
}

//==============================================================================
// ObReorderedLogQueue Implementation
//==============================================================================

ObReorderedLogQueue::ObReorderedLogQueue()
  : is_inited_(false),
    max_size_(0),
    next_expected_log_id_(0) {
}

ObReorderedLogQueue::~ObReorderedLogQueue() {
  destroy();
}

int ObReorderedLogQueue::init(int64_t max_size) {
  int ret = OB_SUCCESS;
  
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    YYSYS_LOG(WARN, "reordered log queue already initialized", K(ret));
  } else if (max_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "invalid max size", K(ret), K(max_size));
  } else {
    max_size_ = max_size;
    next_expected_log_id_ = 0;
    is_inited_ = true;
    
    YYSYS_LOG(INFO, "reordered log queue initialized", K(max_size_));
  }
  
  return ret;
}

void ObReorderedLogQueue::destroy() {
  if (is_inited_) {
    yysys::CThreadGuard guard(&mutex_);
    
    // Clear all entries and free memory
    for (std::map<int64_t, ObRewrittenLogEntry>::iterator it = log_map_.begin();
         it != log_map_.end(); ++it) {
      it->second.reset();
    }
    log_map_.clear();
    
    next_expected_log_id_ = 0;
    max_size_ = 0;
    is_inited_ = false;
  }
}

int ObReorderedLogQueue::push(const ObRewrittenLogEntry& entry) {
  int ret = OB_SUCCESS;
  
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "reordered log queue not initialized", K(ret));
  } else {
    yysys::CThreadGuard guard(&mutex_);
    
    // Check queue capacity
    if (static_cast<int64_t>(log_map_.size()) >= max_size_) {
      ret = OB_SIZE_OVERFLOW;
      YYSYS_LOG(WARN, "reordered queue is full", K(ret), 
                "size", log_map_.size(), K(max_size_));
    } else {
      // Deep copy entry into map
      ObRewrittenLogEntry copied_entry;
      if (OB_SUCCESS != (ret = copied_entry.deep_copy(entry))) {
        YYSYS_LOG(WARN, "deep copy log entry failed", K(ret), K(entry.log_id_));
      } else {
        log_map_[entry.log_id_] = copied_entry;
        
        // If this is the next expected log, signal waiting threads
        if (entry.log_id_ == next_expected_log_id_) {
          has_continuous_log_cond_.signal();
        }
        
        YYSYS_LOG(DEBUG, "pushed log to reordered queue", 
                  K(entry.log_id_), K(entry.log_seq_),
                  "queue_size", log_map_.size(),
                  K(next_expected_log_id_));
      }
    }
  }
  
  return ret;
}

int ObReorderedLogQueue::pop(ObRewrittenLogEntry& entry, int64_t timeout_us) {
  int ret = OB_SUCCESS;
  
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "reordered log queue not initialized", K(ret));
  } else {
    yysys::CThreadGuard guard(&mutex_);
    
    int64_t start_time = yysys::CTimeUtil::getTime();
    
    while (true) {
      // Check if next expected log is available
      std::map<int64_t, ObRewrittenLogEntry>::iterator it = log_map_.find(next_expected_log_id_);
      if (it != log_map_.end()) {
        // Found continuous log, return it
        if (OB_SUCCESS != (ret = entry.deep_copy(it->second))) {
          YYSYS_LOG(WARN, "deep copy log entry failed", K(ret), K(next_expected_log_id_));
        } else {
          // Clean up source entry and remove from map
          it->second.reset();
          log_map_.erase(it);
          next_expected_log_id_++;
          
          YYSYS_LOG(DEBUG, "popped log from reordered queue",
                    K(entry.log_id_), K(entry.log_seq_),
                    "queue_size", log_map_.size(),
                    "next_expected", next_expected_log_id_);
        }
        return ret;
      }
      
      // Check timeout
      int64_t elapsed = yysys::CTimeUtil::getTime() - start_time;
      if (elapsed >= timeout_us) {
        ret = OB_TIMEOUT;
        
        // Log warning if queue has entries but missing expected log
        if (!log_map_.empty()) {
          int64_t min_log_id = log_map_.begin()->first;
          int64_t max_log_id = log_map_.rbegin()->first;
          YYSYS_LOG(WARN, "wait for continuous log timeout, possible gap in log sequence",
                    K(ret), K(next_expected_log_id_),
                    K(min_log_id), K(max_log_id),
                    "queue_size", log_map_.size());
        }
        return ret;
      }
      
      // Wait for signal or timeout
      int64_t remaining_us = timeout_us - elapsed;
      has_continuous_log_cond_.wait(&mutex_, remaining_us);
    }
  }
  
  return ret;
}

int ObReorderedLogQueue::try_pop(ObRewrittenLogEntry& entry) {
  int ret = OB_SUCCESS;
  
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "reordered log queue not initialized", K(ret));
  } else {
    yysys::CThreadGuard guard(&mutex_);
    
    std::map<int64_t, ObRewrittenLogEntry>::iterator it = log_map_.find(next_expected_log_id_);
    if (it != log_map_.end()) {
      // Found continuous log
      if (OB_SUCCESS != (ret = entry.deep_copy(it->second))) {
        YYSYS_LOG(WARN, "deep copy log entry failed", K(ret), K(next_expected_log_id_));
      } else {
        it->second.reset();
        log_map_.erase(it);
        next_expected_log_id_++;
      }
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  
  return ret;
}

bool ObReorderedLogQueue::has_continuous_log() const {
  yysys::CThreadGuard guard(&mutex_);
  return log_map_.find(next_expected_log_id_) != log_map_.end();
}

int ObReorderedLogQueue::get_status(ObReorderedQueueStatus& status) const {
  int ret = OB_SUCCESS;
  
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "reordered log queue not initialized", K(ret));
  } else {
    yysys::CThreadGuard guard(&mutex_);
    
    status.next_expected_log_id_ = next_expected_log_id_;
    status.queue_size_ = static_cast<int64_t>(log_map_.size());
    
    if (!log_map_.empty()) {
      status.max_log_id_in_queue_ = log_map_.rbegin()->first;
      
      // Calculate gap count
      status.hole_count_ = 0;
      for (std::map<int64_t, ObRewrittenLogEntry>::const_iterator it = log_map_.begin();
           it != log_map_.end(); ++it) {
        if (it->first >= next_expected_log_id_) {
          int64_t expected_count = it->first - next_expected_log_id_ + 1;
          int64_t actual_count = static_cast<int64_t>(log_map_.size());
          if (expected_count > actual_count) {
            status.hole_count_ = expected_count - actual_count;
            break;
          }
        }
      }
    } else {
      status.max_log_id_in_queue_ = 0;
      status.hole_count_ = 0;
    }
  }
  
  return ret;
}

void ObReorderedLogQueue::set_next_expected_log_id(int64_t log_id) {
  yysys::CThreadGuard guard(&mutex_);
  next_expected_log_id_ = log_id;
  YYSYS_LOG(INFO, "set next expected log id", K(log_id));
}

int64_t ObReorderedLogQueue::size() const {
  yysys::CThreadGuard guard(&mutex_);
  return static_cast<int64_t>(log_map_.size());
}

bool ObReorderedLogQueue::empty() const {
  yysys::CThreadGuard guard(&mutex_);
  return log_map_.empty();
}

}  // namespace common
}  // namespace yaobase

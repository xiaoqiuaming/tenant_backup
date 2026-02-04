
#ifndef OB_GATHER_STATISTICS_H
#define OB_GATHER_STATISTICS_H

#include "sql/ob_no_children_phy_operator.h"
#include "common/ob_schema_service.h"
#include "sql/ob_sql_context.h"
#include "common/ob_string.h"
#include "common/ob_strings.h"
#include "common/ob_rowkey.h"
#include "rootserver/ob_chunk_server_manager.h"
#include "common/ob_server.h"
#include "common/ob_common_param.h"
#include "common/ob_string_buf.h"
#include "mergeserver/ob_chunk_server_task_dispatcher.h"
#include "common/location/ob_tablet_location_range_iterator.h"
#include "common/ob_gather_table_info.h"
#include "mergeserver/ob_chunk_server_task_dispatcher.h"

using namespace oceanbase::mergeserver;

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerRootRpcProxy;
    class ObMergerAsyncRpcStub;
    class ObChunkServerTaskDispatcher;
  } // end namespace mergeserver

  namespace rootserver
  {
    class ObChunkServerManager;
  }

  namespace sql
  {
    class ObGatherStatistics: public ObNoChildrenPhyOperator
    {
      public:
        
        ObGatherStatistics();

        virtual ~ObGatherStatistics();
        
        // init
        void set_sql_context(const ObSqlContext &context);
        
        common::TableSchema& get_table_schema();
        
        uint64_t get_table_id() const;
        
        uint64_t get_column_id(int64_t idx) const;

        void set_table_id(const uint64_t table_id);

        int set_column_id(const uint64_t column_id);

        void set_if_not_exists(bool if_not_exists);

        void set_row_key_info(const ObRowkeyInfo info);

        void set_context(ObSqlContext *context);

        ObStringBuf& get_buf_pool();

        /// execute the create table statement
        virtual int open();

        virtual int close();

        virtual void reset();

        virtual void reuse();
        
        virtual ObPhyOperatorType get_type() const { return PHY_GATHER_STATISTICS; }

        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        
        /// @note always return OB_ITER_END
        virtual int get_next_row(const ObRow *&row);
        
        /// @note always return OB_NOT_SUPPORTED
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;

      private:
        void grant_owner_privilege();

        // disallow copy
        ObGatherStatistics(const ObGatherStatistics &other);
        ObGatherStatistics& operator=(const ObGatherStatistics &other);

      private:
        // data members
        ObStringBuf   buf_pool_;
        bool          if_not_exists_;
        ObSqlContext  local_context_;
        ObGatherTableInfo gather_info_;
        ObRowkeyInfo  search_key_;
        ObSqlContext* context_;
    };

    inline ObStringBuf& ObGatherStatistics::get_buf_pool()
    {
      return buf_pool_;
    }

    inline void ObGatherStatistics::set_sql_context(const ObSqlContext &context)
    {
      local_context_ = context;
      local_context_.schema_manager_ = NULL;
    }

    inline uint64_t ObGatherStatistics::get_table_id() const
    {
      return gather_info_.get_table_id();
    }

    inline uint64_t ObGatherStatistics::get_column_id(int64_t idx) const
    {
      return gather_info_.get_column_id(idx);
    }

    inline void ObGatherStatistics::set_table_id(const uint64_t table_id)
    {
      gather_info_.set_table_id(table_id);
    }

    inline int ObGatherStatistics::set_column_id(const uint64_t column_id)
    {
      return gather_info_.add_column_id(column_id);
    }

    inline void ObGatherStatistics::set_if_not_exists(bool if_not_exists)
    {
      if_not_exists_ = if_not_exists;
    }
    
    inline int ObGatherStatistics::get_next_row(const ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }

    inline int ObGatherStatistics::get_row_desc(const ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }

    inline void ObGatherStatistics::set_row_key_info(const ObRowkeyInfo info)
    {
      search_key_ = info;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif 

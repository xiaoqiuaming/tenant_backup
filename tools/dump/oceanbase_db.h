/*
 * =====================================================================================
 *
 *       Filename:  OceanbaseDb.h
 *
 *        Version:  1.0
 *        Created:  04/12/2011 04:48:39 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (DBA Group), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#ifndef OB_API_OCEANBASEDB_H
#define  OB_API_OCEANBASEDB_H

#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "common/ob_schema.h"
#include "common/ob_mutator.h"
#include "common/ob_object.h"
#include "common/ob_base_client.h"
#include "common/thread_buffer.h"
#include "sql/ob_sql_result_set.h"
#include "common/ob_general_rpc_stub.h" //add by zhuxh:20160712 [New way of fetching schema]
#include "db_table_info.h"
#include "ob_export_param.h"
#include <string>
#include <map>
#include <vector>

//add by zhuxh:20160120
#define MAX_CLUSTER_NUM 32

#define RPC_WITH_RETIRES(RPC, retries, RET) {                                                       \
  int __RPC_LOCAL_INDEX__ = 0;                                                                      \
  while (__RPC_LOCAL_INDEX__++ < retries) {                                                         \
  (RET) = (RPC);                                                                                  \
  if ((RET) != 0) {                                                                               \
  YYSYS_LOG(WARN, "call {%s} failed, ret = %d, retry = %d", #RPC, (RET), __RPC_LOCAL_INDEX__);  \
  } else {                                                                                        \
  break;                                                                                        \
  }                                                                                               \
  }                                                                                                 \
  }
//add by liyongfeng:20141020 ÿ�����Զ���sleep(5)
#define RPC_WITH_RETRIES_SLEEP(RPC, retries, RET) {                                                            \
  int __RPC_LOCAL_INDEX__ = 0;                                                                            \
  while (__RPC_LOCAL_INDEX__++ < retries) {                                                                \
  (RET) = (RPC);                                                                                        \
  if ((RET) != 0) {                                                                                    \
  YYSYS_LOG(WARN, "call {%s} failed, ret = %d, retry = %d", #RPC, (RET), __RPC_LOCAL_INDEX__);    \
  sleep(5);                                                                                        \
  } else {                                                                                            \
  break;                                                                                            \
  }                                                                                                    \
  }                                                                                                        \
  }

extern const int kDefaultResultBufferSize;
extern const int64_t MAX_TIMEOUT_US;
extern const int64_t SPAN_TIMEOUT_US;


namespace oceanbase {
  namespace api {
    const int TSI_GET_ID = 1001;
    const int TSI_SCAN_ID = 1002;
    const int TSI_MBUF_ID = 1003;

    class DbRecordSet;
    using namespace common;

    const int64_t kDefaultTimeout = 2000000;

    class DbTranscation;
    class OceanbaseDb;

    class RowMutator {
      public:
        friend class DbTranscation;
      public:

        int delete_row();
        int add(const char *column_name, const ObObj &val);
        int add(const std::string &column_name, const ObObj &val);

        int32_t op() const { return op_; }

        RowMutator();
        RowMutator(const std::string &table_name, const ObRowkey &rowkey,
                   DbTranscation *tnx);

      private:
        
        void set_op(int32_t op) { op_ = op; }
        void set(const std::string &table_name, const ObRowkey &rowkey,
                 DbTranscation *tnx);

        std::string table_name_;
        ObRowkey rowkey_;
        DbTranscation *tnx_;
        int32_t op_;
    };

    class DbTranscation {
      public:
        friend class RowMutator;
        friend class OceanbaseDb;
      public:
        DbTranscation();
        DbTranscation(OceanbaseDb *db);

        int insert_mutator(const char* table_name, const ObRowkey &rowkey, RowMutator *mutator);
        int update_mutator(const char* table_name, const ObRowkey &rowkey, RowMutator *mutator);

        int free_row_mutator(RowMutator *&mutator);

        int commit();
        int abort();
        int reset();
        inline void set_db(OceanbaseDb *db)
        {
          db_ = db;
          assert(db_ != NULL);
        }

      private:
        int add_cell(ObMutatorCellInfo &cell);

        OceanbaseDb *db_;
        ObMutator mutator_;
    };

    struct DbMutiGetRow {
        DbMutiGetRow() :columns(NULL) { }

        ObRowkey rowkey;
        const std::vector<std::string> *columns;
        std::string table;
    };

    class OceanbaseDb {
      private:
        typedef std::map<ObRowkey, TabletInfo> CacheRow;
        typedef std::map<std::string, CacheRow > CacheSet;
      public:
        struct DbStats {
            int64_t total_succ_gets;
            int64_t total_fail_gets;
            int64_t total_send_bytes;
            int64_t total_recv_bytes;
            int64_t total_succ_apply;
            int64_t total_fail_apply;

            DbStats() {
              memset(this, 0, sizeof(DbStats));
            }
        };

        // add by zcd, only used in init_execute_sql(), get_result() :b
        // ����ṹ��Ϊ�����ڶ��߳�ʹ��init_execute_sql(), get_result()����
        // ÿ���̶߳����Լ���˽�е���ExecuteSqlParam�����������б��
        struct ExecuteSqlParam
        {
            ObServer ms;
            std::string sql;
            int64_t session_id;
            int64_t client_timeout_timestamp;
            int64_t ms_timeout;
            bool is_first;
        };
        // :end
      public:
        static const uint64_t kTabletTimeout = 10;
        friend class DbTranscation;
      public:
        OceanbaseDb(const char *ip, unsigned short port, int64_t timeout = kDefaultTimeout,
                    uint64_t tablet_timeout = kTabletTimeout);
        ~OceanbaseDb();

        void set_table_param(ObExportTableParam *table_param) { table_param_ = table_param; }
        void set_max_ups_mem(int64_t mem) { this->import_limit_ups_memory = mem; }
        int64_t get_max_ups_mem() { return import_limit_ups_memory; }

        int set_master_rs();
        void splite_string(std::string &info, std::vector<std::string> &vec);
        int check_is_master_rs(const char *ip, unsigned short port, bool &is_master_rs);
        const char* get_master_rs_ip()
        {
          return master_rs_ip_;
        }

        int get(const std::string &table, const std::vector<std::string> &columns,
                const ObRowkey &rowkey, DbRecordSet &rs);

        int get(const std::string &table, const std::vector<std::string> &columns,
                const std::vector<ObRowkey> &rowkeys, DbRecordSet &rs);

        int get(const std::vector<DbMutiGetRow> &rows, DbRecordSet &rs);

        int init();
        static int global_init(const char*log_dir, const char *level);
        int get_ms_location(const ObRowkey &row_key, const std::string &table_name);

        int get_tablet_location(const std::string &table, const ObRowkey &rowkey,
                                common::ObServer &server);

        int fetch_schema(common::ObSchemaManagerV2& schema_manager);

        int search_tablet_cache(const std::string &table, const ObRowkey &rowkey, TabletInfo &loc);
        void insert_tablet_cache(const std::string &table, const ObRowkey &rowkey, TabletInfo &tablet);

        DbStats db_stats() const { return db_stats_; }

        int start_tnx(DbTranscation *&tnx);
        void end_tnx(DbTranscation *&tnx);

        int get_update_server(ObServer &server);

        void set_consistency(bool consistency) { consistency_ = consistency; }

        bool get_consistency() const { return consistency_; }

        int scan(const TabletInfo &tablets, const std::string &table, const std::vector<std::string> &columns,
                 const ObRowkey &start_key, const ObRowkey &end_key, DbRecordSet &rs, int64_t version = 0,
                 bool inclusive_start = false, bool inclusive_end = true);

        int scan(const std::string &table, const std::vector<std::string> &columns,
                 const ObRowkey &start_key, const ObRowkey &end_key, DbRecordSet &rs, int64_t version = 0,
                 bool inclusive_start = false, bool inclusive_end = true);

        int get_memtable_version(int64_t &version);

        int set_limit_info(const int64_t offset, const int64_t count);

        int get_merge_ups_memory(int32_t &state);
        // add by liyongfeng, 20141014, get state of daily merge from rootserver
        int get_daily_merge_state(int32_t &state);//��ȡÿ�պϲ���״̬
        int get_latest_update_server(int32_t &ups_switch);//ob_import����,��ȡ������UPS,���������ȡ����UPS���бȽ�,�����һ��,��ʾob_import��������UPS�л�
        //bool compare_update_server(ObServer server);//��������UPS,���������ȡ����UPS���бȽ�,�����һ��,��ʾob_import��������UPS�л�
        // add:end
        // add by zcd:b
        // ��ȡ��ǰ��Ⱥ�п�����Ϊ��������ʹ�õ�ms���ϣ�ѡȡ�㷨��ʵ��
        int fetch_ms_list(const int master_percent, const int slave_percent, std::vector<ObServer> &array);

        // add by zhangcd 20150814:b
        int fetch_mysql_ms_list(const int master_percent, std::vector<ObServer> &array);
        // add:e

        // ÿ�ο�ʼsql��ѯʱ��Ҫ���ô˺�����ʼ����ѯ����
        int init_execute_sql(const ObServer& ms, const std::string& sql, int64_t ms_timeout, int64_t client_timeout);

        // �õ���һ�����ݰ�
        int get_result(sql::ObSQLResultSet &rs, ObDataBuffer &out_buffer, bool &has_next);

        // ��rootserver�����һ��ms��ַ
        int get_a_ms_from_rs(ObServer &ms);

        // ��ȡ��ǰOceanbaseDb�����д洢�ĳ�ʱʱ��
        int64_t get_timeout_us(){ return timeout_; }

        // ������ֹ�������ݰ�������
        int post_end_next(ObServer server, int64_t session_id);

        // ��ȡ��ǰ�߳�������ִ�е�sql����Ӧ��server��session_id
        int get_thread_server_session(ObServer &server, int64_t& session_id);
        //:end

        int execute_dml_sql(const ObString& ob_sql, const ObServer &ms, int64_t& affected_row);

        // add by lyf :b
        // ��ȡһ��table������tabletinfo��Ϣ
        int get_tablet_info(const std::string &table, const ObRowkey &rowkey, ObDataBuffer &data_buff, ObScanner &scanner);
        //:end
        //add by zhuxh:20160603 [Sequence] :b
        int sequence(bool create, std::string seq_name, int64_t start = 0, int64_t increment = 1, bool default_start = true, bool default_increment = true); //add by zhuxh:20160907 [cb Sequence]
        //add :e

      private:
        int init_get_param(ObGetParam *&param, const std::vector<DbMutiGetRow> &rows);

        int do_server_get(common::ObServer &server,
                          const ObRowkey& row_key, common::ObScanner &scanner,
                          common::ObDataBuffer& data_buff, const std::string &table_name,
                          const std::vector<std::string> &columns);

        int do_muti_get(common::ObServer &server, const std::vector<DbMutiGetRow>& rows,
                        ObScanner &scanner, ObDataBuffer& data_buff);

        common::ObScanParam *get_scan_param(const std::string &table, const std::vector<std::string>& columns,
                                            const ObRowkey &start_key, const ObRowkey &end_key,
                                            bool inclusive_start = false, bool inclusive_end = true,
                                            int64_t data_version = 0);

        void free_tablet_cache();
        void mark_ms_failure(ObServer &server, const std::string &table, const ObRowkey &rowkey);
        void try_mark_server_fail(TabletInfo &tablet_info, ObServer &server, bool &do_erase_tablet);

        int do_server_cmd(const ObServer &server, const int32_t opcode,
                          ObDataBuffer &inout_buffer, int64_t &pos);

        //reference count of db handle
        void unref() { __sync_fetch_and_sub(&db_ref_, 1); }
        void ref() { __sync_fetch_and_add (&db_ref_, 1); }

        // add by zcd :b
        // ����һ���������ms�ϣ����õ�������еĵ�һ�����ݰ�����
        int send_execute_sql(const ObServer& ms, const std::string& sql, const int64_t timeout, int64_t& session_id, sql::ObSQLResultSet &rs, common::ObDataBuffer &out_buffer, bool &has_next
                             , bool omit_db = false);

        // ��ý�����к��������ݰ�����
        int get_next_result(const ObServer& ms, const int64_t timeout, sql::ObSQLResultSet& rs, common::ObDataBuffer &out_buffer, const int64_t session_id, bool &has_next);

        // ��ȡ�߳�˽�е�ObDataBuffer�����洢��data_buffer��
        int get_thread_buffer(common::ObDataBuffer& data_buffer);

        // ��ȡ�߳�˽�е�ExecuteSqlParam�����洢��param��
        int get_ts_execute_sql_param(struct ExecuteSqlParam *&param);

        // ɾ���߳�˽�е��У����߳̽�����ʱ��ᱻ���õ�
        static void destroy_ts_execute_sql_param_key(void *ptr);
        // :end


        // add by zhuxh:20160118 :b
        // to init has_slave_cluster, slave_root_server_ and slave_client_
        int init_slave_cluster_info();
        // add :e

        common::ObServer root_server_;
        common::ObServer update_server_;
        common::ObBaseClient client_;
        const char *rs_ip_list_;
        const char *master_rs_ip_;
        unsigned short rs_port_;

        //add by zhuxh:20160118 :b //check merge of slave cluster
        int arr_slave_client_length;
        common::ObBaseClient arr_slave_client_[MAX_CLUSTER_NUM];
        //add :e

        CacheSet cache_set_;
        yysys::CThreadMutex cache_lock_;

        uint64_t tablet_timeout_;
        int64_t timeout_;
        bool inited_;

        DbStats db_stats_;

        int64_t db_ref_;
        bool consistency_;

        int64_t limit_offset_;
        /// 0 means not limit
        int64_t limit_count_;

        // add by zcd :b
        pthread_key_t send_sql_param_key_;
        // :end

        // add by zcd :b
        common::ThreadSpecificBuffer tsbuffer_;
        // :end

        common::ThreadSpecificBuffer rpc_buffer_; //add by zhuxh:20160712 [New way of fetching schema]
        ObExportTableParam *table_param_;
        int64_t import_limit_ups_memory;
    };
  }
}

#endif

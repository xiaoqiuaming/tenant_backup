#ifndef OCEANBASE_COMMON_COMMON_PARAM_H_
#define OCEANBASE_COMMON_COMMON_PARAM_H_

#include "ob_define.h"
#include "ob_object.h"
#include "ob_range.h"
#include "ob_rowkey.h"
//add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151224:b
#include "ob_transaction.h"
#include "common/hash/ob_hashmap.h"
#include "ob_array.h"
//add duyr 20151224
#include "ob_se_array.h"      //add by maosy [MultiUPS 1.0] [read uncommit]20170525 

namespace oceanbase
{
  namespace common
  {
    // CellInfo only with table_id + column_id + rowkey + vlaue
    struct ObInnerCellInfo
    {
        uint64_t table_id_;
        ObRowkey row_key_;
        uint64_t column_id_;
        ObObj value_;
        ObInnerCellInfo():table_id_(OB_INVALID_ID), row_key_(), column_id_(OB_INVALID_ID), value_()
        {}
    };
    // whole CellInfo with table_id || table_name + column_id || column_name + rowkey + value
    struct ObCellInfo
    {
        ObCellInfo() : table_name_(), table_id_(OB_INVALID_ID),
          row_key_(), column_id_(OB_INVALID_ID),
          column_name_(), value_(), is_from_te_key_(false)  //add hongchen[SECONDARY_INDEX_OPTI_BUGFIX] 20170809
        {}

        ObString table_name_;
        uint64_t table_id_;
        ObRowkey row_key_;
        uint64_t column_id_;
        ObString column_name_;
        ObObj value_;
        bool     is_from_te_key_;  //add hongchen[SECONDARY_INDEX_OPTI_BUGFIX] 20170809

        bool operator == (const ObCellInfo & other) const;
        void reset()
        {
          table_name_.assign(NULL, 0);
          table_id_ = OB_INVALID_ID;
          row_key_.assign(NULL, 0);
          column_id_ = OB_INVALID_ID;
          column_name_.assign(NULL, 0);
          value_.reset();
          is_from_te_key_ = false;;  //add hongchen[SECONDARY_INDEX_OPTI_BUGFIX] 20170809
        }

    };
    //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
    class ObUpsRow;
    class ObRow;
    //    class ObRowDesc;
    enum ObDataMarkDataStoreType
    {
      INVALID_STORE_TYPE = 0,
      CS_SSTABLE_DATA,
      UPS_SSTABLE_DATA,
      UPS_MEMTABLE_DATA
    };

    struct ObDataMark
    {
        int64_t modify_time_;
        int64_t major_version_;
        int64_t minor_ver_start_;
        int64_t minor_ver_end_;
        ObDataMarkDataStoreType data_store_type_;

        ObDataMark()
        {
          reset();
        }
        virtual ~ObDataMark()
        {
          reset();
        }
        void reset()
        {
          modify_time_     = OB_INVALID_DATA;
          major_version_   = OB_INVALID_VERSION;
          minor_ver_start_ = OB_INVALID_VERSION;
          minor_ver_end_   = OB_INVALID_VERSION;
          data_store_type_ = INVALID_STORE_TYPE;
        }

        int64_t to_string(char* buf, int64_t buf_len) const
        {
          int64_t pos = 0;
          databuff_printf(buf, buf_len, pos, "ObDataMark(");
          databuff_printf(buf, buf_len, pos, "modify_time_=%ld,",
                          modify_time_);
          databuff_printf(buf, buf_len, pos, "major_version_=%ld,",
                          major_version_);
          databuff_printf(buf, buf_len, pos, "minor_ver_start_=%ld,",
                          minor_ver_start_);
          databuff_printf(buf, buf_len, pos, "minor_ver_end_=%ld,",
                          minor_ver_end_);
          databuff_printf(buf, buf_len, pos, "data_store_type_=%d,",
                          data_store_type_);
          databuff_printf(buf, buf_len, pos, ")");
          return pos;
        }
    };

    struct ObDataMarkParam
    {
        bool need_modify_time_;
        bool need_major_version_;
        bool need_minor_version_;
        bool need_data_store_type_;
        uint64_t modify_time_cid_;
        uint64_t major_version_cid_;
        uint64_t minor_ver_start_cid_;
        uint64_t minor_ver_end_cid_;
        uint64_t data_store_type_cid_;
        uint64_t table_id_;

        ObDataMarkParam()
        {
          reset();
        }
        virtual ~ObDataMarkParam()
        {
          reset();
        }

        // caution:it's deep copy yet
        ObDataMarkParam(const ObDataMarkParam &other);
        ObDataMarkParam& operator=(const ObDataMarkParam &other);

        void reset();
        bool is_inited()const;
        bool is_valid()const;
        bool is_data_mark_cid(const uint64_t cid)const;
        int64_t to_string(char* buf, int64_t buf_len) const;
        NEED_SERIALIZE_AND_DESERIALIZE;
    };


    //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151130:b   //uncertainty
    class ObUpsRow;
    class ObRow;
    class ObTransVersion:public ObTransID
    {
      public:
        enum TransVerState
        {
          INVALID_STATE = 0,
          PREPARE_STATE,
          COMMIT_STATE
        };
        enum TransDistType
        {
          INVALID_TRANS_TYPE = 0,
          SINGLE_POINT_TRANS,
          DISTRIBUTED_TRANS
        };
      public:
        ObTransVersion();
        virtual ~ObTransVersion()
        {
          this->reset();
        }

        void reset();
        bool is_valid()const;
        bool is_single_point_trans()const;
        bool is_distributed_trans()const;
        int set_trans_dist_type(const TransDistType &type);
        int64_t to_string(char* buf, int64_t len) const;

        //disallow compare
        bool operator < (const ObTransVersion &r_value) const;
        bool operator <= (const ObTransVersion &r_value) const;
        bool operator > (const ObTransVersion &r_value) const;
        bool operator >= (const ObTransVersion &r_value) const;

        //must be deep copy!!!!
        ObTransVersion& operator=(const ObTransVersion &other);
        bool operator == (const ObTransVersion &r_value) const;
        bool operator != (const ObTransVersion &r_value) const;
        //must be deep copy!!!
        void assign(const ObTransID &trans_id);
        int64_t hash() const;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        TransDistType trans_type_;
    };

    struct ObReadAtomicDataMark
    {
        //be careful!!!ups will core if this val is 16K
        static const int64_t  MAX_MARK_BUF_SIZE = 10*1024;//10k
        enum DataStoreType
        {
          INVALID_STORE_TYPE = 0,
          CS_SSTABLE_DATA    = 1,
          UPS_MEMTABLE_DATA  = 2,
          UPS_SSTABLE_DATA   = 3,
        };

        int64_t minor_version_end_;
        int64_t minor_version_start_;
        int64_t major_version_;
        int64_t ups_paxos_id_;
        DataStoreType data_store_type_;

        ObReadAtomicDataMark();
        virtual ~ObReadAtomicDataMark()
        {
          reset();
        }
        ObReadAtomicDataMark(const ObReadAtomicDataMark &other);
        ObReadAtomicDataMark& operator=(const ObReadAtomicDataMark &other);

        //first compare paxos id!then compare other part!!
        int compare(const ObReadAtomicDataMark &other) const;
        bool operator==(const ObReadAtomicDataMark &other) const;
        bool operator!=(const ObReadAtomicDataMark &other) const;
        bool operator<(const ObReadAtomicDataMark &other) const;
        bool operator<=(const ObReadAtomicDataMark &other) const;
        bool operator>(const ObReadAtomicDataMark &other) const;
        bool operator>=(const ObReadAtomicDataMark &other) const;

        void reset();
        bool is_valid() const;
        bool is_paxos_id_useful()const;
        const char* to_cstring() const;
        int64_t to_string(char* buf, int64_t buf_len) const;
        NEED_SERIALIZE_AND_DESERIALIZE;
        inline int64_t hash() const
        {
          common::hash::hash_func<int64_t> hash_int64;
          return (hash_int64(minor_version_start_)
                  + hash_int64(minor_version_end_)
                  + hash_int64(major_version_)
                  + hash_int64(ups_paxos_id_)
                  + hash_int64(static_cast<int64_t>(data_store_type_)));
        }

    };



    /*@berif rpc_scan ������read_atomic����*/
    struct ObReadAtomicParam
    {
        static const int64_t HASH_BUCKET_NUM       = 1024;
        typedef common::hash::ObHashMap<common::ObTransVersion,ObTransVersion::TransVerState,
        common::hash::NoPthreadDefendMode> ExactTransversetMap;
        enum ReadAtomicReadStrategy
        {
          INVALID_STRATEGY = 0,
          NO_READ,
          READ_WITH_COMMIT_DATA,
          READ_WITH_READ_ATOMIC_PARAM
        };

        //      enum ParamPriority
        //      {
        //        LOW_PRIORITY = 0,
        //        MID_PRIORITY,
        //        HIGH_PRIORITY
        //      };

        ObReadAtomicParam():
          need_last_commit_trans_ver_(false),
          need_last_prepare_trans_ver_(false),
          need_prevcommit_trans_verset_(false),
          need_commit_data_(false),
          need_prepare_data_(false),
          need_trans_meta_data_(false),
          need_data_mark_(false),
          need_exact_transver_data_(false),
          need_exact_data_mark_data_(false),
          need_row_key_(false),
          last_commit_trans_ver_cid_(0),
          last_prepare_trans_ver_cid_(0),
          read_atomic_meta_cid_(0),
          commit_data_extendtype_cid_(0),
          prepare_data_extendtype_cid_(0),
          major_ver_cid_(0),
          minor_ver_start_cid_(0),
          minor_ver_end_cid_(0),
          ups_paxos_id_cid_(0),
          data_store_type_cid_(0),
          prevcommit_trans_verset_cid_off_(0),
          prepare_data_cid_off_(0),
          table_id_(OB_INVALID_ID),
          min_used_column_cid_(0),
          rowkey_cell_count_(0),
          max_prevcommit_trans_verset_count_(0),
          table_base_column_num_(0),
          table_composite_column_num_(0),
          is_data_mark_array_sorted_(false),
          exact_data_mark_array_(common::OB_COMMON_MEM_BLOCK_SIZE,
                                 ModulePageAllocator(ObModIds::OB_READ_ATOMIC)),
          exact_trans_verset_map_(),
          is_trans_verset_map_create_(false),
          exact_transver_num_(0)
        {
        }
        virtual ~ObReadAtomicParam()
        {
          reset();
        }

        // caution:it's deep copy yet
        ObReadAtomicParam(const ObReadAtomicParam &other);
        ObReadAtomicParam& operator=(const ObReadAtomicParam &other);

        void reset();
        void reset_exact_transver_map();
        void reset_exact_datamark_array();
        int add_exact_transver(const ObTransVersion &ver);
        int add_exact_data_mark(const ObReadAtomicDataMark &data_mark);
        int sort_exact_data_mark_array();
        bool is_inited()const;
        bool is_need_extend_data()const;
        bool is_need_real_data()const;
        bool is_valid()const;
        bool commit_ext_type_must_valid()const;
        bool prepare_ext_type_must_valid()const;
        //      bool is_has_exact_priority_param(const ParamPriority priority)const;
        int64_t to_string(char* buf, int64_t buf_len) const;
        NEED_SERIALIZE_AND_DESERIALIZE;

        bool need_last_commit_trans_ver_;
        bool need_last_prepare_trans_ver_;
        bool need_prevcommit_trans_verset_;
        bool need_commit_data_;
        bool need_prepare_data_;
        bool need_trans_meta_data_;
        bool need_data_mark_;//need the special mark to indicate where the incr data come from!! ���ֶ��㷨������Ϊtrue
        bool need_exact_transver_data_;//data may comes from prepare list or commit list or undo list  ���ֶ���ʱ�������Ϊtrue
        bool need_exact_data_mark_data_;  // ���ֶ���ʱ�������Ϊtrue
        bool need_row_key_;             //���ֶ� ��һ�ֶ��� ��һ�� ְλtrue
        uint64_t last_commit_trans_ver_cid_;
        uint64_t last_prepare_trans_ver_cid_;
        uint64_t read_atomic_meta_cid_;
        uint64_t commit_data_extendtype_cid_;
        uint64_t prepare_data_extendtype_cid_;
        uint64_t major_ver_cid_;//used for data mark
        uint64_t minor_ver_start_cid_;//used for data mark
        uint64_t minor_ver_end_cid_;//used for data mark
        uint64_t ups_paxos_id_cid_;//used for data mark
        uint64_t data_store_type_cid_;//used for data mark
        uint64_t prevcommit_trans_verset_cid_off_;
        uint64_t prepare_data_cid_off_;
        uint64_t table_id_;
        uint64_t min_used_column_cid_;
        uint64_t rowkey_cell_count_;//the num of all rowkey columns
        uint64_t max_prevcommit_trans_verset_count_;
        uint64_t table_base_column_num_;
        uint64_t table_composite_column_num_;
        //FIXME:READ_ATOMIC if it's necessary to sorted???
        bool is_data_mark_array_sorted_;
        ObArray<ObReadAtomicDataMark,
        ModulePageAllocator> exact_data_mark_array_;//must ascending order
        ExactTransversetMap exact_trans_verset_map_;  /*������ֶ��㷨��  commited �汾����*/

        //they will be filled by add_exact_transver();
        //don't serialize and deserialize!!!
        bool is_trans_verset_map_create_;
        //don't deserialize,just serialize!!!
        int64_t exact_transver_num_;

    };

    struct ObDataMarkHelper
    {
      public:
        static int get_data_mark(const ObRow *row,
                                 const ObDataMarkParam &param,
                                 ObDataMark &out_data_mark);
        static int convert_data_mark_row_to_normal_row(const ObDataMarkParam &param,
                                                       const ObRow *src_row,
                                                       ObRow *des_row);
    };

    //add duyr 20160531:e
    class ObReadAtomicHelper
    {
      public:
        static const int64_t TRANS_VERSION_BUF_SIZE = sizeof(ObTransVersion);
        static const int64_t MAX_TRANS_VERSET_SIZE  = 256;
        static int transver_copy_to_buf(const ObTransVersion &src_transver,
                                        char *des_buf,
                                        const int64_t des_buf_len);
        static int transver_cast_to_buf(const ObTransVersion *src_transver,
                                        char *&des_buf,
                                        int64_t &des_buf_len);
        static int transver_copy_from_buf(const char *src_buf,
                                          const int64_t src_buf_len,
                                          ObTransVersion &des_transver);
        static int transver_cast_from_buf(const char *src_buf,
                                          const int64_t src_buf_len,
                                          ObTransVersion *&des_transver);
        static int init_buf_with_transver(char *des_buf,
                                          const int64_t des_buf_len);

        /**
          *�жϵ�ǰObObj�Ƿ�Ϊ����trans version��ObObj
          *Ŀǰʹ��ObVarcharType���͵�ObObj������ʹ���transver!
          */
        static bool is_trans_ver_obj(const common::ObObj &val,
                                     const bool check_valid = false);
        /**
          *ȡ��ObObj�б����trans version!
          *ֻ��ǳ����
          */
        static int get_trans_ver(const common::ObObj &val,
                                 const ObTransVersion **des_ver = NULL,
                                 const bool must_valid = false);

        /**
          *get incre data mark!
          */
        static int get_data_mark(const common::ObObj &major_ver_val,
                                 const common::ObObj &minor_ver_start_val,
                                 const common::ObObj &minor_ver_end_val,
                                 const common::ObObj &paxos_id_val,
                                 const common::ObObj &data_store_type_val,
                                 ObReadAtomicDataMark &des_mark);

      public:
        /**
          *�����������ύ�汾�Ŷ�Ӧ����ID
          */
        static int gen_prevcommit_trans_verset_cids(const ObReadAtomicParam& param,
                                                    uint64_t *cid_buf,
                                                    const int64_t buf_size);
        static int convert_commit_cid_to_prepare_cid(const ObReadAtomicParam& param,
                                                     const uint64_t commit_cid,
                                                     uint64_t &prepare_cid);
        static int convert_prepare_cid_to_commit_cid(const ObReadAtomicParam& param,
                                                     const uint64_t prepare_cid,
                                                     uint64_t &commit_cid);
        static bool is_read_atomic_special_cid(const ObReadAtomicParam& param,
                                               const uint64_t column_id);
        static bool is_prepare_cid(const ObReadAtomicParam& param,
                                   const uint64_t column_id);
        //      static bool is_prepare_exttype_cid(const ObReadAtomicParam& param,
        //                                      const uint64_t column_id);
        static bool is_trans_ver_cid(const ObReadAtomicParam& param,
                                     const uint64_t column_id);
        static bool is_extend_type_obj(const common::ObObj &val);
        static int get_extend_type(const common::ObObj &val,
                                   int64_t &ext_type);

        static bool is_on_exact_trans_verset(const ObReadAtomicParam& param,
                                             const ObTransVersion *ver);
        static int  get_strategy_with_data_mark(const ObReadAtomicParam& param,
                                                const ObReadAtomicDataMark &data_mark,
                                                ObReadAtomicParam::ReadAtomicReadStrategy &strategy);
      public:
        /**
          *only can be used to fuse CS row data with UPS row data!
          *when use read atomic to read rows!
          *must use this fun to fuse row rather than ObRowFuse::fuse_row!
          *
          */
        static int fuse_row(const ObUpsRow *commit_prepare_ups_row,
                            const ObRow *sstable_row,
                            const ObReadAtomicParam &param,
                            ObRow *result);

        /**
          *   total_ver_count is a input_output param!
          * when it be as input param,it means the total size of the verset_array!
          * when it be as output param,it means the real size used in verset_array!
          *   commit_ext_type or prepare_ext_type will return just
          * ObActionFlag::OP_NOP or ObActionFlag::OP_VALID or ObActionFlag::OP_DEL_ROW
          * or OB_INVALID_DATA
          *   if commit_ext_type or prepare_ext_type is OB_INVALID_DATA,
          * means there is no this kind of extend type!
          * you can't use a invalid extend type to decide anything!
          */
        static  int get_extend_vals_(const ObRow *commit_prepare_row,
                                     const ObReadAtomicParam &param,
                                     const ObTransVersion **last_commit_transver = NULL,
                                     const ObTransVersion **last_prepare_transver= NULL,
                                     const ObTransVersion **verset_array = NULL,
                                     int64_t *total_ver_count = NULL,
                                     int64_t *commit_ext_type = NULL,
                                     int64_t *prepare_ext_type = NULL,
                                     ObReadAtomicDataMark *data_mark = NULL);

        /**
          *can be used to split CS or MS row!(it may can be used to split UPS row,but not test yet!)
          *it uses shallow copy!
          */
        static int split_commit_preapre_row_(const ObRow *commit_prepare_row,
                                             const ObReadAtomicParam &param,
                                             ObRow *commit_row = NULL,
                                             bool *is_commit_row_exist = NULL,
                                             ObRow *prepare_row = NULL,
                                             bool *is_prepare_row_exist = NULL);

        static int add_data_mark_into_row(const ObReadAtomicParam &param,
                                          const ObReadAtomicDataMark &data_mark,
                                          ObRow &des_row);
    };
    //add duyr 20151130:e

    /// @class ObReadParam  OB read parameter, API should not concern these parameters,
    ///   and mergeserver will directly ignore these parameters
    class ObReadParam
    {
      public:
        ObReadParam();
        virtual ~ObReadParam();

        /// @fn get data whose timestamp is newer or as new as the given timestamp,
        ///   -# when reading cs, if not setted, the result is decided by the server;
        ///   -# when reading ups, this parameter must be setted
        void set_version_range(const ObVersionRange & range);
        ObVersionRange get_version_range(void) const;

        /// @fn when reading cs, indicating whether the result (including intermediate result,
        /// like sstable block readed from sstable) of this operation should be cached.
        ///
        /// ups just ignores this parameter
        void set_is_result_cached(const bool cached);
        bool get_is_result_cached()const;

        void set_is_read_consistency(const bool cons);
        bool get_is_read_consistency()const;
        //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151202:b //uncertainty
        virtual const ObReadAtomicParam& get_read_atomic_param() const;
        virtual void  set_read_atomic_param(const ObReadAtomicParam& param);
        //add duyr 20151202:e
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        inline void set_data_mark_param(const ObDataMarkParam &param)
        {
          data_mark_param_ = param;
        }
        inline ObDataMarkParam& get_data_mark_param()
        {
          return data_mark_param_;
        }
        inline const ObDataMarkParam& get_data_mark_param()const
        {
          return data_mark_param_;
        }
        //add duyr 20160531:e
        //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
        inline int set_trans_id(ObSEArray< ObPartitionTransID,MAX_UPS_COUNT_ONE_CLUSTER>& trans_id)
        {
          trans_id_.reset();
          trans_id_= trans_id;
          YYSYS_LOG(DEBUG,"trans id = %s",to_cstring(trans_id_));
          return OB_SUCCESS;
        }
        ObSEArray< ObPartitionTransID,MAX_UPS_COUNT_ONE_CLUSTER>& get_trans_id()
        { return trans_id_; }
        inline ObTransID get_trans_id(int64_t paxos_id)
        {
          int ret = OB_ENTRY_NOT_EXIST ;
          int64_t index = 0 ;
          for(; index < trans_id_.count() ;index ++)
          {
            if(trans_id_.at(index).paxos_id_ == paxos_id)
            {
              ret = OB_SUCCESS ;
              break;
            }
          }

          if(OB_SUCCESS ==ret)
          {
            YYSYS_LOG(DEBUG,"trans id = %s",to_cstring(trans_id_.at(index)));
            return trans_id_.at(index).transid_ ;
          }
          else
          {
            ObTransID trans_id;
            YYSYS_LOG(DEBUG,"trans id = %s",to_cstring(trans_id));
            return trans_id;
          }
        }
        //add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
        int64_t get_published_transid(int64_t paxos_id)
        {
          int64_t index = 0 ;
          YYSYS_LOG(DEBUG,"TEST=%ld",paxos_id);
          int64_t transid = OB_INVALID_DATA;
          for(; index < trans_id_.count() ;index ++)
          {
            if(trans_id_.at(index).paxos_id_ == paxos_id)
            {
              transid = trans_id_.at(index).published_transid_;
            }
          }
          return transid ;
        }
        //add by maosy e
        void reset(void);

        /// serailize or deserialization
        VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;

      protected:
        // RESERVE_PARAM_FIELD
        int serialize_reserve_param(char * buf, const int64_t buf_len, int64_t & pos) const;
        int deserialize_reserve_param(const char * buf, const int64_t data_len, int64_t & pos);
        int64_t get_reserve_param_serialize_size(void) const;

      protected:
        int8_t is_read_master_;
        int8_t is_result_cached_;
        ObVersionRange version_range_;
        //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151202:b  //uncertainty
        ObReadAtomicParam read_atomic_param_;
        //add duyr 20151202:e
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        ObDataMarkParam data_mark_param_;
        //add duyr  20160531:e
        //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
        common::ObSEArray <ObPartitionTransID,MAX_UPS_COUNT_ONE_CLUSTER> trans_id_ ;
        //add by maosy e
    };

    struct ScanFlag
    {
#define SF_BIT_READ_MODE            2
#define SF_BIT_DIRECTION            2
#define SF_BIT_NOT_EXIT_COL_RET_NOP 1
#define SF_BIT_DAILY_MERGE_SCAN     1
#define SF_BIT_FULL_ROW_SCAN        1
#define SF_BIT_ROWKEY_COLUMN_COUNT  16
#define SF_BIT_RESERVED             41
        static const uint64_t SF_MASK_READ_MODE             = (0x1UL<<SF_BIT_READ_MODE)             - 1;
        static const uint64_t SF_MASK_DIRECTION             = (0x1UL<<SF_BIT_DIRECTION)             - 1;
        static const uint64_t SF_MASK_NOT_EXIT_COL_RET_NOP  = (0x1UL<<SF_BIT_NOT_EXIT_COL_RET_NOP)  - 1;
        static const uint64_t SF_MASK_DAILY_MERGE_SCAN      = (0x1UL<<SF_BIT_DAILY_MERGE_SCAN)      - 1;
        static const uint64_t SF_MASK_FULL_ROW_SCAN         = (0x1UL<<SF_BIT_FULL_ROW_SCAN)         - 1;
        static const uint64_t SF_MASK_ROWKEY_COLUMN_COUNT   = (0x1UL<<SF_BIT_ROWKEY_COLUMN_COUNT)   - 1;
        static const uint64_t SF_MASK_RESERVED              = (0x1UL<<SF_BIT_RESERVED)              - 1;
        enum Direction
        {
          FORWARD = 0,
          BACKWARD = 1,
        };

        enum SyncMode
        {
          SYNCREAD = 0,
          ASYNCREAD = 1,
        };

        ScanFlag()
          :  read_mode_(ASYNCREAD), direction_(FORWARD), not_exit_col_ret_nop_(0),
            daily_merge_scan_(0), full_row_scan_(0), rowkey_column_count_(0), reserved_(0)
        {
        }
        ScanFlag(
            const SyncMode mode, const Direction dir,
            const bool nop, const bool merge,
            const bool full, const int64_t count)
        {
          flag_ = 0;
          read_mode_            = mode  & SF_MASK_READ_MODE;
          direction_            = dir   & SF_MASK_DIRECTION;
          not_exit_col_ret_nop_ = nop   & SF_MASK_NOT_EXIT_COL_RET_NOP;
          daily_merge_scan_     = merge & SF_MASK_DAILY_MERGE_SCAN;
          full_row_scan_        = full  & SF_MASK_FULL_ROW_SCAN;
          rowkey_column_count_  = count & SF_MASK_ROWKEY_COLUMN_COUNT;
          //flag_ |= (mode & 0x3);
          //flag_ |= ((dir & 0x3) << 2);
          //flag_ |= ((nop & 0x1) << 4);
          //flag_ |= ((merge & 0x1) << 5);
          //flag_ |= ((full & 0x1) << 6);
          //flag_ |= ((count & 0xFFFF) << 7);
        }
        union
        {
            int64_t flag_;
            struct
            {
                uint64_t read_mode_             : SF_BIT_READ_MODE;
                uint64_t direction_             : SF_BIT_DIRECTION;
                uint64_t not_exit_col_ret_nop_  : SF_BIT_NOT_EXIT_COL_RET_NOP;
                uint64_t daily_merge_scan_      : SF_BIT_DAILY_MERGE_SCAN;
                uint64_t full_row_scan_         : SF_BIT_FULL_ROW_SCAN;
                uint64_t rowkey_column_count_   : SF_BIT_ROWKEY_COLUMN_COUNT;
                uint64_t reserved_              : SF_BIT_RESERVED;
            };
        };
    };

    class ObRowkeyInfo;
    int set_ext_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const int64_t value);
    int set_int_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const int64_t value);
    int get_int_obj_value(const char* buf, const int64_t data_len, int64_t & pos, int64_t & int_value);
    int set_str_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const ObString &value);
    int get_str_obj_value(const char* buf, const int64_t buf_len, int64_t & pos, ObString & str_value);
    int set_rowkey_obj_array(char* buf, const int64_t buf_len, int64_t & pos, const ObObj* array, const int64_t size);
    int get_rowkey_obj_array(const char* buf, const int64_t buf_len, int64_t & pos, ObObj* array, int64_t& size);
    int64_t get_rowkey_obj_array_size(const ObObj* array, const int64_t size);
    int get_rowkey_compatible(const char* buf, const int64_t buf_len, int64_t & pos,
                              const ObRowkeyInfo& info, ObObj* array, int64_t& size, bool &is_binary_rowkey) ;
    template <typename Allocator>
    int get_rowkey_compatible(const char* buf, const int64_t buf_len, int64_t & pos,
                              const ObRowkeyInfo& info, Allocator& allocator, ObRowkey& rowkey, bool &is_binary_rowkey)
    {
      int ret = OB_SUCCESS;
      ObRowkey tmp_rowkey;
      ObObj tmp_obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
      int64_t rowkey_size = OB_MAX_ROWKEY_COLUMN_NUMBER;
      if (OB_SUCCESS == ret)
      {
        ret = get_rowkey_compatible(buf, buf_len, pos, info,
                                    tmp_obj_array, rowkey_size, is_binary_rowkey);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "deserialize rowkey error, ret=%d buf=%p data_len=%ld pos=%ld",
                    ret, buf, buf_len, pos);
        }
        else
        {
          tmp_rowkey.assign(tmp_obj_array, rowkey_size);
          ret = tmp_rowkey.deep_copy(rowkey, allocator);
        }
      }
      return ret;
    }
    class ObSchemaManagerV2;
    int get_rowkey_info_from_sm(const ObSchemaManagerV2* schema_mgr,
                                const uint64_t table_id, const ObString& table_name, ObRowkeyInfo& rowkey_info);
  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_COMMON_PARAM_H_ */

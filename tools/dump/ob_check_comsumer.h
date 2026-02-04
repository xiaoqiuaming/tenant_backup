#ifndef __OB_CHECK_COMSUMER_H__
#define __OB_CHECK_COMSUMER_H__

#include "db_queue_check.h"
#include "file_reader_v2.h"
#include "oceanbase_db.h"
#include "ob_check.h"
#include "ob_check_param.h"
#include "file_appender.h"
#include "ob_mysql_client_helper.h"
#include <vector>

class MergeServerRef
{
  public:
    MergeServerRef();
    MergeServerRef(const ObServer &server);
    ~MergeServerRef();
    int inc_ref();
    int dec_ref();

    inline int get_ref_count()
    {
      return ref_count_;
    }
    inline void set_invalid()
    {
      ref_count_ = INT_MAX;
    }
    inline bool get_isvalid()
    {
      return ref_count_ != INT_MAX;
    }
    inline ObServer get_mergeserver()
    {
      return server_;
    }
    static bool ref_bigger_than(MergeServerRef *left, MergeServerRef *right)
    {
      return left->get_ref_count() > right->get_ref_count();
    }

  private:
    ObServer server_;
    int ref_count_;
};

class MergeServerManager
{
  public:
    MergeServerManager();
    ~MergeServerManager();
    int init(OceanbaseDb *db);

    int sort_array_by_ref()
    {
      std::sort(sort_array_.begin(), sort_array_.end(),
                MergeServerRef::ref_bigger_than);
      return OB_SUCCESS;
    }

    int get_one_ms(ObServer &server)
    {
      yysys::CThreadGuard guard(&lock_);
      int ret = OB_SUCCESS;

      sort_array_by_ref();
      if (sort_array_.size() == 0)
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR, "there are no mergeserver now!");
      }
      if (OB_SUCCESS == ret)
      {
        MergeServerRef *ms_ref = sort_array_.back();
        MergeServerRef **pp_ms_ref = GET_TSI_MULT(MergeServerRef *, 0);
        if (NULL != *pp_ms_ref)
        {
          (*pp_ms_ref)->dec_ref();
        }
        if (ms_ref->get_isvalid())
        {
          ms_ref->inc_ref();

          *pp_ms_ref = ms_ref;
          server = ms_ref->get_mergeserver();
        }
        else
        {
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int set_ms_invalid()
    {
      yysys::CThreadGuard guard(&lock_);
      MergeServerRef **pp_ms_ref = GET_TSI_MULT(MergeServerRef *, 0);
      if (NULL != *pp_ms_ref)
      {
        (*pp_ms_ref)->set_invalid();
        sort_array_by_ref();
        *pp_ms_ref = NULL;
      }
      return OB_SUCCESS;
    }

  private:
    MergeServerRef ms_ref_array_[1024];
    std::vector<MergeServerRef*> sort_array_;
    OceanbaseDb *db_;
    yysys::CThreadMutex lock_;
};

class  ImportComsumer : public QueueComsumer<RecordBlock>
{
  public:
    ImportComsumer(oceanbase::api::OceanbaseDb *db, ObRowBuilder *builder, AppendableFile *bad_file, AppendableFile *correct_file, const TableParam &param);
    ~ImportComsumer();

    virtual int comsume(RecordBlock &obj);
    virtual int init();

    virtual int get_lineno()const;
    virtual int get_bad_lineno() const;
    virtual bool if_print_progress() const;

    struct MaxLenSQLBuffer
    {
        char buf[MAX_CONSTRUCT_SQL_LENGTH];
    };

  private:
    int write_bad_record(RecordBlock &rec);
    int write_correct_record(RecordBlock &rec);

    oceanbase::api::OceanbaseDb *db_;
    ObRowBuilder *builder_;
    const TableParam &param_;
    AppendableFile *bad_file_;
    AppendableFile *correct_file_;
    CharArena allocator_;
    char *line_buffer_;
    //add by liyongfeng 
    yysys::CThreadMutex bad_lock_;
    //add:e
    yysys::CThreadMutex correct_lock_;
    char *correct_line_buffer_;
    MergeServerManager ms_manager_;
    ObMysqlClientHelper mysql_client;
    ObServer ms_;
};

#endif


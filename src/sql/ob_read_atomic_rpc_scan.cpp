//add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151229:b
#include "ob_read_atomic_rpc_scan.h"
#include "common/ob_common_param.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::mergeserver;

ObReadAtomicRpcScan::ObReadAtomicRpcScan():
  ObRpcScan(),
  is_inited_(false),
  is_first_read_rpc_opened_(false),
  is_with_read_atomic_(false),
  is_output_cols_added_(false),
  is_first_read_finish_(false),
  is_second_read_filter_added_(false),
  is_second_read_rpc_opend_(false),
  second_read_need_filter_again_(false),
  min_used_column_id_(0),
  max_used_column_id_(0),
  total_base_column_num_(0),
  first_read_hint_(),
  second_read_hint_(),
  final_row_(),
  final_row_desc_(),
  orig_output_column_ids_(),
  orig_output_column_exprs_(common::OB_COMMON_MEM_BLOCK_SIZE,
                              ModulePageAllocator(ObModIds::OB_READ_ATOMIC_RPC_SCAN)),
  orig_filters_(common::OB_COMMON_MEM_BLOCK_SIZE,
                ModulePageAllocator(ObModIds::OB_READ_ATOMIC_RPC_SCAN)),
  prepare_output_column_exprs_(common::OB_COMMON_MEM_BLOCK_SIZE,
                               ModulePageAllocator(ObModIds::OB_READ_ATOMIC_RPC_SCAN)),
  prepare_output_cids_(),
  first_read_row_cache_(ObModIds::OB_READ_ATOMIC_RPC_SCAN),
  first_read_cache_row_(),
  is_transver_stat_map_created_(false),
  transver_state_map_(),
  second_read_rpc_scan_(),
  data_mark_state_map_(),
  fetch_transver_stat_rpc_(),
  sys_table_rowkey_info_(),
  sys_table_row_desc_(),
  first_rpc_scan_start_time_(-1)
{

}

ObReadAtomicRpcScan::~ObReadAtomicRpcScan()
{
  reset();
}


void ObReadAtomicRpcScan::reset()
{
  ObRpcScan::reset();
  is_inited_ = false;
  is_first_read_rpc_opened_ = false;
  if (is_with_read_atomic_)
  {
    is_with_read_atomic_ = false;
    is_output_cols_added_ = false;
    is_first_read_finish_ = false;
    min_used_column_id_ = 0;
    max_used_column_id_ = 0;
    first_rpc_scan_start_time_=-1;
    total_base_column_num_ = 0;
    first_read_hint_.reset();
    final_row_.clear();
    final_row_desc_.reset();
    orig_output_column_ids_.reset();
    orig_output_column_exprs_.clear();
    prepare_output_column_exprs_.clear();
    //not allow to modify or destry these filters,
    //just clear them!
    orig_filters_.clear();
    prepare_output_cids_.reset();
    first_read_row_cache_.clear();
    first_read_cache_row_.clear();
    is_transver_stat_map_created_ = false;
    transver_state_map_.destroy();
    data_mark_state_map_.destroy();
    paxos_data_mark_map_.destroy();
    is_second_read_filter_added_ = false;
    is_second_read_rpc_opend_ = false;
    second_read_rpc_scan_.reset();
    second_read_hint_.reset();
    second_read_need_filter_again_ = false;
    fetch_transver_stat_rpc_.reset();
    sys_table_row_desc_.reset();
  }
}

void ObReadAtomicRpcScan::reuse()
{
  ObRpcScan::reuse();
  is_inited_ = false;
  is_first_read_rpc_opened_ = false;
  if (is_with_read_atomic_)
  {
    is_with_read_atomic_ = false;
    is_output_cols_added_ = false;
    is_first_read_finish_ = false;
    min_used_column_id_ = 0;
    max_used_column_id_ = 0;
    total_base_column_num_   = 0;
    first_read_hint_.reset();
    final_row_.clear();
    final_row_desc_.reset();
    orig_output_column_ids_.reset();
    orig_output_column_exprs_.clear();
    prepare_output_column_exprs_.clear();
    //not allow to modify or destry these filters,
    //just clear them!
    orig_filters_.clear();
    prepare_output_cids_.reset();
    first_read_row_cache_.reuse();
    first_read_cache_row_.clear();
    is_transver_stat_map_created_ = false;
    transver_state_map_.destroy();
    data_mark_state_map_.destroy();
    paxos_data_mark_map_.destroy();
    is_second_read_filter_added_ = false;
    is_second_read_rpc_opend_ = false;
    second_read_rpc_scan_.reuse();
    second_read_hint_.reset();
    second_read_need_filter_again_ = false;
    fetch_transver_stat_rpc_.reuse();
    sys_table_row_desc_.reset();
  }
}

/*初始化 rpc_scan(一轮rpc_scan 、二轮rpc_scan、fetch系统表rpc_scan)  一轮read_atomic_param 两轮read_atomic_param*/
int ObReadAtomicRpcScan::init(ObSqlContext *context, const common::ObRpcScanHint *hint)
{
  int ret = OB_SUCCESS;
  if (is_inited_ || is_with_read_atomic_)
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(ERROR,"cur rpc scan has been inited!"
              "ret=%d,is_inited_=%d,is_with_read_atomic_=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (OB_SUCCESS != (ret = ObRpcScan::init(context,hint)))
  {
    YYSYS_LOG(WARN,"fail to init ObRpcScan!ret=%d",ret);
  }
  //1==>ensure if this RpcScan will run with read atomic!!
  else if (NULL != hint && NO_READ_ATOMIC_LEVEL != hint->read_atomic_level_)
  {//means this RpcScan run with read atomic!!!
    is_with_read_atomic_ = true;
    first_read_hint_ = *hint;
  }
  else
  {//means it's just a common ObRpcScan,without read atomic!
    is_with_read_atomic_ = false;
  }

  //2==>if this RpcScan run with read atomic,ensure related params!
  if (OB_SUCCESS == ret && is_with_read_atomic_)
  {
    int32_t column_size = 0;
    uint64_t base_table_id      = get_base_table_id();
    const ObColumnSchemaV2 *col       = NULL;
    FetchTransverStatMeth  fetch_meth = NO_FETCH_TRANSVER;
    common::ObReadAtomicParam *first_param  = get_first_read_atomic_param_();
    //add by maosy [FIX_SUPPORT_READ_ATOMIC]20170627 b:
    const ObTableSchema* table_schema =NULL;
    uint64_t main_table_id =OB_INVALID_ID ;
    max_used_column_id_  = 0;
    // add by maosy e
    if (NULL == context
        || NULL == context->schema_manager_
        || OB_INVALID_ID == base_table_id
        || NULL == first_param)
    {
      ret = OB_INVALID_ARGUMENT;
      YYSYS_LOG(ERROR,"invalid argument!"
                "context=%p,table_id=%ld,first_read_atomic_param=%p,ret=%d",
                context,base_table_id,first_param,ret);
    }
    //add by maosy [FIX_SUPPORT_READ_ATOMIC]20170627 b:
    else if(NULL ==(table_schema = context->schema_manager_->get_table_schema(base_table_id)))
    {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR,"fail to get table schema!tid=%ld,ret=%d",base_table_id,ret);
    }
    else if(OB_INVALID_ID!=(main_table_id =table_schema->get_index_helper().tbl_tid))
    {
        max_used_column_id_ = table_schema->get_max_column_id();//如果二级索引里面没有冗余列，511是最大的
        base_table_id = main_table_id ;
    }
    if(OB_SUCCESS != OB_SUCCESS)
    {}
    // add by e
    else if (NULL == (col = context->schema_manager_->get_table_schema(base_table_id,column_size)))
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR,"fail to get column schema!tid=%ld,ret=%d",base_table_id,ret);
    }
    else
    {
      min_used_column_id_  = OB_MAX_TMP_COLUMN_ID;
//      max_used_column_id_  = 0;//del  by maosy [FIX_SUPPORT_READ_ATOMIC]20170627 b:
      for (int32_t i=0;NULL!=col&&i<column_size;i++)
      {
        uint64_t cid = col[i].get_id();
        max_used_column_id_ = (max_used_column_id_ > cid) ? max_used_column_id_ : cid;
        min_used_column_id_ = (min_used_column_id_ < cid) ? min_used_column_id_ : cid;
      }
      total_base_column_num_ = column_size;

      if (max_used_column_id_ <= 0
          || OB_INVALID_ID == max_used_column_id_
          || min_used_column_id_ <= 0
          || OB_INVALID_ID == min_used_column_id_
          || OB_MAX_TMP_COLUMN_ID == min_used_column_id_
          || total_base_column_num_ <= 0)
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR,"fail to get max_used_column_id=%ld and min_used_column_id=%ld,column_size=%ld,ret=%d",
                  max_used_column_id_,min_used_column_id_,total_base_column_num_,ret);
      }
      //first init param of frist round read rpc scan! 设置第一轮读 read_atomic_param
      else if (OB_SUCCESS != (ret = init_read_atomic_param_(first_read_hint_.read_atomic_level_,
                                                            FIRST_READ_ROUND,
                                                            first_param)))
      {
        YYSYS_LOG(WARN,"fail to set read atomic param!ret=%d",ret);
      }
      //then init param of second round read rpc scan! 初始化 二轮读 rpc_scan的read_atomic_param
      else if (need_second_read_() && OB_SUCCESS != (ret = init_second_read_(context)))
      {
        YYSYS_LOG(WARN,"fail to init second read!ret=%d",ret);
      }
        /*初始化 向系统表 __ups_session_info 查询的 rpc_scan*/
      else if (need_fetch_transver_state(fetch_meth)
               && OB_SUCCESS != (ret = init_fetch_transver_state_(context,fetch_meth)))
      {
        YYSYS_LOG(WARN,"fail to init fetch transver state !ret=%d,fetch_meth=%d",ret,fetch_meth);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    is_inited_ = true;
  }
  else
  {
    is_inited_ = false;
  }
  return ret;
}

int ObReadAtomicRpcScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must first init!ret=%d",ret);
  }
  else if (!is_with_read_atomic_ && OB_SUCCESS != (ret = ObRpcScan::get_row_desc(row_desc)))
  {
    YYSYS_LOG(WARN,"fail to get row desc from ObRpcScan!ret=%d",ret);
  }
  else if (is_with_read_atomic_)
  {
    row_desc = &orig_output_column_ids_;
  }
  return ret;
}

int ObReadAtomicRpcScan::add_output_column(const ObSqlExpression &expr,bool change_tid_for_storing) //mod 
{
  int ret = OB_SUCCESS;
  if (!is_inited_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d",ret);
  }
  else if (!is_with_read_atomic_
           && OB_SUCCESS != (ret = ObRpcScan::add_output_column(expr,change_tid_for_storing)))//mod
  {//means this RpcScan run without read atomic!
    YYSYS_LOG(WARN,"fail to add output column into ObRpcScan!ret=%d",ret);
  }
  else if (is_with_read_atomic_)
  {//means this RpcScan run with read atomic!
    bool is_cid = false;
    uint64_t tid = expr.get_table_id();
    uint64_t cid = expr.get_column_id();
    common::ObReadAtomicParam *first_read_param  = get_first_read_atomic_param_();
    if (NULL == first_read_param)
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(ERROR,"read atomic param should not be NULL!tid=%ld,cid=%ld,ret=%d",
                tid,cid,ret);
    }
    else if (OB_SUCCESS == (ret = orig_output_column_exprs_.push_back(expr)))
    {//store all orig output columns!
      orig_output_column_exprs_.at(orig_output_column_exprs_.count() - 1).set_owner_op(this);
    }
    else
    {
      YYSYS_LOG(WARN,"fail to store output expr!expr=[%s],ret=%d",to_cstring(expr),ret);
    }

    YYSYS_LOG(DEBUG,"read_atomic::debug,orig_output_column_expr=[%s],ret=%d",
              to_cstring(expr),ret);

    if (OB_SUCCESS == ret
        && first_read_param->need_prepare_data_
        && OB_INVALID_ID != cid
        && OB_ACTION_FLAG_COLUMN_ID != cid
            //add by maosy [FIX_SUPPORT_READ_ATOMIC]20170627 b:
            && OB_INDEX_VIRTUAL_COLUMN_ID != cid)
        // add by e
    {
      if (OB_SUCCESS != (ret = expr.is_column_index_expr(is_cid)))
      {
        YYSYS_LOG(WARN,"fail to get is column index expr!tid=%ld,cid=%ld,ret=%d",
                tid,cid,ret);
      }
      else
      {
        if (!is_cid)
        {//复合列
          first_read_param->table_composite_column_num_++;
        }

        if (OB_SUCCESS != (ret = store_prepare_output_column_exprs_(expr)))
        {
          YYSYS_LOG(WARN,"fail to store prepare output expr!expr=[%s],ret=%d",
                    to_cstring(expr),ret);
        }
      }
    }

    if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = orig_output_column_ids_.add_column_desc(tid, cid))))
    {
      YYSYS_LOG(WARN, "fail to add column to orig output column ids. ret=%d, tid_=%lu, cid=%lu",
                ret, tid, cid);
    }
  }
  return ret;
}

int ObReadAtomicRpcScan::add_filter(ObSqlExpression *expr)
{
  int ret = OB_SUCCESS;
  if (!is_inited_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d",ret);
  }
  else if (OB_SUCCESS != (ret = ObRpcScan::add_filter(expr)))
  {
    YYSYS_LOG(WARN,"fail to add filter to ObRpcScan!ret=%d",ret);
  }
  else if (is_with_read_atomic_)
  {
    if (OB_SUCCESS != (ret = orig_filters_.push_back(expr)))
    {
      YYSYS_LOG(WARN,"fail to store orig filters!ret=%d",ret);
    }
    else
    {
      //don't call expr->set_owner_op(this) here!!!
      //it has called by ObRpcScan when call ObRpcScan::add_filter(expr)!!!
    }
  }

  return ret;
}

int ObReadAtomicRpcScan::add_group_column(const uint64_t tid, const uint64_t cid)
{
  int ret = OB_SUCCESS;
  if (!is_inited_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d",ret);
  }
  else if (!is_with_read_atomic_ && OB_SUCCESS != (ret = ObRpcScan::add_group_column(tid,cid)))
  {
     YYSYS_LOG(WARN,"fail to add group column to ObRpcScan!ret=%d",ret);
  }
  else if (is_with_read_atomic_)
  {
    ret = OB_NOT_SUPPORTED;
    YYSYS_LOG(ERROR,"read atomic rpc not support push down group yet!table_id=%ld,ret=%d",
              get_base_table_id(),ret);
  }
  return ret;
}

int ObReadAtomicRpcScan::add_aggr_column(const ObSqlExpression &expr)
{
  int ret = OB_SUCCESS;
  if (!is_inited_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d",ret);
  }
  else if (!is_with_read_atomic_ && OB_SUCCESS != (ret = ObRpcScan::add_aggr_column(expr)))
  {
     YYSYS_LOG(WARN,"fail to add aggr column to ObRpcScan!ret=%d",ret);
  }
  else if (is_with_read_atomic_)
  {
    ret = OB_NOT_SUPPORTED;
    YYSYS_LOG(ERROR,"read atomic rpc not support push down aggr yet!table_id=%ld,ret=%d",
              get_base_table_id(),ret);
  }
  return ret;
}

void ObReadAtomicRpcScan::set_rowkey_cell_count(const int64_t rowkey_cell_count)
{
  orig_output_column_ids_.set_rowkey_cell_count(rowkey_cell_count);
  ObRpcScan::set_rowkey_cell_count(rowkey_cell_count);
}

int ObReadAtomicRpcScan::open()
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = get_base_table_id();
  first_rpc_scan_start_time_ = yysys::CTimeUtil::getTime();
  if (!is_inited_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must first init!table_id=%ld,ret=%d",table_id,ret);
  }
  else if (is_first_read_rpc_opened_)
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(ERROR,"RpcScan already opened!table_id=%ld,ret=%d",table_id,ret);
  }
  else if (!is_with_read_atomic_)/*不是 read_atomic方式，走最基本的查询流程*/
  {
    YYSYS_LOG(DEBUG,"NORMAL  RPC  open");
    ret = ObRpcScan::open();
  }
    /*设置第一轮（一轮读 和两轮读算法的第一轮）rpc_scan output_column*/
  else if (!is_output_cols_added_
           && OB_SUCCESS != (ret = internal_add_output_column_(FIRST_READ_ROUND)))
  {
    YYSYS_LOG(WARN,"fail to add output columns for first read!table_id=%ld,ret=%d",
              table_id,ret);
  }
    /*设置二轮读算法的第二轮  output_column*/
  else if (!is_output_cols_added_
           && need_second_read_()
           && OB_SUCCESS != (ret = internal_add_output_column_(SECOND_READ_ROUND)))
  {
    YYSYS_LOG(WARN,"fail to add output columns for second read!table_id=%ld,ret=%d",
              table_id,ret);
  }
  else if (OB_SUCCESS != (ret = cons_final_row_desc_()))
  {
    YYSYS_LOG(WARN,"fail to construct final row desc!table_id=%ld,ret=%d",table_id,ret);
  }
  //open first read rpc scan!  一轮读  ，二轮读的第一轮读
  else if (OB_SUCCESS != (ret = ObRpcScan::open()))
  {
    YYSYS_LOG(WARN,"fail to open ObRpcScan::open!table_id=%ld,ret=%d",table_id,ret);
  }
  else
  {
    is_output_cols_added_ = true;
    final_row_.clear();
    final_row_.set_row_desc(final_row_desc_);
  }

  if (OB_SUCCESS == ret)
  {
    is_first_read_rpc_opened_ = true;
  }
  else
  {
    is_first_read_rpc_opened_ = false;
  }

  if (is_with_read_atomic_)
  {
    YYSYS_LOG(DEBUG,"read_atomic::debug,the orig_output_row_desc=[%s]",to_cstring(orig_output_column_ids_));
    YYSYS_LOG(DEBUG,"read_atomic::debug,the final_row_desc=[%s],ret=%d",to_cstring(final_row_desc_),ret);
  }

  //read_atomic::debug::tmp
  YYSYS_LOG(DEBUG,"read_atomic::debug,atomic rpc finish open!ret=%d",ret);
  return ret;
}

int ObReadAtomicRpcScan::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = get_base_table_id();
  if (!is_inited_ )
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must first init !is_inited=%d,tid=%ld,ret=%d",
              is_inited_,table_id,ret);
  }
  else if (!is_with_read_atomic_
           && OB_SUCCESS != (ret = ObRpcScan::get_next_row(row)))
  {
    if (OB_ITER_END != ret)
    {
      YYSYS_LOG(WARN,"fail to get next row from ObRpcScan!table_id=%ld,ret=%d",
                table_id,ret);
    }
  }
  else if (is_with_read_atomic_
           && OB_SUCCESS != (ret = internal_get_next_row_(row)))
  {
    if (OB_ITER_END != ret)
    {
      YYSYS_LOG(WARN,"fail to get next row from ObReadAtomicRpcScan!table_id=%ld,ret=%d",
                table_id,ret);
    }
  }

  if (is_with_read_atomic_ && NULL != row)
  {
    YYSYS_LOG(DEBUG,"read_atomic::debug,the final row to Clinet!row=[%s],ret=%d",
              to_cstring(*row),ret);
  }
  return ret;
}

int ObReadAtomicRpcScan::close()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  is_first_read_rpc_opened_ = false;
  if (OB_SUCCESS !=(ret = ObRpcScan::close()))
  {
    YYSYS_LOG(WARN,"fail to close ObRpcScan!ret=%d",ret);
  }

  //no matter what happend!must clear!
  if (is_with_read_atomic_
      && OB_SUCCESS != (tmp_ret = clear_with_read_atomic_()))
  {
    YYSYS_LOG(WARN,"fail to reset read atomic!ret=%d",tmp_ret);
    ret = (OB_SUCCESS != ret) ? ret:tmp_ret;
  }
  return ret;
}

namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObReadAtomicRpcScan, PHY_READ_ATOMIC_RPC_SCAN);
  }
}

int64_t ObReadAtomicRpcScan::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (is_with_read_atomic_)
  {
    databuff_printf(buf, buf_len, pos, "ObReadAtomicRpcScan[");
    databuff_printf(buf, buf_len, pos, "orig_output_column_exprs=(");
    for (int64_t i = 0; i < orig_output_column_exprs_.count(); ++i)
    {
      int64_t pos2 = orig_output_column_exprs_.at(i).to_string(buf+pos, buf_len-pos);
      pos += pos2;
      if (i != orig_output_column_exprs_.count() -1)
      {
        databuff_printf(buf, buf_len, pos, ",");
      }
    }
    databuff_printf(buf, buf_len, pos, "),");
    databuff_printf(buf, buf_len, pos, "prepare_output_column_exprs=(");
    for (int64_t i = 0; i < prepare_output_column_exprs_.count(); ++i)
    {
      int64_t pos2 = prepare_output_column_exprs_.at(i).to_string(buf+pos, buf_len-pos);
      pos += pos2;
      if (i != prepare_output_column_exprs_.count() -1)
      {
        databuff_printf(buf, buf_len, pos, ",");
      }
    }
    databuff_printf(buf, buf_len, pos, "),\n");
    pos += ObRpcScan::to_string(buf+pos, buf_len-pos);
    if (need_second_read_())
    {
      databuff_printf(buf, buf_len, pos, "\nsecond_read_rpc_scan=(");
      pos += second_read_rpc_scan_.to_string(buf+pos, buf_len-pos);
      databuff_printf(buf, buf_len, pos, "),\n");
    }

    databuff_printf(buf, buf_len, pos, "\nfetch_transver_state_rpc_scan=(");
    pos += fetch_transver_stat_rpc_.to_string(buf+pos, buf_len-pos);
    databuff_printf(buf, buf_len, pos, "),\n");

    databuff_printf(buf, buf_len, pos, "]");
  }
  else
  {
    pos = ObRpcScan::to_string(buf,buf_len);
  }
  return pos;
}


/*@berif 初始化 read_atomic_param*/
int ObReadAtomicRpcScan::init_read_atomic_param_(const common::ObReadAtomicLevel level,
                                                 const ReadRoundState read_round,
                                                 common::ObReadAtomicParam *des_param)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = get_base_table_id();
  const common::ObRowkeyInfo &row_key_info = get_rowkey_info();
  const int64_t rowkey_cell_count          = row_key_info.get_size();
  uint64_t tmp_total_base_col_num          = (total_base_column_num_
                                              >= (max_used_column_id_ - min_used_column_id_ +1)) ?
                                              total_base_column_num_ : (max_used_column_id_ - min_used_column_id_ +1);
  uint64_t tmp_max_used_column_id          = tmp_total_base_col_num + min_used_column_id_ - 1;

  if (!is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }

  if (OB_SUCCESS == ret)
  {
    switch(level)
    {
      case NO_READ_ATOMIC_LEVEL:
          if (NULL != des_param)
          {
            des_param->reset();
          }
          break;
      case READ_ATOMIC_WEAK:
          if (NULL == des_param)
          {
            ret = OB_NOT_INIT;
            YYSYS_LOG(ERROR,"read_atomic_param is NULL!table_id=%ld,ret=%d",table_id,ret);
          }
          else
          {
            switch(read_round)
            {
              case FIRST_READ_ROUND:
              {
                des_param->reset();
                des_param->need_last_commit_trans_ver_    = false;
                des_param->need_last_prepare_trans_ver_   = true;
                des_param->need_prevcommit_trans_verset_  = true;
                des_param->need_commit_data_              = true;
                des_param->need_prepare_data_             = true;
                des_param->last_commit_trans_ver_cid_     = ++tmp_max_used_column_id;
                des_param->last_prepare_trans_ver_cid_    = ++tmp_max_used_column_id;
                des_param->commit_data_extendtype_cid_    = ++tmp_max_used_column_id;
                des_param->prepare_data_extendtype_cid_   = ++tmp_max_used_column_id;
                des_param->prevcommit_trans_verset_cid_off_   = ++tmp_max_used_column_id;
                des_param->max_prevcommit_trans_verset_count_ = PREV_COMMIT_TRANSVER_COUNT;
                des_param->prepare_data_cid_off_          = des_param->prevcommit_trans_verset_cid_off_
                                                            + des_param->max_prevcommit_trans_verset_count_;
                des_param->table_id_                      = table_id;
                des_param->min_used_column_cid_           = min_used_column_id_;
                des_param->table_base_column_num_         = tmp_total_base_col_num;
                des_param->table_composite_column_num_    = OB_END_RESERVED_COLUMN_ID_NUM;//will be countted when call add_output_column!
                //cur param is not completed!

                break;
              }
              default:
                ret = OB_NOT_SUPPORTED;
                YYSYS_LOG(ERROR,"READ ATOMIC WEAK just support one round read!read_round=%d,ret=%d",
                          read_round,ret);
                break;
            }
          }
          break;
      case READ_ATOMIC_STRONG:
          if (NULL == des_param)
          {
            ret = OB_NOT_INIT;
            YYSYS_LOG(ERROR,"cur_read_atomic_param is NULL!table_id=%ld,ret=%d",table_id,ret);
          }
          else
          {
            switch(read_round)
            {
              case FIRST_READ_ROUND:
                  des_param->reset();
                  des_param->need_last_commit_trans_ver_    = true;
                  des_param->need_last_prepare_trans_ver_   = true;
                  des_param->need_prevcommit_trans_verset_  = true;
                  des_param->need_data_mark_                = true;
                  des_param->need_row_key_                  = false;
                  des_param->last_commit_trans_ver_cid_     = ++tmp_max_used_column_id;
                  des_param->last_prepare_trans_ver_cid_    = ++tmp_max_used_column_id;
//                  des_param->commit_data_extendtype_cid_    = ++tmp_max_used_column_id;
//                  des_param->prepare_data_extendtype_cid_   = ++tmp_max_used_column_id;
                  des_param->major_ver_cid_                 = ++tmp_max_used_column_id;
                  des_param->minor_ver_start_cid_           = ++tmp_max_used_column_id;
                  des_param->minor_ver_end_cid_             = ++tmp_max_used_column_id;
                  des_param->ups_paxos_id_cid_              = ++tmp_max_used_column_id;
                  des_param->data_store_type_cid_           = ++tmp_max_used_column_id;
                  des_param->prevcommit_trans_verset_cid_off_   = ++tmp_max_used_column_id;
                  des_param->max_prevcommit_trans_verset_count_ = PREV_COMMIT_TRANSVER_COUNT;
                  des_param->prepare_data_cid_off_          = des_param->prevcommit_trans_verset_cid_off_
                                                              + des_param->max_prevcommit_trans_verset_count_;
                  des_param->rowkey_cell_count_             = rowkey_cell_count;
                  des_param->table_id_                      = table_id;
                  des_param->min_used_column_cid_           = min_used_column_id_;
                  des_param->table_base_column_num_         = tmp_total_base_col_num;
                  des_param->table_composite_column_num_    = OB_END_RESERVED_COLUMN_ID_NUM;//will be countted when call add_output_column!
                  //cur param is not completed!

                  break;
              case SECOND_READ_ROUND:
                  des_param->reset();
                  des_param->need_exact_transver_data_  = true;
                  des_param->need_exact_data_mark_data_ = true;
                  des_param->need_data_mark_            = true;
                  des_param->commit_data_extendtype_cid_= ++tmp_max_used_column_id;
                  des_param->major_ver_cid_             = ++tmp_max_used_column_id;
                  des_param->minor_ver_start_cid_       = ++tmp_max_used_column_id;
                  des_param->minor_ver_end_cid_         = ++tmp_max_used_column_id;
                  des_param->ups_paxos_id_cid_          = ++tmp_max_used_column_id;
                  des_param->data_store_type_cid_       = ++tmp_max_used_column_id;
                  des_param->table_id_                  = table_id;
                  //cur param is not completed!

                 break;
              default:
                 ret = OB_NOT_SUPPORTED;
                 YYSYS_LOG(ERROR,"READ ATOMIC STRONG just support two round read!read_round=%d,ret=%d",
                           read_round,ret);
                 break;
            }
          }
          break;
      default:
          ret = OB_NOT_SUPPORTED;
          YYSYS_LOG(ERROR,"unknown read atomic level!level=%d,table_id=%ld,ret=%d",
                    level,table_id,ret);
          break;
    }
  }
  return ret;
}


/*@param 初始化 二轮读 rpc_scan的read_atomic_param  和rpc_scan*/
int ObReadAtomicRpcScan::init_second_read_(ObSqlContext *context)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id      = get_table_id();
  const uint64_t base_table_id = get_base_table_id();
  common::ObReadAtomicParam *first_read_param  = get_first_read_atomic_param_();
  common::ObReadAtomicParam *second_read_param = NULL;
  if (!is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (NULL == context
           || NULL == first_read_param)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"sqlcontext  or read atomic param can't be NULL!"
              "context=%p,first_param=%p,table_id=%ld,ret=%d",
              context,first_read_param,base_table_id,ret);
  }
  else
  {
    second_read_hint_ = first_read_hint_;
    if (first_read_param->need_row_key_)
    {//if first round read will get rowkey,
     //second round read better use GET
//     second_read_hint_.read_method_ = ObSqlReadStrategy::USE_GET;
    }
  }

  if (OB_SUCCESS == ret)
  {
    //important!!!!must set phy plan before init!
    second_read_rpc_scan_.set_phy_plan(get_phy_plan());
    if (OB_SUCCESS != (ret = second_read_rpc_scan_.set_table(table_id,base_table_id)))
    {
      YYSYS_LOG(WARN,"fail to set table id for second round read rpc scan!tid=%ld,ret=%d",
                base_table_id,ret);
    }
    else if (OB_SUCCESS != (ret = second_read_rpc_scan_.init(context,&second_read_hint_)))
    {
      YYSYS_LOG(WARN,"fail to init second round read rpc scan!tid=%ld,ret=%d",
                base_table_id,ret);
    }
    else if (NULL == (second_read_param = get_second_read_atomic_param_()))
    {
      ret = OB_NOT_INIT;
      YYSYS_LOG(ERROR,"second read atomic param should not be NULL!ret=%d",ret);
    }
    else if (OB_SUCCESS != (ret = init_read_atomic_param_(first_read_hint_.read_atomic_level_,
                                                          SECOND_READ_ROUND,second_read_param)))
    {
      YYSYS_LOG(WARN,"fail to set second read rpc scan read atomic param!ret=%d",ret);
    }
  }
  return ret;
}

/*初始化 向系统表 __ups_session_info 查询的 rpc_scan*/
int ObReadAtomicRpcScan::init_fetch_transver_state_(ObSqlContext *context,
                                                    const FetchTransverStatMeth fetch_meth)
{
  int ret = OB_SUCCESS;
  if (!is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else
  {
    switch(fetch_meth)
    {
      case NO_FETCH_TRANSVER:
      {
        //it's ok
        break;
      }
      case FETCH_FROM_CACHE:
      {
        //TODO:READ_ATOMIC
        ret = OB_NOT_SUPPORTED;
        YYSYS_LOG(ERROR,"didn't support cache transver state yet!ret=%d",ret);
        break;
      }
      case FETCH_FROM_SYS_TABLE:
      {
        if (OB_SUCCESS != (ret = init_fetch_transver_state_rpc_(context)))
        {
          YYSYS_LOG(WARN,"fail to init fetch transver stat rpc!ret=%d",ret);
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        YYSYS_LOG(ERROR,"unknow fetch transver stat meth[%d]!ret=%d",fetch_meth,ret);
        break;
    }
  }

  YYSYS_LOG(DEBUG,"read_atomic::debug,final fetch_meth=%d,ret=%d",fetch_meth,ret);

  return ret;
}

/*初始化 向系统表 __ups_session_info 查询的 rpc_scan*/
int ObReadAtomicRpcScan::init_fetch_transver_state_rpc_(ObSqlContext *context)
{
  int ret = OB_SUCCESS;
  uint64_t sys_table_id = SYS_TABLE_TRANSVER_STAT_TID;
  const ObTableSchema * schema = NULL;
  const ObColumnSchemaV2 *col  = NULL;
  int32_t  column_num  = 0;
  uint64_t column_id   = OB_INVALID_ID;

  ObRpcScanHint hint;
  hint.read_consistency_ = common::STRONG;
  hint.is_get_skip_empty_row_ = true;
  hint.read_method_ = ObSqlReadStrategy::USE_GET;
  //it's important!
  fetch_transver_stat_rpc_.set_phy_plan(get_phy_plan());

  if (!is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (NULL == context
           || NULL == context->schema_manager_)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"context can't be NULL!context=%p,ret=%d",context,ret);
  }
  else if ( OB_SUCCESS != (ret = fetch_transver_stat_rpc_.set_table(sys_table_id, sys_table_id)))
  {
    YYSYS_LOG(WARN,"fail to set table id for fetch_transver_stat_rpc!ret=%d",ret);
  }
  else if (OB_SUCCESS != (ret = fetch_transver_stat_rpc_.init(context, &hint)))
  {
    YYSYS_LOG(WARN,"fail to init fetch_transver_stat_rpc!ret=%d",ret);
  }
  else if (NULL == (schema = context->schema_manager_->get_table_schema(sys_table_id)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "fail to get table schema. table_id[%ld],ret=%d", sys_table_id,ret);
  }
  else if (NULL == (col = context->schema_manager_->get_table_schema(sys_table_id,column_num)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR,"fail to get column schema!tid=%ld,ret=%d",sys_table_id,ret);
  }
  else
  {
    // copy
    sys_table_rowkey_info_ = schema->get_rowkey_info();
    sys_table_row_desc_.reset();
    sys_table_row_desc_.set_rowkey_cell_count(sys_table_rowkey_info_.get_size());
    fetch_transver_stat_rpc_.set_rowkey_cell_count(sys_table_rowkey_info_.get_size());
    for (int32_t i=0;OB_SUCCESS == ret && i<column_num;i++)
    {
      column_id = col[i].get_id();
      if (OB_SUCCESS != (ret = sys_table_row_desc_.add_column_desc(sys_table_id,column_id)))
      {
        YYSYS_LOG(WARN,"fail to cons sys table row desc!tid=%ld,cid=%ld,ret=%d",
                  sys_table_id,column_id,ret);
      }
      else
      {
        ObSqlExpression output_column;
        output_column.set_tid_cid(sys_table_id,column_id);
        if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(sys_table_id, column_id, output_column)))
        {
          YYSYS_LOG(WARN, "fail to create column expression. ret=%d,tid=%ld,cid=%ld",
                    ret,sys_table_id,column_id);
        }
        else if (OB_SUCCESS != (ret = fetch_transver_stat_rpc_.add_output_column(output_column)))
        {
          YYSYS_LOG(WARN, "fail to add column to fetch transver stat rpc scan operator. ret=%d", ret);
        }
      }
    }
  }

  return ret;
}


int ObReadAtomicRpcScan::store_prepare_output_column_exprs_(const ObSqlExpression &expr)
{
  int ret = OB_SUCCESS;

  YYSYS_LOG(DEBUG,"read_atomic::debug,orig_expr=[%s],ret=%d",
            to_cstring(expr),ret);

  const uint64_t tid   = expr.get_table_id();
  const uint64_t cid   = expr.get_column_id();
  uint64_t prepare_cid = OB_INVALID_ID;
//  int64_t item_count   = 0;
  ObSqlExpression *prepare_output_expr = NULL;
  const common::ObReadAtomicParam *first_read_param = get_first_read_atomic_param_();

  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (NULL == first_read_param || !first_read_param->need_prepare_data_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"read_atomic_param is not valid!read_atomic_param=%p,ret=%d",
              first_read_param,ret);
  }
  else if (OB_INVALID_ID == cid || OB_ACTION_FLAG_COLUMN_ID == cid)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"special column id can't used to construct prepare output cid!"
              "expr=[%s],ret=%d",to_cstring(expr),ret);
  }
  else if (OB_SUCCESS == (ret = prepare_output_column_exprs_.push_back(expr)))
  {//store all orig output columns!
    prepare_output_column_exprs_.at(prepare_output_column_exprs_.count() - 1).set_owner_op(this);
    prepare_output_expr = &(prepare_output_column_exprs_.at(prepare_output_column_exprs_.count() - 1));
  }


  if (OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN,"fail to store prepare expr!expr=[%s],ret=%d",
              to_cstring(expr),ret);
  }
  else if (NULL == prepare_output_expr)
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(ERROR,"prepare output expr should not be NULL!ret=%d",ret);
  }
  else if (OB_SUCCESS != (ret = common::ObReadAtomicHelper::convert_commit_cid_to_prepare_cid(*first_read_param,cid,prepare_cid)))
  {
    YYSYS_LOG(WARN,"fail to construct prepare column id!commit_cid=%ld,prepare_cid=%ld,expr=[%s],ret=%d",
              cid,prepare_cid,to_cstring(expr),ret);
  }
  else if (OB_SUCCESS != (ret = store_prepare_output_cid_(tid,prepare_cid)))
  {
    YYSYS_LOG(WARN,"fail to store prepare cid!tid=%ld,cid=%ld,ret=%d",
              tid,prepare_cid,ret);
  }
  else
  {
    prepare_output_expr->set_tid_cid(tid,prepare_cid);
//    item_count = prepare_output_expr->post_expr_.expr_.count();

    int64_t next_expr_idx = 0;
    bool is_finish = false;
    ObObj *tid_val = NULL;
    ObObj *cid_val = NULL;
    while (OB_SUCCESS == ret)
    {
      is_finish = false;
      uint64_t tmp_tid  = OB_INVALID_ID;
      uint64_t tmp_cid  = OB_INVALID_ID;
      uint64_t tmp_prepare_cid = OB_INVALID_ID;

      YYSYS_LOG(DEBUG,"read_atomic::debug,begin get next tid and cid!next_expr_idx=%ld",next_expr_idx);

      if (OB_SUCCESS != (ret = prepare_output_expr->post_expr_.get_next_tid_cid(tid_val,
                                                                                cid_val,
                                                                                is_finish,
                                                                                next_expr_idx)))
      {
        YYSYS_LOG(WARN,"fail to get next tid cid!ret=%d",ret);
      }
      else if (is_finish)
      {
        break;
      }
      else if (NULL == tid_val || NULL == cid_val)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR,"fail to get tid_val[%p] and cid_val[%p]!ret=%d",tid_val,cid_val,ret);
      }
      else if (OB_SUCCESS != (ret = tid_val->get_int((int64_t&)tmp_tid)))
      {
        YYSYS_LOG(WARN, "fail to get tid!next_expr_idx=%ld,tmp_tid=%ld,err=%d",
                  next_expr_idx,tmp_tid,ret);
      }
      else if (OB_SUCCESS != (ret = cid_val->get_int((int64_t&)tmp_cid)))
      {
        YYSYS_LOG(WARN, "fail to get cid!next_expr_idx=%ld,tmp_tid=%ld,tmp_cid=%ld,err=%d",
                  next_expr_idx,tmp_tid,tmp_cid,ret);
      }
      else if (OB_SUCCESS != (ret = common::ObReadAtomicHelper::convert_commit_cid_to_prepare_cid(*first_read_param,
                                                                                     tmp_cid,tmp_prepare_cid)))
      {
        YYSYS_LOG(WARN,"fail to construct prepare column id!commit_cid=%ld,prepare_cid=%ld,ret=%d",
                  tmp_cid,tmp_prepare_cid,ret);
      }
      else
      {
        cid_val->set_int((int64_t)tmp_prepare_cid);
      }

      YYSYS_LOG(DEBUG,"read_atomic::debug,end get next tid and cid!next_expr_idx=%ld,ret=%d",next_expr_idx,ret);

    }//end of while
    YYSYS_LOG(DEBUG,"read_atomic::debug,finish store one prepare_expr=[%s],ret=%d",
              to_cstring(*prepare_output_expr),ret);
  }

  return ret;
}

int ObReadAtomicRpcScan::store_prepare_output_cid_(const uint64_t tid,const uint64_t cid)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (OB_INVALID_ID == cid
          || OB_ACTION_FLAG_COLUMN_ID == cid)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"special cid can not be prepare data cid!tid=%ld,cid=%ld,ret=%d",
              tid,cid,ret);
  }
  else if (OB_SUCCESS != (ret = prepare_output_cids_.add_column_desc(tid,cid)))
  {
    YYSYS_LOG(WARN,"fail to store prepare data cid!tid=%ld,cid=%ld,ret=%d",
              tid,cid,ret);
  }
  return ret;
}

/*@根据 read_round 设置 对应的rpc_scan 的output_column_,FIRST_READ_ROUND 对应 一轮读 和 （二轮读的第一轮）*/
int ObReadAtomicRpcScan::internal_add_output_column_(const ReadRoundState read_round)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = get_base_table_id();
  const common::ObRowkeyInfo &rowkey_info = get_rowkey_info();
  bool has_add_action_flag           = false;
  bool need_add_rowkey_columns_indep = false;
  bool need_add_commit_columns       = false;
  bool need_add_prepare_columns      = false;
  int64_t commit_columns_count       = orig_output_column_exprs_.count();
  const common::ObReadAtomicParam *param = NULL;
  ObRpcScan *rpc_scan_ptr = NULL;  /*根据read_round保存 一轮读 或者二轮读的rpc_scan的指针*/
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (FIRST_READ_ROUND == read_round)
  {
    param = get_first_read_atomic_param_();
    rpc_scan_ptr = dynamic_cast<ObRpcScan *>(this);
  }
  else if (SECOND_READ_ROUND == read_round)
  {
    param = get_second_read_atomic_param_();
    rpc_scan_ptr = &second_read_rpc_scan_;
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"invalid read round!round=%d,tid=%ld,ret=%d",
              read_round,table_id,ret);
  }

  if (OB_SUCCESS != ret)
  {
  }
  else if (!is_inited_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!table_id=%ld,ret=%d",table_id,ret);
  }
  //param of second read will be finished on get_next_row,
  //it's not completely valid!!it's OK!
  else if (NULL == param
           || NULL == rpc_scan_ptr
           || !param->is_valid())
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(ERROR,"read atomic param or rpc scan should not be NULL!"
              "param=%p,rpc_scan=%p,read_round=%d,table_id=%ld,ret=%d",
              param,rpc_scan_ptr,read_round,table_id,ret);
  }
  else
  {
    if (param->need_row_key_)
    {//means must have all rowkey columns
      need_add_rowkey_columns_indep = true;
      rpc_scan_ptr->ObRpcScan::set_rowkey_cell_count(param->rowkey_cell_count_);
    }

    if (param->need_commit_data_
        || param->need_exact_transver_data_
        || param->need_exact_data_mark_data_)
    {
      need_add_commit_columns = true;
    }

    if (param->need_prepare_data_)
    {
      need_add_prepare_columns = true;
    }

    //set rowkey cell count
    if (!need_add_rowkey_columns_indep)
    {
      if (need_add_commit_columns)
      {
        rpc_scan_ptr->ObRpcScan::set_rowkey_cell_count(orig_output_column_ids_.get_rowkey_cell_count());
      }
      else
      {
        rpc_scan_ptr->ObRpcScan::set_rowkey_cell_count(0);
      }
    }
  }

  //1:add rowkey columns
  if (OB_SUCCESS == ret
      && need_add_rowkey_columns_indep
      && OB_SUCCESS != (ret = add_rowkey_output_column_(read_round)))
  {
    YYSYS_LOG(WARN,"fail to add rowkey columns as output column!read_round=%d,table_id=%ld,ret=%d",
              read_round,table_id,ret);
  }

  //2:add output commit column
  if (OB_SUCCESS == ret && need_add_commit_columns)
  {
    for (int64_t i=0;OB_SUCCESS == ret && i<commit_columns_count;i++)
    {
      ObSqlExpression &expr = orig_output_column_exprs_.at(i);
      if (need_add_rowkey_columns_indep && rowkey_info.is_rowkey_column(expr.get_column_id()))
      {
        //we don't add output rowkey columns come from user!
      }
      else if (OB_SUCCESS != (ret = rpc_scan_ptr->ObRpcScan::add_output_column(expr)))
      {
        YYSYS_LOG(WARN, "fail to add output column to read ObRpcScan!read_round=%d,expr=[%s],ret=%d",
                  read_round,to_cstring(expr),ret);
      }

      if (OB_SUCCESS == ret && OB_ACTION_FLAG_COLUMN_ID == expr.get_column_id())
      {
        has_add_action_flag = true;
      }
    }

    if (OB_SUCCESS == ret
        && SECOND_READ_ROUND == read_round
        && !has_add_action_flag
        && second_read_hint_.read_method_ == ObSqlReadStrategy::USE_GET
        && !second_read_hint_.is_get_skip_empty_row_)
    {//if is_get_skip_empty_row_==true,ObRpcScan will add action column automaticaly when call ObRpcScan::init(),
     //but must add action output column when is_get_skip_empty_row_==false
     //we don't care about the first read rpc scan if has action flag cid,
     //becase the user of the ObReadAtomicRpcScan will guarantee it's right!
     //such as ObTransformer::gen_phy_static_data_scan func will add action flag itself!
      ObSqlExpression special_column;
      special_column.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
      if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, special_column)))
      {
        YYSYS_LOG(WARN, "fail to create action flag column expression. ret=%d,tid=%ld",ret,table_id);
      }
      else if (OB_SUCCESS != (ret = second_read_rpc_scan_.add_output_column(special_column)))
      {
        YYSYS_LOG(WARN, "fail to add action flag column to second read rpc scan!ret=%d,tid=%ld",
                  ret,table_id);
      }
    }
  }

  //3:add all extend output columns!
  if (OB_SUCCESS == ret
      && OB_SUCCESS != (ret = add_extend_output_column_(read_round)))
  {
    YYSYS_LOG(WARN,"fail to add extend output columns!tabld_id=%ld,ret=%d",
              table_id,ret);
  }

  //4:add all prepare output columns!
  if (OB_SUCCESS == ret
      && need_add_prepare_columns
      && OB_SUCCESS != (ret = add_prepare_output_column_(read_round)))
  {
    YYSYS_LOG(WARN,"fail to add prepare output columns!table_id=%ld,ret=%d",
              table_id,ret);
  }

  return ret;
}

int ObReadAtomicRpcScan::add_rowkey_output_column_(const ReadRoundState read_round)
{
  int ret = OB_SUCCESS;
// add by maosy [FIX_SUPPORT_READ_ATOMIC] 20170628:b
//  const uint64_t table_id = get_base_table_id();
  const uint64_t table_id = get_table_id();
//add by maosy e
  const common::ObRowkeyInfo &row_key_info = get_rowkey_info();
  const int64_t column_size = row_key_info.get_size();
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (FIRST_READ_ROUND != read_round
           && SECOND_READ_ROUND != read_round)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"invalid round!read_round=[%d],ret=%d",
              read_round,ret);
  }

  for (int64_t i=0;OB_SUCCESS == ret && i<column_size;i++)
  {
    ObRowkeyColumn column;
    ObSqlExpression rowkey_column;
    if (OB_SUCCESS != (ret = row_key_info.get_column(i,column)))
    {
      YYSYS_LOG(WARN,"fail to get rowkey column!idx=%ld,table_id=%ld,ret=%d",
                i,table_id,ret);
    }
    else
    {
      rowkey_column.set_tid_cid(table_id,column.column_id_);
      if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(table_id, column.column_id_, rowkey_column)))
      {
        YYSYS_LOG(WARN, "fail to create rowkey column expression. ret=%d,tid=%ld,cid=%ld",
                  ret,table_id,column.column_id_);
      }
      else if (FIRST_READ_ROUND == read_round && OB_SUCCESS != (ret = ObRpcScan::add_output_column(rowkey_column)))
      {
        YYSYS_LOG(WARN, "fail to add rowkey column to first read ObRpcScan. ret=%d,tid=%ld,cid=%ld",
                  ret,table_id,column.column_id_);
      }
      else if (SECOND_READ_ROUND == read_round && OB_SUCCESS != (ret = second_read_rpc_scan_.add_output_column(rowkey_column)))
      {
        YYSYS_LOG(WARN, "fail to add rowkey column to second read ObRpcScan. ret=%d,tid=%ld,cid=%ld",
                  ret,table_id,column.column_id_);
      }
    }
  }

  return ret;
}

int ObReadAtomicRpcScan::add_extend_output_column_(const ReadRoundState read_round)
{
  int ret = OB_SUCCESS;
// add by maosy [FIX_SUPPORT_READ_ATOMIC] 20170628:b
//  const uint64_t table_id = get_base_table_id();
  const uint64_t table_id = get_table_id();
// add by maosy e
  const common::ObReadAtomicParam *read_atomic_param = NULL;
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (FIRST_READ_ROUND == read_round)
  {
    read_atomic_param = get_first_read_atomic_param_();
  }
  else if (SECOND_READ_ROUND == read_round)
  {
    read_atomic_param = get_second_read_atomic_param_();
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"invalid read round!round=%d,tid=%ld,ret=%d",
              read_round,table_id,ret);
  }

  if (!is_inited_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN,"must init first!table_id=%ld,ret=%d",table_id,ret);
  }
  else if (NULL == read_atomic_param
           || !read_atomic_param->is_valid()
           || OB_INVALID_ID == table_id)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"not init!table_id=%ld,read_atomic_param=%p,ret=%d",
              table_id,read_atomic_param,ret);
  }
  else
  {
    if (read_atomic_param->need_last_commit_trans_ver_
        && OB_SUCCESS != (ret = add_special_output_column_(table_id,
                                                           read_atomic_param->last_commit_trans_ver_cid_,
                                                           read_round)))
    {
      YYSYS_LOG(WARN,"fail to add last commit trans ver column id!ret[%d],tid[%ld],cid[%lu]",
                ret,table_id,read_atomic_param->last_commit_trans_ver_cid_);
    }
    else if (read_atomic_param->need_last_prepare_trans_ver_
             && OB_SUCCESS != (ret = add_special_output_column_(table_id,
                                                                read_atomic_param->last_prepare_trans_ver_cid_,
                                                                read_round)))
    {
      YYSYS_LOG(WARN,"fail to add last prepare trans ver column id!ret[%d],tid[%ld],cid[%lu]",
                ret,table_id,read_atomic_param->last_prepare_trans_ver_cid_);
    }
    else if (read_atomic_param->need_trans_meta_data_
             && OB_SUCCESS != (ret = add_special_output_column_(table_id,
                                                                read_atomic_param->read_atomic_meta_cid_,
                                                                read_round)))
    {
      YYSYS_LOG(WARN,"fail to add read atomic meta column id!ret[%d],tid[%ld],cid[%lu]",
                ret,table_id,read_atomic_param->read_atomic_meta_cid_);
    }
    else if ((read_atomic_param->need_commit_data_
              || read_atomic_param->need_exact_data_mark_data_
              || read_atomic_param->need_exact_transver_data_)
             && OB_SUCCESS != (ret = add_special_output_column_(table_id,
                                                                read_atomic_param->commit_data_extendtype_cid_,
                                                                read_round)))
    {
      YYSYS_LOG(WARN,"fail to add commit data extendtype column id!ret[%d],tid[%ld],cid[%lu]",
                ret,table_id,read_atomic_param->commit_data_extendtype_cid_);
    }
    else if (read_atomic_param->need_prepare_data_
             && OB_SUCCESS != (ret = add_special_output_column_(table_id,
                                                                read_atomic_param->prepare_data_extendtype_cid_,
                                                                read_round)))
    {
      YYSYS_LOG(WARN,"fail to add prepare data extendtype column id!ret[%d],tid[%ld],cid[%lu]",
                ret,table_id,read_atomic_param->prepare_data_extendtype_cid_);
    }
    else if (read_atomic_param->need_data_mark_
             && OB_SUCCESS != (ret = add_special_output_column_(table_id,
                                                                read_atomic_param->major_ver_cid_,
                                                                read_round)))
    {
      YYSYS_LOG(WARN,"fail to add major version column id!ret[%d],tid[%ld],cid[%lu]",
                ret,table_id,read_atomic_param->major_ver_cid_);
    }
    else if (read_atomic_param->need_data_mark_
             && OB_SUCCESS != (ret = add_special_output_column_(table_id,
                                                                read_atomic_param->minor_ver_start_cid_,
                                                                read_round)))
    {
      YYSYS_LOG(WARN,"fail to add minor ver start column id!ret[%d],tid[%ld],cid[%lu]",
                ret,table_id,read_atomic_param->minor_ver_start_cid_);
    }
    else if (read_atomic_param->need_data_mark_
             && OB_SUCCESS != (ret = add_special_output_column_(table_id,
                                                                read_atomic_param->minor_ver_end_cid_,
                                                                read_round)))
    {
      YYSYS_LOG(WARN,"fail to add minor ver end column id!ret[%d],tid[%ld],cid[%lu]",
                ret,table_id,read_atomic_param->minor_ver_end_cid_);
    }
    else if (read_atomic_param->need_data_mark_
             && OB_SUCCESS != (ret = add_special_output_column_(table_id,
                                                                read_atomic_param->ups_paxos_id_cid_,
                                                                read_round)))
    {
      YYSYS_LOG(WARN,"fail to add ups paxos id column id!ret[%d],tid[%ld],cid[%lu]",
                ret,table_id,read_atomic_param->ups_paxos_id_cid_);
    }
    else if (read_atomic_param->need_data_mark_
             && OB_SUCCESS != (ret = add_special_output_column_(table_id,
                                                                read_atomic_param->data_store_type_cid_,
                                                                read_round)))
    {
      YYSYS_LOG(WARN,"fail to add data store type column id!ret[%d],tid[%ld],cid[%lu]",
                ret,table_id,read_atomic_param->data_store_type_cid_);
    }
    else if (read_atomic_param->need_prevcommit_trans_verset_)
    {
        uint64_t trans_verset_cids[common::ObReadAtomicHelper::MAX_TRANS_VERSET_SIZE];
        int64_t buf_size = common::ObReadAtomicHelper::MAX_TRANS_VERSET_SIZE;
        ret = common::ObReadAtomicHelper::gen_prevcommit_trans_verset_cids(*read_atomic_param,
                                                                    trans_verset_cids,
                                                                    buf_size);

        if (OB_SUCCESS == ret)
        {
          int64_t max_verset_size = read_atomic_param->max_prevcommit_trans_verset_count_;
          for (int64_t i=0; OB_SUCCESS == ret && i<max_verset_size && i<buf_size;i++)
          {
            ret = add_special_output_column_(table_id,trans_verset_cids[i],read_round);
          }
        }
    }
  }
  return ret;
}

int ObReadAtomicRpcScan::add_prepare_output_column_(const ReadRoundState read_round)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = prepare_output_column_exprs_.count();
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (FIRST_READ_ROUND != read_round
           && SECOND_READ_ROUND != read_round)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"invalid round!read_round=[%d],ret=%d",
              read_round,ret);
  }

  for (int64_t i=0;OB_SUCCESS == ret && i< column_count;i++)
  {
    ObSqlExpression &expr = prepare_output_column_exprs_.at(i);
    if (FIRST_READ_ROUND == read_round && OB_SUCCESS != (ret = ObRpcScan::add_output_column(expr)))
    {
      YYSYS_LOG(WARN, "fail to add output column to first read ObRpcScan!expr=[%s],ret=%d",
                to_cstring(expr),ret);
    }
    else if (SECOND_READ_ROUND == read_round && OB_SUCCESS != (ret = second_read_rpc_scan_.add_output_column(expr)))
    {
      YYSYS_LOG(WARN, "fail to add output column to second read ObRpcScan!expr=[%s],ret=%d",
                to_cstring(expr),ret);
    }
  }

  return ret;
}

int ObReadAtomicRpcScan::add_special_output_column_(const uint64_t tid,
                                                    const uint64_t cid,
                                                    const ReadRoundState read_round)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (FIRST_READ_ROUND != read_round
           && SECOND_READ_ROUND != read_round)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"invalid round!read_round=[%d],ret=%d",
              read_round,ret);
  }

  if (OB_SUCCESS == ret)
  {
    ObSqlExpression special_column;
    special_column.set_tid_cid(tid, cid);
    if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(tid, cid, special_column)))
    {
      YYSYS_LOG(WARN, "fail to create column expression. ret=%d,tid=%ld,cid=%ld",
                ret,tid,cid);
    }
    else if (FIRST_READ_ROUND == read_round && OB_SUCCESS != (ret = ObRpcScan::add_output_column(special_column)))
    {
      YYSYS_LOG(WARN, "fail to add special column to first read ObRpcScan. ret=%d,tid=%ld,cid=%ld",
                ret,tid,cid);
    }
    else if (SECOND_READ_ROUND == read_round && OB_SUCCESS != (ret = second_read_rpc_scan_.add_output_column(special_column)))
    {
      YYSYS_LOG(WARN, "fail to add special column to second read ObRpcScan. ret=%d,tid=%ld,cid=%ld",
                ret,tid,cid);
    }
  }
  return ret;
}

int ObReadAtomicRpcScan::add_second_read_rpc_filter_(bool &need_add_filter_again,
                                                     const bool is_first_time)
{
  int ret = OB_SUCCESS;
  ObSqlExpression *new_in_expr_filter = NULL;
  const common::ObReadAtomicParam *first_read_param = get_read_atomic_param();
  need_add_filter_again = false;

  YYSYS_LOG(DEBUG,"read_atomic::debug,begin add_second_read_rpc_filter_!is_first_time=%d",is_first_time);

  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  //check if need new in expr as filter!!!
  else if (ObSqlReadStrategy::USE_GET == second_read_hint_.read_method_
      && second_read_hint_.read_method_ != first_read_hint_.read_method_)
  {
    if (NULL == first_read_param
        || !first_read_param->need_row_key_)
    {
      ret = OB_INVALID_ARGUMENT;
      YYSYS_LOG(ERROR,"first read round must output rowkey!ret=%d,first_read_param=%p",
                ret,first_read_param);
    }
    else
    {
      new_in_expr_filter = ObSqlExpression::alloc();
      if (NULL == new_in_expr_filter)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        YYSYS_LOG(ERROR, "no memory to alloc for second read in expr filter!ret=%d",ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (NULL != new_in_expr_filter)
    {//construct new in expr as filter for second read!
      if (OB_SUCCESS != (ret = cons_new_in_expr_with_first_read_row_(*new_in_expr_filter,
                                                                     need_add_filter_again,
                                                                     is_first_time)))
      {
        YYSYS_LOG(WARN,"fail to cons new in expr!ret=%d,need_add_filter_again=%d,cur_is_first_time=%d",
                  ret,need_add_filter_again,is_first_time);
      }
      else if (is_second_read_filter_added_
          && OB_SUCCESS != (ret = second_read_rpc_scan_.reset_filter()))
      {
        YYSYS_LOG(WARN,"fail to reset filters of second read rpc scan!ret=%d",ret);
      }
      else if (OB_SUCCESS != (ret = second_read_rpc_scan_.add_filter(new_in_expr_filter)))
      {
        YYSYS_LOG(WARN,"fail to add second read rpc in expr filter!ret=%d,in_expr=[%s]",
                  ret,to_cstring(*new_in_expr_filter));
      }

      if (OB_SUCCESS == ret)
      {
        is_second_read_filter_added_ = true;
      }

      YYSYS_LOG(INFO,"read_atomic::debug,end add_second_read_rpc_filter_!"
                "use new in expr as second read filter!filter=[%s],ret=%d",
                to_cstring(*new_in_expr_filter),ret);
    }
    else if (!is_second_read_filter_added_)
    {//use old filters as filter for second read
      is_second_read_filter_added_ = true;
      const int64_t filter_count = orig_filters_.count();
      for (int64_t i=0;OB_SUCCESS == ret && i<filter_count;i++)
      {
        ObSqlExpression *new_filter = NULL;
        ObSqlExpression *filter = const_cast<ObSqlExpression *>(orig_filters_.at(i));
        if (NULL == filter)
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(ERROR,"filter should not be NULL!idx=%ld,ret=%d",i,ret);
        }
        else if (NULL == (new_filter = ObSqlExpression::alloc()))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          YYSYS_LOG(ERROR, "no memory to alloc for second read in expr filter!ret=%d",ret);
        }
        else
        {//deep copy!!!!
          *new_filter = *filter;
        }

        if (OB_SUCCESS != ret)
        {
        }
        else if (OB_SUCCESS != (ret = second_read_rpc_scan_.add_filter(new_filter)))
        {
          YYSYS_LOG(WARN,"fail to add second read rpc scan filter!filter[%s],ret=%d",
                    to_cstring(*new_filter),ret);
        }

        if (OB_SUCCESS != ret && NULL != new_filter)
        {
          ObSqlExpression::free(new_filter);
          new_filter = NULL;
        }

        if (NULL != new_filter)
          YYSYS_LOG(DEBUG,"read_atomic::debug,end add_second_read_rpc_filter_!"
                    "use old fiter expr as second read filter!filter=[%s],ret=%d",
                  to_cstring(*new_filter),ret);
      }
    }
  }

  if (OB_SUCCESS != ret && NULL != new_in_expr_filter)
  {
    ObSqlExpression::free(new_in_expr_filter);
    new_in_expr_filter = NULL;
  }

  return ret;
}

int ObReadAtomicRpcScan::cons_new_in_expr_with_first_read_row_(ObSqlExpression &new_in_expr_filter,
                                                               bool &need_add_filter_again,
                                                               const bool is_first_time)
{
  int ret = OB_SUCCESS;
  ObRowDesc row_desc;
  int64_t row_num           = 0;
  int64_t rowkey_cell_size  = 0;
  bool cur_is_first_time    = is_first_time;
  bool is_left_param_end    = false;
  bool is_row_empty         = false;
  (void)is_row_empty;
// add by maosy [FIX_SUPPORT_READ_ATOMIC] 20170628:b
//  const uint64_t table_id   = get_base_table_id();
  const uint64_t table_id   = get_table_id();
// add by maosy e
  const common::ObRowkeyInfo &rowkey_info = get_rowkey_info();
  need_add_filter_again     = false;

  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (first_read_row_cache_.is_empty())
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(ERROR,"first read row cache must has data!ret=%d",ret);
  }

  while(OB_SUCCESS == ret
        && row_num < MAX_IN_EXPR_LENGTH
        && rowkey_cell_size < MAX_ROWKEY_CELL_SIZE)
  {
    is_row_empty = false;
    if (cur_is_first_time && OB_SUCCESS != (ret = first_read_row_cache_.get_next_row(first_read_cache_row_)))
    {
      if (OB_ITER_END != ret)
      {
        YYSYS_LOG(WARN,"get row from first round cache fail!ret=%d",ret);
      }
    }

    YYSYS_LOG(INFO,"read_atomic::debug,the %ldth first_cache_row[%s],cur_is_first_time=%d,ret=%d",
              row_num,to_cstring(first_read_cache_row_),cur_is_first_time,ret);

    cur_is_first_time = false;

    //ignor empty row!
    if (OB_SUCCESS == ret)
    {
//      if (OB_SUCCESS != (ret = first_read_cache_row_.get_is_row_empty(is_row_empty)))
//      {
//        YYSYS_LOG(WARN,"fail to get is row empty!ret=%d",ret);
//      }
//      else if (is_row_empty)
//      {
//        YYSYS_LOG(INFO,"read_atomic::debug,first_read_cache_row_[%s] is empty!",
//                  to_cstring(first_read_cache_row_));
//        continue;
//      }
    }

    //finish left param!
    if (OB_SUCCESS == ret && !is_left_param_end)
    {
      const ObRowDesc *orig_row_desc = first_read_cache_row_.get_row_desc();
      if (NULL == orig_row_desc)
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR,"orig row must have row desc!ret=%d",ret);
      }
      else
      {
        row_desc.assign(*orig_row_desc);
        if (OB_SUCCESS != (ret = finish_in_expr_left_param_(row_desc,
                                                            rowkey_info,
                                                            table_id,
                                                            new_in_expr_filter)))
        {
          YYSYS_LOG(WARN,"fail to finish left param of in expr!ret=%d",ret);
        }
      }
      is_left_param_end = true;
    }//end of finish left param

    //add mid value of in expr
    if (OB_SUCCESS == ret
        && OB_SUCCESS != (ret = add_in_expr_mid_param_(first_read_cache_row_,
                                                       row_desc,
                                                       rowkey_info,
                                                       table_id,
                                                       new_in_expr_filter,
                                                       rowkey_cell_size)))
    {
      YYSYS_LOG(WARN,"fail to add mid value of in expr!ret=%d",ret);
    }

    if (OB_SUCCESS == ret)
    {//finish one row!
      row_num++;
    }

    if (OB_SUCCESS == ret && OB_SUCCESS != (ret = first_read_row_cache_.get_next_row(first_read_cache_row_)))
    {
      if (OB_ITER_END != ret)
      {
        YYSYS_LOG(WARN,"get row from first round cache fail!ret=%d",ret);
      }
    }

  }//end of while

  if (OB_SUCCESS == ret
      && (row_num >= MAX_IN_EXPR_LENGTH
          || rowkey_cell_size >= MAX_ROWKEY_CELL_SIZE))
  {//means need construct in expr filter agin!
    need_add_filter_again = true;
  }


  if ((OB_SUCCESS == ret || OB_ITER_END == ret) && row_num <= 0)
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR,"first round cache has no data can be used!ret=%d,row_num=%ld",
              ret,row_num);
  }
  else if (OB_ITER_END == ret)
  {//means all rows of first read row cache are used!
    ret = OB_SUCCESS;
  }

  if (OB_SUCCESS == ret
      && OB_SUCCESS != (ret = finish_in_expr_end_param_(row_num,
                                                        new_in_expr_filter)))
  {
    YYSYS_LOG(WARN,"fail to finish right param of in expr!ret=%d",ret);
  }

  return ret;
}

int ObReadAtomicRpcScan::finish_in_expr_left_param_(const ObRowDesc &row_desc,
                                                    const common::ObRowkeyInfo &rowkey_info,
                                                    const uint64_t table_id,
                                                   ObSqlExpression &in_expr)
{
  int ret = OB_SUCCESS;
  ExprItem expr_item;
  const int64_t  rowkey_column_num  = rowkey_info.get_size();

  if (OB_SUCCESS == ret)
  {
     expr_item.type_ = T_REF_COLUMN;
     expr_item.value_.cell_.tid = table_id;
     uint64_t tid = OB_INVALID_ID;

     for (int i = 0; OB_SUCCESS == ret && i < row_desc.get_column_num(); ++i)
     {
       if (OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, expr_item.value_.cell_.cid)))
       {
         YYSYS_LOG(WARN,"get cid failed!tid[%ld],cid[%ld],ret[%d]",
                   tid,expr_item.value_.cell_.cid,ret);
       }
       else if (rowkey_info.is_rowkey_column(expr_item.value_.cell_.cid))//if rowkey column, add it into row_filter
       {
         if (OB_SUCCESS != (ret = in_expr.add_expr_item(expr_item)))
         {
           YYSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
         }
       }
     }// end for

     if (OB_SUCCESS == ret)
     {
       expr_item.type_ = T_OP_ROW;
       expr_item.value_.int_ = rowkey_column_num;
       if (OB_SUCCESS != (ret = in_expr.add_expr_item(expr_item)))
       {
         YYSYS_LOG(WARN,"failed to add expr item, err=%d", ret);
       }
     }

     if (OB_SUCCESS == ret)
     {
       expr_item.type_ = T_OP_LEFT_PARAM_END;
       // a in (a,b,c) => 1 Dim;
       expr_item.value_.int_ = 2;
       if (OB_SUCCESS != (ret = in_expr.add_expr_item(expr_item)))
       {
         YYSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
       }
     }

  }//end of add filter

  return ret;
}


int ObReadAtomicRpcScan::add_in_expr_mid_param_(const common::ObRow &row,
                                                const ObRowDesc &row_desc,
                                                const common::ObRowkeyInfo &rowkey_info,
                                                const uint64_t table_id,
                                                ObSqlExpression &in_expr,
                                                int64_t &rowkey_cell_size)
{
  int ret = OB_SUCCESS;
  ExprItem  expr_item;
  const ObObj*  cell = NULL;
  const int64_t  rowkey_column_num  = rowkey_info.get_size();
  const int64_t column_num = row_desc.get_column_num();

  for (int64_t i=0; OB_SUCCESS == ret && i < column_num;i++)
  {
    uint64_t tid = OB_INVALID_ID;
    uint64_t cid = OB_INVALID_ID;
    ObConstRawExpr     col_val;

    if (OB_UNLIKELY(OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, cid))))
    {
      YYSYS_LOG(WARN,"get cid failed!tid[%ld],cid[%ld],ret[%d]",tid,cid,ret);
    }
    else if (rowkey_info.is_rowkey_column(cid))//if rowkey column, add it into row_filter
    {

      if (OB_SUCCESS != (ret = row.get_cell(table_id,cid,cell)))
      {
          YYSYS_LOG(WARN,"get rowkey column cell failed!table_id[%ld],key_cid[%ld],ret[%d]",
                   table_id,cid,ret);
      }
      else if (NULL == cell)
      {
         ret = OB_ERR_UNEXPECTED;
         YYSYS_LOG(ERROR,"cell should not be NULL!ret=%d",ret);
      }
      else if (OB_SUCCESS != (ret = col_val.set_value_and_type(*cell)))
      {
          YYSYS_LOG(WARN, "failed to set column value,table_id=%ld, err=%d", table_id,ret);
      }
      else
      {
          rowkey_cell_size += cell->get_serialize_size();

          //FIXME:READ_ATOMIC should use in_expr.add_expr_obj(*cell)??
          if ((ret = col_val.fill_sql_expression(in_expr)) != OB_SUCCESS)
          {
            YYSYS_LOG(WARN,"fail to add in expr obj!table_id=%ld,cell=[%s],ret=%d",
                      table_id,to_cstring(*cell),ret);
          }
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    expr_item.type_ = T_OP_ROW;
    expr_item.value_.int_ = rowkey_column_num;
    if (OB_SUCCESS != (ret = in_expr.add_expr_item(expr_item)))
    {
      YYSYS_LOG(WARN, "Failed to add expr item, err=%d", ret);
    }
  }

  return ret;
}


int ObReadAtomicRpcScan::finish_in_expr_end_param_(const int64_t row_num,
                                                  ObSqlExpression &in_expr)
{
  int ret = OB_SUCCESS;
  ExprItem  expr_item;
  expr_item.type_ = T_OP_ROW;
  expr_item.value_.int_ = row_num;
  ExprItem expr_in;
  expr_in.type_ = T_OP_IN;
  expr_in.value_.int_ = 2;
  if (OB_SUCCESS != (ret = in_expr.add_expr_item(expr_item)))
  {
      YYSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = in_expr.add_expr_item(expr_in)))
  {
      YYSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = in_expr.add_expr_item_end()))
  {
      YYSYS_LOG(WARN,"Failed to add expr item end, err=%d", ret);
  }
  return ret;
}

/*@berif 构建row_desc*/
int ObReadAtomicRpcScan::cons_final_row_desc_()
{
  int ret = OB_SUCCESS;
  ObSqlExpression expr;
  const int64_t output_col_num = orig_output_column_ids_.get_column_num();
  const uint64_t table_id = get_base_table_id();
  const ObSqlReadParam *sql_param = get_sql_read_param();
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (NULL == sql_param
          || OB_INVALID_ID == table_id)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!sql_param=%p,table_id=%ld,ret=%d",sql_param,table_id,ret);
  }

  if (OB_SUCCESS == ret)
  {
    final_row_desc_.reset();
    final_row_desc_.set_rowkey_cell_count(orig_output_column_ids_.get_rowkey_cell_count());
    const ObProject &project = sql_param->get_project();

    if (!is_output_column_(OB_INVALID_ID,OB_ACTION_FLAG_COLUMN_ID))
    {//try add action flag column!
      for(int64_t i=0; OB_SUCCESS == ret&& i<project.get_output_columns().count();i++)
      {
        if(OB_SUCCESS != (ret = project.get_output_columns().at(i, expr)))
        {
          YYSYS_LOG(WARN, "get expression from out columns fail:ret[%d] i[%ld]", ret, i);
        }
        else if (OB_INVALID_ID == expr.get_table_id()
                 && OB_ACTION_FLAG_COLUMN_ID == expr.get_column_id())
        {
          if (OB_SUCCESS != (final_row_desc_.add_column_desc(OB_INVALID_ID,
                                                             OB_ACTION_FLAG_COLUMN_ID)))
          {
            YYSYS_LOG(WARN,"add column desc fail!ret[%d],idx[%ld],tid[%ld],cid[%ld]",
                      ret,i,expr.get_table_id(),expr.get_column_id());
          }
          break;
        }
      }
    }

    for (int64_t i=0;OB_SUCCESS == ret && i<output_col_num;i++)
    {
      uint64_t tid = OB_INVALID_ID;
      uint64_t cid = OB_INVALID_ID;
      if (OB_SUCCESS != (ret = orig_output_column_ids_.get_tid_cid(i,tid,cid)))
      {
        YYSYS_LOG(WARN,"fail to get tid[%ld] cid[%ld] from output columns!idx=%ld,ret=%d",
                  tid,cid,i,ret);
      }
      else if (OB_SUCCESS != (ret = final_row_desc_.add_column_desc(tid,cid)))
      {
        YYSYS_LOG(WARN,"add column desc fail!ret[%d],idx[%ld],tid[%ld],cid[%ld]",
                  ret,i,tid,cid);
      }
    }
  }

  return ret;
}

bool ObReadAtomicRpcScan::is_prepare_data_column_(const uint64_t tid, const uint64_t cid)
{
  bool ret = false;
  if (OB_INVALID_INDEX != prepare_output_cids_.get_idx(tid,cid))
  {
    ret = true;
  }
  return ret;
}

bool ObReadAtomicRpcScan::is_output_column_(const uint64_t tid, const uint64_t cid)
{
  bool ret = false;
  if (OB_INVALID_INDEX != orig_output_column_ids_.get_idx(tid,cid))
  {
    ret = true;
  }
  return ret;
}

/*@berif  readatomic 读取一行数据*/
int ObReadAtomicRpcScan::internal_get_next_row_(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const bool is_first_time = !is_first_read_finish_;
  const uint64_t table_id = get_base_table_id();
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (!is_first_read_finish_ && OB_SUCCESS != (ret = execute_first_round_read_()))
  {
    YYSYS_LOG(WARN,"fail to execute first round read!table_id=%ld,ret=%d",
              table_id,ret);
  }
  else
  {
    is_first_read_finish_ = true;
    if (OB_SUCCESS != (ret = finish_final_row_(is_first_time)))
    {
      if (OB_ITER_END != ret)
      {
        YYSYS_LOG(WARN,"fail to finish final row!ret=%d",ret);
      }
    }
    row = &final_row_;
  }

  if (OB_SUCCESS != ret && OB_ITER_END != ret)
  {
      dump_data_mark_state_(YYSYS_LOG_LEVEL_WARN);
      dump_transver_state_(YYSYS_LOG_LEVEL_WARN);
  }
  return ret;
}

/*执行一轮读  读取所有行的数据，并缓存到row_store中.如果需要二轮读，设置二轮读data_mark 和transversion_set*/
int ObReadAtomicRpcScan::execute_first_round_read_()
{
  int ret = OB_SUCCESS;
  FetchTransverStatMeth fetch_meth = NO_FETCH_TRANSVER;
  const common::ObRow *orig_row = NULL;
  const bool need_second_read   = need_second_read_();
  const uint64_t table_id       = get_base_table_id();
  const common::ObReadAtomicParam *first_read_param = get_first_read_atomic_param_();
  common::ObReadAtomicParam *second_read_param      = get_second_read_atomic_param_();

  if (!is_inited_
      || !is_with_read_atomic_
      || !is_first_read_rpc_opened_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must first init then open!is_inited=%d,is_with_read_atomic_=%d,"
              "is_first_read_rpc_opened_=%d,tid=%ld,ret=%d",
              is_inited_,is_with_read_atomic_,is_first_read_rpc_opened_,table_id,ret);
  }
  else if (NULL == first_read_param
           || (need_second_read && NULL == second_read_param))
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must have read atomic param!first_param=%p,second_param=%p,need_second_read=%d,ret=%d",
              first_read_param,
              second_read_param,
              need_second_read,ret);
  }
  else if (OB_SUCCESS != (ret = clear_with_read_atomic_()))
  {
    YYSYS_LOG(WARN,"fail to reset read atomic!ret=%d",ret);
  }
  else
  {
    //first read round ! 读取一轮读中所有的数据
    while(OB_SUCCESS == ret)
    {
      orig_row = NULL;
      bool has_set_first_read_row_desc = false;
      common::ObReadAtomicDataMark data_mark;
      const ObRowStore::StoredRow *stored_row = NULL;
      const common::ObTransVersion *last_commit_transver  = NULL;
      const common::ObTransVersion *last_prepare_transver = NULL;
      const common::ObTransVersion *prevcommit_verset_array[common::ObReadAtomicHelper::MAX_TRANS_VERSET_SIZE];
      int64_t total_ver_count = common::ObReadAtomicHelper::MAX_TRANS_VERSET_SIZE;

      if (OB_SUCCESS != (ret = ObRpcScan::get_next_row(orig_row)))/*获得一行数据*/
      {
        if (OB_ITER_END != ret)
        {
          YYSYS_LOG(WARN,"fail to get orig row from first read ObRpcScan!table_id=%ld,ret=%d",table_id,ret);
        }
      }
      else if (NULL == orig_row)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR,"should not get NULL!table_id=%ld,ret=%d",table_id,ret);
      }
        /*将数据缓存起来*/
      else if (OB_SUCCESS != (ret = first_read_row_cache_.add_row(*orig_row,stored_row)))
      {
        YYSYS_LOG(WARN,"fail to cache the orig_row[%s],table_id=%ld,ret=%d",
                  to_cstring(*orig_row),table_id,ret);
      }
        /*获得last_commit_transver  last_prepare_transver、  prevcommit_version集合、data_mark */
      else if (OB_SUCCESS != (ret = common::ObReadAtomicHelper::get_extend_vals_(orig_row,*first_read_param,
                                                                                &last_commit_transver,
                                                                                &last_prepare_transver,
                                                                                prevcommit_verset_array,
                                                                                &total_ver_count,
                                                                                NULL,NULL,&data_mark)))
      {
        YYSYS_LOG(WARN,"fail to get extend vals!table_id=%ld,ret=%d",table_id,ret);
      }
      else if (data_mark.is_valid()
               && OB_SUCCESS != (ret = set_data_mark_state_(data_mark,FIRST_READ_ROUND)))
      {
        YYSYS_LOG(WARN,"fail to store first read data mark!data_mark=[%s],ret=%d",
                  to_cstring(data_mark),ret);
      }
      else if(data_mark.is_valid()&&data_mark.is_paxos_id_useful()
              && OB_SUCCESS!=(ret=set_paxos_data_mark_and_check_safe(data_mark,FIRST_READ_ROUND)))
      {
        YYSYS_LOG(WARN,"fail to store first set data mark!data_mark=[%s],ret=%d",
                  to_cstring(data_mark),ret);
      }
      else if (NULL != last_commit_transver
               && last_commit_transver->is_valid()
               && OB_SUCCESS != (ret = set_transver_state_(*last_commit_transver,common::ObTransVersion::COMMIT_STATE)))
      {
        YYSYS_LOG(WARN,"fail to store commit transver[%s] state!table_id=%ld,ret=%d",
                  to_cstring(*last_commit_transver),table_id,ret);
      }
      else if (NULL != last_prepare_transver
               && last_prepare_transver->is_valid()
               && OB_SUCCESS != (ret = set_transver_state_(*last_prepare_transver,common::ObTransVersion::PREPARE_STATE)))
      {
        YYSYS_LOG(WARN,"fail to store prepare transver[%s] state!table_id=%ld,ret=%d",
                  to_cstring(*last_prepare_transver),table_id,ret);
      }
      else
      {
        if (!has_set_first_read_row_desc && NULL != orig_row->get_row_desc())
        {
          first_read_cache_row_.set_row_desc(*(orig_row->get_row_desc()));
          has_set_first_read_row_desc = true;
        }

        for (int32_t i=0;OB_SUCCESS == ret && i<total_ver_count;i++)
        {
          if (NULL != prevcommit_verset_array[i]
              && prevcommit_verset_array[i]->is_valid()
              && OB_SUCCESS != (ret = set_transver_state_(*(prevcommit_verset_array[i]),common::ObTransVersion::COMMIT_STATE)))
          {
            YYSYS_LOG(WARN,"fail to store the %dth prevcommit transver[%s],table_id=%ld,ret=%d",
                      i,to_cstring(*(prevcommit_verset_array[i])),table_id,ret);
          }
        }
      }

      //READ_ATOMIC read_atomic::debug
      YYSYS_LOG(DEBUG,"read_atomic::debug,MS_final_data_mark=[%s],ret=%d",to_cstring(data_mark),ret);
      if (NULL != orig_row)
        YYSYS_LOG(DEBUG,"read_atomic::debug,orig_row=[%s],ret=%d",to_cstring(*orig_row),ret);
      if (NULL != last_commit_transver)
        YYSYS_LOG(DEBUG,"read_atomic::debug,last_commit_transver=[%s]",to_cstring(*last_commit_transver));
      if (NULL != last_prepare_transver)
        YYSYS_LOG(DEBUG,"read_atomic::debug,last_prepare_transver=[%s]",to_cstring(*last_prepare_transver));

    }//end of while

    if (OB_ITER_END == ret)
    {
      //first read round is finish succ!
      if (OB_SUCCESS != (ret = ObRpcScan::close()))
      {
        YYSYS_LOG(WARN,"fail to close first read rpc!ret=%d",ret);
      }
      else
      {
        is_first_read_rpc_opened_ = false;
      }
    }


    //read_atomic::debug
    YYSYS_LOG(DEBUG,"read_atomic::debug,the first read round data mark state is:");
    dump_data_mark_state_(YYSYS_LOG_LEVEL_DEBUG);

    if (OB_SUCCESS != ret)
    {
    }
    else if (need_fetch_transver_state(fetch_meth)
             &&need_fetch_sys_table()   //uncertainty 
             && OB_SUCCESS != (ret = fetch_transver_final_stat_(fetch_meth)))
    {
      YYSYS_LOG(WARN,"fail to fetch transvers final state!ret=%d,fetch_meth=%d",ret,fetch_meth);
    }
      /*填写 两轮度的 data_mark 和 transversion_set*/
    else if (need_second_read)
    {
      if (second_read_param->need_exact_data_mark_data_
          && OB_SUCCESS != (ret = fill_exact_data_mark_set_(*second_read_param)))
      {
        YYSYS_LOG(WARN,"fail to fill the exact data mark!ret=%d",ret);
      }
      else if (second_read_param->need_exact_transver_data_
          && OB_SUCCESS != (ret = fill_exact_commit_transver_set_(*second_read_param)))
      {
        YYSYS_LOG(WARN,"fail to fill the exact commit transver!ret=%d",ret);
      }
      else if (!(second_read_param->is_valid()))
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR,"invalid second read atomic param!param=[%s],ret=%d",
                  to_cstring(*second_read_param),ret);
      }
    }

  }

  return ret;
}

int ObReadAtomicRpcScan::fetch_transver_final_stat_(const FetchTransverStatMeth fetch_meth)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else
  {
    switch(fetch_meth)
    {
      case NO_FETCH_TRANSVER:
      {
        //it's ok
        break;
      }
      case FETCH_FROM_CACHE:
      {
        if (OB_SUCCESS != (ret = fetch_transver_final_stat_from_cache_()))
        {
          YYSYS_LOG(WARN,"fail to fetch transver stat from cache!ret=%d",ret);
        }
        break;
      }
      case FETCH_FROM_SYS_TABLE:
      {
        if (OB_SUCCESS != (ret = fetch_transver_final_stat_from_systable_()))
        {
          YYSYS_LOG(WARN,"fail to fetch transver stat from sys table!ret=%d",ret);
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        YYSYS_LOG(ERROR,"unknow fetch transver stat meth[%d]!ret=%d",fetch_meth,ret);
        break;
    }
  }

  YYSYS_LOG(DEBUG,"read_atomic::debug,finish fetch transver stat!fetch_meth=%d,ret=%d",fetch_meth,ret);

  return ret;
}

int ObReadAtomicRpcScan::fetch_transver_final_stat_from_cache_()
{
  int ret = OB_SUCCESS;
  //TODO:READ_ATOMIC
  //need finish asyn cache transver state!
  ret = OB_NOT_SUPPORTED;
  YYSYS_LOG(ERROR,"not finish fetch transver stat from cache yet!ret=%d",ret);
  return ret;
}

int ObReadAtomicRpcScan::fetch_transver_final_stat_from_systable_()
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(DEBUG,"fetch_transver_final_stat_from_systable_");
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (is_transver_stat_map_created_ && transver_state_map_.size() > 0)
  {
    //will look up the hash map!!!
    bool is_first_time = true;
    TransVersionMap::const_iterator begin = transver_state_map_.begin();
    TransVersionMap::const_iterator end   = transver_state_map_.end();
    TransVersionMap::const_iterator iter  = begin;
    TransVersionMap::const_iterator prev_iter = iter;

    //read_atomic::debug
    int tmp_count=0;

    while(OB_SUCCESS == ret && iter != end)
    {
      prev_iter = iter;
      if (common::ObTransVersion::COMMIT_STATE == iter->second)
      {
        iter++;
        continue;
      }
      //iter++ will happened on add_fetch_transver_stat_rpc_filter_
      else if (OB_SUCCESS != (ret = add_fetch_transver_stat_rpc_filter_(iter,end,is_first_time)))
      {
        YYSYS_LOG(WARN,"fail to add filter for fetch transver state rpc!ret=%d",ret);
      }
      else if (prev_iter == iter && iter != end)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR,"iter must move forward!ret=%d",ret);
      }
      else
      {
        is_first_time = false;
        fetch_transver_stat_rpc_.set_phy_plan(get_phy_plan());
        if (OB_SUCCESS != (ret = fetch_transver_stat_rpc_.open()))
        {
          YYSYS_LOG(WARN,"fail to open fetch transver state rpc!ret=%d",ret);
        }
      }

      YYSYS_LOG(DEBUG,"read_atomic::debug,the %dth open fetch transver rpc!ret=%d",
                tmp_count++,ret);

      //get row from rpc!
      while(OB_SUCCESS == ret)
      {
        bool is_row_empty = false;
        int64_t rowkey_cell_idx = 0;
        bool  is_commit = false;
        int64_t tmp_column_num = 0;
        const ObRowDesc* tmp_row_desc = NULL;
        char tmp_ups_ip[OB_MAX_SERVER_ADDR_SIZE];
        memset(tmp_ups_ip,'\0',sizeof(tmp_ups_ip));
        int64_t  port = 0;
        int64_t  descriptor    = 0;
        int64_t  start_time_us = 0;
        common::ObTransVersion transver;
        const common::ObRow *transver_stat_row = NULL;

        ret = fetch_transver_stat_rpc_.get_next_row(transver_stat_row);

        if (NULL != transver_stat_row)
          YYSYS_LOG(DEBUG,"read_atomic::debug,the orig_transver_stat_row=[%s],ret=%d",
                    to_cstring(*transver_stat_row),ret);

        if (OB_SUCCESS != ret && OB_ITER_END != ret)
        {
          YYSYS_LOG(WARN,"fail to get row from sys table!ret=%d",ret);
        }
        else if (OB_ITER_END == ret)
        {
          break;
        }
        else if (NULL == transver_stat_row)
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR,"trans version state row is NULL!ret=%d",ret);
        }
        else if (OB_SUCCESS != (ret = transver_stat_row->get_is_row_empty(is_row_empty)))
        {
          YYSYS_LOG(WARN,"fail to check if is empty row!row=[%s],ret=%d",
                    to_cstring(*transver_stat_row),ret);
        }
        else  if (OB_SUCCESS == ret && is_row_empty)
        {
          YYSYS_LOG(DEBUG,"read_atomic::debug,transver stat row=[%s] is empty!continue!",
                    to_cstring(*transver_stat_row));
          continue;
        }
        else if (NULL == (tmp_row_desc = transver_stat_row->get_row_desc()))
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR,"fail to get row desc!ret=%d",ret);
        }
        else
        {
          tmp_column_num = tmp_row_desc->get_column_num();
        }

        //FIXME:READ_ATOMIC it's ugly!hard code!!
        for (int64_t i=0;OB_SUCCESS == ret && i <tmp_column_num;i++)
        {
          uint64_t tid=OB_INVALID_ID;
          uint64_t cid=OB_INVALID_ID;
          const ObObj *pcell = NULL;
          if (OB_SUCCESS != (ret = tmp_row_desc->get_tid_cid(i,tid,cid)))
          {
            YYSYS_LOG(WARN,"fail to get %ldth tid[%ld] and cid[%ld]!ret=%d",
                      i,tid,cid,ret);
          }
          else if (OB_SUCCESS != (ret = transver_stat_row->get_cell(tid,cid,pcell)))
          {
            YYSYS_LOG(WARN,"fail to get %ldth rowkey cell!ret=%d",i,ret);
          }
          else if (!sys_table_rowkey_info_.is_rowkey_column(cid))
          {
            if (ObBoolType == pcell->get_type()
                && OB_SUCCESS != (ret = pcell->get_bool(is_commit)))
            {
              YYSYS_LOG(WARN,"fail to get commit state!ret=%d",ret);
            }
          }
          else
          {
            switch(rowkey_cell_idx)
            {
              case 0:
                 if (ObIntType != pcell->get_type())
                 {
                   ret = OB_ERROR;
                   YYSYS_LOG(ERROR,"start_time_us cell must be int!ret=%d",ret);
                 }
                 else if (OB_SUCCESS != (ret = pcell->get_int(start_time_us)))
                 {
                   YYSYS_LOG(WARN,"fail to get start time us!ret=%d",ret);
                 }
                 break;
              case 1:
                if (ObIntType != pcell->get_type())
                {
                  ret = OB_ERROR;
                  YYSYS_LOG(ERROR,"descriptor_ cell must be int!ret=%d",ret);
                }
                else if (OB_SUCCESS != (ret = pcell->get_int(descriptor)))
                {
                  YYSYS_LOG(WARN,"fail to get descriptor_!ret=%d",ret);
                }
                 break;
              case 2:
                if (ObVarcharType != pcell->get_type()
                    || pcell->get_data_length() >= OB_MAX_SERVER_ADDR_SIZE)
                {
                  ret = OB_ERROR;
                  YYSYS_LOG(ERROR,"ups_ip cell must be varchar!type=%d,ret=%d",
                            pcell->get_type(),ret);
                }
                else
                {
                  memcpy(tmp_ups_ip,(const char *)(pcell->get_data_ptr()),pcell->get_data_length());
                }
                 break;
              case 3:
                if (ObIntType != pcell->get_type())
                {
                  ret = OB_ERROR;
                  YYSYS_LOG(ERROR,"port cell must be int!ret=%d",ret);
                }
                else if (OB_SUCCESS != (ret = pcell->get_int(port)))
                {
                  YYSYS_LOG(WARN,"fail to get port!ret=%d",ret);
                }
                 break;
              default:
                 break;
            }
            rowkey_cell_idx++;
          }
        }//end of for

        if (OB_SUCCESS == ret && is_commit)
        {
          transver.start_time_us_ = start_time_us;
          transver.descriptor_    = static_cast<uint32_t>(descriptor);
          transver.set_trans_dist_type(common::ObTransVersion::DISTRIBUTED_TRANS);
          if (transver.ups_.set_ipv4_addr(tmp_ups_ip,static_cast<int32_t>(port)) != true)
          {
            ret = OB_ERROR;
            YYSYS_LOG(ERROR,"fail to set ups_ip!ups_ip=%p,port=%ld,transver=[%s],ret=%d",
                      tmp_ups_ip,port,to_cstring(transver),ret);
          }
          else if (!transver.is_valid())
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"commit trans version must be valid!ret=%d,trans_ver=[%s]",
                      ret,to_cstring(transver));
          }
          else if (OB_SUCCESS != (ret = set_transver_state_(transver,common::ObTransVersion::COMMIT_STATE)))
          {
            YYSYS_LOG(WARN,"fail to store commit transver[%s]!ret=%d",
                      to_cstring(transver),ret);
          }
        }

        YYSYS_LOG(DEBUG,"read_atomic::debug,finish get one transver state row!transver=[%s],is_commit=%d,ret=%d",
                  to_cstring(transver),is_commit,ret);

      }//end of second while,finish get one row!!!

      //must close rpc!!!!!!
      if (OB_ITER_END == ret && OB_SUCCESS != (ret = fetch_transver_stat_rpc_.close()))
      {
        YYSYS_LOG(WARN,"fail to close fetch transver stat rpc!ret=%d",ret);
      }

    }//end of first while
    //no mather what happend!must close rpc!
    fetch_transver_stat_rpc_.close();
  }

  return ret;
}

int ObReadAtomicRpcScan::fill_exact_data_mark_set_(common::ObReadAtomicParam &read_atomic_param)
{
  int ret = OB_SUCCESS;
  read_atomic_param.reset_exact_datamark_array();
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (data_mark_state_map_.created() && data_mark_state_map_.size() > 0)
  {
    DataMarkStatMap::const_iterator begin = data_mark_state_map_.begin();
    DataMarkStatMap::const_iterator end   = data_mark_state_map_.end();
    DataMarkStatMap::const_iterator iter  = begin;
    while(OB_SUCCESS == ret && iter != end)
    {
      if (!(iter->second.is_valid()))
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR,"invalid data mark stat!ret=%d,data_mark[%s],stat=[%s]",
                  ret,to_cstring(iter->first),to_cstring(iter->second));
      }
      else if (OB_SUCCESS != (ret = read_atomic_param.add_exact_data_mark(iter->first)))
      {
        YYSYS_LOG(WARN,"fail to add exact data mark!ret=%d,data_mark[%s],stat=[%s]",
                  ret,to_cstring(iter->first),to_cstring(iter->second));
      }
      iter++;
    }

    //FIXME:READ_ATOMIC it's not necessary???
    if (OB_SUCCESS == ret
        && OB_SUCCESS != (ret = read_atomic_param.sort_exact_data_mark_array()))
    {
      YYSYS_LOG(WARN,"fail to sort exact data mark array!ret=%d",ret);
    }
  }

  return ret;
}


/*@berif  填充二轮读算法中需要的  transver 集合信息
* @param  read_atomic_param [out] 填充了 commit transver 集合
* */
int ObReadAtomicRpcScan::fill_exact_commit_transver_set_(common::ObReadAtomicParam &read_atomic_param)
{
  int ret = OB_SUCCESS;
  read_atomic_param.reset_exact_transver_map();
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (is_transver_stat_map_created_ && transver_state_map_.size() > 0)
  {
    TransVersionMap::const_iterator begin = transver_state_map_.begin();
    TransVersionMap::const_iterator end   = transver_state_map_.end();
    TransVersionMap::const_iterator iter  = begin;
    while(OB_SUCCESS == ret && iter != end)
    {
      if (common::ObTransVersion::COMMIT_STATE == iter->second
          && OB_SUCCESS != (ret = read_atomic_param.add_exact_transver(iter->first)))
      {
        YYSYS_LOG(WARN,"fail to store exact commit transversion[%s],ret=%d!",
                  to_cstring(iter->first),ret);
      }
      iter++;
    }
  }
  return ret;
}

int ObReadAtomicRpcScan::add_fetch_transver_stat_rpc_filter_(TransVersionMap::const_iterator &iter,
                                                             TransVersionMap::const_iterator end,
                                                             const bool is_first_time)
{
  int ret = OB_SUCCESS;
  ObRow tmp_row;
  ObString ups_ip;
  char buf[OB_MAX_SERVER_ADDR_SIZE];
  const int64_t column_num = sys_table_row_desc_.get_column_num();
  int64_t rowkey_cell_idx = 0;
  uint64_t sys_table_id = SYS_TABLE_TRANSVER_STAT_TID;
  ObSqlExpression *new_in_expr_filter = NULL;
  int64_t row_num = 0;
  int64_t rowkey_cell_size = 0;

  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (iter == end)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"invalid iterator!ret=%d",ret);
  }
  else if (!is_first_time && OB_SUCCESS != (ret = fetch_transver_stat_rpc_.reset_filter()))
  {
    YYSYS_LOG(WARN,"fail to reset filters of fetch transver state rpc!ret=%d",ret);
  }
  else if (NULL == (new_in_expr_filter = ObSqlExpression::alloc()))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(ERROR, "no memory to alloc filter!ret=%d",ret);
  }
  else if (OB_SUCCESS != (ret = finish_in_expr_left_param_(sys_table_row_desc_,
                                                           sys_table_rowkey_info_,
                                                           sys_table_id,
                                                           *new_in_expr_filter)))
  {
    YYSYS_LOG(WARN,"fail to finish left param of in expr!ret=%d",ret);
  }

  if (NULL != new_in_expr_filter)
      YYSYS_LOG(DEBUG,"read_atomic::debug,the begin transver filter=[%s],is_first_time=%d,ret=%d",
                to_cstring(*new_in_expr_filter),is_first_time,ret);

  //read_atomic::debug
  int tmp_cout=0;

  while(OB_SUCCESS == ret
        && iter != end
        && row_num < MAX_IN_EXPR_LENGTH
        && rowkey_cell_size < MAX_ROWKEY_CELL_SIZE)
  {

    YYSYS_LOG(DEBUG,"read_atomic::debug,look up the %dth iter!row_num=%ld,rowkey_cell_size=%ld",
              tmp_cout++,row_num,rowkey_cell_size);

    YYSYS_LOG(DEBUG,"read_atomic::debug,orig_transver=[%s],commit_state=%d,ret=%d",
              to_cstring(iter->first),iter->second,ret);

    rowkey_cell_idx = 0;
    ups_ip.reset();
    tmp_row.clear();
    tmp_row.set_row_desc(sys_table_row_desc_);
    memset(buf, 0 , sizeof(buf));
    if (iter->first.ups_.ip_to_string(buf, sizeof(buf)) != true)
    {
      ret = OB_CONVERT_ERROR;
      YYSYS_LOG(ERROR, "server ip is invalid, ret=%d", ret);
    }
    else
    {
      ups_ip.assign_ptr(buf,static_cast<int32_t>(strlen(buf)));

      //FIXME:READ_ATOMIC it's ugly!hard code!!
      //fill cur trans version into a tmp row!
      for (int64_t i=0;OB_SUCCESS == ret && i<column_num;i++)
      {
        uint64_t tid = OB_INVALID_ID;
        uint64_t cid = OB_INVALID_ID;
        ObObj    tmp_cell;
        if (OB_SUCCESS != (ret = sys_table_row_desc_.get_tid_cid(i,tid,cid)))
        {
          YYSYS_LOG(WARN,"fail to get tid[%ld] and cid[%ld],ret=%d",
                    tid,cid,ret);
        }
        else if (sys_table_rowkey_info_.is_rowkey_column(cid))
        {
          switch(rowkey_cell_idx)
          {
            case 0:
               tmp_cell.set_int(iter->first.start_time_us_);
               break;
            case 1:
               tmp_cell.set_int(static_cast<int64_t>(iter->first.descriptor_));
               break;
            case 2:
               tmp_cell.set_varchar(ups_ip);
               break;
            case 3:
               tmp_cell.set_int(iter->first.ups_.get_port());
               break;
            default:
               tmp_cell.set_null();
               break;

          }
          rowkey_cell_idx++;

          if (OB_SUCCESS != (ret = tmp_row.set_cell(tid,cid,tmp_cell)))
          {
            YYSYS_LOG(WARN,"fail to set %ldth rowkey cell[%s]!ret=%d",
                      i,to_cstring(tmp_cell),ret);
          }
        }
      }
    }

    YYSYS_LOG(DEBUG,"read_atomic::debug,the tmp transversion row=[%s],ret=%d",
              to_cstring(tmp_row),ret);

    if (OB_SUCCESS == ret
        && OB_SUCCESS !=(ret = add_in_expr_mid_param_(tmp_row,
                                                      sys_table_row_desc_,
                                                      sys_table_rowkey_info_,
                                                      sys_table_id,
                                                      *new_in_expr_filter,
                                                      rowkey_cell_size)))
    {
      YYSYS_LOG(WARN,"fail to add mid param of in expr!tmp_row=[%s],ret=%d",
                to_cstring(tmp_row),ret);
    }

    if (OB_SUCCESS == ret)
    {
      row_num++;
    }

    //it's important!!!!!
    iter++;
  }//end of while

  if (OB_SUCCESS == ret)
  {
    if (row_num <= 0)
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(ERROR,"must has least one row!ret=%d,row_num=%ld",ret,row_num);
    }
    else if (OB_SUCCESS != (ret = finish_in_expr_end_param_(row_num,*new_in_expr_filter)))
    {
      YYSYS_LOG(WARN,"fail to finish end param of in expr!ret=%d",ret);
    }
    else if (OB_SUCCESS != (ret = fetch_transver_stat_rpc_.add_filter(new_in_expr_filter)))
    {
      YYSYS_LOG(WARN,"fail to add filter for fetch transver stat rpc!ret=%d",ret);
    }
  }

  if (NULL != new_in_expr_filter)
    YYSYS_LOG(DEBUG,"read_atomic::debug,the final transver filter=[%s],ret=%d",
              to_cstring(*new_in_expr_filter),ret);

  if (OB_SUCCESS != ret && NULL != new_in_expr_filter)
  {
    ObSqlExpression::free(new_in_expr_filter);
  }

  return ret;
}

/*@berif 返回最终的row，如果需要二轮读需要进行二轮读*/
int ObReadAtomicRpcScan::finish_final_row_(const bool is_first_time)
{
  int ret = OB_SUCCESS;
  const common::ObRow *src_row = NULL;/*用于保存 一轮读算法 和二轮读算法的结果*/
  const ObReadAtomicParam *src_param = NULL;
  bool cur_is_first_time   = is_first_time;
  bool is_final_row_exist  = false;
  const bool need_second_read = need_second_read_();
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }

  while(OB_SUCCESS == ret)
  {
    is_final_row_exist = false;
    src_row            = NULL;
    src_param          = NULL;
    if (need_second_read)/*进行二轮读，获得一行数据到src_row*/
    {//src_row come from second read
      src_param = get_second_read_atomic_param_();
      if (OB_SUCCESS != (ret = execute_second_round_read_(cur_is_first_time,src_row)))
      {
        if (OB_ITER_END != ret)
        {
          YYSYS_LOG(WARN,"fail to get src row=%p from second read!ret=%d",src_row,ret);
        }
      }
    }
    else
    {//src_row comes from first read
      if (OB_SUCCESS != (ret = first_read_row_cache_.get_next_row(first_read_cache_row_)))
      {
        if (OB_ITER_END != ret)
        {
          YYSYS_LOG(WARN,"get row from first round cache fail!ret=%d",ret);
        }
      }
      else
      {
        src_row   = &first_read_cache_row_;
        src_param = get_read_atomic_param();
      }

    }
    cur_is_first_time = false;

    if (OB_SUCCESS == ret)
    {
      if (NULL == src_row || NULL == src_param)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR,"src_row[%p] or src_param[%p] should not be NULL!ret=%d",src_row,src_param,ret);
      }
      else if (OB_SUCCESS != (ret = conver_to_final_row_(*src_row,
                                                         src_param,
                                                         is_final_row_exist)))
      {
        YYSYS_LOG(WARN,"fail to reconstruct final row!src_row=[%s],final_row=[%s],ret=%d",
                  to_cstring(*src_row),
                  to_cstring(final_row_),ret);
      }
      else if (is_final_row_exist
               && OB_SUCCESS != (ret = filter_final_row_(is_final_row_exist)))
      {
        YYSYS_LOG(WARN,"fail to filter final row!ret=%d",ret);
      }
    }


    if (OB_SUCCESS == ret)
    {
      if (!is_final_row_exist && (first_read_hint_.read_method_ == ObSqlReadStrategy::USE_SCAN
                                  || first_read_hint_.is_get_skip_empty_row_))
      {
        YYSYS_LOG(INFO,"read_atomic::debug,src_row=[%s] will ignore!ret=%d",
                  to_cstring(*src_row),ret);
        continue;
      }
      else
      {
//        if (!is_final_row_exist)
//        {
//          final_row_.reset(false, ObRow::DEFAULT_NULL);
//        }
        break;
      }
    }
  }//end while
  return ret;
}

/*@berif 执行二轮读算法的第二轮读，获得一行数据到second_read_row*/
int ObReadAtomicRpcScan::execute_second_round_read_(const bool is_first_time,
                                                    const common::ObRow *&second_read_row)
{
  int ret = OB_SUCCESS;
  uint64_t table_id         = get_base_table_id();
  bool need_open_again     = false;
  bool cur_is_first_time   = is_first_time;
  ObReadAtomicDataMark data_mark;
  const ObReadAtomicParam *second_param = NULL;
  second_read_row = NULL;
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (first_read_row_cache_.is_empty())
  {//second round read execute based on the data from first read!
    ret = OB_ITER_END;
  }
  else if (NULL == (second_param = get_second_read_atomic_param_()))
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"second read atomic param can't be NULL!ret=%d",ret);
  }

  while(OB_SUCCESS == ret && NULL == second_read_row)
  {
    YYSYS_LOG(DEBUG,"read_atomic::debug,begin second read!src_row=%p,"
              "cur_is_first_time=%d,is_second_read_rpc_opend_=%d,"
              "second_read_need_filter_again_=%d",
              second_read_row,cur_is_first_time,
              is_second_read_rpc_opend_,
              second_read_need_filter_again_);

    need_open_again = (cur_is_first_time || second_read_need_filter_again_);
    if (is_second_read_rpc_opend_)
    {
      if (OB_SUCCESS != (ret = second_read_rpc_scan_.get_next_row(second_read_row)))
      {
        if (OB_ITER_END != ret)
        {
          YYSYS_LOG(WARN,"fail to get next row from second read rpc scan!ret=%d",ret);
        }
      }
      else if (NULL == second_read_row)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR,"fail to get row from second read rpc scan!ret=%d",ret);
      }
      else
      {//OK,get one row!
//        break;
      }
    }
    else if (need_open_again
             && OB_SUCCESS != (ret = add_second_read_rpc_filter_(second_read_need_filter_again_,
                                                                 cur_is_first_time)))
    {
      YYSYS_LOG(WARN,"fail to add filter for second read rpc!ret=%d",ret);
    }
    else if (need_open_again)
    {
      second_read_rpc_scan_.set_phy_plan(get_phy_plan());
      if (OB_SUCCESS != (ret = second_read_rpc_scan_.open()))
      {
        YYSYS_LOG(WARN,"fail to open second read rpc!ret=%d",ret);
      }
      else
      {
        is_second_read_rpc_opend_ = true;
        cur_is_first_time  = false;
        continue;
      }
    }
    else if (OB_SUCCESS == ret && !need_open_again)
    {
      ret = OB_ITER_END;
    }
    cur_is_first_time  = false;

    if (OB_ITER_END == ret
        && is_second_read_rpc_opend_)
    {
      if (OB_SUCCESS != (ret =  second_read_rpc_scan_.close()))
      {
        YYSYS_LOG(WARN,"fail to close second read rpc scan!ret=%d",ret);
      }
      else
      {//means we get all data from second read,it's iter end!
        is_second_read_rpc_opend_ = false;
        ret = OB_ITER_END;
      }
    }


    YYSYS_LOG(DEBUG,"read_atomic::debug,end second read!src_row=%p,"
              "cur_is_first_time=%d,is_second_read_rpc_opend_=%d,second_read_need_filter_again_=%d,ret=%d",
              second_read_row,cur_is_first_time,
              is_second_read_rpc_opend_,
              second_read_need_filter_again_,ret);
    if (NULL != second_read_row)
      YYSYS_LOG(DEBUG,"read_atomic::debug,second read row=[%s]",to_cstring(*second_read_row));

    if (OB_ITER_END == ret && second_read_need_filter_again_)
    {
      ret = OB_SUCCESS;
      continue;
    }
  }//end of while

  if (OB_SUCCESS == ret && second_param->need_data_mark_)
  {//means we get a correct second read row!
    if (OB_SUCCESS != (ret = common::ObReadAtomicHelper::get_extend_vals_(second_read_row,
                                                                          *second_param,
                                                                          NULL,NULL,NULL,NULL,
                                                                          NULL,NULL,&data_mark)))
    {
      YYSYS_LOG(WARN,"fail to get extend vals from second read row!table_id=%ld,ret=%d",table_id,ret);
    }
    else if (data_mark.is_valid()
             && OB_SUCCESS != (ret = set_data_mark_state_(data_mark,SECOND_READ_ROUND)))
    {
      YYSYS_LOG(WARN,"fail to store data mark[%s] from second read!ret=%d",
                to_cstring(data_mark),ret);
    }
    else if(data_mark.is_valid()&&data_mark.is_paxos_id_useful()
            && OB_SUCCESS!=(ret=set_paxos_data_mark_and_check_safe(data_mark,SECOND_READ_ROUND)))
    {
      YYSYS_LOG(WARN,"fail to store SECOND_READ_ROUND set data mark!data_mark=[%s],ret=%d",
                to_cstring(data_mark),ret);
    }
  }

  if (OB_ITER_END == ret)
  {
    //FIXME:READ_ATOMIC
    // we need correct check way!
//    if (!is_consistent_data_checked_with_data_mark())
//    {
//      ret = OB_ERROR;
//      YYSYS_LOG(USER_ERROR,"the read atomic data is not safe!please retry!");
//      YYSYS_LOG(ERROR,"the read atomic data is not safe!ret=%d",ret);
//      dump_data_mark_state_(YYSYS_LOG_LEVEL_ERROR);
//      dump_transver_state_(YYSYS_LOG_LEVEL_ERROR);
//    }

    //read_atomic::debug
    YYSYS_LOG(DEBUG,"read_atomic::debug,the seond read final data mark state is:");
    dump_data_mark_state_(YYSYS_LOG_LEVEL_DEBUG);
  }

  return ret;
}

/* @berif 将得到的一行数据  转化为最终的数据（final_row_）
 * @param   src_row [in]  保存了一轮或者二轮读的最终完整的数据*/
int ObReadAtomicRpcScan::conver_to_final_row_(const common::ObRow &src_row,
                                              const common::ObReadAtomicParam *param,
                                              bool &is_final_row_exist)
{
  int ret = OB_SUCCESS;
  const common::ObTransVersion *last_prepare_transver= NULL;
  bool use_prepare_data  = false;
  uint64_t table_id      = get_base_table_id();
  is_final_row_exist     = false;

  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }
  else if (NULL == param
          || OB_INVALID_ID == table_id)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"invalid argument!param=%p,table_id=%ld,ret=%d",
              param,table_id,ret);
  }
  else if (OB_SUCCESS != (ret = common::ObReadAtomicHelper::get_extend_vals_(&src_row,
                                                                             *param,NULL,
                                                                             &last_prepare_transver)))
  {
    YYSYS_LOG(WARN,"fail to get last_prepare_transver from src_row=[%s],table_id=%ld,ret=%d",
              to_cstring(src_row),table_id,ret);
  }
  else if (NULL != last_prepare_transver
           && param->need_prepare_data_
           && last_prepare_transver->is_valid()
           && is_committed_transver_(*last_prepare_transver))
  {//final row data will comme from the prepare data!
     use_prepare_data = true;
  }


  //1:use prepare data as final row!
  if (OB_SUCCESS == ret && use_prepare_data)
  {
    if (OB_SUCCESS != (ret = common::ObReadAtomicHelper::split_commit_preapre_row_(&src_row,
                                                                                   *param,
                                                                                   NULL,NULL,
                                                                                   &final_row_,
                                                                                   &is_final_row_exist)))
    {
      YYSYS_LOG(WARN,"fail to get prepare row!table_id=%ld,ret=%d",table_id,ret);
    }
  }
  else  if (OB_SUCCESS == ret)
  {//use commit data as final row!
    if (OB_SUCCESS != (ret = common::ObReadAtomicHelper::split_commit_preapre_row_(&src_row,
                                                                                   *param,
                                                                                   &final_row_,
                                                                                   &is_final_row_exist,
                                                                                   NULL,NULL)))
    {
      YYSYS_LOG(WARN,"fail to get commit row!table_id=%ld,ret=%d",table_id,ret);
    }
  }

  //tmp test
  YYSYS_LOG(DEBUG,"read_atomic::debug,finish covert one final row!"
            "table_id=%ld,orig row = [%s],use_prepare_dat=%d,"
            "final_row=[%s],is_final_row_exist=%d,ret=%d",
            table_id,to_cstring(src_row),use_prepare_data,
            to_cstring(final_row_),is_final_row_exist,ret);
  return ret;
}

int ObReadAtomicRpcScan::filter_final_row_(bool &is_final_row_output)
{
  int ret = OB_SUCCESS;
  const ObObj *result = NULL;
  const int64_t filter_count = orig_filters_.count();
  is_final_row_output = true;
  if (!is_inited_ || !is_with_read_atomic_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"must init first!ret=%d,is_init=%d,is_with_read_atomic=%d",
              ret,is_inited_,is_with_read_atomic_);
  }

  for (int64_t i=0;OB_SUCCESS == ret && i<filter_count;i++)
  {
    ObSqlExpression *filter = const_cast<ObSqlExpression *>(orig_filters_.at(i));
    if (NULL == filter)
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(ERROR,"filter should not be NULL!idx=%ld,ret=%d",i,ret);
    }
    else if (OB_SUCCESS != (ret = filter->calc(final_row_, result)))
    {
      YYSYS_LOG(WARN, "failed to calc expression, err=%d", ret);
    }
    else if (!result->is_true())
    {
      is_final_row_output = false;
      break;
    }
  }
  return ret;
}


/*@berif 设置 trans_ver 在transver_state_map_中 对应的状态 为ver_state
* @note 注意 state 只能从无到有  从prepare 到commit*/
int ObReadAtomicRpcScan::set_transver_state_(const common::ObTransVersion &trans_ver,
                                             const common::ObTransVersion::TransVerState ver_state)
{
  int ret = OB_SUCCESS;
  common::ObTransVersion::TransVerState old_state = common::ObTransVersion::INVALID_STATE;
  int insert_ret = OB_SUCCESS;
  int get_ret    = OB_SUCCESS;

  if (!is_transver_stat_map_created_
      && OB_SUCCESS != (ret = transver_state_map_.create(HASH_BUCKET_NUM)))
  {
    YYSYS_LOG(WARN,"fail to create transver_state_map_!ret=%d",ret);
  }
  else
  {
    is_transver_stat_map_created_ = true;
  }

  if (OB_SUCCESS == ret && trans_ver.is_valid())
  {//we only store the valid trans version
    get_ret = transver_state_map_.get(trans_ver,old_state);
    if (hash::HASH_NOT_EXIST == get_ret || hash::HASH_EXIST == get_ret)
    {
      if (hash::HASH_NOT_EXIST == get_ret)
      {
        insert_ret = transver_state_map_.set(trans_ver,ver_state);
      }
      else if (common::ObTransVersion::PREPARE_STATE == old_state
               && common::ObTransVersion::COMMIT_STATE == ver_state)
      {
        insert_ret = transver_state_map_.set(trans_ver,ver_state,1);
      }
      else
      {
        insert_ret = OB_SUCCESS;
      }

      if (OB_SUCCESS != insert_ret
          && hash::HASH_OVERWRITE_SUCC != insert_ret
          && hash::HASH_INSERT_SUCC != insert_ret)
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR,"fail to set transver[%s] state!old_state=%d,new_state=%d,hash_set_ret=%d,ret=%d",
                  to_cstring(trans_ver),old_state,ver_state,insert_ret,ret);
      }
    }
    else
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR,"fail to get transver[%s] state!old_state=%d,new_state=%d,hash_get_ret=%d,ret=%d",
                to_cstring(trans_ver),old_state,ver_state,get_ret,ret);
    }

    YYSYS_LOG(DEBUG,"read_atomic::debug,finish store transver state!"
              "ver=[%s],old_state=%d,new_state=%d,"
              "hash_get=%d,hash_set=%d,ret=%d",
              to_cstring(trans_ver),old_state,ver_state,
              get_ret,insert_ret,ret);
  }
  return ret;
}

bool ObReadAtomicRpcScan::is_committed_transver_(const common::ObTransVersion &trans_ver)
{
  bool bret = false;
  common::ObTransVersion::TransVerState ver_state = common::ObTransVersion::INVALID_STATE;
  int get_ret    = OB_SUCCESS;
  if (trans_ver.is_valid() && is_transver_stat_map_created_)
  {
    get_ret = transver_state_map_.get(trans_ver,ver_state);
    if (hash::HASH_EXIST == get_ret && common::ObTransVersion::COMMIT_STATE == ver_state)
    {
      bret = true;
    }
  }
  else
  {
    YYSYS_LOG(WARN,"invalid trans version[%s],is_transver_stat_map_created_=%d!",
              to_cstring(trans_ver),is_transver_stat_map_created_);
  }
  return bret;
}

int ObReadAtomicRpcScan::set_paxos_data_mark_and_check_safe(const common::ObReadAtomicDataMark &data_mark, const ReadRoundState read_round)
{
  int ret = OB_SUCCESS;
  ObReadAtomicDataMark old_data_mark;
  int insert_ret = OB_SUCCESS;
  int get_ret    = OB_SUCCESS;

  if (FIRST_READ_ROUND != read_round
      && SECOND_READ_ROUND != read_round)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"invalid read round=%d,ret=%d",read_round,ret);
  }
  else if(!data_mark.is_valid()||!data_mark.is_paxos_id_useful())
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"paxos is not useful or data mark is invalid,ret=%d",ret);
  }
  else if (!paxos_data_mark_map_.created()
           && OB_SUCCESS != (ret = paxos_data_mark_map_.create(HASH_BUCKET_NUM)))
  {
    YYSYS_LOG(WARN,"fail to create paxos_data_mark_map_!ret=%d",ret);
  }

  if (OB_SUCCESS == ret && data_mark.is_valid())
  {//we only store the valid data_mark
    get_ret = paxos_data_mark_map_.get(data_mark.ups_paxos_id_,old_data_mark);
    if (hash::HASH_NOT_EXIST == get_ret || hash::HASH_EXIST == get_ret)
    {
      if(hash::HASH_NOT_EXIST == get_ret)
      {
        insert_ret =paxos_data_mark_map_.set(data_mark.ups_paxos_id_,data_mark);
        if(hash::HASH_INSERT_SUCC!=insert_ret)
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR,"fail to get data_mark[%s] state!"
            "input_read_round=%d,hash_get_ret=%d,ret=%d",
                    to_cstring(data_mark),read_round,get_ret,ret);
        }
      }
      else
      {
        if(!old_data_mark.is_valid()||!old_data_mark.is_paxos_id_useful())
        {
          ret = OB_INVALID_ARGUMENT;
          YYSYS_LOG(ERROR,"paxos is not useful or old_data_mark is invalid,ret=%d",ret);
        }
        else if(old_data_mark!= data_mark)
        {
          ret =OB_DIFFERENT_DATA_MARK;
          YYSYS_LOG(WARN,"old data_mark is different from new data_mark,old_datamark:%s,new_datamark:%s,input_read_round=%d,ret=%d",
                    to_cstring(old_data_mark),to_cstring(data_mark),read_round,ret);
        }
      }
    }
    else
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR,"fail to get data_mark[%s] state!"
        "input_read_round=%d,hash_get_ret=%d,ret=%d",
                to_cstring(data_mark),read_round,get_ret,ret);
    }

  }
  return ret;
}

/*？？？？？？？？？*/
int ObReadAtomicRpcScan::set_data_mark_state_(const common::ObReadAtomicDataMark &data_mark,
                                              const ReadRoundState read_round)
{
  int ret = OB_SUCCESS;
  DataMarkState store_data_mark_stat;
  int insert_ret = OB_SUCCESS;
  int get_ret    = OB_SUCCESS;
  int overwrite_key = 0;

  if (FIRST_READ_ROUND != read_round
      && SECOND_READ_ROUND != read_round)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR,"invalid read round=%d,ret=%d",read_round,ret);
  }
  else if (!data_mark_state_map_.created()
          && OB_SUCCESS != (ret = data_mark_state_map_.create(HASH_BUCKET_NUM)))
  {
    YYSYS_LOG(WARN,"fail to create data_mark_state_map_!ret=%d",ret);
  }

  if (OB_SUCCESS == ret && data_mark.is_valid())
  {//we only store the valid data_mark
    get_ret = data_mark_state_map_.get(data_mark,store_data_mark_stat);
    if (hash::HASH_NOT_EXIST == get_ret || hash::HASH_EXIST == get_ret)
    {
      overwrite_key = 1;//will overwrite
      if (hash::HASH_NOT_EXIST == get_ret)
      {
        overwrite_key = 0;//won't overwrite!
      }

      if (FIRST_READ_ROUND == read_round)
      {
        store_data_mark_stat.first_read_round_row_num_++;
      }
      else
      {
        store_data_mark_stat.second_read_round_row_num_++;
      }

      if (OB_SUCCESS == ret)
      {
        if (store_data_mark_stat.is_valid())
        {
           insert_ret = data_mark_state_map_.set(data_mark,
                                                 store_data_mark_stat,
                                                 overwrite_key);
        }
        else
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(ERROR,"invalid new state!input_read_round=%d,"
                    "store_data_mark_stat=[%s],"
                    "overwrite_key=%d,ret=%d",
                    read_round,
                    to_cstring(store_data_mark_stat),
                    overwrite_key,ret);
        }
      }

      if (OB_SUCCESS == ret
          && OB_SUCCESS != insert_ret
          && hash::HASH_OVERWRITE_SUCC != insert_ret
          && hash::HASH_INSERT_SUCC != insert_ret)
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR,"fail to set data_mark[%s] state!"
                  "input_read_round=%d,store_data_mark_stat=[%s],"
                  "hash_set_ret=%d,overwrite_key=%d,ret=%d",
                  to_cstring(data_mark),read_round,
                  to_cstring(store_data_mark_stat),
                  insert_ret,overwrite_key,ret);
      }
    }
    else
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR,"fail to get data_mark[%s] state!"
                "input_read_round=%d,store_data_mark_stat=[%s],"
                "hash_get_ret=%d,ret=%d",
                to_cstring(data_mark),read_round,
                to_cstring(store_data_mark_stat),
                get_ret,ret);
    }

  }

  YYSYS_LOG(DEBUG,"read_atomic::debug,finish store one data mark!"
            "data_mark=[%s],input_read_round=%d,"
            "store_data_mark_stat=[%s],"
            "overwrite_key=%d,hash_get_ret=%d,"
            "hash_set_ret=%d,ret=%d",
            to_cstring(data_mark),read_round,
            to_cstring(store_data_mark_stat),
            overwrite_key,get_ret,insert_ret,ret);
  return ret;
}

int ObReadAtomicRpcScan::clear_with_read_atomic_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObReadAtomicParam *first_param  = get_first_read_atomic_param_();
  ObReadAtomicParam *second_param = get_second_read_atomic_param_();
  if (NULL != first_param)
  {
    first_param->reset_exact_datamark_array();
    first_param->reset_exact_transver_map();
  }
  if (NULL != second_param)
  {
    second_param->reset_exact_datamark_array();
    second_param->reset_exact_transver_map();
  }
  is_first_read_finish_ = false;
  first_read_row_cache_.clear();
  first_read_cache_row_.clear();
  is_transver_stat_map_created_ = false;
  second_read_need_filter_again_ = false;
  transver_state_map_.destroy();
  paxos_data_mark_map_.destroy();
  data_mark_state_map_.destroy();
  is_second_read_rpc_opend_ = false;
  if (OB_SUCCESS != (ret = second_read_rpc_scan_.close()))
  {
    YYSYS_LOG(WARN,"fail to close second read rpc scan!tmp_ret=%d",ret);
  }
  //no matter what happend!must close!
  if (OB_SUCCESS != (tmp_ret = fetch_transver_stat_rpc_.close()))
  {
    YYSYS_LOG(WARN,"fail to close fetch transver stat rpc!ret=%d",tmp_ret);
    ret = (OB_SUCCESS != ret) ? ret:tmp_ret;
  }

  return ret;
}


/*@berif  是否需要进行二轮算法，READ_ATOMIC_STRONG需要进行二轮读，READ_ATOMIC_WEAK 一轮读*/
bool ObReadAtomicRpcScan::need_second_read_() const
{
  bool bret = false;
  if (is_with_read_atomic_)
  {
    switch(first_read_hint_.read_atomic_level_)
    {
      case NO_READ_ATOMIC_LEVEL:
      case READ_ATOMIC_WEAK:
      {
        //just need first read!
        bret = false;
        break;
      }
      case READ_ATOMIC_STRONG:
      {
        bret = true;
        break;
      }
      default:
        YYSYS_LOG(WARN,"unknow read atomic level[%d]!",first_read_hint_.read_atomic_level_);
        bret = false;
        break;
    }
  }
  return bret;
}


/*@berif  遇到未决版本  是否要查系统表*/    //uncertainty 
bool ObReadAtomicRpcScan::need_fetch_sys_table() const
{
  bool bret = true;
  if (is_with_read_atomic_)
  {
    switch(first_read_hint_.read_atomic_level_)
    {
      case NO_READ_ATOMIC_LEVEL:
      {
        bret = false;
        break;
      }
      case READ_ATOMIC_WEAK:
      {
        bret = !is_first_read_in_time_limit();
        break;
      }
      case READ_ATOMIC_STRONG:
      {
        bret = true;
        break;
      }
      default:
        bret= false;
        YYSYS_LOG(WARN,"unknow read atomic level[%d]!",first_read_hint_.read_atomic_level_);
    }
  }
  return bret;
}


/*@berif  判断READ_ATOMIC_WEAK  first_rpc_scan 是否在 time_limit中
 *@return  false 不在time_limit中，需要查系统表
           true   在time_limit中，不需要查系统表
 */
bool ObReadAtomicRpcScan::is_first_read_in_time_limit() const
{
  bool bret = false;
  if (!is_with_read_atomic_)
  {
    bret= true;
  }
  else if (first_read_hint_.read_atomic_level_==READ_ATOMIC_WEAK)
  {
      int64_t first_rpc_scan_elapse = yysys::CTimeUtil::getTime() - first_rpc_scan_start_time_;
      YYSYS_LOG(DEBUG, "read_atomic_weak_time_limit_:%ld,first_rpc_scan_elapse:%ld",
                first_read_hint_.read_atomic_weak_time_limit_, first_rpc_scan_elapse/1000);
      if(first_read_hint_.read_atomic_weak_time_limit_ < first_rpc_scan_elapse / 1000)
      {
        bret = false;
      }
      else //在time_limit中，不需要查系统表
      {
        bret = true;
      }
  }
  else/*READ_ATOMIC_STRONG  一定要查系统表*/
  {
    bret = false;
  }
  return bret;
}
/*READ_ATOMIC_WEAK 和 READ_ATOMIC_STRONG 需要 获得 trans_version*/
bool ObReadAtomicRpcScan::need_fetch_transver_state(FetchTransverStatMeth &fetch_meth) const
{
  bool bret = false;
  fetch_meth = NO_FETCH_TRANSVER;

  if (is_with_read_atomic_)
  {
    switch(first_read_hint_.read_atomic_level_)
    {
      case NO_READ_ATOMIC_LEVEL:
      {
        bret = false;
        fetch_meth = NO_FETCH_TRANSVER;
        break;
      }
      case READ_ATOMIC_WEAK:
      {
        //TODO:READ_ATOMIC
        //we should use the cached transver state,rather than fetch from sys table!
        bret = true;
        fetch_meth = FETCH_FROM_SYS_TABLE;
        break;
      }
      case READ_ATOMIC_STRONG:
      {
        bret = true;
        fetch_meth = FETCH_FROM_SYS_TABLE;
        break;
      }
      default:
        YYSYS_LOG(WARN,"unknow read atomic level[%d]!",first_read_hint_.read_atomic_level_);
        bret = false;
        fetch_meth = NO_FETCH_TRANSVER;
        break;
    }
  }
  return bret;
}

bool ObReadAtomicRpcScan::is_consistent_data_checked_with_data_mark()
{
  bool bret = true;
  const ObReadAtomicParam *first_param  = get_first_read_atomic_param_();
  const ObReadAtomicParam *second_param = get_second_read_atomic_param_();
  if (is_with_read_atomic_
      && data_mark_state_map_.created()
      && data_mark_state_map_.size() > 0
      && NULL != first_param
      && first_param->need_data_mark_
      && NULL != second_param
      && second_param->need_data_mark_)
  {
    DataMarkStatMap::const_iterator begin = data_mark_state_map_.begin();
    DataMarkStatMap::const_iterator end   = data_mark_state_map_.end();
    DataMarkStatMap::const_iterator iter  = begin;
    int64_t first_read_total_row_num  = 0;
    int64_t second_read_total_row_num = 0;
    int64_t idx = 0;
    for (;iter != end;iter++,idx++)
    {
      first_read_total_row_num  += iter->second.first_read_round_row_num_;
      second_read_total_row_num += iter->second.second_read_round_row_num_;
//      if (common::ObReadAtomicDataMark::UPS_MEMTABLE_DATA == iter->first.data_store_type_
//          && iter->second.first_read_round_row_num_ > 0
//          && iter->second.first_read_round_row_num_ != iter->second.second_read_round_row_num_)
//      {
//        bret = false;
//        YYSYS_LOG(WARN,"some data is not consistent!"
//                  "data_mark=[%s],data_mark_stat=[%s],idx=%ld,"
//                  "first_param=[%s],\nsecond_param=[%s]",
//                  to_cstring(iter->first),
//                  to_cstring(iter->second),idx,
//                  to_cstring(*(get_first_read_atomic_param_())),
//                  to_cstring(*(get_second_read_atomic_param_())));
//        dump_data_mark_state_(YYSYS_LOG_LEVEL_ERROR);
//        dump_transver_state_(YYSYS_LOG_LEVEL_ERROR);
//        break;
//      }
    }

    //select/*+READ_ATOMIC_STRONG*/ * from test1 where c2 in (0,2)
    //first read will be 3 rows(the extend datas of 0,1,2,the filters won't be used!)
    //but second read will be 2 rows(0,2,filters will be used!!!!)
//    if (second_read_total_row_num < first_read_total_row_num)
//    {
//      bret = false;
//      YYSYS_LOG(WARN,"second read lose row data!!"
//                "first_read_total_row_num=%ld,"
//                "second_read_total_row_num=%ld",
//                first_read_total_row_num,second_read_total_row_num);
//    }
  }
  return bret;
}

void ObReadAtomicRpcScan::dump_transver_state_(const int32_t log_level)
{
  YYSYS_LOGGER.logMessage(YYSYS_LOG_NUM_LEVEL(log_level),"///***************dump_transver_state_begin*****************///");
  YYSYS_LOGGER.logMessage(YYSYS_LOG_NUM_LEVEL(log_level),"transver_state_size=%ld",transver_state_map_.size());
  if (transver_state_map_.created() && transver_state_map_.size() > 0)
  {
    TransVersionMap::const_iterator begin = transver_state_map_.begin();
    TransVersionMap::const_iterator end   = transver_state_map_.end();
    TransVersionMap::const_iterator iter  = begin;
    int64_t idx = 0;
    for (;iter != end;iter++,idx++)
    {
      YYSYS_LOGGER.logMessage(YYSYS_LOG_NUM_LEVEL(log_level),"<idx=%ld,transver=[%s],state=[%d]>",
                idx,to_cstring(iter->first),iter->second);
    }
  }
  YYSYS_LOGGER.logMessage(YYSYS_LOG_NUM_LEVEL(log_level),"///***************dump_transver_state_end*****************///");
}

void ObReadAtomicRpcScan::dump_data_mark_state_(const int32_t log_level)
{
  YYSYS_LOGGER.logMessage(YYSYS_LOG_NUM_LEVEL(log_level),"///***************dump_data_mark_state_begin*****************///");
  YYSYS_LOGGER.logMessage(YYSYS_LOG_NUM_LEVEL(log_level),"data_mark_state_size=%ld",data_mark_state_map_.size());
  if (data_mark_state_map_.created() && data_mark_state_map_.size() > 0)
  {
    DataMarkStatMap::const_iterator begin = data_mark_state_map_.begin();
    DataMarkStatMap::const_iterator end   = data_mark_state_map_.end();
    DataMarkStatMap::const_iterator iter  = begin;
    int64_t idx = 0;
    for (;iter != end;iter++,idx++)
    {
      YYSYS_LOGGER.logMessage(YYSYS_LOG_NUM_LEVEL(log_level),"<idx=%ld,data_mark=[%s],data_mark_stat=[%s]>",
                idx,to_cstring(iter->first),
                to_cstring(iter->second));
    }
  }
  YYSYS_LOGGER.logMessage(YYSYS_LOG_NUM_LEVEL(log_level),"///***************dump_data_mark_state_end*****************///");
}

PHY_OPERATOR_ASSIGN(ObReadAtomicRpcScan)
{//FIXME:READ_ATOMIC it's finished???
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObReadAtomicRpcScan);
  reset();
  if (OB_SUCCESS != (ret = ObRpcScan::assign(other)))
  {
    YYSYS_LOG(WARN,"fail to assign ObRpcScan!ret=%d",ret);
  }
  else if (OB_SUCCESS != (ret = second_read_rpc_scan_.assign(&(o_ptr->second_read_rpc_scan_))))
  {
    YYSYS_LOG(WARN,"fail to assign second read rpc scan!ret=%d",ret);
  }
  else if (OB_SUCCESS != (ret = fetch_transver_stat_rpc_.assign(&(o_ptr->fetch_transver_stat_rpc_))))
  {
    YYSYS_LOG(WARN,"fail to assign fetch transver state rpc scan!ret=%d",ret);
  }
  else
  {
    is_inited_ = o_ptr->is_inited_;
    is_with_read_atomic_ = o_ptr->is_with_read_atomic_;
    min_used_column_id_ = o_ptr->min_used_column_id_;
    max_used_column_id_ = o_ptr->max_used_column_id_;
    total_base_column_num_   = o_ptr->total_base_column_num_;
    first_read_hint_         = o_ptr->first_read_hint_;
    second_read_hint_        = o_ptr->second_read_hint_;
    sys_table_rowkey_info_   = o_ptr->sys_table_rowkey_info_;
    if ((ret = orig_output_column_ids_.assign(o_ptr->orig_output_column_ids_)) != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "Assign ObRowDesc failed, ret=%d", ret);
    }
    else if ((ret = prepare_output_cids_.assign(o_ptr->prepare_output_cids_)) != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "Assign ObRowDesc failed, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = sys_table_row_desc_.assign(o_ptr->sys_table_row_desc_)))
    {
      YYSYS_LOG(WARN,"fail to assign sys table row desc!ret=%d",ret);
    }
    else
    {
      for (int64_t i = 0;OB_SUCCESS == ret && i < o_ptr->orig_output_column_exprs_.count(); i++)
      {
        if ((ret = orig_output_column_exprs_.push_back(o_ptr->orig_output_column_exprs_.at(i))) == OB_SUCCESS)
        {
          orig_output_column_exprs_.at(i).set_owner_op(this);
        }
        else
        {
          break;
        }
      }

      for (int64_t i = 0;OB_SUCCESS == ret && i < o_ptr->prepare_output_column_exprs_.count(); i++)
      {
        if ((ret = prepare_output_column_exprs_.push_back(o_ptr->prepare_output_column_exprs_.at(i))) == OB_SUCCESS)
        {
          prepare_output_column_exprs_.at(i).set_owner_op(this);
        }
        else
        {
          break;
        }
      }

      for (int64_t i = 0;OB_SUCCESS == ret && i < o_ptr->orig_filters_.count(); i++)
      {
        if ((ret = orig_filters_.push_back(o_ptr->orig_filters_.at(i))) != OB_SUCCESS)
        {
          YYSYS_LOG(WARN,"fail to add filters!ret=%d",ret);
        }
      }

    }
  }

  //read_atomic::debug::tmp
  YYSYS_LOG(INFO,"read_atomic::debug,atomic rpc finish assign!ret=%d",ret);
  return ret;
}

//add duyr 20151229:e

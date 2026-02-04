#include <getopt.h>
#include "ob_check.h"
#include "ob_check_producer.h"
#include "ob_check_comsumer.h"
#include "ob_check_param.h"
#include "common/ob_version.h"
#include "common/file_utils.h"
#include <string>

using namespace std;

#if 0
//global param
bool g_gbk_encoding = false;
bool g_print_lineno_taggle = false;
bool trim_end_blank = false;
//add by pangtz
bool error_arr[15] = {true, true, true,true, true, true,true, true, true,true, true, true,true, true,true};
//add e
//add by liyongfeng
#endif

void print_version()
{
  fprintf(stderr, "ob_check (%s %s)\n", PACKAGE_STRING, RELEASEID);
  fprintf(stderr, "GIT_VERSION: %s\n", git_version());
  fprintf(stderr, "BUILD_TIME: %s %s\n", build_date(), build_time());
  fprintf(stderr, "Copyright (c) 2013-2014 Taobao Inc.\n");
}
//add:e

void usage(const char *prog)
{
  UNUSED(prog);
  fprintf(stderr,
          "===== Examples =====\n"
          "\t(1)\n"
          "\t./ob_check -h 182.119.80.67,182.119.80.68,182.119.80.69 -p 2500 --dbname database_name -t table_name -l t.log --badfile t.bad -f t.del --progress\n"
          "\t(2)\n"
          "\t~/oceanbase/bin/ob_check -h 182.119.80.67,182.119.80.68,182.119.80.69 -p 2500 --dbname TANG -t students -l t.log --badfile t.bad -f t.del --case-sensitive --progress --concurrency 100\n"
          "===== Basic Arguments =====\n"
          "\t-h host (rootserver ip list)\n"
          "\t-p port (rootserver port, always *500)\n"
          "\t--dbname database_name (specify the database name of the table)\n"
          "\t-t table_name\n"
          "\t[-l log_file]\n"
          "\t[--badfile badfile_name]\n"
          "\t-f data_file\n"
          "\t[--progress] (to show how many rows processed succesfully, unsuccessfully and totally every 10 seconds)\n"
          "===== Advanced Arguments =====\n"
          "\t[--rowbyrow] (producer read the file rowbyrow)\n"
          "\t[--concurrency] (default is 100)\n"
          "\t[--username username] (specify the username of oceanbase)\n"
          "\t[--passwd passwd] (specify the password of the username)\n"
          "\t[--delima] (ASCII of column separator)\n"
          "\t[--rec-delima] (ASCII of row separator)\n"
          "\t[--case-sensitive] (which works when a name of database or table is case sensitive. For instance, when you are to import data into default database TANG)\n"

          "\t[-V] (version)\n"
          "\t[-H] (help)\n"
          "===== Return Value =====\n"
          "\t0 normal\n"
          "\t1 non-data error, such as no data file, no config file, schema error...,  bad file may exist\n"
          "\t2 data error or unknown error, bad file exist\n"
          "\n***** Developer Arguments *****\n"
          "\t[-q queue_size] (default 1000)\n"
          "\t[-g log_level]\n"
          "\t[--buffersize buffer_size] (KB RELEASEID default is %dKB)\n"
          "\t[--timeout timeout] (the maxmium time we could wait while connecting or querying)\n"
          //add e
          , kReadBufferSize / 1024);
}

/*
 * add by pangtz:20141126 �ж�append�Ļ�������Ƿ��ǹ涨��ʽ
 *
 */
int is_date_valid(const char * date)
{
  int ret = OB_SUCCESS;
  if(strlen(date) != 10)
    ret = OB_ERROR;
  if(ret == OB_SUCCESS){
    int num, year, month, day;
    num = sscanf(date, "%4d-%2d-%2d", &year, &month, &day);
    if(num ==3){
      ret = OB_SUCCESS;
    }else{
      error_arr[DATE_FORMAT_ERROR] = false;
      ret = OB_ERROR;
    }
  }
  return ret;
}
//add:end


int run_comsumer_queue(FileReader &reader, TableParam &param, ObRowBuilder *builder, 
                       OceanbaseDb *db, size_t queue_size, ObImportLogInfo &logInfo)
{
  int ret = OB_SUCCESS;
  AppendableFile *bad_file = NULL;
  if (param.bad_file_ != NULL)
  {
    YYSYS_LOG(INFO, "using bad file name = %s", param.bad_file_);
    ret = AppendableFile::NewAppendableFile(param.bad_file_, bad_file);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(ERROR, "can't create appendable file %s", param.bad_file_);
      error_arr[CREATE_BAD_FILE_ERROR] = false;
    }
  }

  AppendableFile *correct_file = NULL;
  if (param.correctfile_ != NULL)
  {
    YYSYS_LOG(INFO, "using correct file name = %s", param.correctfile_);
    ret = AppendableFile::NewAppendableFile(param.correctfile_, correct_file);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(ERROR, "can't create appendable file %s", param.correctfile_);
      return ret;
    }
  }

  reader.set_bad_file(bad_file);
  ImportProducer producer(reader, param.delima, param.rec_delima, param.is_rowbyrow, param.record_persql_);
  ImportComsumer comsumer(db, builder, bad_file, correct_file, param);

  YYSYS_LOG(INFO, "[delima]: type = %d, part1 = %d, part2 = %d", param.delima.delima_type(),
            param.delima.part1_, param.delima.part2_);
  YYSYS_LOG(INFO, "[rec_delima]: type = %d, part1 = %d, part2 = %d", param.rec_delima.delima_type(),
            param.rec_delima.part1_, param.rec_delima.part2_);

  ComsumerQueue<RecordBlock> queue(db, &producer, &comsumer, queue_size);
  if (queue.produce_and_comsume(1, param.ignore_merge_ ? 0 : 1, param.concurrency) != 0)
  {
    ret = OB_ERROR;
    error_arr[PRODUCE_AND_COMSUME_ERROR] = false;
  }
  else
  {
    queue.dispose();
  }

  if (bad_file != NULL)
  {
    delete bad_file;
    bad_file = NULL;
  }
  if (correct_file != NULL)
  {
    delete correct_file;
    correct_file = NULL;
  }
  logInfo.set_wait_time_sec(queue.get_waittime_sec());
  logInfo.set_wait_ups_mem_time(queue.get_ups_time_sec());

  return ret;
}

int parse_table_title(Slice &slice, const ObSchemaManagerV2 &schema, TableParam &table_param)
{
  int ret = OB_SUCCESS;
  int token_nr = ObRowBuilder::kMaxRowkeyDesc + OB_MAX_COLUMN_NUMBER;
  TokenInfo tokens[token_nr];

  Tokenizer::tokenize(slice, table_param.delima, token_nr, tokens);
  int rowkey_count = 0;
  table_param.col_descs.clear();
#if 0
  if (0 != table_param.input_column_nr)
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "input_column_nr should not be set[%d]", table_param.input_column_nr);
    return ret;
  }
  table_param.input_column_nr = token_nr;
#endif
  const ObTableSchema *table_schema = schema.get_table_schema(table_param.table_name.c_str());

  if (NULL == table_schema)
  {
    ret = OB_ERROR;
    error_arr[PARSE_TABLE_TITLE_ERROR] = false;
    YYSYS_LOG(ERROR, "cannot find table named [%s]", table_param.table_name.c_str());
  }

  if (OB_SUCCESS == ret)
  {
    const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();

    for (int i = 0; i < token_nr; i ++)
    {
      std::string column_name(tokens[i].token, 0, tokens[i].len);
      const ObColumnSchemaV2* column_schema = schema.get_column_schema(table_param.table_name.c_str(), column_name.c_str());
      if (NULL == column_schema)
      {
        ret = OB_ERROR;
        error_arr[PARSE_TABLE_TITLE_ERROR] = false;
        YYSYS_LOG(ERROR, "can't find column[%s] in table[%s]", column_name.c_str(), table_param.table_name.c_str());
        break;
      }
      else
      {
        ColumnDesc col_desc;
        col_desc.name = column_name;
        col_desc.offset = i;
        col_desc.len = static_cast<int>(tokens[i].len);
        table_param.col_descs.push_back(col_desc);
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (rowkey_count != rowkey_info.get_size())
      {
        ret = OB_ERROR;
        error_arr[PARSE_TABLE_TITLE_ERROR] = false;
        YYSYS_LOG(ERROR, "don't contain all rowkey column, please check data file. list rowkey count[%d]. actual rowkeycount[%ld]",
                  rowkey_count, rowkey_info.get_size());
      }
    }
  }
  return ret;
}
//modify by pangtz:20141206 ����ObImportLogInfo logInfo����
int do_work(//const char *config_file, const char *table_name,
            const char *host, unsigned short port, size_t queue_size,
            TableParam &cmd_table_param, ObImportLogInfo &logInfo)
//add e
{
  ImportParam param;
  TableParam table_param;
  int ret = OB_SUCCESS;

#if 0
  int ret = param.load(config_file);
  if (ret != OB_SUCCESS)
  {
    error_arr[13] = false;//add by pangtz:20141205
    YYSYS_LOG(ERROR, "can't load config file, please check file path");
    return ret;
  }
  ret = param.get_table_param(table_name, table_param);
  if (ret != OB_SUCCESS)
  {
    error_arr[11] = false;//add by pangtz:20141205
    YYSYS_LOG(ERROR, "no table=%s in config file, please check it", table_name);
    return ret;
  }
#endif

  if (OB_SUCCESS == ret)
  {
    table_param.delima_str = cmd_table_param.delima_str;
    string_2_delima(table_param.delima_str, table_param.delima);

    table_param.rec_delima_str = cmd_table_param.rec_delima_str;
    string_2_delima(table_param.rec_delima_str, table_param.rec_delima);

    table_param.has_null_flag = cmd_table_param.has_null_flag;
    table_param.null_flag = cmd_table_param.null_flag;
    table_param.has_substr = true;
  }
  if (OB_SUCCESS == ret)
  {
    table_param.table_name = cmd_table_param.table_name;
    if (!cmd_table_param.case_sensitive)
    {
      string & s = table_param.table_name;
      for (int i = 0; s[i]; i++)
      {
        s[i] = static_cast<char>(tolower(s[i]));
      }
    }
    table_param.has_decimal_to_varchar = cmd_table_param.has_decimal_to_varchar;
    table_param.decimal_to_varchar_grammar = cmd_table_param.decimal_to_varchar_grammar;

    table_param.has_column_map = cmd_table_param.has_column_map;
    table_param.column_map_grammar = cmd_table_param.column_map_grammar;

    if (table_param.has_column_map && !cmd_table_param.case_sensitive && NULL != table_param.column_map_grammar)
    {
      char * & s = table_param.column_map_grammar;
      for (int i = 0; s[i]; i++)
      {
        s[i] = static_cast<char>(tolower(s[i]));
      }
    }
    table_param.has_char_delima = cmd_table_param.has_char_delima;
    table_param.char_delima = cmd_table_param.char_delima;
    table_param.columns_with_char_delima_grammar = cmd_table_param.columns_with_char_delima_grammar;

    table_param.all_columns_have_char_delima =
        table_param.has_char_delima
        &&
        (
          ! table_param.columns_with_char_delima_grammar
          ||
          strcmp(table_param.columns_with_char_delima_grammar, "ALL") == 0
          );

    table_param.varchar_not_null = cmd_table_param.varchar_not_null;

    //trim_end_blank = table_param.is_trim_; //delete by pangtz:20141127
    if (cmd_table_param.data_file.size() != 0)
    {
      /* if cmd line specifies data file, use it */
      table_param.data_file = cmd_table_param.data_file;
    }

    //add by pangtz:20141126 ��trim�����������и�ֵ��table_param
    table_param.trim_flag = cmd_table_param.trim_flag;
    //add:end
    table_param.is_delete = cmd_table_param.is_delete;
    table_param.is_rowbyrow = cmd_table_param.is_rowbyrow;

    table_param.progress = cmd_table_param.progress;
    if (table_param.progress)
    {
      fprintf(stdout, "processed good bad\n");
    }
    table_param.keep_double_quotation_marks = cmd_table_param.keep_double_quotation_marks;
  }

  //add by pangtz:20141126 ��ȡappend������ֵ��table_param��
  if( OB_SUCCESS == ret && cmd_table_param.is_append_date_)
  {
    table_param.is_append_date_ = cmd_table_param.is_append_date_;
    if(cmd_table_param.appended_date.size() != 0)
    {
      if(is_date_valid(cmd_table_param.appended_date.c_str()) == OB_SUCCESS)
      {
        table_param.appended_date = cmd_table_param.appended_date;
      }
      else
      {
        error_arr[PARAM_APPEND_ACCDATE_ERROR] = false;
        YYSYS_LOG(ERROR, "param append accdate error, the string \"%s\" doesn't satisfy the format YYYY-mm-dd, please check it", cmd_table_param.appended_date.c_str());
        return OB_ERROR;
      }
    }
    else
    {
      error_arr[PARAM_APPEND_ACCDATE_ERROR] = false;
      YYSYS_LOG(ERROR, "param append accdate error, the string \"%s\" doesn't satisfy the format YYYY-mm-dd, please check it", cmd_table_param.appended_date.c_str());
      return OB_ERROR;
    }
  }
  //add:end

  if (OB_SUCCESS == ret)
  {
    if (cmd_table_param.bad_file_ != NULL)
    {
      table_param.bad_file_ = cmd_table_param.bad_file_;
    }

    if (cmd_table_param.correctfile_ != NULL)
    {
      table_param.correctfile_ = cmd_table_param.correctfile_;
    }

    if (cmd_table_param.concurrency != 0)
    {
      table_param.concurrency = cmd_table_param.concurrency;
    }

    if (table_param.data_file.empty())
    {
      error_arr[DATAFILE_NOT_EXIST] = false;//add by pangtz:20141205
      YYSYS_LOG(ERROR, "no datafile is specified, no work to do, quiting");
      return OB_ERROR;
    }
    table_param.is_insert = cmd_table_param.is_insert;
    table_param.is_replace = cmd_table_param.is_replace;
    table_param.is_delete = cmd_table_param.is_delete;
    table_param.record_persql_ = cmd_table_param.record_persql_;
    table_param.ignore_merge_ = cmd_table_param.ignore_merge_;
    table_param.import_limit_ups_memory = cmd_table_param.import_limit_ups_memory;
    table_param.user_name_ = cmd_table_param.user_name_;
    table_param.passwd_ = cmd_table_param.passwd_;

    table_param.db_name_ = cmd_table_param.db_name_;
    if ( ! cmd_table_param.case_sensitive)
    {
      char * & s = table_param.db_name_;
      for (int i = 0; s[i]; i++)
      {
        s[i] = static_cast<char>(tolower(s[i]));
      }
    }

    table_param.timeout_ = cmd_table_param.timeout_;
  }

  if (OB_SUCCESS == ret)
  {
    FileReader reader(table_param.data_file.c_str(), g_gbk_encoding);
    if (reader.fail())
    {
      ret = OB_INIT_FAIL;
      YYSYS_LOG(ERROR, "can't init file reader");
    }
    reader.set_dio(!cmd_table_param.not_dio);
    OceanbaseDb db(host, port, 8 * kDefaultTimeout);
    if (OB_SUCCESS == ret)
    {
      ret = db.init();
      if (ret != OB_SUCCESS)
      {
        error_arr[SYSTEM_ERROR] = false;//add by pangtz:20141205
        YYSYS_LOG(ERROR, "can't init database,%s:%d", host, port);
      }
    }

    ObSchemaManagerV2 *schema = NULL;
    if (ret == OB_SUCCESS)
    {
      schema = new(std::nothrow) ObSchemaManagerV2;

      if (schema == NULL)
      {
        error_arr[MEMORY_SHORTAGE] = false;//add by pangtz:20141205
        YYSYS_LOG(ERROR, "no enough memory for schema");
        ret = OB_ERROR;
      }
    }

    if (ret == OB_SUCCESS)
    {
      RPC_WITH_RETIRES(db.fetch_schema(*schema), 5, ret);
      if (ret != OB_SUCCESS)
      {
        error_arr[SYSTEM_ERROR] = false;//add by pangtz:20141205
        YYSYS_LOG(ERROR, "can't fetch schema from root server [%s:%d]", db.get_master_rs_ip(), port);
      }
    }

    if (ret == OB_SUCCESS)
    {
      ret = reader.open();
      if (OB_SUCCESS != ret)
      {
        error_arr[DATAFILE_NOT_EXIST] = false;//add by pangtz:20141205
        YYSYS_LOG(ERROR, "can't open reader: ret[%d]", ret);
      }
    }

#if 0 //delete by pangtz:20141205
    if (ret == OB_SUCCESS)
    {
      if (table_param.has_table_title)
      {
        YYSYS_LOG(INFO, "parse table title from data file");
        ret = reader.get_records(rec_block, table_param.rec_delima, table_param.delima, 1);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(ERROR, "can't get record ret[%d]", ret);
        }

        if (ret == OB_SUCCESS)
        {
          if (!rec_block.next_record(slice))
          {
            ret = OB_ERROR;
            YYSYS_LOG(ERROR, "can't get title row");
          }
          else if (OB_SUCCESS != (ret = parse_table_title(slice, *schema, table_param)))
          {
            ret = OB_ERROR;
            YYSYS_LOG(ERROR, "can't parse table title ret[%d]", ret);
          }
        }
      }
    }
#endif

    if (ret == OB_SUCCESS)
    {
      /* setup ObRowbuilder */
      ObRowBuilder builder(schema, table_param);
      std::vector<SequenceInfo> vseq;

      if ( OB_SUCCESS != (ret = builder.set_column_desc(vseq)))
      {
        error_arr[COLUMN_DESC_SET_ERROR] = false;
        YYSYS_LOG(ERROR, "Can't setup column descriptions.");
      }
      else if (OB_SUCCESS != (ret = cb_sequence(db, table_param.db_name_, table_param.table_name.c_str(), vseq, true)))
      {
        error_arr[COLUMN_DESC_SET_ERROR] = false;
        YYSYS_LOG(ERROR, "Creating Sequence failed.");
      }
      if (ret == OB_SUCCESS)
      {
        ObString complete_table_name;
        char buffer[OB_MAX_COMPLETE_TABLE_NAME_LENGTH];
        complete_table_name.assign_buffer(buffer, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
        if (table_param.db_name_ != NULL && strlen(table_param.db_name_) > 0)
        {
          complete_table_name.write(table_param.db_name_, (int)strlen(table_param.db_name_));
          complete_table_name.write(".", 1);
        }
        complete_table_name.write(table_param.table_name.c_str(), (int)table_param.table_name.length());

        const ColumnInfo *column_desc = NULL;
        int column_desc_nr = 0;
        if (OB_SUCCESS != builder.get_column_desc(column_desc, column_desc_nr))
        {
          error_arr[COLUMN_DESC_SET_ERROR] = false;
          YYSYS_LOG(ERROR, "column desc not set!");
          ret = OB_ERROR;
        }
        else if (!builder.has_all_rowkey(column_desc, column_desc_nr, complete_table_name))
        {
          error_arr[ROWKEY_SET_ERROR] = false;
          YYSYS_LOG(ERROR, "rowkey not set correct!");
          ret = OB_ERROR;
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = run_comsumer_queue(reader, table_param, &builder, &db, queue_size, logInfo);
      }
      //add by pangtz:20141206 ��ȡ������¼��ʧ�ܼ�¼��Ϣ
      logInfo.set_processed_lineno(builder.get_lineno());
      logInfo.set_bad_lineno(builder.get_bad_lineno());
      //add e

      //add by liyongfeng:20141020 �ж�monitor��ص�״̬

      if (OB_SUCCESS == ret)
      {
        if(!g_cluster_state)
        {
          //add by pangtz:20141204
          error_arr[RS_ERROR] = false;
          //add e
          YYSYS_LOG(ERROR, "root server has switched or no root server, please re-run ob_import");
          ret = OB_ERROR;
        }
      }
      //add:end
    }

    if (schema != NULL)
    {
      delete schema;
    }
  }
  return ret;
}


void handle_signal(int signo)
{
  switch (signo)
  {
    case 40:
      g_print_lineno_taggle = true;
      break;
    default:
      break;
  }
}


int main(int argc, char *argv[])
{
  srand(static_cast<unsigned int>(time(NULL)));
  const char *table_name = NULL;
  const char *host = NULL;
  const char *log_file = NULL;
  const char *log_level = "INFO";
  TableParam cmd_table_param;
  cmd_table_param.concurrency = 0;
  cmd_table_param.is_replace = false;
  cmd_table_param.is_delete = false;
  cmd_table_param.is_insert = false;
  cmd_table_param.record_persql_ = 50;
  cmd_table_param.ignore_merge_ = false;
  cmd_table_param.user_name_ = "admin";
  cmd_table_param.passwd_ = "admin";
  cmd_table_param.db_name_ = NULL;
  cmd_table_param.timeout_ = 60;
  cmd_table_param.not_dio = false;
  cmd_table_param.import_limit_ups_memory = OB_IMPORT_LIMIT_UPS_MEMORY;
  unsigned short port = 0;
  size_t queue_size = 1000;
  int ret = 0;
  signal(40, handle_signal);

  enum ParamKey{
    e_table_name = 't',
    e_delima = 2002,
    e_rec_delima = 2003,
    e_null_flag = 2004,
    e_substr = 2005,
    e_decimal_to_varchar = 2006,

    e_column_map = 2007,
    e_char_delima = 2008,
    e_columns_with_char_delima = 2009,
    e_varchar_not_null = 2010,
    e_progress = 2011,
    e_case_sensitive = 2012,
    e_keep_double_quotation_marks = 2013,
    e_del = 2014
  };

  static struct option long_options[] =
  {
    {"g2u", 0, 0, 1000},
    {"buffersize", 1, 0, 1001},
    {"badfile", 1, 0, 1002},
    {"concurrency", 1, 0, 1003},
    //      {"del", 0, 0, 1004},
    {"rowbyrow", 0, 0, 1005},
    {"append",1, 0, 1006},//add by pangtz:20141126 ����append������ѡ��
    {"trim", 1, 0, 1007},//add by pangtz:20141126 ����trim������ѡ��
    {"rep", 0, 0, 1008},
    {"ins", 0, 0, 1009},
    {"record-persql", 1, 0, 1010},
    {"ignore-merge", 0, 0, 1011},
    {"correctfile", 1, 0, 1012},
    {"username", 1, 0, 1013},
    {"passwd", 1, 0, 1014},
    {"dbname", 1, 0, 1015},
    {"timeout", 1, 0, 1016},
    {"not-dio", 0, 0, 1017},
    {"limit-ups-mem", 1, 0, 1018},
    {"table-name", 1, 0, e_table_name},
    {"delima", 1, 0, e_delima},
    {"rec-delima", 1, 0, e_rec_delima},
    {"null-flag", 1, 0, e_null_flag},
    {"substr", 1, 0, e_substr},
    {"decimal-to-varchar", 1, 0, e_decimal_to_varchar},
    {"column-mapping", 1, 0, e_column_map},
    {"char-delima", 1, 0, e_char_delima},
    {"columns-with-char-delima", 1, 0, e_columns_with_char_delima},
    {"varchar-not-null", 0, 0, e_varchar_not_null},
    {"progress", 0, 0, e_progress},
    {"case-sensitive", 0, 0, e_case_sensitive},
    {"keep-double-quotation-marks", 0, 0, e_keep_double_quotation_marks},
    {"del", 0, 0, e_del},
    {0, 0, 0, 0}
  };
  int option_index = 0;

  while ((ret = getopt_long(argc, argv, "h:p:t:c:l:g:q:f:HV", long_options, &option_index)) != -1)
  {
    switch (ret)
    {
      //          case 'c':
      //            config_file = optarg;
      //            break;
      case 't':
        table_name = optarg;
        cmd_table_param.table_name = optarg;
        break;
      case 'h':
        host = optarg;
        break;
      case 'l':
        log_file = optarg;
        break;
      case 'g':
        log_level = optarg;
        break;
      case 'p':
        port = static_cast<unsigned short>(atoi(optarg));
        break;
      case 'q':
        queue_size = static_cast<size_t>(atol(optarg));
        break;
      case 'f':
        cmd_table_param.data_file = optarg;
        break;
      case 1000:
        g_gbk_encoding = true;
        break;
      case 1001:
        kReadBufferSize = static_cast<int>(atol(optarg)) * 1024;
        break;
      case 1002:
        cmd_table_param.bad_file_ = optarg;
        break;
      case 1003:
        cmd_table_param.concurrency = static_cast<int>(atol(optarg));
        break;
        //          case 1004:
        //            cmd_table_param.is_delete = true;
        //            break;
      case 1005:
        cmd_table_param.is_rowbyrow = true;
        break;
        //add by pangtz:20141119 ����append������ѡ��
      case 1006:
        cmd_table_param.is_append_date_ = true;
        cmd_table_param.appended_date = optarg;
        break;
        //add:end
        //add by pangtz:20141126 ����trim������ѡ��
      case 1007:
        cmd_table_param.trim_flag = static_cast<int>(atol(optarg));
        break;
        //add:end
        //add by liyongfeg
      case 'V':
        print_version();
        exit(0);
        break;
        //        case 'H':
        //add:e
      case 1008:
        cmd_table_param.is_replace = true;
        break;
      case 1009:
        cmd_table_param.is_insert = true;
        break;
      case 1010:
        cmd_table_param .record_persql_ = static_cast<int64_t>(atoll(optarg));
        break;
      case 1011:
        cmd_table_param.ignore_merge_ = true;
        break;
      case 1012:
        cmd_table_param.correctfile_ = optarg;
        break;
      case 1013:
        cmd_table_param.user_name_ = optarg;
        break;
      case 1014:
        cmd_table_param.passwd_ = optarg;
        break;
      case 1015:
        cmd_table_param.db_name_ = optarg;
        break;
      case 1016:
        cmd_table_param.timeout_ = atoi(optarg);
        break;
      case 1017:
        cmd_table_param.not_dio = true;
        break;
      case 1018:
        cmd_table_param.import_limit_ups_memory = static_cast<int64_t>(atol(optarg)) * 1024 * 1024 *1024;
                                                  break;
      case e_delima:
        cmd_table_param.delima_str = optarg;
        break;
      case e_rec_delima:
        cmd_table_param.rec_delima_str = optarg;
        break;
      case e_null_flag:
        cmd_table_param.has_null_flag = true;
        cmd_table_param.null_flag = static_cast<char>(atoi(optarg));
        break;
      case e_substr:
        cmd_table_param.has_substr = true;
        cmd_table_param.substr_grammar = optarg;
        break;
      case e_decimal_to_varchar:
        cmd_table_param.has_decimal_to_varchar = true;
        cmd_table_param.decimal_to_varchar_grammar = optarg;
        break;
      case e_column_map:
        cmd_table_param.has_column_map = true;
        cmd_table_param.column_map_grammar = optarg;
        break;
      case e_char_delima:
        cmd_table_param.has_char_delima = true;
        cmd_table_param.char_delima = static_cast<char>(atoi(optarg));
        break;
      case e_columns_with_char_delima:
        cmd_table_param.has_char_delima = true;
        cmd_table_param.columns_with_char_delima_grammar = optarg;
        break;
      case e_varchar_not_null:
        cmd_table_param.varchar_not_null = true;
        break;
      case e_progress:
        cmd_table_param.progress = true;
        break;
      case e_case_sensitive:
        cmd_table_param.case_sensitive = true;
        break;
      case e_keep_double_quotation_marks:
        cmd_table_param.keep_double_quotation_marks = true;
        break;
      case e_del:
        cmd_table_param.is_delete = true;
        break;
      default:
        usage(argv[0]);
        exit(0);
        break;
    }
  }
  cmd_table_param.ignore_merge_ = false;
  cmd_table_param.import_limit_ups_memory = INT64_MAX;
  if (cmd_table_param.record_persql_ <= 0)
  {
    usage(argv[0]);
    exit(0);
  }
  //    if (!config_file || !table_name || !host || !port) {
  if (!table_name  || !cmd_table_param.db_name_ || !host || !port)
  {
    usage(argv[0]);
    exit(0);
  }

  int tmp_value = (0x1 & (cmd_table_param.is_delete ? 0xf : 0x0)) |
                  (0x2 & (cmd_table_param.is_replace ? 0xf : 0x0)) |
                  (0x4 & (cmd_table_param.is_insert ? 0xf : 0x0));
  if (tmp_value == 0)
  {
    cmd_table_param.is_insert = true;
  }
  else if (!(tmp_value == 1 || tmp_value == 2 || tmp_value == 4))
  {
    usage(argv[0]);
    exit(0);
  }
  //add by pangtz:20141206
  ret = 0;
  ObImportLogInfo logInfo;
  logInfo.set_begin_time();
  //add e
  if (log_file != NULL)
    YYSYS_LOGGER.setFileName(log_file,true);
  OceanbaseDb::global_init(log_file, log_level);
  //modify by pangtz:20141206 ����ObImportLogInfo logInfo����
  //return do_work(config_file, table_name, host, port, queue_size, cmd_table_param);
  //    ret = do_work(config_file, table_name, host, port, queue_size, cmd_table_param, logInfo);
  ret = do_work(host, port, queue_size, cmd_table_param, logInfo);
  //mod e
  //add by pangtz:20141206
  logInfo.set_table_name(table_name);
  logInfo.set_datafile_name(cmd_table_param.data_file);
  logInfo.set_final_ret(ret);
  logInfo.set_end_time();
  sleep(1);
  logInfo.print_error_log();
  return logInfo.get_final_ret();
  //add e
}

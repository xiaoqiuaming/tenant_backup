#include <string>
#include <vector>
#include <getopt.h>
#include <regex.h>
#include <string.h>
#include <ctype.h>
#include <mysql/mysql.h>
#include <algorithm>
#include "oceanbase_db.h"
#include "common/ob_string.h"
#include "common/ob_version.h"
#include "file_writer.h"
#include "ob_export_param.h"
#include "ob_export_extra_param.h"
#include "ob_export_producer.h"
#include "ob_export_consumer.h"
#include "ob_export_monitor.h"
#include "ob_export_queue.h"
#include "common/data_buffer.h"
#include "sql/ob_sql_result_set.h"

using namespace oceanbase::api;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace std;

bool trim_end_blank = false;
static bool flag = true;

void print_version()
{
  fprintf(stderr, "ob_export (%s %s)\n", PACKAGE_STRING, RELEASEID);
  fprintf(stderr, "GIT_VERSION: %s\n", git_version());
  fprintf(stderr, "BUILD_TIME: %s %s\n", build_date(), build_time());
  fprintf(stderr, "Copyrigth (c) 2013-2014 bankcommon Inc.\n");
}

void usage(const char *prog) //mod by zhuxh [modified for bug fix] 20170518
{
  fprintf(stderr,
          "\n"
          "#======================= Example =======================\n"
          "~/cb2/tools/dump/ob_export -h 182.119.80.102 -p 2500 -t epccdb.epcc_tran_flw -l e.log -f epcc_tran_flw.dat --username user_name --passwd user_password --delima 64 --select-statement \"SELECT EPF_BATCHID,EPF_PAYERACCTYPE,EPF_PAYEEACCTYPE,EPF_SERIALNO,EPF_CURRENCY,EPF_AMOUNT,EPF_TRANTYPE,EPF_TRANSTT,EPF_INSTGID,EPF_PAYERINSTGID,CASE WHEN EPF_RPFLAG = '1' THEN EPF_BANKSERIALNO ELSE '999' END EPF_BANKSERIALNO,EPF_PAYERINSTGID,EPF_PAYEEINSTGID,CASE WHEN EPF_RPFLAG = '2' THEN EPF_BANKSERIALNO ELSE '999' END EPF_BANKSERIALNO,EPF_PAYEEINSTGID,EPF_RESERVEACCNO FROM epccdb.epcc_tran_flw WHERE EPF_BATCHID BETWEEN 'B201703010001' AND 'B201703019999' \"\n"
          "\n"
          "#======================= Usage =========================\n"
          "# Commonly used params:\n"
          "\t-t database_name.table_name\n"
          "\t-h rs_ip\n"
          "\t-p rs_port\n"
          "\t-l log_file\n"
          "\t-f data_file_name\n"
          "\t--username username\n"
          "\t--passwd password\n"
          "\t[--delima column_delima], denary column separator, 15 as default\n"
          "\t[--select-statement SQL]\n"
          "\t[-V] [version]\n"
          "\t[-H] [help]\n"

          "\n"
          "# Use carefully if you truly understand:\n"
          "\t[--multi-tables] \n"
          "\t\tUsed only for NOT TOO LARGE result set. You need this param in any case as follows:\n"
          "\t\t(1) All rowkeys with equal sign as verb in 'where' condition. E.G. select * from t where c0 is null and c1 = 'hello'. c0 and c1 are all of rowkeys\n"
          "\t\t(2) Multiple tables mentioned in sql. E.G. join sql.\n"
          "\t\t(3) You think the sql executes swifter when indexes is used.\n"
          "\t\tThe param of [-t] is useless, but you still need to write a following existing table]\n"
          "\t[--masterpercent] how much percent of ms in master cluster can be used, 100%% as default\n"
          "\t[--slavepercent]  how much percent of ms in slave  cluster can be used, 0%%   as default\n"
          "\t[--rec-delima record_delima], denary row separator, 10 as default\n"
          "\t[--g2u] no param, charset GBK convert to UTF-8\n"
          "\t[--u2g] no param, charset UTF-8 convert to GBK\n"
          "\t[--add-quotes] add quotes(\") to columns values\n" //add dinggh [add_quotes] 20160908
          "\t[-c config_file]\n"
          "\t[--case-sensitive] When you has uppercase name of database or table. E.G. when you try to export a table from default database TANG.\n"

          "\n"
          "# Only for developer\n"
          "\t[--badfile] write bad records, if provided, default is not storing bad records\n"
          "\t[--init-limittime] the limit time for each sql(in seconds), 20 min as default, max value is 2100(seconds)\n"
          "\t[--max-limittime]  the limit time for each tablet, 30 min as default\n"
          "\t[--consumer-concurrency] concurrency of consumers, between 1 and 30, 3 as default\n"
          "\t[--multi-files] Different tablets writen into different files\n"
          "\t[-q queue_size] 1000 as default\n"
          "\t[-g log_level], 5 options, DEBUG|WARN|INFO|TRACE|ERROR, INFO as default\n"
          "\t[--maxfilesize] data file max file size, %ld MB as default\n"
          "\t[--maxrecordnum] data file max reocrd num, %ld Million as default\n"
          , /*basename(prog),*/ kDefaultMaxFileSize / (1024*1024), kDefaultMaxRecordNum);
  UNUSED(prog);
}

#if 1
struct ExportCmdParam
{
    ExportCmdParam() : config_file(NULL),
      table_name(NULL),
      host(NULL),
      port(0),
      log_file(NULL),
      queue_size(1000),
      data_file(NULL),
      log_level(NULL),
      export_sql(NULL),
      delima('\017'),
      rec_delima('\n'),
      bad_file(NULL),
      master_percent(100),//mod qianzm 20160705
      slave_percent(50),
      init_limittime(20*60*1000*1000),//20����
      max_limittime(MAX_TIMEOUT_US),//25����
      consumer_concurrency(3),
      producer_concurrency(0),  //add by zhuxh [producer concurrency set] 20170518
      is_multi_tables(0), //add qianzm [multi-tables] 20151228
      is_multi_files(false),//add qianzm [export_by_tablets] 20160524
      case_sensitive(false),//add qianzm [db_name_sensitive] 20160617
      //add qianzm [charset] 20160629:b
      g2u(false),
      u2g(false),
      //add 20160629:e
      add_quotes(NULL),//add dinggh [add_quotes] 20160908
      user_name(NULL),
      passwd(NULL)
    { }

    void printDebug()
    {
      YYSYS_LOG(TRACE, "config file=%s", config_file);
      YYSYS_LOG(TRACE, "table name=%s", table_name);
      YYSYS_LOG(TRACE, "host=%s, port=%u", host, port);
      YYSYS_LOG(TRACE, "log file=%s", log_file);
      YYSYS_LOG(TRACE, "queue size=%u", queue_size);
      YYSYS_LOG(TRACE, "data file=%s", data_file);
      YYSYS_LOG(TRACE, "log level=%s", log_level);
      YYSYS_LOG(TRACE, "master percent=%d%%", master_percent);
      YYSYS_LOG(TRACE, "slave percent=%d%%", slave_percent);
      YYSYS_LOG(TRACE, "init_limittime=%ldus", init_limittime);
      YYSYS_LOG(TRACE, "max_limittime=%ldus", max_limittime);
      YYSYS_LOG(TRACE, "consumer_concurrency=%d", consumer_concurrency);
      YYSYS_LOG(TRACE, "is_multi_tables=%d", is_multi_tables);//add qianzm [multi-tables] 20151228
      YYSYS_LOG(TRACE, "user name=%s", user_name);
    }

    const char *config_file;
    const char *table_name;
    const char *host;
    unsigned short port;
    const char *log_file;
    unsigned int queue_size;
    const char *data_file;
    const char *log_level;
    const char *export_sql;
    RecordDelima delima;
    RecordDelima rec_delima;
    const char *bad_file;
    int master_percent;
    int slave_percent;
    int64_t init_limittime;
    int64_t max_limittime;
    int consumer_concurrency;
    int producer_concurrency; //add by zhuxh [producer concurrency set] 20170518
    int is_multi_tables;//add qianzm [multi-tables] 20151228
    bool is_multi_files;//add qianzm [export_by_tablets] 20160524
    bool case_sensitive;//add qianzm [db_name_sensitive] 20160617
    //add qianzm [charset] 20160629:b
    bool g2u;
    bool u2g;
    //add 20160629:e
    const char *add_quotes;//add dinggh [add_quotes] 20160908
    string real_table_name;//add qianzm [db_name_sensitive] 20160803
    const char *user_name;
    const char *passwd;
};
#endif

int format_time_str(const int64_t time_us, char *buffer, int64_t capacity, const char *format = "%Y-%m-%d %H:%M:%S")
{
  struct tm time_struct;//broken-down time�ֽ�ʱ��
  int64_t time_s = time_us / 1000000;//΢��תΪ��
  int ret = OB_SUCCESS;
  if (NULL != localtime_r(&time_s, &time_struct))
  {
    if (0 == strftime(buffer, capacity, format, &time_struct))
    {
      ret = OB_ERROR;
    }
  }
  return ret;
}

void parse_delima(const char *delima_str, RecordDelima &delima)
{
  const char *end_pos = delima_str + strlen(delima_str);
  if (find(delima_str, end_pos, ',') == end_pos)
  {
    delima = RecordDelima(static_cast<char>(atoi(delima_str)));
  }
  else
  {
    int part1, part2;
    sscanf(delima_str, "%d,%d", &part1, &part2);
    delima = RecordDelima(static_cast<char>(part1), static_cast<char>(part2));
  }
}

int parse_options(int argc, char *argv[], ExportCmdParam &cmd_param)
{
  int ret = OB_SUCCESS;
  int res = 0;

  int option_index = 0;

  static struct option long_options[] = {
    {"delima", 1, 0, 1000},
    {"rec-delima", 1, 0, 1001},//mod by zhuxh:20151230
    {"select-statement", 1, 0, 1002},//mod by zhuxh:20151230
    {"maxfilesize", 1, 0, 1003},
    {"maxrecordnum", 1, 0, 1004},
    {"badfile", 1, 0, 1005},
    {"masterpercent", 1, 0, 1006},
    {"slavepercent", 1, 0, 1007},
    {"init-limittime", 1, 0, 1008},
    {"max-limittime", 1, 0, 1009},
    {"consumer-concurrency", 1, 0, 1010},
    {"multi-tables", 0, 0, 1011},//mod by zhuxh:20151230
    {"multi-files",0, 0, 1012},//add qianzm [export_by_tablets] 20160524
    {"case-sensitive", 0, 0, 1013},//add qianzm [db_name_sensitive] 20160617
    //add qianzm [charset] 20160629:b
    {"g2u", 0, 0, 1014},
    {"u2g", 0, 0, 1015},
    //add 20160629:e
    {"add-quotes", 1, 0, 1016},//add dinggh [add-quotes] 20160908

    {"producer-concurrency", 1, 0, 1017},  //add by zhuxh [producer concurrency set] 20170518
    {"username", 1, 0, 1018},
    {"passwd", 1, 0, 1019},
    {0, 0, 0, 0}
  };

  while ((OB_SUCCESS == ret) && ((res = getopt_long(argc, argv, "c:t:h:p:l:q:f:g:HV", long_options, &option_index)) != -1))
  {
    switch (res)
    {
      case 'c':
        cmd_param.config_file = optarg;
        break;
      case 't':
        //del by qianzm [db_name_sensitive] 20160617
        cmd_param.table_name = optarg;
        //del 20160617:e
        break;
      case 'h':
        cmd_param.host = optarg;
        break;
      case 'p':
        cmd_param.port = static_cast<unsigned short>(atoi(optarg));
        break;
      case 'l':
        cmd_param.log_file = optarg;
        break;
      case 'q':
        cmd_param.queue_size = static_cast<unsigned int>(atoi(optarg));
        break;
      case 'f':
        cmd_param.data_file = optarg;
        break;
      case 'g':
        cmd_param.log_level = optarg;
        break;
      case 1000:
        parse_delima(optarg, cmd_param.delima);
        break;
      case 1001:
        parse_delima(optarg, cmd_param.rec_delima);
        break;
      case 1002:
        cmd_param.export_sql = optarg;
        break;
      case 1003:
        kDefaultMaxFileSize = static_cast<int64_t>(atol(optarg)) * 1024 * 1024;
        flag = true;
        break;
      case 1004:
        kDefaultMaxRecordNum = static_cast<int64_t>(atol(optarg)) * 100 * 100 * 100;
        flag = false;
        break;
      case 1005:
        cmd_param.bad_file = optarg;
        break;
      case 1006:
      {
        errno = 0;
        int percent = 0;
        int n = sscanf(optarg, "%d%%", &percent);
        if(1 == n)
        {
          cmd_param.master_percent = percent;
        }
        else
        {
          cmd_param.master_percent = -1;
        }
      }
        break;
      case 1007:
      {
        errno = 0;
        int percent = 0;
        int n = sscanf(optarg, "%d%%", &percent);
        if(1 == n)
        {
          cmd_param.slave_percent = percent;
        }
        else
        {
          cmd_param.slave_percent = -1;
        }
      }
        break;
      case 1008:
      {
        cmd_param.init_limittime = static_cast<int64_t>(atol(optarg)) * 1000 * 1000;
      }
        break;
      case 1009:
      {
        cmd_param.max_limittime = static_cast<int64_t>(atol(optarg)) * 1000 * 1000;
      }
        break;
      case 1010:
        cmd_param.consumer_concurrency = static_cast<int>(atoi(optarg));
        break;
        //add qianzm [multi-tables] 20151229
      case 1011:
        cmd_param.is_multi_tables = 1;
        break;
        //add qianzm [export_by_tablets] 20160524
      case 1012:
        cmd_param.is_multi_files = true;
        break;
        //add 20160524:e
        //add qianzm [db_name_sensitive] 20160617:b
      case 1013:
        cmd_param.case_sensitive = true;
        break;
        //add 20160615:e
        //add qianzm [charset] 20160629:b
      case 1014:
        cmd_param.g2u = true;
        break;
      case 1015:
        cmd_param.u2g = true;
        break;
        //add 20160629:e
        //add dinggh [add_quotes] 20160908:b
      case 1016:
        cmd_param.add_quotes = optarg;
        break;
        //add 20160908:e

        //add by zhuxh [producer concurrency set] 20170518 :b
      case 1017:
        cmd_param.producer_concurrency = static_cast<int>(atoi(optarg));
        break;
        //add by zhuxh [producer concurrency set] 20170518 :e

      case 1018:
        cmd_param.user_name = optarg;
        break;
      case 1019:
        cmd_param.passwd = optarg;
        break;
      case 'V':
        print_version();
        exit(OB_SUCCESS);
        break;
      case 'H':
        usage(argv[0]);
        exit(OB_SUCCESS);
        break;
      default:
        usage(argv[0]);
        exit(OB_ERROR);
        break;
    }
  }
  if (//(NULL == cmd_param.config_file) //mod by zhuxh:20151230
      /*||*/ (NULL == cmd_param.table_name)
      || (NULL == cmd_param.host)
      || (0 == cmd_param.port)
      || (NULL == cmd_param.data_file)
      || (0 > kDefaultMaxFileSize)
      || (0 > kDefaultMaxRecordNum)
      || (0 == cmd_param.queue_size)
      || (0 > cmd_param.slave_percent)
      || (100 < cmd_param.slave_percent)
      || (0 > cmd_param.master_percent)
      || (100 < cmd_param.master_percent)
      || (0 > cmd_param.init_limittime)
      || (2100000000 < cmd_param.init_limittime)
      || (0 > cmd_param.max_limittime)
      || (0 >= cmd_param.consumer_concurrency)
      || (30 < cmd_param.consumer_concurrency)
      || (NULL == cmd_param.user_name)
      || (NULL == cmd_param.passwd)
      )
  {
    usage(argv[0]);
    ret = OB_ERROR;
  }

  return ret;
}

int get_rowkey_column_name(const char *table_name, const ObSchemaManagerV2 *schema, std::vector <std::string> &rowkey_column_names)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &rowkey_info = schema->get_table_schema(table_name)->get_rowkey_info();
  const ObTableSchema *tmp_schema = schema->get_table_schema(table_name);

  if (tmp_schema == NULL)
  {
    YYSYS_LOG(ERROR, "can not find table schema of %s", table_name);
    ret = OB_ERROR;
  }
  else
  {
    uint64_t table_id = tmp_schema->get_table_id();
    for (int64_t i = 0; i < rowkey_info.get_size(); i++)
    {
      ObRowkeyColumn rowkey_column;
      rowkey_info.get_column(i, rowkey_column);
      int32_t idx = 0;
      const ObColumnSchemaV2 *col_schema = schema->get_column_schema(table_id, rowkey_column.column_id_, &idx);
      rowkey_column_names.push_back(col_schema->get_name());
    }
  }
  return ret;
}

int execute_sql(OceanbaseDb &db, ObExportTableParam &table_param, string &sql, const ObServer &ms, ObSQLResultSet &result_set)
{
  int ret = OB_SUCCESS;
  db.set_table_param(&table_param);
  YYSYS_LOG(DEBUG, "real table name=%s", table_param.table_name.c_str());
  if (OB_SUCCESS != (ret = db.init_execute_sql(ms, sql, db.get_timeout_us(), db.get_timeout_us())))
  {
    YYSYS_LOG(ERROR, "init_execute_sql failed! ret=%d", ret);
  }
  TabletBlock *tmp_block = new TabletBlock();
  if (NULL == tmp_block)
  {
    YYSYS_LOG(ERROR, "allocate memory for tmp_block failed! ret=[%d]", ret);
  }
  if (OB_SUCCESS != (ret = tmp_block->init()))
  {
    YYSYS_LOG(ERROR, "first tmp_block init failed! ret=[%d]", ret);
    delete tmp_block;
  }
  bool has_next = false;
  ObDataBuffer &out_buffer = tmp_block->get_data_buffer();
  if (OB_SUCCESS != (ret = db.get_result(result_set, out_buffer, has_next)))
  {
    YYSYS_LOG(ERROR, "get result from mergeserver failed! ret=%d", ret);
  }
  if (tmp_block != NULL)
  {
    delete tmp_block;
    tmp_block = NULL;
  }
  return ret;
}

int get_sql_result_column_names(OceanbaseDb &db, ObExportTableParam &table_param, const ObServer &ms, std::vector <std::string> &result_column_names)
{
  int ret = OB_SUCCESS;
  string tmp_export_sql_str = table_param.export_sql;
  string tmp_str = " limit 1";
  if (table_param.exist_limit)
  {
    size_t index_limit = tmp_export_sql_str.find("limit");
    if (index_limit != std::string::npos)
    {
      tmp_export_sql_str = tmp_export_sql_str.substr(0, index_limit) + tmp_str;
    }
  }
  else
  {
    tmp_export_sql_str += tmp_str;
  }
  YYSYS_LOG(INFO, "tmp_export_sql_str = %s", tmp_export_sql_str.c_str());
  ObSQLResultSet result_set;
  if (OB_SUCCESS != (ret = execute_sql(db, table_param, tmp_export_sql_str, ms, result_set)))
  {
    YYSYS_LOG(ERROR, "get result from mergeserver failed! ret=%d", ret);
  }
  else
  {
    for (int i = 0; i < result_set.get_fields().count(); i++)
    {
      result_column_names.push_back(result_set.get_fields().at(i).cname_.ptr());
    }
  }
  return ret;
}

bool result_contains_all_rowkey(std::vector <std::string> &result_column_names, std::vector <std::string> &rowkey_column_names,ObExportTableParam &table_param)
{
  bool key_find = false;
  bool contains_all_rowkey = true;
  for (size_t i = 0; i < rowkey_column_names.size(); i++)
  {
    key_find = false;
    for (unsigned int j = 0; j < result_column_names.size(); j++)
    {
      if (0 == strcasecmp(result_column_names[j].c_str(), rowkey_column_names[i].c_str()))
      {
        key_find = true;
        break;
      }
    }
    if (!key_find)
    {
      contains_all_rowkey = false;
      table_param.rowkey_not_in_result.push_back(rowkey_column_names[i]);
      YYSYS_LOG(INFO, "rowkey column %s is not assign in sql!", rowkey_column_names[i].c_str());
    }
  }
  return contains_all_rowkey;
}

int add_rowkey_to_export_sql(ObExportTableParam &table_param, vector <std::string> &result_column_names)
{
  int ret = OB_SUCCESS;
  string sql_select = "";
  size_t index_note = table_param.export_sql.find("*/");
  if (index_note != std::string::npos)
  {
    sql_select = table_param.export_sql.substr(0, index_note + 3);
  }
  else
  {
    sql_select = "select /*+not_use_index()*/ ";
  }
  for (unsigned int i = 0; i < table_param.rowkey_not_in_result.size(); i++)
  {
    sql_select += table_param.rowkey_not_in_result[i] + ",";
  }
  for (unsigned int j = 0; j < result_column_names.size() - 1; j++)
  {
    sql_select += result_column_names[j] + ",";
  }
  sql_select += result_column_names[result_column_names.size() - 1];
  string tmp_sql = table_param.export_sql;
  for (unsigned int k = 0; k< tmp_sql.length(); k++)
  {
    tmp_sql[k] = static_cast<char>(tolower(tmp_sql[k]));
  }
  YYSYS_LOG(DEBUG, "tmp_sql = %s", tmp_sql.c_str());
  size_t index_from = tmp_sql.find("from");
  if (index_from != std::string::npos)
  {
    table_param.export_sql = sql_select + " " + table_param.export_sql.substr(index_from);
    YYSYS_LOG(INFO, "table_param.export_sql = %s", table_param.export_sql.c_str());
  }
  else
  {
    YYSYS_LOG(ERROR, "Key from can not find in export sql!");
    ret = OB_ERROR;
  }
  return ret;
}

int get_rowkey_not_in_result_index(OceanbaseDb &db, ObExportTableParam &table_param, const ObServer &ms)
{
  int ret = OB_SUCCESS;
  string tmp_export_sql_str = table_param.export_sql;
  string tmp_str = " limit 1";
  if (table_param.exist_limit)
  {
    size_t index_limit = tmp_export_sql_str.find("limit");
    if (index_limit != std::string::npos)
    {
      tmp_export_sql_str = tmp_export_sql_str.substr(0, index_limit) + tmp_str;
    }
  }
  else
  {
    tmp_export_sql_str += tmp_str;
  }
  YYSYS_LOG(INFO, "tmp_export_sql_str = %s", tmp_export_sql_str.c_str());
  ObSQLResultSet result_set;
  if (OB_SUCCESS != (ret = execute_sql(db, table_param, tmp_export_sql_str, ms, result_set)))
  {
    YYSYS_LOG(ERROR, "get result from mergeserver failed! ret=%d", ret);
  }
  else
  {
    for (size_t i = 0; i < table_param.rowkey_not_in_result.size(); i++)
    {
      for (int j = 0; j < result_set.get_fields().count(); j++)
      {
        if (0 == strcasecmp(result_set.get_fields().at(j).cname_.ptr(), table_param.rowkey_not_in_result[i].c_str()))
        {
          table_param.rowkey_index.push_back(j);
          YYSYS_LOG(DEBUG, "rowkey_not_in_result_index=[%d]", j);
          break;
        }
      }
    }
  }
  return ret;
}

int get_explain_sql_result(ObExportTableParam &table_param, const ObServer &ms, char *explain_result)
{
  int ret = OB_SUCCESS;
  MYSQL *mysql = NULL;
  MYSQL_RES *results = NULL;
  MYSQL_ROW record;

  mysql = mysql_init(NULL);
  if (NULL == mysql)
  {
    YYSYS_LOG(ERROR, "mysql init failed");
    ret = OB_ERROR;
    return ret;//add by wanggx
  }

  char ms_ip[20] = {'\0'};
  int32_t ms_port = ms.get_port();
  snprintf(ms_ip, 20, "%d.%d.%d.%d",
           ms.get_ipv4()        & 0xFF,
          (ms.get_ipv4() >> 8 ) & 0xFF,
          (ms.get_ipv4() >> 16) & 0xFF,
          (ms.get_ipv4() >> 24) & 0xFF);
  YYSYS_LOG(DEBUG, "mysql client connect to ms:%s, port:%d", ms_ip, ms_port);

  string real_table_name = table_param.table_name;
  string::size_type split_pos = real_table_name.find('.', 0);
  string db_name = real_table_name.substr(0, split_pos);
  YYSYS_LOG(DEBUG, "db_name=%s", db_name.c_str());

  if (!mysql_real_connect(mysql, ms_ip, table_param.user_name.c_str(), table_param.passwd.c_str(), db_name.c_str(), ms_port, NULL, 0))
  {
    YYSYS_LOG(ERROR, "connect to mysql error %s", mysql_error(mysql));
    ret = OB_ERROR;
  }
  else
  {
    string explain_export_sql_str = "explain " + table_param.export_sql;
    ret = mysql_query(mysql, explain_export_sql_str.c_str());
    if (ret != 0)
    {
      YYSYS_LOG(ERROR, "explain sql error, error message is %s", mysql_error(mysql));
      ret = OB_ERROR;
    }
    else
    {
      results = mysql_store_result(mysql);
      if (NULL != results)
      {
        record = mysql_fetch_row(results);
        strncpy(explain_result, record[0], strlen(record[0]));
        explain_result[strlen(record[0])] = '\0';
        YYSYS_LOG(DEBUG, "explain result is:%s", explain_result);
      }
    }
  }
  mysql_free_result(results);
  mysql_close(mysql);
  mysql = NULL;
  return ret;
}

int get_table_count_from_explain_result(const char *explain_result, int &table_count)
{
  int ret = OB_SUCCESS;
  string explain_result_str = explain_result;
  string::size_type table_item_start_pos = explain_result_str.find("TableItemList");
  string::size_type table_item_end_pos = explain_result_str.find("ColumnItemList");
  string table_item = explain_result_str.substr(table_item_start_pos, table_item_end_pos - table_item_start_pos - 1);

  string::size_type pos = 0;
  table_count = 0;
  while ((pos = table_item.find("TableId", pos)) != string::npos)
  {
    table_count++;
    pos++;
  }
  YYSYS_LOG(INFO, "table_count=%d", table_count);

  return ret;
}

bool contains_set_operation(const char *explain_result)
{
  string explain_result_str = explain_result;
  string::size_type statement_list_start_pos = explain_result_str.find("StmtList");
  string::size_type statement_list_end_pos = explain_result_str.find("ExprList");
  string statement_list = explain_result_str.substr(statement_list_start_pos, statement_list_end_pos - statement_list_start_pos - 1);

  string::size_type pos = 0;
  int32_t query_count = 0;
  while ((pos = statement_list.find("QueryId", pos)) != string::npos)
  {
    query_count++;
    pos++;
  }
  YYSYS_LOG(INFO, "query_count=%d", query_count);
  if (query_count >= 2)
    return true;
  else
    return false;
}

bool contains_sub_query(const char *explain_result)
{
  bool result = false;
  string explain_result_str = explain_result;
  string::size_type physical_plan_pos = explain_result_str.find("Explain()");
  string physical_plan = explain_result_str.substr(physical_plan_pos);

  char pattern[40] = "=+sub query[0-9]+=+";
  regex_t reg;
  regmatch_t pm;
  if(0 !=regcomp(&reg, pattern, REG_EXTENDED | REG_NOSUB))
  {
    YYSYS_LOG(ERROR, "compile regex=%s fail", pattern);
  }
  else if (0 != regexec(&reg, physical_plan.c_str(), 1, &pm, REG_NOTEOL))
  {
    YYSYS_LOG(INFO, "not exist sub query clause in physical_plan!");
  }
  else
  {
    YYSYS_LOG(INFO, "exist sub query clause in physical_plan!");
    result = true;
  }
  regfree(&reg);
  return result;
}

bool contains_join(const char *explain_result)
{
  bool result = false;
  string explain_result_str = explain_result;
  string::size_type physical_plan_pos = explain_result_str.find("Explain()");
  string physical_plan = explain_result_str.substr(physical_plan_pos);

  char pattern[20] = "Join\\(Join Type";
  regex_t reg;
  regmatch_t pm;
  if(0 !=regcomp(&reg, pattern, REG_EXTENDED | REG_NOSUB))
  {
    YYSYS_LOG(ERROR, "compile regex=%s fail", pattern);
  }
  else if (0 != regexec(&reg, physical_plan.c_str(), 1, &pm, REG_NOTEOL))
  {
    YYSYS_LOG(INFO, "not exist join clause in physical_plan!");
  }
  else
  {
    YYSYS_LOG(INFO, "exist join clause in physical_plan!");
    result = true;
  }
  regfree(&reg);
  return result;
}

bool contains_group_by(const char *explain_result)
{
  bool result = false;
  string explain_result_str = explain_result;
  string::size_type physical_plan_pos = explain_result_str.find("Explain()");
  string physical_plan = explain_result_str.substr(physical_plan_pos);

  char pattern[50] = "GroupBy\\(group_cols=\\[<[0-9]+\\,[0-9]+>\\]";
  regex_t reg;
  regmatch_t pm;
  if(0 !=regcomp(&reg, pattern, REG_EXTENDED | REG_NOSUB))
  {
    YYSYS_LOG(ERROR, "compile regex=%s fail", pattern);
  }
  else if (0 != regexec(&reg, physical_plan.c_str(), 1, &pm, REG_NOTEOL))
  {
    YYSYS_LOG(INFO, "not exist group by clause in physical_plan!");
  }
  else
  {
    YYSYS_LOG(INFO, "exist group by clause in physical_plan!");
    result = true;
  }
  regfree(&reg);
  return result;
}

bool contains_order_by(const char *explain_result)
{
  bool result = false;
  string explain_result_str = explain_result;
  string::size_type physical_plan_pos = explain_result_str.find("Explain()");
  string::size_type table_rpc_scan_pos = explain_result_str.find("TableRpcScan", physical_plan_pos);
  string physical_plan;
  if(table_rpc_scan_pos != string::npos)
  {
    physical_plan = explain_result_str.substr(physical_plan_pos, table_rpc_scan_pos - physical_plan_pos);
  }
  else
  {
    physical_plan = explain_result_str.substr(physical_plan_pos);
  }

  char pattern[60] = "Sort\\(columns=\\[<[0-9]+\\,[0-9]+\\,ASC|DESC>\\]";
  regex_t reg;
  regmatch_t pm;
  if(0 !=regcomp(&reg, pattern, REG_EXTENDED | REG_NOSUB))
  {
    YYSYS_LOG(ERROR, "compile regex=%s fail", pattern);
  }
  else if (0 != regexec(&reg, physical_plan.c_str(), 1, &pm, REG_NOTEOL))
  {
    YYSYS_LOG(INFO, "not exist order by clause in physical_plan!");
  }
  else
  {
    YYSYS_LOG(INFO, "exist order by clause in physical_plan!");
    result = true;
  }
  regfree(&reg);
  return result;
}

bool contains_limit(const char *explain_result, ObExportTableParam &table_param)
{
  bool result = false;
  string explain_result_str = explain_result;
  string::size_type physical_plan_pos = explain_result_str.find("Explain()");
  string physical_plan = explain_result_str.substr(physical_plan_pos);

  char pattern[40] = "Limit\\(limit=expr<NULL,[0-9]+>=";
  regex_t reg;
  regmatch_t pm;
  if(0 !=regcomp(&reg, pattern, REG_EXTENDED | REG_NOSUB))
  {
    YYSYS_LOG(ERROR, "compile regex=%s fail", pattern);
  }
  else if (0 != regexec(&reg, physical_plan.c_str(), 1, &pm, REG_NOTEOL))
  {
    YYSYS_LOG(INFO, "not exist limit clause in physical_plan!");
  }
  else
  {
    table_param.exist_limit = true;
    YYSYS_LOG(INFO, "exist limit clause in physical_plan!");
    result = true;
  }
  regfree(&reg);
  return result;
}

bool contains_in_or_not_in(const char *explain_result)
{
  bool result = false;
  string explain_result_str = explain_result;
  string::size_type physical_plan_pos = explain_result_str.find("Explain()");
  string physical_plan = explain_result_str.substr(physical_plan_pos);

  char pattern[30] = "\\|IN|NOT_IN<[0-9]+>\\|";
  regex_t reg;
  regmatch_t pm;
  if(0 !=regcomp(&reg, pattern, REG_EXTENDED | REG_NOSUB))
  {
    YYSYS_LOG(ERROR, "compile regex=%s fail", pattern);
  }
  else if (0 != regexec(&reg, physical_plan.c_str(), 1, &pm, REG_NOTEOL))
  {
    YYSYS_LOG(INFO, "not exist in/not in clause in physical_plan!");
  }
  else
  {
    YYSYS_LOG(INFO, "exist in/not in clause in physical_plan!");
    result = true;
  }
  regfree(&reg);
  return result;
}

int get_in_or_not_in_column_id(const char *explain_result, vector<int> &result_column_id)
{
  int ret = OB_SUCCESS;
  string explain_result_str = explain_result;
  string::size_type in_start_pos = explain_result_str.find("Filter(filters");
  string::size_type in_end_pos = explain_result_str.find("IN<");
  string in_clause = explain_result_str.substr(in_start_pos, in_end_pos - in_start_pos - 1);

  string::size_type in_column_start_pos = in_clause.rfind("expr<");
  string::size_type in_column_end_pos = in_clause.rfind("LEFT_PARAM_END<");
  string in_column_str = in_clause.substr(in_column_start_pos, in_column_end_pos - in_column_start_pos - 1);

  string::size_type pos = 0;
  int table_id = 0;
  int column_id = 0;
  while((pos = in_column_str.find("COL<", pos)) != string::npos)
  {
    sscanf(in_column_str.c_str() + pos, "COL<%d,%d>", &table_id, &column_id);
    pos++;
    result_column_id.push_back(column_id);
    YYSYS_LOG(DEBUG, "in get_in_or_not_in_column_id, column_id=%d", column_id);
  }
  return ret;
}

int get_in_or_not_in_column_name(const char *explain_result, vector<std::string> &result_column_names)
{
  int ret = OB_SUCCESS;
  vector<int> in_column_ids;
  if(OB_SUCCESS != (ret = get_in_or_not_in_column_id(explain_result, in_column_ids)))
  {
    YYSYS_LOG(WARN, "get_in_or_not_in_column_id error!");
  }
  string explain_result_str = explain_result;
  string::size_type column_item_start_pos = explain_result_str.find("ColumnItemList");
  string::size_type column_item_end_pos = explain_result_str.find("ExprList");
  string column_item = explain_result_str.substr(column_item_start_pos, column_item_end_pos - column_item_start_pos - 1);

  map<int, string> column_id_to_name_map;
  int column_id = 0;
  char column_name_str[1024] = {'\0'};
  string column_name;
  string tmp_column_str;
  string::size_type pos = 0;
  string::size_type column_name_end_pos = 0;
  while ((pos = column_item.find("ColumnId", pos)) != string::npos)
  {
    YYSYS_LOG(DEBUG, "column_item=%s", column_item.c_str() + pos);
    sscanf(column_item.c_str() + pos, "ColumnId\":%d,\"ColumnName\":\"%s\"", &column_id, column_name_str);
    tmp_column_str = column_name_str;
    column_name_end_pos = tmp_column_str.find("\",\"");
    column_name = tmp_column_str.substr(0, column_name_end_pos);
    pos++;
    column_id_to_name_map.insert(map<int, string>::value_type(column_id, column_name));
  }

  map<int, string>::iterator it;
  for(unsigned int i = 0 ; i < in_column_ids.size(); i++)
  {
    it = column_id_to_name_map.find(in_column_ids[i]);
    if(it != column_id_to_name_map.end())
    {
      result_column_names.push_back(it->second);
      YYSYS_LOG(DEBUG, "in column_id_to_name_map, column_name=%s", (it->second).c_str());
    }
  }
  return ret;
}

bool is_prefix_in_rowkey(vector<std::string> &in_column_names, vector<std::string> &rowkey_column_names)
{
  bool result = true;
  vector<std::string>::iterator it;
  for (unsigned int i = 0; i <rowkey_column_names.size() && i < in_column_names.size(); i++)
  {
    it = find(in_column_names.begin(), in_column_names.end(), rowkey_column_names[i]);
    if(it == in_column_names.end())
    {
      YYSYS_LOG(INFO, "The rowkey column %s does not exist in in_clause", rowkey_column_names[i].c_str());
      result = false;
      break;
    }
  }
  return result;
}

int run_dump_table(ObExportTableParam &table_param, const ExportCmdParam &cmd_param)
{
  int ret = OB_SUCCESS;

  int64_t start_timestamp = yysys::CTimeUtil::getTime();

  OceanbaseDb db(cmd_param.host, cmd_param.port, cmd_param.init_limittime);
  if (OB_SUCCESS != (ret = db.init()))
  {
    YYSYS_LOG(ERROR, "OceanbaseDb init failed, ret=[%d]", ret);
    return ret;
  }
  /* ��ȡschema */
  ObSchemaManagerV2 *schema = NULL;
  if (OB_SUCCESS == ret)
  {
    schema = new(std::nothrow) ObSchemaManagerV2;
    if (NULL == schema)
    {
      YYSYS_LOG(ERROR, "no enough memory for schema");
      ret = OB_ERROR;
    }
  }
  if (OB_SUCCESS == ret)
  {
    RPC_WITH_RETIRES(db.fetch_schema(*schema), 5, ret);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(ERROR, "failed to fetch schema from root server, ret=[%d]", ret);
    }
  }

  if (OB_SUCCESS != ret)
  {
    YYSYS_LOG(ERROR, "fetch schema failed, ret=[%d]", ret);
    return ret;
  }

  std::vector<ObServer> mysql_ms_vec;
  if (OB_SUCCESS != (ret = db.fetch_mysql_ms_list(cmd_param.master_percent, mysql_ms_vec)))
  {
    YYSYS_LOG(ERROR, "fetch_mysql_ms_list failed! ret=[%d]", ret);
  }
  if (mysql_ms_vec.size() == 0)
  {
    YYSYS_LOG(ERROR, "there is no available ms!");
    ret = OB_ERROR;
  }

  char explain_result[2048000];

  int32_t table_count = 0;
  if(OB_SUCCESS != (ret = get_explain_sql_result(table_param, mysql_ms_vec[0], explain_result)))
  {
    YYSYS_LOG(ERROR, "get_explain_sql_result error!");
    return ret;
  }
  else if (OB_SUCCESS != (ret = get_table_count_from_explain_result(explain_result, table_count)))
  {
    YYSYS_LOG(ERROR, "get_table_count_from_explain_result error!");
  }

  std::vector<std::string> rowkey_column_names;
  if (OB_SUCCESS != (ret = get_rowkey_column_name(table_param.table_name.c_str(), schema, rowkey_column_names)))
  {
    YYSYS_LOG(ERROR, "get_rowkey_column_name error!");
    return ret;
  }

  bool sql_contains_in_or_not_in = contains_in_or_not_in(explain_result);
  std::vector<std::string> in_column_names;
  if (sql_contains_in_or_not_in)
  {
    if (OB_SUCCESS != (ret = get_in_or_not_in_column_name(explain_result, in_column_names)))
    {
      YYSYS_LOG(ERROR, "get_in_or_not_in_column_name error!");
    }
  }

  if(!contains_limit(explain_result, table_param) && !(contains_set_operation(explain_result)) && table_count == 1 
     && !(contains_sub_query(explain_result)) && !(contains_join(explain_result)) && !(contains_group_by(explain_result))
     && !(contains_order_by(explain_result)) && (!sql_contains_in_or_not_in
         || (sql_contains_in_or_not_in && !is_prefix_in_rowkey(in_column_names, rowkey_column_names))))
  {
    table_param.is_multi_tables_ = 0;
  }
  else
  {
    table_param.is_multi_tables_ = 1;
  }
  YYSYS_LOG(INFO, "after ob_export set multi_tables parameter, table_param.is_multi_tables_ = %d", table_param.is_multi_tables_);

  if (table_param.is_multi_tables_ == 0)
  {
    string tmp_sql = table_param.export_sql;
    size_t hint_start_pos = table_param.export_sql.find("/*+");
    if (hint_start_pos != std::string::npos)
    {
      table_param.export_sql = "select /*+not_use_index(), " + table_param.export_sql.substr(hint_start_pos +3);
    }
    else
    {
      table_param.export_sql = "select /*+not_use_index()*/ " + table_param.export_sql.substr(7); //skip select
    }
    YYSYS_LOG(INFO, "after add hint, table_param.export_sql = %s", table_param.export_sql.c_str());
  }
  /* ��ȡMS list,������ȡ������ */
  std::vector<ObServer> ms_vec;
  int concurrency = 0;
  if (OB_SUCCESS != (ret = db.fetch_ms_list(cmd_param.master_percent, cmd_param.slave_percent, ms_vec)))
  {
    YYSYS_LOG(ERROR, "fetch_ms_list failed! ret=[%d]", ret);
  }

  if (ms_vec.size() == 0)
  {
    YYSYS_LOG(ERROR, "there is no available ms!");
    ret = OB_ERROR;
  }
  //add by zhuxh [producer concurrency set] 20170518 :b
  else if( cmd_param.producer_concurrency > 0 ) //When you set producer number by command param
  {
    concurrency = cmd_param.producer_concurrency;
  }
  //add by zhuxh [producer concurrency set] 20170518 :e
  else if (table_param.exist_limit)
  {
    concurrency = 1;
  }
  else
  {
    concurrency = static_cast<unsigned int>(ms_vec.size());
  }

  if (OB_SUCCESS == ret && table_param.is_multi_tables_ == 0)
  {
//    std::vector <std::string> rowkey_column_names;
//    if (OB_SUCCESS != (ret = get_rowkey_column_name(table_param.table_name.c_str(), schema, rowkey_column_names)))
//    {
//      YYSYS_LOG(ERROR, "get_rowkey_column_name error!");
//      return ret;
//    }
    std::vector <std::string> result_column_names;
    ret = get_sql_result_column_names(db, table_param, ms_vec[0], result_column_names);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(ERROR, "failed to get sql result columns, ret=[%d]",ret);
      return ret;
    }
    if (!result_contains_all_rowkey(result_column_names, rowkey_column_names, table_param))
    {
      if (OB_SUCCESS != (ret = add_rowkey_to_export_sql(table_param, result_column_names)))
      {
        YYSYS_LOG(ERROR, "failed to add rowkey to export sql, ret=[%d]", ret);
        return ret;
      }
      ret = get_rowkey_not_in_result_index(db, table_param, ms_vec[0]);
      if (ret != OB_SUCCESS)
      {
        YYSYS_LOG(ERROR, "failed to get rowkey not in result index, ret=[%d]", ret);
        return ret;
      }
    }
  }
#if 1
  /* ��ʼ��bad file */
  AppendableFile *bad_file = NULL;
  if (OB_SUCCESS == ret)
  {
    if (cmd_param.bad_file != NULL)
    {
      YYSYS_LOG(DEBUG, "using bad file name = %s", cmd_param.bad_file);
      ret = AppendableFile::NewAppendableFile(cmd_param.bad_file, bad_file);
      if (ret != OB_SUCCESS)
      {
        YYSYS_LOG(ERROR, "can't create appendable bad file [%s]", cmd_param.bad_file);
        ret = OB_SUCCESS;
      }
    }
    else
    {
      YYSYS_LOG(WARN, "user not specify bad file");
    }
  }
#endif
  /* ��ʼ�������ļ� */
  FileWriter fw;
  if (OB_SUCCESS == ret)
  {
    if ((ret = fw.init_file_name(cmd_param.data_file)) != OB_SUCCESS)
    {
      YYSYS_LOG(ERROR, "FileWrite init failed, ret=[%d]", ret);
    }
    else
    {
      if (flag)//������������ָ�����Ⱥ�˳��,�����һ��Ϊ׼;�����ָ��,ʹ��kDefaultMaxFileSize
      {
        fw.set_max_file_size(kDefaultMaxFileSize);
      }
      else
      {
        fw.set_max_record_count(kDefaultMaxRecordNum);
      }
    }
  }
  
  DbRowFormator formator(schema, table_param);
  if (OB_SUCCESS == ret)/* ��ʼ��formator */
  {
    if (OB_SUCCESS != (ret = formator.init()))
    {
      YYSYS_LOG(ERROR, "db row formator init failed, ret=[%d]", ret);
    }
    //del by zhuxh:20161018 [add class Charset]
    /*
    //add qianzm [charset] 20160629:b
    else if (cmd_param.u2g)
    {
      formator.set_u2g();
    }
    else if (cmd_param.g2u)
    {
      formator.set_g2u();
    }
    //add 20160629:e
    */
  }

  //���߳��������Ϊ�߳��쳣�˳��ж�ʹ��
  ErrorCode err_code;
  if (OB_SUCCESS == ret)
  {
    //����̣߳������ص�ǰ�����������̵߳�״̬���û������룬����������������һЩ��β����
    ExportMonitor monitor(&db, &err_code);
    monitor.start_monitor();

    //ExportProducer producer(&db, schema, cmd_param.table_name, &err_code, table_param, &formator, ms_vec,  cmd_param.max_limittime,
    //                        /*add qianzm [export_by_tablets] 20160524*/cmd_param.is_multi_files);
    ExportProducer producer(&db, schema, cmd_param.real_table_name.c_str(), &err_code, table_param, &formator, ms_vec,  cmd_param.max_limittime, cmd_param.is_multi_files, concurrency);//mod qianzm [db_name_sensitive] 20160803 //mod by zhuxh [producer concurrency set] 20170518
    ExportConsumer consumer(&fw, &formator, &err_code, table_param, bad_file,
                            /*add qianzm [export_by_tablets] 20160524*/cmd_param.is_multi_files);
    ComsumerQueue<TabletBlock*> queue(&producer, &consumer, cmd_param.queue_size);

    //add qianzm [multi-tables] 20151228
    if (table_param.is_multi_tables_ != 0)
    {
      queue.set_is_multi_tables();
      if (queue.produce_and_comsume(1, cmd_param.consumer_concurrency) != 0)/* ����Ĳ������Ѿ�ȥ��,��Ҫ��ȡMS listͬʱ��ȡ������ */
      {
        YYSYS_LOG(ERROR, "queue.produce_and_comsume failed!");
        ret = OB_ERROR;
      }
      else
      {
        queue.dispose();
      }
    }
    //add e
    else
    {
      //add qianzm [export_by_tablets] 20160415:b
      string data_file_name(cmd_param.data_file);
      queue.set_data_file_name(data_file_name);
      //add 20160415:e
      if (queue.produce_and_comsume(concurrency, cmd_param.consumer_concurrency) != 0)/* ����Ĳ������Ѿ�ȥ��,��Ҫ��ȡMS listͬʱ��ȡ������ */
      {
        YYSYS_LOG(ERROR, "queue.produce_and_comsume failed!");
        ret = OB_ERROR;
      }
      else
      {
        queue.dispose();
      }
    }
    //֪ͨ����߳��˳�
    monitor.set_exit_signal(true);
    monitor.wait();

    //�жϹ����߳��˳���״̬��������쳣�˳�����Ҫ�����쳣��������û�
    if (OB_SUCCESS != err_code.get_err_code())
    {
      ret = err_code.get_err_code();
      YYSYS_LOG(ERROR, "consumer producer exit with fail code, ret=[%d]", ret);
    }

    //���������ִ�н�������ִ��ʱ�䡢��¼������Ϣ�������Ļ
    if (OB_SUCCESS == ret)
    {
      int64_t end_timestamp = yysys::CTimeUtil::getTime();
      char buffer_start[1024];
      char buffer_end[1024];

      format_time_str(start_timestamp, buffer_start, 1024);
      format_time_str(end_timestamp, buffer_end, 1024);
      fprintf(stdout, "total record count=%ld, success record count=%ld, failed record count=%ld\n",
              consumer.get_total_succ_rec_count() + consumer.get_total_failed_rec_count(),
              consumer.get_total_succ_rec_count(), consumer.get_total_failed_rec_count());

      YYSYS_LOG(INFO, "total record count=%ld, success record count=%ld, failed record count=%ld",
                consumer.get_total_succ_rec_count() + consumer.get_total_failed_rec_count(),
                consumer.get_total_succ_rec_count(), consumer.get_total_failed_rec_count());

      fprintf(stdout, "start time=%s, end time=%s, total time=%lds\n",
              buffer_start, buffer_end,
              (end_timestamp - start_timestamp)/(1000*1000));

      YYSYS_LOG(INFO, "start time=%s, end time=%s, total time=%lds",
                buffer_start, buffer_end,
                (end_timestamp - start_timestamp)/(1000*1000));
    }
  }

  
  if (bad_file != NULL)
  {
    delete bad_file;
    bad_file = NULL;
  }
  if (schema != NULL)
  {
    delete schema;
    schema = NULL;
  }

  return ret;
}

int do_work(const ExportCmdParam& cmd_param)
{
  int ret = OB_SUCCESS;
  
  ExportParam param;
  ObExportTableParam table_param;

  //del by [727]
//  table_param.user_name = cmd_param.user_name;
//  table_param.passwd = cmd_param.passwd;

//  table_param.is_multi_tables_ = cmd_param.is_multi_tables;//add qianzm [multi-tables] 20151228
//  //�½�һ������־�ļ�������Ӧ�ĺ�׺���ļ�����ִ��ʧ�ܵ�sqlд����ļ���
//  table_param.tablet_info_file = cmd_param.log_file;// add qianzm [export_by_tablets] 20160415
  //���غͽ��������ļ�
  //mod qianzm [no config file] 20151109:b
  if (!cmd_param.config_file || 0 == strncmp(cmd_param.config_file,"null",4))//mod by zhuxh:20151230
  {
    //table_param.table_name = cmd_param.table_name;
    table_param.table_name = cmd_param.real_table_name;//mod qianzm [db_name_sensitive] 20160803
  }
  else
  {
    if (OB_SUCCESS != (ret = param.load(cmd_param.config_file))) {
      YYSYS_LOG(ERROR, "can't load config file, please check file path[%s]", cmd_param.config_file);
    }
    //else if (OB_SUCCESS != (ret = param.get_table_param(cmd_param.table_name, table_param))) {
    else if (OB_SUCCESS != (ret = param.get_table_param(cmd_param.real_table_name.c_str(), table_param))) {//mod qianzm [db_name_sensitive] 20160803
      YYSYS_LOG(ERROR, "no table=%s in config file, please check it", cmd_param.table_name);
    }
  }
  //mod :e
  
  //��ȡ��table_param��,��Ҫ��������������һЩ���������ں�,�������в���Ϊ��
  if (OB_SUCCESS == ret)
  {
    //[727]-b
    table_param.user_name = cmd_param.user_name;
    table_param.passwd = cmd_param.passwd;

    table_param.is_multi_tables_ = cmd_param.is_multi_tables;//add qianzm [multi-tables] 20151228
    //�½�һ������־�ļ�������Ӧ�ĺ�׺���ļ�����ִ��ʧ�ܵ�sqlд����ļ���
    table_param.tablet_info_file = cmd_param.log_file;// add qianzm [export_by_tablets] 20160415
    //[727]-e

    //add dinggh [add_quotes] 20160908:b
    if (cmd_param.add_quotes != NULL)
    {
      string value = cmd_param.add_quotes;
      Tokenizer::tokenize(value, table_param.add_quotes, ',');
      if (table_param.add_quotes.size() <= 0)
      {
        YYSYS_LOG(WARN, "add_quotes has no values");
      }
    }
    //add dinggh 20160908:e
    //�Էָ����������
    if(cmd_param.delima.type_ == RecordDelima::CHAR_DELIMA)
    {
      table_param.delima.set_char_delima(cmd_param.delima.part1_);
    }
    else
    {
      table_param.delima.set_short_delima(cmd_param.delima.part1_, cmd_param.delima.part2_);
    }

    //  cmd_param.delima.type_ == RecordDelima::CHAR_DELIMA ? table_param.delima.set_char_delima(cmd_param.delima.part1_) : table_param.delima.set_short_delima(cmd_param.delima.part1_, cmd_param.delima.part2_);

    if(cmd_param.rec_delima.type_ == RecordDelima::CHAR_DELIMA)
    {
      table_param.rec_delima.set_char_delima(cmd_param.rec_delima.part1_);
    }
    else
    {
      table_param.rec_delima.set_short_delima(cmd_param.rec_delima.part1_, cmd_param.rec_delima.part2_);
    }
    //  cmd_param.rec_delima.type_ == RecordDelima::CHAR_DELIMA ? table_param.rec_delima.set_char_delima(cmd_param.rec_delima.part1_) : table_param.rec_delima.set_short_delima(cmd_param.rec_delima.part1_, cmd_param.rec_delima.part2_);

    //�Բ�ѯSQL��������
    if (NULL != cmd_param.export_sql)
    {
      //add by zhuxh [no index for range] 20170518 :b
      const char * s = cmd_param.export_sql;
      int i=0;

      for( ; s[i] && isspace(s[i]); i++ );
      if( strncasecmp(s+i,"select",6) == 0 )
      {
        if( ! cmd_param.is_multi_tables )
        {
          char pattern[20] = "/\\*\\+.*\\*/";
          regex_t reg;
          regmatch_t pm;
          if (0 != (ret = regcomp(&reg, pattern, REG_EXTENDED)))
          {
            YYSYS_LOG(ERROR, "compile regex=%s fail, ret=%d", pattern, ret);
          }
          else if (0 != (ret = regexec(&reg, s, 1, &pm, REG_NOTEOL)))
          {
            YYSYS_LOG(INFO, "not exist hint in sql=%s ret=%d", s, ret);
          }
          if (ret == 0)
          {
            YYSYS_LOG(INFO, "exist hint in sql=%s", s);
            i = pm.rm_so;
            for (; s[i] && (s[i] == '/' || s[i] == '*' || s[i] == ' '); i++);
            ++i;
            table_param.export_sql += "select /*+not_use_index(), ";
          }
          else
          {
            table_param.export_sql += "select /*+not_use_index()*/ ";
            i+=6;
          }
          regfree(&reg);

//          char pattern_in[15] = "in\\s*\\(.*\\)";
//          regex_t reg_in;
//          regmatch_t pm_in;
//          if (0 != (ret = regcomp(&reg_in, pattern_in, REG_EXTENDED | REG_ICASE)))
//          {
//            YYSYS_LOG(ERROR, "compile regex=%s fail, ret=%d", pattern_in, ret);
//          }
//          else if (0 != (ret = regexec(&reg_in, s, 1, &pm_in, REG_NOTEOL)))
//          {
//            YYSYS_LOG(INFO, "not exist in clause in sql=%s ret=%d", s, ret);
//          }
//          else
//          {
//            YYSYS_LOG(INFO, "exist in clause in sql=%s", s);
//            table_param.is_multi_tables_ = 1;
//          }
//          regfree(&reg_in);
        }

//        char pattern[15] = "limit\\s+[0-9]+";
//        regex_t reg;
//        regmatch_t pm;
//        if (0 != (ret = regcomp(&reg, pattern, REG_EXTENDED | REG_ICASE)))
//        {
//          YYSYS_LOG(ERROR, "compile regex=%s faile, ret=%d", pattern, ret);
//          table_param.export_sql += s + i;
//        }
//        else if (0 != (ret = regexec(&reg, s, 1, &pm, REG_NOTEOL)))
//        {
//          YYSYS_LOG(INFO, "not exist limit in sql=%s ret=%d", s, ret);
//          table_param.export_sql += s + i;
//          table_param.exist_limit = false;
//        }
//        else
//        {
//          YYSYS_LOG(INFO, "exist limit in sql=%s and remove it", s);
//          table_param.exist_limit = true;
//          for (; s[i] && i < static_cast<int>(strlen(s)); ++i)
//          {
//            if (i >= pm.rm_so && i <= pm.rm_eo)
//            {
//              table_param.limit += s[i];
//              continue;
//            }
//            table_param.export_sql += s[i];
//          }
//          YYSYS_LOG(INFO, "after remove limit sql=%s", table_param.export_sql.c_str());
//        }
//        regfree(&reg);
        table_param.export_sql += s+i;
        table_param.exist_limit = false;
      }
      else
        ret = OB_INIT_SQL_CONTEXT_ERROR;
      //add by zhuxh [no index for range] 20170518 :e
    }
    else if (table_param.export_sql.empty())
    {
      //YYSYS_LOG(ERROR, "no select statement specified, please check");
      //ret = OB_ERROR;
      //����������Ļ�,��ʹ��Ĭ��SQL,����ѯȫ�����ݵ�SQL
      //mod qianzm [db_name_sensitive] 20160617:b
      //table_param.export_sql = "select * from " + table_param.table_name;
      if (!cmd_param.case_sensitive)
      {
        /*for(int i=0;table_param.table_name[i];i++) //add by zhuxh 20151230
              table_param.table_name[i] = static_cast<char>(tolower(table_param.table_name[i]));*/
        table_param.table_name = cmd_param.real_table_name;//mod qianzm [db_name_sensitive] 20160803
        table_param.export_sql = "select /*+not_use_index()*/ * from " + table_param.table_name; //mod by zhuxh [no index for range] 20170518
      }
      else
      {
        string t_name = "\"";
        for (int i=0; i<(int)table_param.table_name.length(); i++)
        {
          if (table_param.table_name[i] != '.')
            t_name = t_name + table_param.table_name[i];
          else
            t_name = t_name + "\"" + table_param.table_name[i] + "\"";//�����ͱ�������Ҫ������˫����
        }
        t_name += "\"";
        table_param.export_sql = "select /*+not_use_index()*/ * from " + t_name; //mod by zhuxh [no index for range] 20170518
      }
      //mod 20160617:e
    }
    char buff[OB_MAX_SQL_LENGTH];
    if (OB_SUCCESS != (ret = remove_last_comma(table_param.export_sql.c_str(), buff, OB_MAX_SQL_LENGTH)))
    {
      YYSYS_LOG(ERROR, "remove the last comma failed!");
    }
    table_param.export_sql = buff;
  }

  //add by zhuxh:20161018 [add class Charset] :b
  if ( cmd_param.u2g && cmd_param.g2u )
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "Parameter u2g & g2u cannot be both used, ret=[%d]", ret);
  }
  else if (cmd_param.g2u)
  {
    table_param.code_conversion_flag = e_g2u;
  }
  else if (cmd_param.u2g)
  {
    table_param.code_conversion_flag = e_u2g;
  }
  //add by zhuxh:20161018 [add class Charset] :e

  if (OB_SUCCESS == ret)
  {
    ret = run_dump_table(table_param, cmd_param);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(ERROR, "run dump table failed! ret=[%d]", ret);
    }
  }
  return ret;
}

int main(int argc, char *argv[])
{
  /*
 * �����в����Ľṹ�壬�洢���е������в���ֵ
 */
  ExportCmdParam cmd_param;
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = parse_options(argc, argv, cmd_param))) {
    return ret;
  }
  //add qianzm [db_name_sensitive] 20160803:b
  cmd_param.real_table_name = cmd_param.table_name;
  if (!cmd_param.case_sensitive)
  {
    for(int i=0; cmd_param.table_name[i]; i++)
      cmd_param.real_table_name[i] = static_cast<char>(tolower(cmd_param.table_name[i]));
  }
  //add 20160803:e

  cmd_param.is_multi_tables = true;
  if (NULL != cmd_param.log_file)
    YYSYS_LOGGER.setFileName(cmd_param.log_file, true);
  if (NULL != cmd_param.log_level) {
    OceanbaseDb::global_init(NULL, cmd_param.log_level);
  } else {
    OceanbaseDb::global_init(NULL, "INFO");
  }

  string command_line_str;
  for(int i = 0; i < argc; i++)
  {
    if (i > 1 && strcmp(argv[i - 1], "--select-statement") == 0)
    {
      command_line_str.append("\"");
      command_line_str.append(argv[i]);
      command_line_str.append("\"");
    }
    else
    {
      command_line_str.append(argv[i]);
    }
    command_line_str.append(" ");
  }
  YYSYS_LOG(DEBUG, "ob_export command line parameters are:\n%s", command_line_str.c_str());

  cmd_param.printDebug();

  /*
 * ��ʼ������������
 */

  if (OB_SUCCESS != (ret = do_work(cmd_param)))
  {
    YYSYS_LOG(ERROR, "do work error! ret=[%d]", ret);
    fprintf(stderr, "some error occur, please check the logfile!\n");
  }
  else
  {
    fprintf(stdout, "all work done!\n");
  }

  return ret;
}

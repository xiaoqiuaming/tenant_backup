#ifndef __OB_CHECK_PARAM_H__
#define  __OB_CHECK_PARAM_H__

#include <vector>
#include <string>
#include <map>
#include <set>
#include <limits.h>
#include "tokenizer_v2.h"
#include "common/ob_schema.h"
#include "db_record.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::api;

#ifndef SC
#define SC(X) strcat(str,X)
#endif

#ifndef MAX_CONSTRUCT_SQL_LENGTH
#define MAX_CONSTRUCT_SQL_LENGTH (1<<19)
#endif

#ifndef MAX_TOKEN_LENGTH
#define MAX_TOKEN_LENGTH (1<<19)
#endif

#define IMPORT_DEFAULT_READ_BUFFER_SIZE (1<<19)
#define IMPORT_ERROR_TYPE_NUM 42
#define IMPORT_SEQUENCE_MAX_LENGTH 200

enum ImportErrorType{
  NORMAL = 0,
  TABLE_NOT_EXIST = 1,
  DATAFILE_NOT_EXIST = 2,
  DATAFILE_COLUMN_NUM_ERROR = 3,
  DATA_ERROR = 4,
  NOT_NULL_CONSTRAIN_ERROR = 5,
  SYSTEM_ERROR = 6,
  LOCK_CONFLICT = 7,
  VARCHAR_OVER_FLOW = 8,
  OVER_MAX_ROWKEY_NUM = 9,
  LINE_TOO_LONG = 10,
  RS_ERROR = 11,
  CONF_FILE_ERROR = 12,
  DELETE_ERROR = 13,
  TABLE_CONF_NOT_EXIST = 14,
  MEMORY_SHORTAGE = 15,
  WRONG_TIME_FORMAT = 16,
  READ_FILE_ERROR = 17,
  CREATE_BAD_FILE_ERROR = 18,
  PARSE_TABLE_TITLE_ERROR = 19,
  DATE_FORMAT_ERROR = 20,
  PRODUCE_AND_COMSUME_ERROR = 21,
  PARAM_APPEND_ACCDATE_ERROR = 22,
  COLUMN_DESC_SET_ERROR = 23,
  ROWKEY_SET_ERROR = 24,
  GET_TABLE_PARAM_ERROR = 25,
  ALLOCATE_MEMORY_ERROR = 26,
  MS_MANAGER_INIT_ERROR = 27,
  BAD_FILE_ERROR = 28,
  MEMORY_OVERFLOW = 29,
  SQL_BUILDING_ERROR = 30,
  GET_ONE_MS_ERROR = 31,
  MS_REF_COUNT_ERROR = 32,
  FETCH_MS_LIST_ERROR = 33,
  SUBSTR_GRAMMAR_ERROR = 34,
  SUBSTR_COLUMN_NOT_EXIST = 35,
  DECIMAL_2_VARCHAR_COLUMN_NOT_EXIST = 36,
  MAPPING_COLUMN_NOT_EXIST = 37,
  COLUMN_WITH_CHAR_DELIMA_NOT_EXIST = 38,
  INCOMPLETE_DATA = 39,
  G2U_ERROR = 40,
  BUFFER_OVER_FLOW = 41
};

extern bool g_gbk_encoding;
extern bool g_print_lineno_taggle;
extern bool trim_end_blank;
extern bool error_arr[IMPORT_ERROR_TYPE_NUM];
extern int kReadBufferSize;

struct ColumnDesc {
  std::string name;
  int offset;
  int len;
};

struct RowkeyDesc {
  int offset;
  int type;
  int pos;
};

struct ColumnInfo {
  const ObColumnSchemaV2 *schema;
  int offset;
  const static int DEFAULT_TOKEN_COLUMN;
  const static int SEQUENCE;
  std::string default_token;
  ColumnInfo():schema(NULL),offset(DEFAULT_TOKEN_COLUMN)
  {}
};


struct SubstrInfo {
    const static int UNDEFINED;
    std::string column_name;
    int datafile_column_id;
    int beg_pos;
    int len;
    SubstrInfo()
      : datafile_column_id(UNDEFINED),
      beg_pos(0),
      len(INT_MAX >> 1)
      { }
};

struct ColumnMapInfo
{
    const static int UNDEFINED;
    const static int SEQUENCE;
    std::string column_name;
    int datafile_column_id;
    std::string default_token;
    int seq_index;

    ColumnMapInfo()
      : datafile_column_id(UNDEFINED),seq_index(UNDEFINED)
    {}
    void clear()
    {
      column_name.clear();
      datafile_column_id = UNDEFINED;
      default_token.clear();
      seq_index = UNDEFINED;
    }
};

struct SequenceInfo
{
    const static int64_t UNDEFINED;
    string column_name;
    int64_t start;
    int64_t increment;
};

//add end

struct TableParam {
  TableParam() 
    : delima(15),
      delima_str(NULL),
      rec_delima('\n'), 
      rec_delima_str(NULL),
      has_nop_flag(false), 
      has_null_flag(false), 
      null_flag('\0'),
      concurrency(100),
      has_table_title(false),
      bad_file_(NULL),
      is_delete(false),
      is_rowbyrow(false),
      trim_flag(-1), //add by pangtz:20141126
      is_append_date_(false),//add by pangtz:20141126
      has_decimal_to_varchar(false),
      decimal_to_varchar_grammar(NULL),
      has_substr(true), //add by pangtz:20141217
      substr_grammar(NULL),
      record_persql_(100),
      correctfile_(NULL),
      user_name_(NULL),
      passwd_(NULL),
      db_name_(NULL),
      timeout_(0),
      has_column_map(false),
      column_map_grammar(NULL),
      has_char_delima(false),
      char_delima('\"'),
      columns_with_char_delima_grammar(NULL),
      all_columns_have_char_delima(false),
      varchar_not_null(false),
      progress(false),
      case_sensitive(false),
      keep_double_quotation_marks(false),
      import_limit_ups_memory(OB_IMPORT_LIMIT_UPS_MEMORY)
      { }

  std::vector<ColumnDesc> col_descs;
  std::string table_name;
  std::string data_file;
  std::string appended_date;//add by pangtz:20141126
//int input_column_nr;
  RecordDelima delima;
  char * delima_str;
  RecordDelima rec_delima;
  char * rec_delima_str;
  bool has_nop_flag;
  char nop_flag;
  bool has_null_flag;
  char null_flag;
  int concurrency;                              /* default 5 threads */
  bool has_table_title;
  const char *bad_file_;
  bool is_delete;
  bool is_replace;
  bool is_insert;
  bool is_rowbyrow;
  int trim_flag; //add by pangtz:20141115
  bool is_append_date_; //add by pangtz:20141126
  bool has_decimal_to_varchar;
  char * decimal_to_varchar_grammar;
  mutable std::set<int> decimal_to_varchar_set;
  bool has_substr;//add by pangtz:20141217 for substr
  char * substr_grammar;
  mutable std::map<int, SubstrInfo> substr_map;
  int64_t record_persql_;
  bool ignore_merge_;
  const char *correctfile_;
  const char *user_name_;
  const char *passwd_;
  char * db_name_;
  int timeout_;
  bool not_dio;
  bool has_column_map;
  char *column_map_grammar;
  bool has_char_delima;
  char char_delima;
  char * columns_with_char_delima_grammar;
  bool all_columns_have_char_delima;
  mutable std::set<int> columns_with_char_delima_set;
  bool varchar_not_null;
  bool progress;
  bool case_sensitive;
  bool keep_double_quotation_marks;
  int64_t import_limit_ups_memory;
};

class ImportParam {
  public:
    ImportParam();

    int load(const char *file);

    int get_table_param(const char *table_name, TableParam &param);

    void PrintDebug();
  private:
    std::vector<TableParam> params_;
};

#endif

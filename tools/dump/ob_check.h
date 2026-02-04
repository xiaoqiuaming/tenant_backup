#ifndef __OB_CHECK_H__
#define  __OB_CHECK_H__

#include <cstdlib>
#include <vector>
#include <map>
#include <unistd.h>
#include "common/utility.h"
#include "common/ob_define.h"
#include "ob_check_param.h"
#include "tokenizer_v2.h"
#include "file_reader_v2.h"
#include "oceanbase_db.h"
#include "file_appender.h"
#include "yysys.h" //add by pangtz:20141205
#include "oceanbase_db.h"
using namespace oceanbase::common;
using namespace oceanbase::api;
using namespace oceanbase::sql;


//add by pangtz:20141203 [ob_import������־��]
class ObImportLogInfo{
    public:

        ObImportLogInfo();
        ~ObImportLogInfo(){}

        inline void set_datafile_name(string name){datafile_name = name;}
        inline std::string get_datafile_name(){return datafile_name;}
        inline void set_table_name(string name){table_name = name;}
        inline std::string get_table_name(){return table_name;}

        inline void set_wait_time_sec(int64_t time_sec){wait_time_sec_ = time_sec;}
        inline int64_t get_wait_time_sec(){ return wait_time_sec_;}
        inline void set_wait_ups_mem_time(int64_t time_sec){wait_ups_mem_time_ = time_sec;}
        inline int64_t get_wait_ups_mem_time(){return wait_ups_mem_time_;}
        inline void set_begin_time(){import_begin_time_ =  yysys::CTimeUtil::getTime();}

        inline void set_end_time(){import_end_time_ = yysys::CTimeUtil::getTime();}
        inline const char *get_begin_time()
        {
            std::string begin_time_str = const_cast<char *>(time2str(import_begin_time_));
            return begin_time_str.substr(0,19).data();
        }
        inline const char *get_end_time()
        {
            std::string end_time_str = const_cast<char *>(time2str(import_end_time_));
            return end_time_str.substr(0,19).data();
        }
        inline int64_t get_during_time(){return (import_end_time_ - import_begin_time_)/1000000;}
        inline void set_processed_lineno(int64_t proc_lineno){processed_lineno_ = proc_lineno;}
        inline void set_bad_lineno(int64_t bad_lineno){bad_lineno_ = bad_lineno;}
        inline int64_t get_processed_lineno() const {return processed_lineno_;}
        inline int64_t get_bad_lineno() const {return bad_lineno_;}
        inline int64_t get_succ_lineno() const {return (processed_lineno_ - bad_lineno_);}
        inline void set_final_ret(int ret)
        {
            if(ret == 0 && bad_lineno_ == 0){
               final_ret_ = 0;
            }else if(ret == 0 && bad_lineno_ != 0){
               final_ret_ = 2;
            }else if(ret != 0){
                final_ret_ = 1;
            }

        }
        inline int get_final_ret(){return final_ret_;}

        void print_error_log();
    private:
        int64_t import_begin_time_;
        int64_t import_end_time_;
        int64_t during_time_;
        int64_t processed_lineno_;
        int64_t bad_lineno_;
        int64_t succ_lineno_;
        std::string datafile_name;
        std::string table_name;
        int final_ret_;
        int64_t wait_time_sec_;
        int64_t wait_ups_mem_time_;
};
//add e

class TestRowBuilder;

class ObRowBuilder {
  public:
    friend class TestRowBuilder;
  public:
    enum RowkeyDataType{
      INT8,
      INT64,
      INT32,
      VARCHAR,
      DATETIME,
      INT16
    };

  static const int kMaxRowkeyDesc = OB_MAX_ROWKEY_COLUMN_NUMBER;
  public:
    ObRowBuilder(ObSchemaManagerV2 *schema, TableParam &param);
    ~ObRowBuilder();

    int parse_decimal_2_varchar_grammar();
    bool is_decimal_to_varchar(int offset) const;
    bool has_char_delima(int offset) const;
    int fill_columns_desc_( const ObColumnSchemaV2 * column_schema, int column_schema_nr, std::vector<SequenceInfo> &vseq);
    int fill_columns_with_char_delima_set();
    int set_column_desc( std::vector<SequenceInfo> &vseq);
    int get_column_desc(const ColumnInfo *&columns_desc, int &columns_desc_nr);

    int build_sql(RecordBlock &block, ObString &sql);
    int check_format(const TokenInfo *tokens,
                     const int token_nr,
                     const ColumnInfo *columns_desc,
                     const int columns_desc_nr, char token_buf[]) const;
    int check_format_strict(const TokenInfo *tokens,
                            const int token_nr,
                            const ColumnInfo *columns_desc,
                            const int columns_desc_nr, char token_buf[]) const;
    int tokens_format(const TokenInfo *tokens_in,
                      int token_in_nr,
                      char *tokens_buffer,
                      int64_t tokens_buffer_size,
                      TokenInfo *tokens_out,
                      int &tokens_out_nr,
                      const TableParam &table_param,
                      const ColumnInfo *columns_desc,
                      const int columns_desc_nr,
                      char token_buf[]
                      ) const;

    int construct_replace_sql(const TokenInfo *tokens,
                              const int token_nr,
                              const ColumnInfo *columns_desc,
                              const int columns_desc_nr,
                              const ObString table_name,
                              ObString &sql);

    int construct_insert_sql(const TokenInfo *tokens,
                             const int token_nr,
                             const ColumnInfo *columns_desc,
                             const int columns_desc_nr,
                             const ObString table_name,
                             ObString &sql);

    int construct_delete_sql(const TokenInfo *tokens,
                             const int token_nr,
                             const ColumnInfo *columns_desc,
                             const int columns_desc_nr,
                             const int64_t *rowkey_offset,
                             int64_t rowkey_offset_nr,
                             const ObString table_name,
                             ObString &sql);

    int construct_replace_or_insert_sql(
        const char *function_name,
        const TokenInfo *tokens,
        const int token_nr,
        const ColumnInfo *columns_desc,
        const int columns_desc_nr,
        const ObString table_name,
        ObString &sql);
    int columns_desc_fail_at(const ColumnInfo *columns_desc,
                             const int columns_desc_nr
                             );
    int add_token_2_str(const TokenInfo *token,
                        const ColumnInfo *column_desc, int offset,
                        char str[]
                        );

    int tokens_2_str(
        const TokenInfo *tokens,
        const int token_nr,
        const ColumnInfo *columns_desc,
        char str[]
        );
    int tokens_2_str(
                     const TokenInfo *tokens,
                     const int token_nr,
                     const ColumnInfo *columns_desc,
                     const int64_t *offsets,
                     int64_t offset_nr,
                     char str[]
        );
    int columns_2_str(
                      const ColumnInfo *columns_desc,
                      const int columns_desc_nr,
                      char str[]
        );
    int columns_2_str(
                      const ColumnInfo *columns_desc,
                      const int columns_desc_nr,
                      const int64_t *offsets,
                      int64_t offset_nr,
                     char str[]
        );
    int tokens_and_columns_2_str(const TokenInfo *tokens,
                                 const ColumnInfo *columns_desc,
                                 const int64_t *rowkey_offset,
                                 int64_t rowkey_offset_nr,
                                 char str[]
                                 );
    int add_escape_char(const char *str,
                        const int64_t str_len,
                        char *out_buffer,
                        int64_t capacity,
                        int64_t &out_buffer_len,
                        bool between_quotation_marks) const;
    bool has_all_rowkey(const ColumnInfo *columns_desc,
                        const int columns_desc_nr,
                        const ObString table_name);

    //int set_rowkey_desc(const std::vector<RowkeyDesc> &rowkeys);

    bool check_valid();

    inline int get_lineno() const
    {
      return atomic_read(&lineno_);
    }

    //add by pangtz:20141203 [bad_lineno_���úͻ�ȡ]
    inline void add_bad_lineno()
    {
         atomic_add(1, &bad_lineno_);
    }
    inline int64_t get_bad_lineno()
    {
        return atomic_read(&bad_lineno_);
    }
    //add e

    inline void add_bad_lineno(int num)
    {
      atomic_add(num, &bad_lineno_);
    }


  private:
    ObSchemaManagerV2 *schema_;
    ColumnInfo columns_desc_[OB_MAX_COLUMN_NUMBER];
    mutable int columns_desc_nr_;

    //RowkeyDesc rowkey_desc_[kMaxRowkeyDesc];
    int64_t rowkey_desc_nr_;
    int64_t rowkey_offset_[kMaxRowkeyDesc];
    mutable atomic_t lineno_;
    //add by pangtz:20141203 [��¼bad��¼��]
    mutable atomic_t bad_lineno_;
    //add e
    int64_t rowkey_max_size_;
    TableParam &table_param_;
    AppendableFile *bad_file_;
//  char *line_buffer_;
    static const int64_t LINE_BUFFER_SIZE = 1024 * 1024 * 2; //2M
    CharArena allocator_;
};

template <typename T>
int strs_2_idx_vector(const char *strs, const char delima, const T *a, const int al, bool(*eq)(const std::string &, const T &), std::vector<int> & vi)
{
  if (!strs) return OB_SUCCESS;
  int ret = OB_SUCCESS;
  std::vector<string> vs;
  Tokenizer::tokenize(strs, vs, delima);
  std::vector<string>::iterator it = vs.begin();
  for (; it != vs.end(); it++)
  {
    int i = 0;
    for (; i < al && !eq(*it, a[i]); i++);
    if (i >= al)
    {
      ret = OB_ERROR;
      break;
    }
    else
    {
      vi.push_back(i);
    }
  }
  return ret;
}

bool str_eq_schema(const std::string & str, const ObColumnSchemaV2 & sch);
bool str_eq_column_info(const std::string & str, const ColumnInfo & ci);
bool has_char_delima(const char * str, int len, char delima);
bool is_token_null(const TokenInfo & token_info, const ColumnInfo & columns_desc, bool has_null_flag, char null_flag, bool varchar_not_null);
bool check_token_null(const TokenInfo & token_info, const ColumnInfo & columns_desc);
int check_str_to_int32(const char * str, const int64_t len, int64_t & value);
int check_str_to_int64(const char * str, const int64_t len, int64_t & value);

void string_2_delima(const char * str, RecordDelima & delima);
int parse_column_map_grammar(const char * grammar, int & columns_sum, std::vector<ColumnMapInfo> & v, std::map<int, SubstrInfo> & m, std::vector<SequenceInfo> & vseq);

template<class T>
int numbers_2_idx_vector(std::vector<T> & vi, const char * numbers, int len = INT_MAX, const char delima = ',')
{
  int ret = OB_SUCCESS;
  int i = 0;
  T n = 0;
  T sgn = 1;
  for (;; i++)
  {
    if (!numbers[i] || i == len || numbers[i] == delima)
    {
      vi.push_back(n * sgn);
      n = 0;
      sgn = 1;
      if (!numbers[i] || i == len)
      {
        break;
      }
    }
    else if (isdigit(numbers[i]))
    {
      n = numbers[i] - '0' + n * 10;
    }
    else if (n == 0 && numbers[i] == '-')
    {
      sgn = -1;
    }
    else
    {
      YYSYS_LOG(ERROR, "Not a Number.");
      ret = OB_ERROR;
      break;
    }
  }
  return ret;
}

bool type_has_quotation_marks(ObObjType tp);

bool type_supports_sequence(ObObjType tp);
void get_sequence_prefix(const char *db_name, const char *table_name, char str[IMPORT_SEQUENCE_MAX_LENGTH]);
int cb_sequence(OceanbaseDb & db, const char *db_name, const char *table_name, const std::vector<SequenceInfo> & vseq, bool create);
bool decimal_needs_point(const TokenInfo *token_info);

#endif

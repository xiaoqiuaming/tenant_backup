#ifndef DUMPRT_H
#define DUMPRT_H

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_result.h"
#include "common/ob_base_client.h"
#include "common/serialization.h"
#include "common/file_directory_utils.h"
#include "rootserver/ob_root_admin_cmd.h"
#include "rootserver/ob_root_server_config.h"
#include "rootserver/ob_root_table2.h"
#include "rootserver/ob_root_server2.h"
#include "rootserver/ob_chunk_server_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
using namespace oceanbase::rootserver;

namespace oceanbase
{
  namespace roottable
  {
    struct CmdLineParam
    {
        const char *rootserver_ip_;
        int32_t rootserver_port_;
        const char *cmd_type_;
        const char *cmd_content_;
        int64_t table_id_;
        const char *output_format_;
        const char *roottable_directory_;
        bool newest_;
        CmdLineParam()
        {
          rootserver_ip_ = NULL;
          rootserver_port_ = 0;
          cmd_type_ = NULL;
          cmd_content_ = NULL;
          table_id_ = 0;
          output_format_ = NULL;
          roottable_directory_ = NULL;
          newest_ = false;
        }
    };

    struct TabletStatByTable
    {
        int64_t table_id_;
        int64_t tablet_num_per_cs_[ObChunkServerManager::MAX_SERVER_COUNT];
        int64_t tablet_size_per_cs_[ObChunkServerManager::MAX_SERVER_COUNT];

        TabletStatByTable()
        {
          table_id_ = 0;
          memset(tablet_num_per_cs_, 0, sizeof(tablet_num_per_cs_));
          memset(tablet_size_per_cs_, 0, sizeof(tablet_size_per_cs_));
        }
    };

    class DumpRootTable
    {
      public:
        DumpRootTable();
        ~DumpRootTable();

        void parse_cmd_line(int argc, char **argv);

        int init();
        int handle();

      private:
        int init_root_server();
        int init_root_table_info();
        int init_cs_info();
        int do_check_point();
        int handle_tablet_stat_by_table();
        int print_tablet_stat_by_table(const TabletStatByTable *tablet_stat, int64_t len);
        bool is_file_existed(const char *filename);
        int format_size(int64_t raw_size, char *formatted_size, int64_t max_formatted_size_length);

        void usage();
        ObServer *pserver_;
        ObBaseClient client_;
        CmdLineParam clp_;
        ObRootServerConfig config_;
        ObTabletInfoManager *rt_tim_;
        ObRootTable2 *rt_table_;
        ObChunkServerManager server_manager_;
        static const char *DEFAULT_IP;
        static const int32_t DEFAULT_PORT = 2500;
        static const char *CMD_TYPE_DUMP;
        static const char *CMD_TYPE_CHECK;
        static const char *CMD_CONTENT_TABLET_STAT;
        static const char *CMD_CONTENT_TABLET_DETAIL;
        static const char *CMD_CONTENT_CS_TABLET_STAT;
        static const char *CMD_CONTENT_CHECK_INTEGRITY;
        static const char *CMD_CONTENT_CHECK_LOST;
        static const char *CMD_CONTENT_CHECK_CRC;
        static const char *OUTPUT_FORMAT_PLAIN;
        static const char *OUTPUT_FORMAT_JSON;
        static const char *OUTPUT_FORMAT_COMMAND;
        static const int32_t MIN_PORT = 1;
        static const int32_t MAX_PORT = 65535;
        static const int64_t DEFAULT_REQUEST_TIMEOUT_US = 20000000;
    };
    const char *DumpRootTable::DEFAULT_IP = "127.0.0.1";
    const char *DumpRootTable::CMD_TYPE_DUMP = "dump_roottable";
    const char *DumpRootTable::CMD_TYPE_CHECK = "check_roottable";
    const char *DumpRootTable::CMD_CONTENT_TABLET_STAT = "tablet_stat";
    const char *DumpRootTable::CMD_CONTENT_TABLET_DETAIL = "tablet_detail";
    const char *DumpRootTable::CMD_CONTENT_CS_TABLET_STAT = "cs_tablet_stat";
    const char *DumpRootTable::CMD_CONTENT_CHECK_INTEGRITY = "check_integrity";
    const char *DumpRootTable::CMD_CONTENT_CHECK_LOST = "check_lost";
    const char *DumpRootTable::CMD_CONTENT_CHECK_CRC = "check_crc";
    const char *DumpRootTable::OUTPUT_FORMAT_PLAIN = "plain";
    const char *DumpRootTable::OUTPUT_FORMAT_JSON = "json";
    const char *DumpRootTable::OUTPUT_FORMAT_COMMAND = "command";
  }
}

#endif // DUMPRT_H

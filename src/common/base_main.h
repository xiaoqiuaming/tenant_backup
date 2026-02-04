/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-09-26
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#ifndef OCEANBASE_ROOTSERVER_BASE_MAIN_H_
#define OCEANBASE_ROOTSERVER_BASE_MAIN_H_
#include <yysys.h>

#include "ob_define.h"
namespace oceanbase
{
  namespace common
  {
    extern bool PACKET_RECORDER_FLAG;
    class BaseMain
    {
      public:
        //static BaseMain* instance() {
        //  return instance_;
        //}
        virtual ~BaseMain();
        virtual int start(const int argc, char *argv[]);
        virtual void destroy();

        /* restart server if and only if the *restart server flag* is
         * set. @func destroy should been executed before. */
        static  bool restart_server(int argc, char *argv[]);
        /* set *restart_server* flag, after that invoke restart_server
         * funtion to restart. otherwise restart_server function will
         * do nothing. */
        static  void set_restart_flag(bool restart = true);
      protected:
        BaseMain(const bool daemon = true);
        virtual void do_signal(const int sig);
        void add_signal_catched(const int sig);
        static BaseMain* instance_;
        virtual void print_usage(const char *prog_name);
        virtual void print_version();
        virtual void parse_cmd_line(const int argc, char *const argv[]);
        virtual int do_work() = 0;
        char config_[OB_MAX_FILE_NAME_LENGTH];
        char proxy_config_file_[OB_MAX_FILE_NAME_LENGTH];
        char cmd_data_dir_[OB_MAX_FILE_NAME_LENGTH];
        char cmd_prefix_dir_[OB_MAX_FILE_NAME_LENGTH];
        char cmd_rs_ip_[OB_IP_STR_BUFF];
        char cmd_master_rs_ip_[OB_IP_STR_BUFF];
        char cmd_datadir_[OB_MAX_FILE_NAME_LENGTH];
        char cmd_appname_[OB_MAX_APP_NAME_LENGTH];
        char cmd_devname_[OB_MAX_APP_NAME_LENGTH];
        char cmd_extra_config_[OB_MAX_EXTRA_CONFIG_LENGTH];
        int32_t cmd_cluster_id_;
        int32_t cmd_paxos_id_;//add shili [MUTIUPS] [START UPS]  20150427        
        int32_t cmd_rs_port_;
        int32_t cmd_master_rs_port_;
        int32_t cmd_port_;
        int32_t cmd_inner_port_;
        int32_t cmd_obmysql_port_;
        //add peiouya [MultiUPS] [UPS_Manage_Function] 20150520:b
        int32_t cmd_use_paxos_num_;
        int32_t cmd_use_cluster_num_;
        bool cmd_is_use_paxos_;
        //add for [118-rs_boot_configure_alter]-b
        int32_t cmd_input_count_; // ����CMD�����������(��ʱֻ����-G,-K,-F,-U,-u)
        //add [118]-e
        //add 20150520:e
        //add for [491-check_parameters_when_rs_reboot]-b
        bool cmd_input_G_;
        bool cmd_input_K_;
        //add for [491]-e
        //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
        int64_t cmd_start_major_version_;
        //add 20150521:e
        char cmd_ms_type_[OB_MAX_MS_TYPE_LENGTH];
        //add pangtianze [Paxos rs_election] 20150721:b
        int32_t cmd_max_rs_count_;
        //add:e
        //add wangjiahao [Paxos ups_replication] 20150601:b
        // modify for [paxos_ups_quorum_manage]-b
//        int32_t cmd_ups_quorum_scale_;
        char cmd_ups_quorum_scale_[OB_MAX_PAXOS_UPS_QUORUM_SCALES_LENGTH];
        // modify for [paxos_ups_quorum_manage]-e
        //add:e
        const char *pid_dir_;
        const char *log_dir_;
        const char *server_name_;
      private:
        bool use_daemon_;
        static void sign_handler(const int sig);
        static bool restart_;
    };

  }
}
#endif

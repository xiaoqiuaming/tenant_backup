/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *
 *   Version: 0.1
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - some work details if you want
 *      Author Name ...
 *
 */
#ifndef OCEANBASE_COMMON_OB_DEFINE_H_
#define OCEANBASE_COMMON_OB_DEFINE_H_

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <openssl/md5.h>

namespace oceanbase
{
  namespace common
  {
    const int OB_SUCCESS = EXIT_SUCCESS;
    const int OB_ERROR = EXIT_FAILURE;

    const uint16_t OB_COMPACT_COLUMN_INVALID_ID = UINT16_MAX;
    const uint64_t OB_INVALID_ID = UINT64_MAX;
    const int OB_INVALID_INDEX = -1;
    const int64_t OB_INVALID_VERSION = -1;
    //add wangjiahao [Paxos ups_replication_tmplog] 20150715 :b
    const int64_t OB_INVALID_TERM = -1;
    const int64_t OB_DEFAULT_TERM = 0;
    const int64_t OB_INVALID_VALUE = -1;
    //add pangtianze [Paxos] 20161020:b
    const int32_t OB_MAX_RS_COUNT = 9;
    const int32_t OB_MAX_UPS_COUNT = 9;
    //add:e
    const int64_t OB_MAX_ITERATOR = 16;
    const int64_t MAX_IP_ADDR_LENGTH = 32;
    const int64_t MAX_IP_PORT_LENGTH = MAX_IP_ADDR_LENGTH + 5;

    //[mysql8]
    const char* const OB_DEFAULT_AUTH_PLUGIN = "mysql_native_password";

    //modlijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160303:b
    //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150806:b
    const int64_t OB_INVALID_PAXOS_ID = -1;
    //add 20150806::e
    //mod 20160303:e
    //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160228:b
    const uint64_t OB_DEFAULT_OFFLINE_VERSION = -1;
    const int32_t OB_BITSET_INDEX = 0;
    const uint32_t OB_BITSET_MAX_WORD = 511;
    const int64_t OB_USE_NUM_DEFAULT_VALUE = 0;
    const char* const PAXOS_STAT = "paxos_stat";
    const char* const CLUSTER_STAT = "cluster_stat";
    //add 20160228:e

    //add for [513-delete cluster paxos]-b
    enum ObChangePaxosClusterType
    {
        ADD_PAXOS_GROUP = 0,
        DEL_PAXOS_GROUP = 1,
        TAKE_PAXOS_GROUP_ONLINE = 2,
        TAKE_PAXOS_GROUP_OFFLINE = 3,
        ADD_CLUSTER = 4,
        DEL_CLUSTER = 5,
        TAKE_CLUSTER_ONLINE = 6,
        TAKE_CLUSTER_OFFLINE = 7,
    };
    const char* const Op_Types[]={"add_paxos_group", "del_paxos_group", "take_paxos_group_online","take_paxos_group_offline", "add_cluster", "del_cluster", "take_cluster_online", "take_cluster_offline"};

    const char* const PAXOS_GROUP_ID_PARAM = "paxos_group_id";
    const char* const CLUSTER_ID_PARAM = "cluster_id";
    const char* const NEW_PAXOS_GROUP_NUM_PARAM = "new_paxos_group_num";
    const char* const NEW_CLUSTER_NUM_PARAM = "new_cluster_num";
    //add for [513-delete cluster paxos]-e

    //////////////////////////////////////////////////////////////// begin ob error code
    //error code for common -1 ---- -1000
    const int OB_OBJ_TYPE_ERROR = -1;
    const int OB_INVALID_ARGUMENT = -2;
    const int OB_ARRAY_OUT_OF_RANGE = -3;
    const int OB_SERVER_LISTEN_ERROR = -4;
    const int OB_INIT_TWICE = -5;
    const int OB_NOT_INIT = -6;
    const int OB_NOT_SUPPORTED = -7;
    const int OB_ITER_END = -8;
    const int OB_IO_ERROR = -9;
    const int OB_ERROR_FUNC_VERSION = -10;
    const int OB_PACKET_NOT_SENT = -11;
    const int OB_RESPONSE_TIME_OUT = -12;
    const int OB_ALLOCATE_MEMORY_FAILED = -13;
    const int OB_MEM_OVERFLOW = -14;
    const int OB_ERR_SYS = -15;
    const int OB_ERR_UNEXPECTED = -16;
    const int OB_ENTRY_EXIST = -17;
    const int OB_ENTRY_NOT_EXIST = -18;
    const int OB_SIZE_OVERFLOW = -19;
    const int OB_REF_NUM_NOT_ZERO = -20;
    const int OB_CONFLICT_VALUE = -21;
    const int OB_ITEM_NOT_SETTED = -22;
    const int OB_EAGAIN = -23;
    const int OB_BUF_NOT_ENOUGH = -24;
    const int OB_PARTIAL_FAILED = -25;
    const int OB_READ_NOTHING = -26;
    const int OB_FILE_NOT_EXIST = -27;
    const int OB_DISCONTINUOUS_LOG = -28;
    const int OB_SCHEMA_ERROR = -29;
    const int OB_DATA_NOT_SERVE = -30;       // not host this data
    const int OB_UNKNOWN_OBJ = -31;
    const int OB_NO_MONITOR_DATA = -32;
    const int OB_SERIALIZE_ERROR = -33;
    const int OB_DESERIALIZE_ERROR = -34;
    const int OB_AIO_TIMEOUT = -35;
    const int OB_NEED_RETRY = -36; // need retry
    const int OB_TOO_MANY_SSTABLE = -37;
    const int OB_NOT_MASTER = -38; // !!! don't modify this value, OB_NOT_MASTER = -38
    const int OB_TOKEN_EXPIRED = -39;
    const int OB_ENCRYPT_FAILED = -40;
    const int OB_DECRYPT_FAILED = -41;
    const int OB_USER_NOT_EXIST = -42;
    const int OB_PASSWORD_WRONG = -43;
    const int OB_SKEY_VERSION_WRONG = -44;
    const int OB_NOT_A_TOKEN = -45;
    const int OB_NO_PERMISSION = -46;
    const int OB_COND_CHECK_FAIL = -47;
    const int OB_NOT_REGISTERED = -48;
    const int OB_PROCESS_TIMEOUT = -49;
    const int OB_NOT_THE_OBJECT = -50;
    const int OB_ALREADY_REGISTERED = -51;
    const int OB_LAST_LOG_RUINNED = -52;
    const int OB_NO_CS_SELECTED = -53;
    const int OB_NO_TABLETS_CREATED = -54;
    const int OB_INVALID_ERROR = -55;
    const int OB_CONN_ERROR = -56;
    const int OB_DECIMAL_OVERFLOW_WARN = -57;
    const int OB_DECIMAL_UNLEGAL_ERROR = -58;
    const int OB_OBJ_DIVIDE_BY_ZERO = -59;
    const int OB_OBJ_DIVIDE_ERROR = -60;
    const int OB_NOT_A_DECIMAL = -61;
    const int OB_DECIMAL_PRECISION_NOT_EQUAL = -62;
    const int OB_EMPTY_RANGE = -63; // get emptry range
    const int OB_SESSION_KILLED = -64;
    const int OB_LOG_NOT_SYNC = -65;
    const int OB_DIR_NOT_EXIST = -66;
    const int OB_NET_SESSION_END = -67;
    const int OB_INVALID_LOG = -68;
    const int OB_FOR_PADDING = -69;
    const int OB_INVALID_DATA = -70;
    const int OB_ALREADY_DONE = -71;
    const int OB_CANCELED = -72;
    const int OB_LOG_SRC_CHANGED = -73;
    const int OB_LOG_NOT_ALIGN = -74;
    const int OB_LOG_MISSING = -75;
    const int OB_NEED_WAIT = -76;
    const int OB_NOT_IMPLEMENT = -77;
    const int OB_DIVISION_BY_ZERO = -78;
    const int OB_VALUE_OUT_OF_RANGE = -79;
    const int OB_EXCEED_MEM_LIMIT = -80;
    const int OB_RESULT_UNKNOWN = -81;
    const int OB_ERR_DATA_FORMAT = -82;
    const int OB_MAYBE_LOOP = -83;
    const int OB_NO_RESULT= -84;
    const int OB_QUEUE_OVERFLOW = -85;
    const int OB_START_LOG_CURSOR_INVALID = -99;
    const int OB_LOCK_NOT_MATCH = -100;
    const int OB_DEAD_LOCK = -101;
    const int OB_PARTIAL_LOG = -102;
    const int OB_CHECKSUM_ERROR = -103;
    const int OB_INIT_FAIL = -104;
    const int OB_ASYNC_CLIENT_WAITING_RESPONSE = -108;
    const int OB_STATE_NOT_MATCH = -109;
    const int OB_READ_ZERO_LOG = -110;
    const int OB_SWITCH_LOG_NOT_MATCH = -111;
    const int OB_LOG_NOT_START = -112;
    const int OB_IN_FATAL_STATE = -113;
    const int OB_IN_STOP_STATE = -114;
    const int OB_UPS_MASTER_EXISTS = -115;
    const int OB_LOG_NOT_CLEAR = -116;
    const int OB_FILE_ALREADY_EXIST = -117;
    const int OB_UNKNOWN_PACKET = -118;
    const int OB_TRANS_ROLLBACKED = -119;
    const int OB_LOG_TOO_LARGE = -120;

    const int OB_RPC_SEND_ERROR = -121;
    const int OB_RPC_POST_ERROR = -122;
    const int OB_LIBEASY_ERROR = -123;
    const int OB_CONNECT_ERROR = -124;
    const int OB_NOT_FREE = -125;
    const int OB_INIT_SQL_CONTEXT_ERROR = -126;
    const int OB_SKIP_INVALID_ROW = -127;

    const int OB_SYS_CONFIG_TABLE_ERROR = -131;
    const int OB_READ_CONFIG_ERROR = -132;

    const int OB_TRANS_NOT_MATCH = -140;
    const int OB_TRANS_IS_READONLY = -141;
    const int OB_ROW_MODIFIED = -142;
    const int OB_VERSION_NOT_MATCH = -143;
    const int OB_BAD_ADDRESS = -144;
    const int OB_DUPLICATE_COLUMN = -145;
    const int OB_ENQUEUE_FAILED= -146;
    const int OB_INVALID_CONFIG = -147;
    const int OB_WAITING_COMMIT = -148;
    const int OB_STMT_EXPIRED = -149;
    const int OB_DISCARD_PACKET = -150;
    //add liu jun. [MultiUPS] [rules_cache] 20150511:b
    const int OB_TABLE_NODE_NOT_EXIST = -200;
    const int OB_PART_NODE_NOT_EXIST = -201;
    const int OB_GROUP_NOT_EXIST = -202;
    const int OB_ERR_NO_POSITION = -203;
    const int OB_EMPTY_RESULT_SET = -204;
    //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160413:b
#define IS_PARTITION_INTERNAL_ERR(e) (OB_TABLE_NODE_NOT_EXIST == e  \
    || OB_GROUP_NOT_EXIST == e \
    || OB_ERR_NO_PARTITION == e)
    const int OB_INCOMPLETE_ROW = -205;
    //add 20160413:e
    //add 20150511:e
    //add zhaoqiong [Schema Manager] 20150514:b
    const int OB_SCHEMA_SYNC_ERROR = -151;
    //add:e

    //add liuxiao[secondary index] 20150609
    const int OB_INDEX_NOT_EXIST = -152;
    //add e
   //add dolphin[coalesce return type]@20151201:b
    const int OB_VARCHAR_DECIMAL_INVALID = -153;
    const int OB_CONSISTENT_MATRIX_DATATYPE_INVALID = -154;

    const int OB_TABLE_UPDATE_LOCKED = -155; //add zhaoqiong [Truncate Table]:20160318
    //add gaojt [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
    const int OB_UD_PARALLAL_DATA_NOT_SAFE = -156;
    //add gaojt 20160531:e
    //add lbzhong [Paxos Cluster.Balance] 20160704:b
    const int OB_CLUSTER_ID_ERROR = -157;
    const int OB_REPLICA_NUM_OVERFLOW = -158;
    const int OB_NO_CS_ALIVE = -159;
    //add:e
    //add pangtianze [Paxos] 20160926:b
    const int OB_IS_UPS_MASTER = -160;
    const int OB_SERVER_STILL_ONLINE = -161;
    //add:e
    //add bingo [Paxos ms in slave rs] 20170517:b
    const int OB_NEED_RESET_MS_LIST = -162;
    //add:e

    //add for [update rowkey]-b
    const int OB_NOT_SUPPORT_FOR_UPDATE_ROWKEY = -163;
    const int OB_NOT_EQUAL_ROWS = -164;
    //add for [update rowkey]-e

    //add for [sum_int32 overflow rowkey]-b
    const int OB_SUM_OVERFLOW = -165;
    //add for [sum_int32 overflow rowkey]-e

  //error code for chunk server -1001 ---- -2000
    const int OB_CS_CACHE_NOT_HIT = -1001;   // ����û������
    const int OB_CS_TIMEOUT = -1002;         // ��ʱ
    const int OB_CS_TABLET_NOT_EXIST = -1008; // tablet not exist
    const int OB_CS_EMPTY_TABLET = -1009;     // tablet has no data.
    const int OB_CS_EAGAIN = -1010;           //����

    const int OB_GET_NEXT_COLUMN = -1011;
    const int OB_GET_NEXT_ROW = -1012; // for internal use, scan next row.
    //const int OB_DESERIALIZE_ERROR = -1013;//�����л�ʧ��
    const int OB_INVALID_ROW_KEY = -1014;//���Ϸ���rowKey
    const int OB_SEARCH_MODE_NOT_IMPLEMENT = -1015; // search mode not implement, internal error
    const int OB_INVALID_BLOCK_INDEX = -1016; // illegal block index data, internal error
    const int OB_INVALID_BLOCK_DATA = -1017;  // illegal block data, internal error
    const int OB_SEARCH_NOT_FOUND = -1018;    // value not found? for cs_get
    const int OB_BEYOND_THE_RANGE = -1020;    // search key or range not in current tablet
    const int OB_CS_COMPLETE_TRAVERSAL = -1021; //complete traverse block cache
    const int OB_END_OF_ROW = -1022;
    const int OB_CS_MERGE_ERROR = -1024;
    const int OB_CS_SCHEMA_INCOMPATIBLE = -1025;
    const int OB_CS_SERVICE_NOT_STARTED = -1026;
    const int OB_CS_LEASE_EXPIRED = -1027;
    const int OB_CS_MERGE_HAS_TIMEOUT = -1028;
    const int OB_CS_TABLE_HAS_DELETED = -1029;
    const int OB_CS_MERGE_CANCELED = -1030;
    const int OB_CS_COMPRESS_LIB_ERROR = -1031;
    const int OB_CS_OUTOF_DISK_SPACE = -1032;
    const int OB_CS_MIGRATE_IN_EXIST = -1033;
    const int OB_AIO_BUF_END_BLOCK = -1034;
    const int OB_AIO_EOF = -1035;
    const int OB_AIO_BUSY = -1036;
    const int OB_WRONG_SSTABLE_DATA = -1037;
    const int OB_COLUMN_GROUP_NOT_FOUND = -1039;
    const int OB_NO_IMPORT_SSTABLE = -1040;
    const int OB_IMPORT_SSTABLE_NOT_EXIST = -1041;
    //add liumz, [secondary index static_index_build] 20150320:b
    const int OB_CS_STATIC_INDEX_TIMEOUT = -1042;
    //add:e
    //add wenghaixixing [secondary index static_index_build.exceptional_handle] 20150407
    const int OB_TABLET_HAS_NO_LOCAL_SSTABLE = -1043;
    const int OB_TABLET_FOR_INDEX_ALL_FAILED = -1044;
    const int OB_INDEX_BUILD_FAILED = -1045;
    //add e

    const int OB_EXPIRE_CONDITION_ERROR = -1046;

    const int OB_CS_EMPTY_MERGE = -1047;

    const int OB_STATISTICS_COLLECTION_RUNNING = -1048;
    const int OB_COLLECTION_DURING_MERGE = -1049;

    //error code for update server -2001 ---- -3000
    const int OB_UPS_TRANS_RUNNING = -2001;     // ��������ִ��
    const int OB_FREEZE_MEMTABLE_TWICE = -2002; // memtable has been frozen
    const int OB_DROP_MEMTABLE_TWICE = -2003;   // memtable has been dropped
    const int OB_INVALID_START_VERSION = -2004; // memtable start version invalid
    const int OB_UPS_NOT_EXIST = -2005;         // not exist
    const int OB_UPS_ACQUIRE_TABLE_FAIL = -2006;// acquire table via version fail
    const int OB_UPS_INVALID_MAJOR_VERSION = -2007;
    const int OB_UPS_TABLE_NOT_FROZEN = -2008;
    const int OB_UPS_CHANGE_MASTER_TIMEOUT = -2009;
    const int OB_FORCE_TIME_OUT = -2010;
    const int OB_BEGIN_TRANS_LOCKED = -2011;
    //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
    const int OB_NOT_NEED_FROZEN = -2012;
    //add 20150521:e

    const int OB_CHECK_VERSION_RETRY = -2014;

    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    const int OB_ALREADY_WRITE_STAT_LOG = -2100;
    const int OB_MEM_VERSION_NOT_MATCH = -2101;
    //add 20150701:e
    //add liuzy [MultiUPS] [UPS_Manager] 20151214:b
    const int OB_UPS_MANAGER_NODE_EXISTS = -2102;
    const int OB_UPS_MANAGER_NODE_INEXISTENT = -2103;
    const int OB_UPS_MANAGER_UP_NODE_INEXISTENT = -2104;
    const int OB_UPS_MANAGER_LEFT_NODE_INEXISTENT = -2105;
    const int OB_UPS_MANAGER_IDX_INVALID = -2106;
    const int OB_NEW_PAXOS_GROUP_NUM_OUT_OF_RANGE = -2107;
    const int OB_UPS_MANAGER_POINTER_ERROR = -2108;
    const int OB_NEW_CLUSTER_NUM_OUT_OF_RANGE = -2109; //add liuzy [MultiUPS] [add_cluster_interface] 20160311
    const int OB_CURRENT_PAXOS_HAS_ENOUGH_UPS = -2110;
    //add 20151214:e
    //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160223:b
    const int OB_CURRENT_SYSTEM_MERGE_DOING = -2111;
    const int OB_CURRENT_PAXOS_GROUP_OFFLINE = -2112;
    const int OB_SET_PAXOS_OFFLINE_BITSET_FAILED = -2113;
    //add 20160223:e
    //add liuzy [MultiUPS] [add_cluster_interface] 20160311:b
    const int OB_UPS_MANAGER_SYNC_MOD_WRONG = -2114;
    //add 20160311:e
    //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160325:b
    const int OB_CURRENT_CLUSTER_OFFLINE = -2115;
    const int OB_SET_CLUSTER_OFFLINE_BITSET_FAILED = -2116;
    const int OB_UPS_REMAIN_COUNT_INVALID = -2117;
    const int OB_RS_REMAIN_COUNT_INVALID = -2118;
    const int OB_RS_INVALID_LAST_FROZEN_VERSION = -2119;
    const int OB_RS_INVALID_CLUSTER_STAT_INFO = -2120;
    const int OB_CURRENT_CLUSTER_HAS_MASTER_RS = -2121;
    //add 20160325:e
    //add liuzy [MultiUPS] [take_online_interface] 20160418:b
    const int OB_CURRENT_PAXOS_GROUP_ONLINE = -2122;
    const int OB_CURRENT_CLUSTER_ONLINE = -2123;
    const int OB_CLUSTER_COUNT_INCONSISTENT_WITH_CONFIG = -2124;
    const int OB_PAXOS_GROUP_COUNT_INCONSISTENT_WITH_CONFIG = -2125;
    //add 20160418:e
    //add by maosy  [MultiUps 1.0] [secondary index optimize]20170719 b:
    const int OB_MEMTABLE_UNEXPECT_ERROR  = -2126 ;
    // add by e

    const int OB_UPS_TABLE_NOT_EXIST = -2127;

    const int OB_FETCH_DIS_TRANS_STAT_ERR = -2128;

    const int OB_NOT_THE_LAST_IDX = -2129;

    const int OB_PAXOS_GROUP_NOT_OFFLINE_CURRENT_VERSION = -2130;
    const int OB_CLUSTER_NOT_OFFLINE_CURRENT_VERSION = -2131;

    const int OB_RS_INVALID_CLUSTER_OR_PAXOS_NUM = -2132;

    //error code for root server -3001 ---- -4000
    const int OB_ERROR_TIME_STAMP = -3001;
    const int OB_ERROR_INTRESECT = -3002;
    const int OB_ERROR_OUT_OF_RANGE = -3003;
    const int OB_RS_STATUS_INIT = -3004;
    const int OB_IMPORT_NOT_IN_SERVER = -3005;
    const int OB_FIND_OUT_OF_RANGE = -3006;
    const int OB_CONVERT_ERROR = -3007;
    const int OB_MS_ITER_END = -3008;
    const int OB_MS_NOT_EXIST = -3009;
    const int OB_BYPASS_TIMEOUT = -3010;
    const int OB_BYPASS_DELETE_DONE = -3011;
    const int OB_RS_STATE_NOT_ALLOW = -3012;
    const int OB_BYPASS_NEED_REPORT = -3014;
    const int OB_ROOT_TABLE_CHANGED = -3015;
    const int OB_ROOT_REPLICATE_NO_DEST_CS_FOUND = -3016;
    const int OB_ROOT_TABLE_RANGE_NOT_EXIST = -3017;
    const int OB_ROOT_MIGRATE_CONCURRENCY_FULL = -3018;
    const int OB_ROOT_MIGRATE_INFO_NOT_FOUND = -3019;
    const int OB_NOT_DATA_LOAD_TABLE = -3020;
    const int OB_DATA_LOAD_TABLE_DUPLICATED = -3021;
    const int OB_ROOT_TABLE_ID_EXIST = -3022;
    const int OB_CLUSTER_ALREADY_MASTER = -3023;
    const int OB_IP_PORT_IS_NOT_SLAVE_CLUSTER = -3024;
    const int OB_CLUSTER_IS_NOT_SLAVE = -3025;
    const int OB_CLUSTER_IS_NOT_MASTER = -3026;
    const int OB_CONFIG_NOT_SYNC = -3027;
    const int OB_IP_PORT_IS_NOT_CLUSTER = -3028;
    const int OB_MASTER_CLUSTER_NOT_EXIST = -3029;
    const int OB_CLUSTER_INFO_NOT_EXIST = -3030;
    const int OB_GET_CLUSTER_MASTER_UPS_FAILED = -3031;
    const int OB_MULTIPLE_MASTER_CLUSTERS_EXIST = -3032;
    const int OB_MULTIPLE_MASTER_CLUSTERS_NOT_EXIST = -3033;
    const int OB_MASTER_CLUSTER_ALREADY_EXISTS = -3034;
    const int OB_CREATE_TABLE_TWICE = -3035;
    const int OB_BROADCAST_MASTER_CLUSTER_ADDR_FAIL = -3036;
    //add pangtianze [Paxos ups_replication] 20150604:b
    const int OB_NOT_ENOUGH_UPS_ONLINE = -3037;
    const int OB_LEADER_EXISTED = -3038;
    const int OB_REGISTER_TO_SLAVE_RS = -3039;
    //add:e
    //add chujiajia [Paxos rs_election] 20161101:b
    const int OB_SERVER_COUNT_ENOUGH = -3041;
    //const int OB_SEND_COMMAND_TO_SLAVE = -3042;
    const int OB_IS_RS_MASTER = -3043;
    const int OB_INVALID_QUORUM_SCALE = -3045;
    const int OB_INVALID_PAXOS_NUM = -3046;
    const int OB_UPS_NOT_FOLLOWER = -3047;
    //add:e
    //add huangjianwei [Paxos ups_replication] 20160310:b
    const int OB_IS_ALREADY_THE_MASTER = -3048;
    //add:e
    //add pangtianze [Paxos rs_election] 20160926:b
    const int OB_RS_DOING_ELECTION = -3051;
    const int OB_RS_NOT_EXIST = -3052;
    //add:e
    //add pangtianze [Paxos] 20170523:b
    const int OB_UPS_TIMEOUT = -3053;
    //add:e
    //add wangdonghui [ups_replication] 20170822 :b
    const int OB_GET_TERM_TIMEOUT = -3054;
    //add :e
    const int OB_DATA_SOURCE_NOT_EXIST = -3100;
    const int OB_DATA_SOURCE_TABLE_NOT_EXIST = -3101;
    const int OB_DATA_SOURCE_RANGE_NOT_EXIST = -3102;
    const int OB_DATA_SOURCE_DATA_NOT_EXIST = -3103;
    const int OB_DATA_SOURCE_SYS_ERROR = -3104;
    const int OB_DATA_SOURCE_TIMEOUT = -3105;
    const int OB_DATA_SOURCE_CONCURRENCY_FULL = -3106;
    const int OB_DATA_SOURCE_WRONG_URI_FORMAT = -3107;
    const int OB_SSTABLE_VERSION_UNEQUAL = -3108;
    const int OB_DATA_SOURCE_READ_ERROR = -3109;
    const int OB_ROOT_MIGRATE_INFO_EXIST = -3110;
    const int OB_ROOT_RANGE_NOT_CONTINUOUS = -3111;
    const int OB_DATA_SOURCE_TABLET_VERSION_ERROR = -3112;
    const int OB_DATA_LOAD_TABLE_STATUS_ERROR = -3113;

    //error code for merge server -4000 ---- -5000
    const int OB_INNER_STAT_ERROR = -4001;     // inner stat check error
    const int OB_OLD_SCHEMA_VERSION = -4002;   // find old schema version
    const int OB_INPUT_PARAM_ERROR = -4003;    // check input param error
    const int OB_NO_EMPTY_ENTRY = -4004;       // not find empty entry
    const int OB_RELEASE_SCHEMA_ERROR = -4005; // release schema error
    const int OB_ITEM_COUNT_ERROR = -4006;     // fullfill item count error
    const int OB_OP_NOT_ALLOW = -4007;         // fetch new schema not allowed
    const int OB_CHUNK_SERVER_ERROR = -4008;   // chunk server cached is error
    const int OB_NO_NEW_SCHEMA = -4009;        // no new schema when parse error
    const int OB_MS_SUB_REQ_TOO_MANY = -4010; // too many sub scan request
    const int OB_TOO_MANY_BLOOM_FILTER_TASK = -4011;
    const int OB_NOT_SUPPORT_MULTI_STATEMENTS = -4012;//add wangjg [multi_statements] 20170804
    const int OB_QUERY_OPT_HAVE_SYSTEM_TABLE = -4013;

    // SQL specific error code, -5000 ~ -6000
    const int OB_ERR_SQL_START = -5000;
    const int OB_ERR_PARSER_INIT = -5000;
    const int OB_ERR_PARSE_SQL = -5001;
    const int OB_ERR_RESOLVE_SQL = -5002;
    const int OB_ERR_GEN_PLAN = -5003;
    const int OB_ERR_UNKNOWN_SYS_FUNC = -5004;
    const int OB_ERR_PARSER_MALLOC_FAILED = -5005;
    const int OB_ERR_PARSER_SYNTAX = -5006;
    const int OB_ERR_COLUMN_SIZE = -5007;
    const int OB_ERR_COLUMN_DUPLICATE = -5008;
    const int OB_ERR_COLUMN_UNKNOWN = -5009;
    const int OB_ERR_OPERATOR_UNKNOWN = -5010;
    const int OB_ERR_STAR_DUPLICATE = -5011;
    const int OB_ERR_ILLEGAL_ID = -5012;
    const int OB_ERR_WRONG_POS = -5013;
    const int OB_ERR_ILLEGAL_VALUE = -5014;
    const int OB_ERR_COLUMN_AMBIGOUS = -5015;
    const int OB_ERR_LOGICAL_PLAN_FAILD = -5016;
    const int OB_ERR_SCHEMA_UNSET = -5017;
    const int OB_ERR_ILLEGAL_NAME = -5018;
    const int OB_ERR_TABLE_UNKNOWN = -5019;
    const int OB_ERR_TABLE_DUPLICATE = -5020;
    const int OB_ERR_NAME_TRUNCATE = -5021;
    const int OB_ERR_EXPR_UNKNOWN = -5022;
    const int OB_ERR_ILLEGAL_TYPE = -5023;
    const int OB_ERR_PRIMARY_KEY_DUPLICATE = -5024;
    const int OB_ERR_ALREADY_EXISTS = -5025;
    const int OB_ERR_CREATETIME_DUPLICATE = -5026;
    const int OB_ERR_MODIFYTIME_DUPLICATE = -5027;
    const int OB_ERR_ILLEGAL_INDEX = -5028;
    const int OB_ERR_INVALID_SCHEMA = -5029;
    const int OB_ERR_INSERT_NULL_ROWKEY = -5030; // SQLSTATE '23000'
    const int OB_ERR_COLUMN_NOT_FOUND = -5031;   // SQLSTATE '42S22'
    const int OB_ERR_DELETE_NULL_ROWKEY = -5032;
    const int OB_ERR_INSERT_INNER_JOIN_COLUMN = -5033;
    const int OB_ERR_USER_EMPTY = -5034;
    const int OB_ERR_USER_NOT_EXIST = -5035;
    const int OB_ERR_NO_PRIVILEGE = -5036;
    const int OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY = -5037;
    const int OB_ERR_WRONG_PASSWORD = -5038;

    const int OB_ERR_WEAK_PASSWORD = -5091; //[703]

    const int OB_ERR_USER_IS_LOCKED = -5039;
    const int OB_ERR_UPDATE_ROWKEY_COLUMN = -5040;
    const int OB_ERR_UPDATE_JOIN_COLUMN = -5041;
    const int OB_ERR_INVALID_COLUMN_NUM = -5042; // SQLSTATE 'S1002'
    const int OB_ERR_PREPARE_STMT_UNKNOWN = -5043; // SQLSTATE 'HY007'
    const int OB_ERR_VARIABLE_UNKNOWN = -5044;
    const int OB_ERR_SESSION_INIT = -5045;
    const int OB_ERR_OLDER_PRIVILEGE_VERSION = -5046;
    const int OB_ERR_LACK_OF_ROWKEY_COL = -5047;
    const int OB_ERR_EXCLUSIVE_LOCK_CONFLICT = -5048;
    const int OB_ERR_SHARED_LOCK_CONFLICT = -5049;
    const int OB_ERR_USER_EXIST = -5050;
    const int OB_ERR_PASSWORD_EMPTY = -5051;
    const int OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE = -5052;
    const int OB_ERR_WRONG_DYNAMIC_PARAM = -5053; // SQLSTATE '07001'
    const int OB_ERR_PARAM_SIZE = -5054;
    const int OB_ERR_FUNCTION_UNKNOWN = -5055;
    const int OB_ERR_CREAT_MODIFY_TIME_COLUMN = -5056;
    const int OB_ERR_MODIFY_PRIMARY_KEY = -5057;
    const int OB_ERR_PARAM_DUPLICATE = -5058;
    const int OB_ERR_TOO_MANY_SESSIONS = -5059;
    const int OB_ERR_TRANS_ALREADY_STARTED = -5060;
    const int OB_ERR_TOO_MANY_PS = -5061;
    const int OB_ERR_NOT_IN_TRANS = -5062;
    const int OB_ERR_HINT_UNKNOWN = -5063;
    const int OB_ERR_WHEN_UNSATISFIED = -5064;
    const int OB_ERR_QUERY_INTERRUPTED = -5065;
    const int OB_ERR_SESSION_INTERRUPTED = -5066;
    const int OB_ERR_UNKNOWN_SESSION_ID = -5067;
    const int OB_ERR_PROTOCOL_NOT_RECOGNIZE = -5068;
    const int OB_ERR_WRITE_AUTH_ERROR = -5069; //write auth packet to client failed ���Լ�ص����ӻ������Ͽ�
    const int OB_ERR_PARSE_JOIN_INFO = -5070;

    const int OB_ERR_COLUMN_TYPE_NOT_COMPATIBLE = -5071;//add qianzm [set_operation] 20151222

    const int OB_ERR_PS_TOO_MANY_PARAM = -5072;
    const int OB_ERR_READ_ONLY = -5073;
     //add liu jun.[MultiUPS] [sql_api] 20150320:b
    const int OB_ERR_FUNCTION_CONTEXT_NUM = -5074;
    const int OB_ERR_FUNCTION_CONTEXT_ILLEGAL = -5075;
    const int OB_ERR_FUNCTION_EXIST = -5076;
    const int OB_ERR_FUNCTION_NOT_EXISTS = -5077;
    const int OB_ERR_TABLE_NAME_LENGTH = -5078;
    const int OB_ERR_PARAMETER_LIST = -5079;
    const int OB_ERR_DOUBLE_CHECK_NAME = -5080;
    const int OB_ERR_NOT_ROWKEY  = -5081;
    //add lqc [MultiUps 1.0] 20170518 b
    const int OB_ERR_PARTITION_COLUMN_IS_NULL = -5082;
    //add e


    enum ObTablePartitionType
    {
      OB_WITHOUT_PARTITION = 0,
      OB_DIRECT_PARTITION = 1,
      OB_REFERENCE_PARTITION = 2,
      OB_TABLE_PARTITON_MAX_TYPE
    };
    //add  20150320:e

    //add lijianqiang [MultiUPS] [SELECT_MERGE] 20151228:b
    enum ObScannerType
    {
      OB_SCANNER_TYPE            = 0,//for ObScanner
      OB_NEW_SCANNER_TYPE       = 1,//for ObNewScanners
      OB_SCANNER_TYPE_NUM
    };
    //add 20151228:e
    //add wuna [MultiUPS] [sql_api] 20151207:b
    const int OB_ERR_NO_PARTITION = -5083;
    const int OB_ERR_PARTITION_VALUE_DUPLICATE = -5084;

    const int OB_ERR_PARTITION_INCONSISTENCY_NUM = -5088; //[601]
    const int OB_ERR_INVALID_COLUMN_TYPE = -5089;   //[601]


    enum ObFunctionPartitionType
    {
      HASHFUNC = 0,
      RANGEFUNC = 1,
      ENUMFUNC = 2,
      LISTFUNC = 3,
      PRERANGE = 4,
      HARDCODEFUNC = 5,
      OB_FUNC_TYPE_NUM
    };
    enum ObPartitionForm
    {
      OB_UNKNOWN_FORM_PARTITION = 0,
      OB_HASH_PARTITION = 1,
      OB_RANGE_PARTITION = 2,
      OB_RANGE_COLUMNS_PARTITION = 3,
      OB_LIST_PARTITION = 4,
      OB_LIST_COLUMNS_PARTITION = 5,
      OB_PARTITION_FORM_NUM
    };
 //add 20151207:e

    enum UpsExecutorType
    {
        SELECT_OLD_VALUES = 0,
        DELETE_OLD_ROWS,
        INSERT_NEW_ROWS
    };

    //add peioy [Alter_Rename] [JHOBv0.1] 20150210:b
    //const int OB_ERR_TABLE_NAME_LENGTH = -5090;
    const int OB_ERR_COLUMN_NAME_LENGTH = -5085;
    //add 20150210:e

    //add liumz, [multi_database.priv_management]:20150608:b
    const int OB_ERR_SESSION_UNSET = -5086;
    //add:e

    const int OB_DISABLE_DIS_TRANS = -5087;

    //add peiouya [DATE_TIME] 20150914:b , -5101 ~ 5190
    const int OB_VALUE_OUT_OF_DATES_RANGE = -5101;
    const int OB_ERR_TIMESTAMP_TYPE_FORMAT = -5102; //add liuzy [datetime bug] 20150928
    const int OB_ERR_DATE_TYPE_FORMAT = -5103;      //add liuzy [datetime bug] 20150928
    const int OB_ERR_TIME_TYPE_FORMAT = -5104;      //add liuzy [datetime bug] 20150928
    const int OB_ERR_VALUE_OF_DATE = -5105;         //add liuzy [datetime bug] 20150928
    const int OB_ERR_VALUE_OF_TIME = -5106;         //add liuzy [datetime bug] 20150928
    //add 20150914:e
    //add wanglei:b -5201 ~ -5220
    const int OB_ERR_CAN_NOT_USE_SEMI_JOIN = -5201;
    const int OB_ERR_HAS_NO_EQUI_COND = -5202;
    const int OB_ERR_INDEX_NUM_IS_ZERO = -5203;
    const int OB_ERR_POINTER_IS_NULL = -5204; //add 20151260 9:00
    const int OB_ERR_FUNC_DEV = -5205; //add 20151260 9:00
    //add:e

    //add shili [LONG_TRANSACTION_LOG]  20160926:b
    const int OB_INVALID_SESSION_ID= -5300;
    //add e
    //add peouya [NotNULL_check] [JHOBv0.1] 20131208:b
    const int OB_ERR_INSERT_NULL_COLUMN = -5500;  //the column not in rowkey
    const int OB_ERR_UPDATE_NULL_COLUMN = -5501;  //the column not be set NULL but set NULL
    const int OB_ERR_REPLACE_NULL_COLUMN = -5502;  //the column not in rowkey
    const int OB_ERR_VARIABLE_NULL = -5503;        //�ǿ�Լ�����У�����ֵ���ܱ����ó�NULL
    const int OB_ERR_VARIABLE_CONSTRAINT_SAVE = -5504;        //prepare�б���Լ����Ϣ�������
    const int OB_ERR_VARIABLE_CONSTRAINT_GET  = -5505;        //��ȡ�����prepare�б���Լ����Ϣ����
    //add 20131208:e

    const int OB_ERR_SUB_QUERY_NULL_COLUMN = -5506;
    //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140606:b
    const int OB_ERR_SUB_QUERY_OVERFLOW = -5600;
    const int OB_ERR_SUB_QUERY_RESULTSET_EXCESSIVE= -5601;
    //add 20140606:e
    //add wenghaixing [database manage]20150610
    const int OB_ERR_DATABASE_NOT_EXSIT = -5602;
    const int OB_ERR_NO_ACCESS_TO_DATABASE = -5603;
    const int OB_ERR_STILL_HAS_TABLE_IN_DATABASE = -5604;
    const int OB_ERR_DROP_DEFAULT_DB = -5605;
    //add e

    const int OB_ERR_ILLEGAL_DEFAULT_VALUE = -5609;

    //add dragon [varchar limit] 2016-8-10 10:36:27
    //��stmt��column_items_��û���ҵ�ָ����column
    const int OB_ERR_STMT_COL_NOT_FOUND = -5610;
    //�û�ָ����varchar���ȳ���schema�е�����
    const int OB_ERR_VARCHAR_TOO_LONG = -5611;
    //oceanbase��ʹ�ÿ�ָ��
    const int OB_ERR_NULL_POINTER = -5612;
    //add e
    // add by maosy [Delete_Update_Function_for_snpshot]  20161212
    const int OB_ERR_BATCH_EMPTY_ROW = -5613;
    const int OB_ERR_REPEAT_UPDATE = -5614;
    // add by maosy e

    const int OB_SQL_CAN_NOT_PULL_UP = -5615;
     //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150706:b
    const int OB_ERR_COLUMN_NAME_NOT_EXIST = -5700;
    const int OB_ERR_TABLE_ID = -5701;
    const int OB_ERR_PAXOS_ID = -5702;
    const int OB_ERR_DIFFERENT_OPERATOR = -5703;
    const int OB_ERR_NOT_EXIST_OPERATOR = -5704;
    const int OB_CS_UPS_QUERY_TIMEOUT = -5705;
    const int OB_DIFFERENT_DATA_VERSION = -5706;
    const int OB_NOT_EXIST_PARTITION_MANAGER = -5707;
    const int OB_MASTER_UPS_NOT_EXIST = -5708;
    const int OB_ERR_ROWKEY_SIZE = -5709;
    const int OB_EMPTY_ARRAY = -5710;
    const int OB_DIFFERENT_SIZE = -5711;
    const int OB_DIFFERENT_DATA_MARK = -5712;
    //add 20150706:e
    //add wangjiahao [Paxos ups_replication] 20150804 :b
    const int OB_ERR_TERM = -5713;
    const int OB_CLPT_NOFIND = -5714;
    const int OB_NOT_ENOUGH_SLAVE = -5715;//add wangdonghui [ups_replication] 20160929 :b:e
    const int OB_HAVNT_DONE = -5716;//add wangdonghui [ups_replication] 20160929 :b:e
    //add :e
    //add wenghaixing [secondary index static_index_build]20150318
    const int OB_GET_TABLETS = -5802;
    const int OB_GET_RANGES = -5803;
    const int OB_GET_NOTHING = -5804;
    //add e

    const int OB_ERROR_TABLE_SIZE = -5810;

    const int OB_GROUP_NAME_NOT_EXIST = -5820;
    const int OB_PAXOS_IDX_OUT_RANGE = -5821;
    const int OB_PAXOS_IDX_OFFLINE = -5822;
    const int OB_CURRENT_PAXOS_ALETR_GROUP = -5823;

    //[view]
    const int OB_NOT_INCLUDE_REQUIRED_COLUMN_LIST = -5851;
    const int OB_ERR_OBJECT_TYPE = -5852;
    const int OB_ERR_INCLUDE_RECURSION_VIEW = -5853;
    const int OB_ERR_VIEW_UNKNOWN = -5854;

    const int OB_ERR_SQL_END = -5999;
#define IS_SQL_ERR(e) ((OB_ERR_SQL_END <= e && OB_ERR_SQL_START >= e) \
                      || OB_ERR_EXCLUSIVE_LOCK_CONFLICT == e \
                      || OB_ERR_SHARED_LOCK_CONFLICT == e\
                      || OB_INCOMPLETE_ROW == e) //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM]

    // Fatal errors and the client should close the connection, -8000 ~ -8999
    const int OB_ERR_SERVER_IN_INIT = -8001;
    const int OB_PACKET_CHECKSUM_ERROR = -8002;
    //////////////////////////////////////////////////////////////// end of ob error code

    typedef int64_t    ObDateTime;
    typedef int64_t    ObPreciseDateTime;
    //add peiouya [DATE_TYPE] 20150831:b
    typedef int64_t    ObDate;
    typedef int64_t    ObTime;
    //add 20150831:e
    typedef int64_t    ObInterval;  //add peiouya [DATE_TIME] 20150908
    typedef ObPreciseDateTime ObModifyTime;
    typedef ObPreciseDateTime ObCreateTime;

    const int64_t OB_MAX_SQL_LENGTH = 10240;
    const int64_t OB_SQL_LENGTH = 1024;
    const int64_t OB_MAX_URI_LENGTH = 1024;
    const int64_t OB_MAX_SERVER_ADDR_SIZE = 128;
    const uint64_t OB_MAX_VALID_COLUMN_ID = 10240;   // user table max valid column id
    //mod liumz, [max table number]20160707:b
    //const int64_t OB_MAX_TABLE_NUMBER = 2048;
    const int64_t OB_MAX_TABLE_NUMBER = 2048 * 4;
    //mod:e
    const int64_t OB_MAX_JOIN_INFO_NUMBER = 10;
    const int64_t OB_MAX_ROW_KEY_LENGTH = 16384; // 16KB
    const int64_t OB_MAX_ROW_KEY_SPLIT = 32;
    const int64_t OB_MAX_ROWKEY_COLUMN_NUMBER = 16;
    const int64_t OB_MAX_COLUMN_NAME_LENGTH = 128;  //this means we can use 127 chars for a name.
    const int64_t OB_MAX_APP_NAME_LENGTH = 128;
    const int64_t OB_MAX_DATA_SOURCE_NAME_LENGTH = 128;
    const int64_t OB_MAX_YUNTI_USER_LENGTH = 128;
    const int64_t OB_MAX_YUNTI_GROUP_LENGTH = 128;
    const int64_t OB_MAX_INSTANCE_NAME_LENGTH = 128;
    const int64_t OB_MAX_HOST_NAME_LENGTH = 128;
    const int64_t OB_MAX_MS_TYPE_LENGTH = 10;

    const int64_t OB_MAX_DEBUG_MSG_LEN = 1024;
    const int64_t OB_MAX_COMPRESSOR_NAME_LENGTH = 128;
    const int64_t OB_MAX_TABLE_NAME_LENGTH = 256;
    const int64_t OB_MAX_DATBASE_NAME_LENGTH = 16; //add dolphin [database manager]@20150605
    const int64_t OB_MAX_COMPLETE_TABLE_NAME_LENGTH = OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1; // add zhangcd 20150724
    const int64_t OB_MAX_FILE_NAME_LENGTH = 512;
    const int64_t OB_MAX_SECTION_NAME_LENGTH = 128;
    const int64_t OB_MAX_FLAG_NAME_LENGTH = 128;
    const int64_t OB_MAX_FLAG_VALUE_LENGTH = 512;
    const int64_t OB_MAX_TOKEN_BUFFER_LENGTH = 80;
    const int64_t OB_MAX_PACKET_LENGTH = 1<<21; // max packet length, 2MB
    const int64_t OB_MAX_ROW_NUMBER_PER_QUERY = 65536;
    const int64_t OB_MAX_BATCH_NUMBER = 100;
    const int64_t OB_MAX_TABLET_LIST_NUMBER = 1<<10;
    //add liumz, [bugfix: secondary index histogram]20161109:b
    const int64_t OB_MAX_TABLET_NUMBER_PER_TABLE_CS = 10<<10;
    const int64_t OB_MAX_TABLET_NUMBER_PER_TABLE = 100<<10;
    const int64_t OB_MAX_HISTOGRAM_SAMPLE_PER_TABLE = 1<<21;
    //add:e
    const int64_t OB_MAX_DISK_NUMBER = 16;
    const int64_t OB_MAX_TIME_STR_LENGTH = 64;
    const int64_t OB_IP_STR_BUFF = 30;
    const int64_t OB_RANGE_STR_BUFSIZ = 512;
    const int64_t OB_MAX_FETCH_CMD_LENGTH = 2048;
    const int64_t OB_MAX_EXPIRE_CONDITION_LENGTH = 512;
    const int64_t OB_MAX_TABLE_COMMENT_LENGTH = 512;
    const int64_t OB_MAX_COLUMN_GROUP_NUMBER = 16;
    const int64_t OB_MAX_THREAD_READ_SSTABLE_NUMBER = 16;
    const int64_t OB_MAX_GET_ROW_NUMBER = 10240;
    const uint64_t OB_FULL_ROW_COLUMN_ID = 0;
    const uint64_t OB_DELETE_ROW_COLUMN_ID = 0;
    const int64_t OB_DIRECT_IO_ALIGN_BITS = 9;
    const int64_t OB_DIRECT_IO_ALIGN = 1<<OB_DIRECT_IO_ALIGN_BITS;
    const int64_t OB_MAX_COMPOSITE_SYMBOL_COUNT = 256;
    const int64_t OB_SERVER_VERSION_LENGTH = 64;
    const int64_t OB_SERVER_TYPE_LENGTH = 64;
    const int64_t OB_MAX_USERNAME_LENGTH = 32;
    const int64_t OB_MAX_PASSWORD_LENGTH = 32;
    const int64_t OB_MAX_CIPHER_LENGTH = MD5_DIGEST_LENGTH * 2;

    const int64_t OB_MAX_RESULT_MESSAGE_LENGTH = 1024;
    const int64_t OB_MAX_LOG_BUFFER_SIZE = 1966080L;  // 1.875MB

    const int64_t OB_IMPORT_LIMIT_UPS_MEMORY = 350L * 1024 * 1024 * 1024;

    //add zhaoqiong[roottable tablet management]20150127:b
    const int32_t OB_MAX_COPY_COUNT = 6;
    //add 20150127 e
     //add liu jun.[MultiUPS] [sql_api] 20150423:b
    const int64_t OB_INVALID_FROZEN_VERSION = -1984;
    const int64_t OB_MAX_PREFIX_GROUP_NAME_LENGTH = 128;
    const int64_t OB_MAX_POST_GROUP_NAME_LENGTH = 128;
    //add 20150423:e

    //add peiouya [MultiUPS] [Create_table] 20150514:b
    const int32_t OB_DEFAULT_COPY_COUNT = 3;
    //add:e
    const int32_t OB_SAFE_COPY_COUNT = 3;
    const int32_t OB_DEC_AND_LOCK = 2626; /* used by remoe_plan in ObPsStore */
    // OceanBase Log Synchronization Type
    const int64_t OB_LOG_NOSYNC = 0;
    const int64_t OB_LOG_SYNC = 1;
    const int64_t OB_LOG_DELAYED_SYNC = 2;
    const int64_t OB_LOG_NOT_PERSISTENT = 4;

    const int64_t OB_MAX_UPS_LEASE_DURATION_US = INT64_MAX;

    const int64_t OB_EXECABLE = 1;
    const int64_t OB_WRITEABLE = 2;
    const int64_t OB_READABLE = 4;
    const int64_t OB_SCHEMA_START_VERSION = 100;
    const int64_t OB_SYS_PARAM_ROW_KEY_LENGTH = 192;
    const int64_t OB_MAX_SYS_PARAM_NAME_LENGTH = 128;

    const int64_t OB_MAX_PREPARE_STMT_NUM_PER_SESSION = 512;
    const int64_t OB_MAX_VAR_NUM_PER_SESSION = 1024;
    const int64_t OB_DEFAULT_SESSION_TIMEOUT = 100L * 1000L * 1000L; // 10s
    const int64_t OB_DEFAULT_STMT_TIMEOUT = 3L * 1000L * 1000L; // 1s
    const int64_t OB_DEFAULT_INTERNAL_TABLE_QUERY_TIMEOUT = 10L * 1000L * 1000L; // 10s
    static const int64_t CORE_SCHEMA_VERSION = 1984;
    //mod zhaoqiong [Schema Manager] 20150327:b
    //static const int64_t CORE_TABLE_COUNT = 3;
    static const int64_t CORE_TABLE_COUNT = 4;
    //mod:e
    const int64_t OB_AIO_TIMEOUT_US = 5L * 1000L * 1000L; //5s
    const int64_t OB_DEFAULT_MAX_PARALLEL_COUNT = 20;

    //Oceanbase network protocol
    /*  4bytes    4bytes        4bytes       4bytes
     * -------------------------------------------------
     * | flag | channel id | packet code | data length |
     * -------------------------------------------------
     */
    const int OB_TBNET_HEADER_LENGTH = 16;  //16 bytes packet header

    const int OB_TBNET_PACKET_FLAG = 0x416e4574;
    const int OB_SERVER_ADDR_STR_LEN = 128; //used for buffer size of easy_int_addr_to_str

    /*   3bytes   1 byte
     * ------------------
     * |   len  |  seq  |
     * ------------------
     */
    const int OB_MYSQL_PACKET_HEADER_SIZE = 4; /** 3bytes length + 1byte seq*/

    const int64_t OB_UPS_START_MAJOR_VERSION = 2;
    const int64_t OB_UPS_START_MINOR_VERSION = 1;
    const int64_t OB_NEWEST_DATA_VERSION = -2;

    const int32_t OB_CONNECTION_FREE_TIME_S = 900;

    /// @see ob_object.cpp and ob_expr_obj.cpp
    static const float FLOAT_EPSINON = static_cast<float>(1e-6);
    static const double DOUBLE_EPSINON = 1e-14;

    const uint64_t OB_UPS_MAX_MINOR_VERSION_NUM = 2048;
    const int64_t OB_MAX_COMPACTSSTABLE_NUM = 64;

    enum ObRole
    {
      OB_INVALID = 0,
      OB_ROOTSERVER = 1,  // rs
      OB_CHUNKSERVER = 2, // cs
      OB_MERGESERVER = 3, // ms
      OB_UPDATESERVER = 4,// ups
      OB_PROXYSERVER = 5,
      OB_BACKUPSERVER = 6, //add zhaoqiong [fixed for Backup]:20150811
    };

    static const int OB_FAKE_MS_PORT = 2828;
    static const uint64_t OB_MAX_PS_PARAM_COUNT = 65535;
    static const uint64_t OB_MAX_PS_FIELD_COUNT = 65535;
    static const uint64_t OB_ALL_MAX_COLUMN_ID = 65535;
    // internal columns id
    const uint64_t OB_NOT_EXIST_COLUMN_ID = 0;
    const uint64_t OB_CREATE_TIME_COLUMN_ID = 2;
    const uint64_t OB_MODIFY_TIME_COLUMN_ID = 3;
    const uint64_t OB_DEFAULT_COLUMN_GROUP_ID = 0;
    const int64_t OB_END_RESERVED_COLUMN_ID_NUM = 16;
    const uint64_t OB_APP_MIN_COLUMN_ID = 16;
    const uint64_t OB_ACTION_FLAG_COLUMN_ID = OB_ALL_MAX_COLUMN_ID - OB_END_RESERVED_COLUMN_ID_NUM + 1; /* 65520 */
    const uint64_t OB_MAX_TMP_COLUMN_ID = OB_ALL_MAX_COLUMN_ID - OB_END_RESERVED_COLUMN_ID_NUM;
    //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170523:b
    const uint64_t OB_DELETED_FLAG_COLUMN_ID = OB_ALL_MAX_COLUMN_ID+1;
    //add e

    const uint64_t OB_NOP_FLAG_COLUMN_ID = OB_ALL_MAX_COLUMN_ID + 2;


    // internal columns name
    //extern const char *OB_CREATE_TIME_COLUMN_NAME;
    //extern const char *OB_MODIFY_TIME_COLUMN_NAME;
    static const char * const CLUSTER_ID_FILE = "etc/cluster.id";

    // internal tables name
    const char* const FIRST_TABLET_TABLE_NAME = "__first_tablet_entry";
    const char* const OB_ALL_COLUMN_TABLE_NAME = "__all_all_column";
    const char* const OB_ALL_JOININFO_TABLE_NAME = "__all_join_info";
    const char* const OB_ALL_SERVER_STAT_TABLE_NAME = "__all_server_stat";
    const char* const OB_ALL_SYS_PARAM_TABLE_NAME = "__all_sys_param";
    const char* const OB_ALL_SYS_CONFIG_TABLE_NAME = "__all_sys_config";
    const char* const OB_ALL_SYS_STAT_TABLE_NAME = "__all_sys_stat";
    const char* const OB_ALL_USER_TABLE_NAME = "__all_user";
    const char* const OB_ALL_TABLE_PRIVILEGE_TABLE_NAME = "__all_table_privilege";
    const char* const OB_ALL_TRIGGER_EVENT_TABLE_NAME= "__all_trigger_event";
    const char* const OB_ALL_SYS_CONFIG_STAT_TABLE_NAME = "__all_sys_config_stat";
    const char* const OB_ALL_SERVER_SESSION_TABLE_NAME = "__all_server_session";
    const char* const OB_ALL_CLUSTER = "__all_cluster";
    const char* const OB_ALL_SERVER = "__all_server";
    const char* const OB_ALL_CLIENT = "__all_client";
    const char* const OB_TABLES_SHOW_TABLE_NAME = "__tables_show";
    //add liumengzhan_show_index [20141208]
    const char* const OB_INDEX_SHOW_TABLE_NAME = "__index_show";
    //add:e

    const char* const OB_ALL_INDEX_SHOW_TABLE_NAME = "__all_index_show";
    const char* const OB_CREATE_INDEX_SHOW_TABLE_NAME = "__create_index_show";

    const char* const OB_TRUNCATE_OP_TABLE_NAME = "__all_truncate_op"; //add zhaoqiong [Truncate Table]:20160318

    const char* const OB_UDI_MONITOR_TABLE_NAME = "__all_udi_monitor_list";
    const char* const OB_STATISTIC_INFO_TABLE_NAME = "__all_statistic_info";

    const char* const OB_DATABASES_SHOW_TABLE_NAME = "__databases_show"; // add by zhangcd [multi_database.show_tables] 20150617
    const char* const OB_VARIABLES_SHOW_TABLE_NAME = "__variables_show";
    const char* const OB_CREATE_TABLE_SHOW_TABLE_NAME = "__create_table_show";

    //[view]
    const char* const OB_CREATE_VIEW_SHOW_TABLE_NAME = "__create_view_show";

    const char* const OB_TABLE_STATUS_SHOW_TABLE_NAME = "__table_status_show";
    const char* const OB_SCHEMA_SHOW_TABLE_NAME = "__schema_show";
    const char* const OB_COLUMNS_SHOW_TABLE_NAME = "__columns_show";
    const char* const OB_SERVER_STATUS_SHOW_TABLE_NAME = "__server_status_show";
    const char* const OB_PARAMETERS_SHOW_TABLE_NAME = "__parameters_show";
    const char* const OB_FUNCTIONS_SHOW_TABLE_NAME = "__functions_show";  //add liu jun.[MultiUPS] [sql_api] 20150325
    const char* const OB_TABLE_RULES_SHOW_TABLE_NAME = "__table_rules_show";  //add wuna.[MultiUPS] [sql_api] 20160223
    const char* const OB_PARTITION_GROUPS_SHOW_TABLE_NAME = "__partition_groups_show";  //add wuna.[MultiUPS] [sql_api] 20160223

    const char* const OB_PAXOS_ID_SHOW_TABLE_NAME = "__paxos_id_show";

    //const char* const OB_ALL_STATEMENT_TABLE_NAME = "__all_statement";//uncertainty �ظ�����
    const char* const range_pre_name_str = "__range@";//add wuna.[MultiUPS] [sql_api] 20160223
    const char* const list_pre_name_str = "__list@";//add wuna.[MultiUPS] [sql_api] 20160223
    //add liu jun.[MultiUPS] [sql_api] 20150418:b
    const char* const OB_ALL_PARTITION_RULES_NAME = "__all_partition_rules";
    const char* const OB_ALL_TABLE_RULES_NAME = "__all_table_rules";
    const char* const OB_ALL_ALL_GROUP_NAME = "__all_all_group";
    //add liu jun.20150418:e
    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    const char* const OB_UPS_SESSION_INFO_NAME = "__ts_session_info";
    //add 20150701:e
    //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160325:b
    const char* const OB_ALL_CLUSTER_STAT_INFO_TABLE_NAME = "__all_cluster_stat_info";
    //add 20160325:e

    const char* const OB_ALL_STATEMENT_TABLE_NAME = "__all_statement";
    //add zhaoqiong [Schema Manager] 20150327:b
    const char* const OB_ALL_DDL_OPERATION_NAME= "__all_ddl_operation";
    //add:e

    const char* const OB_ALL_CHANGE_CLUSTER_PAXOS_TABLE_NAME = "__all_change_cluster_paxos";

    //add dolphin [schema manager]@20150605:b
    const char* const OB_ALL_DATABASE_NAME = "__all_database";
    const char* const OB_ALL_DATABASE_PRIVILEGE_NAME = "__all_database_privilege";
    const char* const OB_DEFAULT_DATABASE_NAME = "";
    //add:e
    //add wenghaixing [secondary index.cluster] 20150629
    const char* const OB_INDEX_PROCESS_TABLE_NAME ="__index_process_info";
    //add e
    //add wenghaixing [secondary index create fix]20141225
    const char* const OB_INDEX_VIRTUAL_COL_NAME="yb_virtual_col";
    //add e
    //add liuxiao [secondary index col checksum] 20150317
    //const char* const OB_CCHECKSUM_INFO = "__all_cchecksum_info";
    const char* const OB_ALL_CCHECKSUM_INFO_TABLE_NAME = "__all_cchecksum_info";
    //add:e
    //add lijianqiang [sequence] 20150324b:
    /*Exp:������ϵͳ���ĵ�����Ϊ:'__all_sequence'*/
    const char* const OB_ALL_SEQUENCE_TABLE_NAME = "__all_sequence";
    //add 20150324:e
    // internal params
    const char* const OB_GROUP_AGG_PUSH_DOWN_PARAM = "yb_group_agg_push_down_param";
    const char* const OB_QUERY_TIMEOUT_PARAM = "yb_query_timeout";
    const char* const OB_READ_CONSISTENCY = "yb_read_consistency";
    const char* const OB_MAX_REQUEST_PARALLEL_COUNT = "yb_max_parallel_count";
    const char* const OB_DEFAULT_SYS_CHARSET = "utf8";
    //add liu jun.[MultiUPS] [sql_api] 20150422:b
    const char* const OB_DEFAULT_GROUP_NAME = "__user_default_group";
    //add 20150422:e

    //[view]
    const char *const OB_ALL_VIEW_TABLE_NAME = "__all_view";

    ///////////////////////////////////////////////////////////
    //                 SYSTEM TABLES                         //
    ///////////////////////////////////////////////////////////
    // SYTEM TABLES ID (0, 500), they should not be mutated
    static const uint64_t OB_NOT_EXIST_TABLE_TID = 0;
    static const uint64_t OB_FIRST_META_VIRTUAL_TID = OB_INVALID_ID - 1; // not a real table
    static const uint64_t OB_FIRST_TABLET_ENTRY_TID = 1;
    static const uint64_t OB_ALL_ALL_COLUMN_TID = 2;
    static const uint64_t OB_ALL_JOIN_INFO_TID = 3;
    static const uint64_t OB_ALL_SYS_PARAM_TID = 4;
    static const uint64_t OB_ALL_SYS_STAT_TID = 5;
    static const uint64_t OB_USERS_TID = 6;
    static const uint64_t OB_TABLE_PRIVILEGES_TID = 7;
    static const uint64_t OB_ALL_CLUSTER_TID = 8;
    static const uint64_t OB_ALL_SERVER_TID = 9;
    static const uint64_t OB_TRIGGER_EVENT_TID = 10;
    static const uint64_t OB_ALL_SYS_CONFIG_TID = 11;
    static const uint64_t OB_ALL_SYS_CONFIG_STAT_TID = 12;
    static const uint64_t OB_ALL_CLIENT_TID = 13;
     //add liu jun.[MultiUPS] [sql_api] 20150418:b
    ///////////////////////////////////////////////////////////
    //                 MULTI_UPS TABLES                         //
    ///////////////////////////////////////////////////////////
    static const uint64_t OB_ALL_PARTITION_RULES_TID = 100;
    static const uint64_t OB_ALL_TABLE_RULES_TID = 101;
    static const uint64_t OB_ALL_ALL_GROUP_TID = 102;
    //add liu jun.20150418:e
    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    static const uint64_t OB_UPS_SESSION_INFO_TID = 103;
    //add 20150701:e
    //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160325:b
    static const uint64_t OB_ALL_CLUSTER_STAT_INFO_TID = 104;
    //add 20160325:e

    static const uint64_t OB_ALL_CHANGE_CLUSTER_PAXOS_TID = 105;

    //add zhaoqiong [Schema Manager] 20150327:b
    static const uint64_t OB_DDL_OPERATION_TID = 14;
    //add:e
    //add dolphin [database manager]@20150605:b
    static const uint64_t OB_ALL_DATABASE_TID = 15;
    static const uint64_t OB_ALL_DATABASE_PRIVILEGE_TID = 16;
    //add:e
    //add lijianqiang [sequence] 20150324b:
    static const uint64_t OB_ALL_SEQUENCE_TID = 17;
    //add 20150324:e
    static const uint64_t OB_ALL_TRUNCATE_OP_TID = 18; //add zhaoqiong [Truncate Table]:20160318:b

    static const uint64_t OB_ALL_STATISTIC_INFO_TID = 19;
    static const uint64_t OB_UDI_MONITOR_LIST_TID = 20;

    //add wenghaixing [secondary index]20141105
   /*
    *����������������ϵͳ���������������֮�䴴��������ͬ��������
    */
   static const uint64_t OB_INDEX_PROCESS_TID=802;
   //add e

   //[view]
   static const uint64_t OB_ALL_VIEW_TID = 21;

    ///////////////////////////////////////////////////////////
    //                 VIRUTAL TABLES                        //
    ///////////////////////////////////////////////////////////
    // VIRTUAL TABLES ID (500, 700), they should not be mutated
#define IS_VIRTUAL_TABLE(tid) ((tid) > 500 && (tid) < 700)
    static const uint64_t OB_TABLES_SHOW_TID = 501;
    /*  uncertainty ��ͻ�ظ�����
        static const uint64_t OB_COLUMNS_SHOW_TID = 502;
    static const uint64_t OB_VARIABLES_SHOW_TID = 503;
    static const uint64_t OB_TABLE_STATUS_SHOW_TID = 504;
    static const uint64_t OB_SCHEMA_SHOW_TID = 505;
    static const uint64_t OB_CREATE_TABLE_SHOW_TID = 506;
    static const uint64_t OB_PARAMETERS_SHOW_TID = 507;
    */
    /* mod liu jun.[MultiUps_Live] 20150325:b
    static const uint64_t OB_SERVER_STATUS_SHOW_TID = 508;
    static const uint64_t OB_ALL_SERVER_STAT_TID = 509;
    static const uint64_t OB_ALL_SERVER_SESSION_TID = 510;
    static const uint64_t OB_ALL_STATEMENT_TID = 511;
    mod 20150325:e*/
    /*mod by wuna [MultiUps][sql_api] 20160223:b*/

    //add liumengzhan_show_index [20141208]
    static const uint64_t OB_INDEX_SHOW_TID = 502;
    //add:e
    static const uint64_t OB_COLUMNS_SHOW_TID = 503;
    static const uint64_t OB_VARIABLES_SHOW_TID = 504;
    static const uint64_t OB_TABLE_STATUS_SHOW_TID = 505;
    static const uint64_t OB_SCHEMA_SHOW_TID = 506;
    static const uint64_t OB_CREATE_TABLE_SHOW_TID = 507;
    static const uint64_t OB_PARAMETERS_SHOW_TID = 508;
    static const uint64_t OB_DATABASES_SHOW_TID = 509;// add by zhangcd [multi_database.show_databases] 20150617
   /*
    static const uint64_t OB_SERVER_STATUS_SHOW_TID = 510;// uncertainty �ظ������ͻ
   //static const uint64_t OB_ALL_SERVER_STAT_TID = 511;
    static const uint64_t OB_ALL_SERVER_SESSION_TID = 512;
    static const uint64_t OB_ALL_STATEMENT_TID = 513;
    */
    //add liu jun.[MultiUps_Live] 20150325:b
    static const uint64_t OB_FUNCTIONS_SHOW_TID = 510;//uncertainty  �Qһ��ֵ
    static const uint64_t OB_TABLE_RULES_SHOW_TID = 511;//uncertainty  �Qһ��ֵ
    static const uint64_t OB_PARTITION_GROUPS_SHOW_TID = 512;
    static const uint64_t OB_SERVER_STATUS_SHOW_TID = 513;
    static const uint64_t OB_ALL_SERVER_STAT_TID = 514;
    static const uint64_t OB_ALL_SERVER_SESSION_TID = 515;
    static const uint64_t OB_ALL_STATEMENT_TID = 516;
    //add 20150325:e

    static const uint64_t OB_CREATE_INDEX_SHOW_TID = 517;
    static const uint64_t OB_PAXOS_ID_SHOW_TID = 518;
    static const uint64_t OB_ALL_INDEX_SHOW_TID = 519;

    //[view]
    static const uint64_t OB_CREATE_VIEW_SHOW_TID = 520;

    //add liuxiao [secondary index col checksum]20150316
    /*
     *�����ڲ���__all_cchecksum_info��table_id
     */
    static const uint64_t OB_ALL_CCHECKSUM_INFO_TID = 801;
    //add e
    //add wangyao [sequence for construct other row]
    static const uint64_t OB_TEMP_SEQUENCE_TABLE_TID = 901;
    //add e
#define IS_SHOW_TABLE(tid) (((tid) >= OB_TABLES_SHOW_TID && (tid) <= OB_SERVER_STATUS_SHOW_TID)\
    || OB_CREATE_INDEX_SHOW_TID == (tid)\
    || OB_CREATE_VIEW_SHOW_TID == (tid)\
    || OB_PAXOS_ID_SHOW_TID == (tid)\
    || OB_ALL_INDEX_SHOW_TID == (tid))

    ///////////////////////////////////////////////////////////
    //                 USER TABLES                           //
    ///////////////////////////////////////////////////////////
    // USER TABLE ID (1000, MAX)
    static const uint64_t OB_APP_MIN_TABLE_ID = 1000;
    inline bool IS_SYS_TABLE_ID(uint64_t tid)
    { return (tid < OB_APP_MIN_TABLE_ID); };

    static const uint64_t OB_ALL_STAT_COLUMN_MAX_COLUMN_ID = 45;
    static const uint64_t OB_ALL_ALL_COLUMN_MAX_COLUMN_ID = 45;
    static const uint64_t OB_ALL_JOIN_INFO_MAX_COLUMN_ID = 45;
    static const uint64_t OB_ALL_SYS_STAT_MAX_COLUMN_ID = 45;
    static const uint64_t OB_ALL_SYS_PARAM_MAX_COLUMN_ID = 45;
    static const uint64_t OB_ALL_SYS_CONFIG_MAX_COLUMN_ID = 45;
    static const uint64_t OB_ALL_CLUSTER_MAX_COLUMN_ID = 45;
    static const uint64_t OB_ALL_SERVER_MAX_COLUMN_ID = 45;
    //mod liujun.[MultiUps_Live] 20150317:b
    static const uint64_t OB_ALL_PARTITION_FUNCTION_MAX_COLUMN_ID = 45;
    //mod e.

    static const uint64_t OB_MAX_CLUSTER_NAME = 128;
    static const uint64_t OB_MAX_CLUSTER_INFO = 128;
    //add jinty [Paxos Cluster.Balance] 20160708:b
    static const int64_t OB_MAX_SERVER_INFO = 128;
    static const int64_t SERVER_TYPE_LENGTH = 16;
    static const int64_t SERVER_IP_LENGTH = 32;
    //add e
    //add e
    static const int64_t OB_MAX_CLUSTER_COUNT = 6;

    const int32_t OB_START_CLUSTER_ID = 0;

    //add lbzhong [Paxos Cluster.Balance] 20160704:b
    // 0 < cluster_id <= 5
    const int32_t OB_MAX_CLUSTER_ID = OB_MAX_CLUSTER_COUNT - 1;
    //index[0] is not used
    const int32_t OB_CLUSTER_ARRAY_LEN = OB_MAX_CLUSTER_COUNT;
    const int32_t OB_EXTERNAL_CLUSTER_ID = -1;
    //const int32_t OB_DEFAULT_COPY_COUNT = -1; // for create table statement
    const char* const OB_CLUSTER_REPLICA_NUM_PREFIX = "cluster_replica_num_";
    //add:e
    // internal user id
    static const uint64_t OB_ADMIN_UID = 1;
    const char* const OB_ADMIN_USER_NAME = "admin";
    //add wenghaixing [database manage]20150613
    static const uint64_t OB_BASE_DB_ID = 0;
    const char* const OB_DATASOURCE_USER = "dsadmin";
    static const uint64_t OB_DSADMIN_UID = 2;
    const char* const OB_DEFAULT_DB_NAME = "TANG";
    //add e

    // ob_malloc & ob_tc_malloc
    static const int64_t OB_TC_MALLOC_BLOCK_HEADER_SIZE = 64; // >= sizeof(TSIBlockCache::Block)
    static const int64_t OB_MALLOC_BLOCK_SIZE = 64*1024LL+1024LL;
    static const int64_t OB_TC_MALLOC_BLOCK_SIZE = OB_MALLOC_BLOCK_SIZE - OB_TC_MALLOC_BLOCK_HEADER_SIZE;

    /// һ���п��԰��������Ԫ��/����
    /// modify wenghaixing [secondary index alter_table_debug]20150611
    //static const int64_t OB_ROW_MAX_COLUMNS_COUNT = 512; // used in ObRow
    static const int64_t OB_ROW_MAX_COLUMNS_COUNT = 511;
    static const uint64_t OB_INDEX_VIRTUAL_COLUMN_ID = 511;
    ///modify e
    static const int64_t OB_MAX_VARCHAR_LENGTH = 64 * 1024;
    static const int64_t OB_COMMON_MEM_BLOCK_SIZE = OB_MAX_VARCHAR_LENGTH; // 64KB, ASK zhuweng before you want to modify
    static const int64_t OB_MAX_ROW_LENGTH = OB_MAX_PACKET_LENGTH - 512L * 1024L;
    static const int64_t OB_MAX_COLUMN_NUMBER = OB_ROW_MAX_COLUMNS_COUNT; // used in ObSchemaManagerV2
    static const int64_t OB_MAX_USER_DEFINED_COLUMNS_COUNT = OB_ROW_MAX_COLUMNS_COUNT - OB_APP_MIN_COLUMN_ID;
     static const int32_t OB_MAX_PARAMETERS_NUM = 64;
    //add wenghaixing [secondary index] 20141025
    static const int64_t OB_MAX_INDEX_COLUMNS=100;
    static const int64_t OB_MAX_INDEX_NUMS=10;
    enum IndexStatus
    {

      NOT_AVALIBALE=0,
      AVALIBALE,//1
      //modify wenghaixing [secondary index col checksum ]20141217
      //ERROR old code
      ERROR,//2
      WRITE_ONLY,//3
      INDEX_INIT,//4
      //modify e
    };
    //add e
    //add liumz, [static_index range_intersect]20170915:b
    enum IndexPhase
    {
      INIT_PHASE = 0,
      LOCAL_PHASE,
      GLOABL_PHASE,
    };
    //add:e
    static const int64_t OB_PREALLOCATED_NUM = 21;

    const char* const SYS_DATE = "$SYS_DATE";
    //add peiouya [Expire_condition_modify] 20140909:b
    const char* const SYS_DAY = "$SYS_DAY";
    //add 20140909:e
    //add liu jun. [MultiUPS] [sql_api] 20150707:b
    const char PARTITION_LIST_SEPARATOR = ',';
    const char * const GROUP_SEPARATOR = "#";
    //add 20150707:e
    const char* const OB_DEFAULT_COMPRESS_FUNC_NAME = "none";

    static const int64_t OB_MAX_CONFIG_NAME_LEN = 64;
    static const int64_t OB_MAX_CONFIG_VALUE_LEN = 1024;
    static const int64_t OB_MAX_CONFIG_INFO_LEN = 1024;
    static const int64_t OB_MAX_CONFIG_NUMBER = 1024;
    static const int64_t OB_MAX_EXTRA_CONFIG_LENGTH = 4096;

    static const int64_t OB_TABLET_MAX_REPLICA_COUNT = 6;

    const char* const OB_META_TABLE_NAME_PREFIX = "__m_";
    static const int64_t OB_META_TABLE_NAME_PREFIX_LENGTH = strlen(OB_META_TABLE_NAME_PREFIX);

    static const int64_t OB_DEFAULT_SSTABLE_BLOCK_SIZE = 16*1024; // 16KB
    static const int64_t OB_DEFAULT_MAX_TABLET_SIZE = 256*1024*1024; // 256MB
    static const int64_t OB_MYSQL_PACKET_BUFF_SIZE = 6 * 1024; //6KB
    static const int64_t OB_MAX_ERROR_MSG_LEN = 128;
    static const int64_t OB_MAX_ERROR_CODE = 10000;
    static const int64_t OB_MAX_THREAD_NUM = 1024;
    static const int64_t OB_CHAR_SET_NAME_LENGTH = 16;

    static const int64_t MAX_SQL_ERR_MSG_LENGTH = 256;

    //add peiouya [MultiUPS] [UPS_Manage_Function] 20150504:b
    //WARN:if modify following two items, please modify
    //ob_root_server_config.h:use_cluster_num and use_paxos_id_num's upper border.
    //static const int64_t MAX_CLUSTER_COUNT = 5;
    static const int64_t MAX_UPS_COUNT_ONE_CLUSTER = 9;
    //add 20150504:e

    static const int64_t OB_MAX_PAXOS_GROUP_COUNT = MAX_UPS_COUNT_ONE_CLUSTER;
    static const int64_t OB_MAX_PAXOS_UPS_QUORUM_SCALES_LENGTH = 20; //OB_MAX_PAXOS_GROUP_COUNT * (OB_MAX_DIGIT_AMOUNT_IN_UPS_QUORUM_SCALE[1] + 1

    //add lqc [MultiUps 1.0] [#13] 20170405 b
    static const int64_t OB_MAX_MOD_NUM = 13;
    //add e
    //add peiouya [MultiUPS] [UPS_Manage_Function] 20150729:b
    static const int64_t SYS_TABLE_PAXOS_ID = 0;
    static const int64_t INVALID_INDEX = -1;
    //add 20150729:e
    //add pangtianze [Paxos] 20170516:b
    static const int64_t OB_ALL_CLUSTER_FLAG = -1;
    //add:e

    enum ObDmlType
    {
      OB_DML_UNKNOW   = 0,
      OB_DML_REPLACE  = 1,
      OB_DML_INSERT   = 2,
      OB_DML_UPDATE   = 3,
      OB_DML_DELETE   = 4,
      //add by maosy [MultiUps 1.0] [secondary index optimize]20170401 b:
      OB_DML_INDEX_INSERT =5,
      // add by maosy e
      OB_DML_NUM,
    };
    //add by maosy [MultiUps 1.0] [secondary index optimize] 20170622 b:
    enum ObApplyState
    {
        NO_INDEX =0, //û���������ı�
        PRIMARY_TABLE =1,//��������Ӧ������
        INDEX_TABLE =2,//������
        INDEX_REPLACE_DEL =3//���������������ڽ��е�apply����replace�����ĵ�һ�׶Σ�ɾ����
    };
    // add by maosy e
    //add zhaoqiong [Schema Manager] 20150327:b
    enum DdlType
    {
      CREATE_TABLE = 1,
      DROP_TABLE = 2,
      ALTER_TABLE = 3,
      REFRESH_SCHEMA = 4
    };
    //add:e

    enum MinorFrozeStatus
    {
        UPS_NOT_FROZEN = 0,
        UPS_FROZEN = 1,
        UPS_FROZEN_FAIL = 2,
    };

  } // end namespace common
} // end namespace oceanbase
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&);               \
  void operator=(const TypeName&)

// ���� serialize ���� pos������������������������
// serialize�����л������ݴ�(buf+pos)����ʼд�룬
// д����ɺ����pos�����д��������Ҫ����(buf+buf_len)��
// serialize����ʧ�ܡ�
//
// ���� deserialize ���� pos������������������������
// deserialize��(buf+pos)����ʼ�ض������ݽ��з����л���
// ��ɺ����pos����������л���������Ҫ����(buf+data_len)��
// deserialize����ʧ�ܡ�

#define NEED_SERIALIZE_AND_DESERIALIZE \
int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; \
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
    int64_t get_serialize_size(void) const

#define INLINE_NEED_SERIALIZE_AND_DESERIALIZE \
inline int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; \
  inline int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
    inline int64_t get_serialize_size(void) const

#define VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE \
virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; \
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
    virtual int64_t get_serialize_size(void) const

#define PURE_VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE \
virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const = 0; \
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos) = 0; \
    virtual int64_t get_serialize_size(void) const = 0

#define DEFINE_SERIALIZE(TypeName) \
  int TypeName::serialize(char* buf, const int64_t buf_len, int64_t& pos) const


#define DEFINE_DESERIALIZE(TypeName) \
  int TypeName::deserialize(const char* buf, const int64_t data_len, int64_t& pos)

#define DEFINE_GET_SERIALIZE_SIZE(TypeName) \
  int64_t TypeName::get_serialize_size(void) const
#ifndef UNUSED
#define UNUSED(v) ((void)(v))
#endif

#define DATABUFFER_SERIALIZE_INFO \
  data_buffer_.get_data(), data_buffer_.get_capacity(), data_buffer_.get_position()

#define OB_LIKELY(x)       __builtin_expect(!!(x),1)
#define OB_UNLIKELY(x)     __builtin_expect(!!(x),0)

#define ARRAYSIZEOF(a) (sizeof(a)/sizeof(a[0]))

#define OB_ASSERT(x) do{ if(!(x)) YYSYS_LOG(ERROR, "assert fail, exp=%s", #x); assert(x);} while(false)

#define ATOMIC_CAS(val, cmpv, newv) __sync_val_compare_and_swap((val), (cmpv), (newv))
#define ATOMIC_ADD(val, addv) __sync_add_and_fetch((val), (addv))
#define ATOMIC_SUB(val, subv) __sync_sub_and_fetch((val), (subv))
#define ATOMIC_INC(val) __sync_add_and_fetch((val), 1)
#define ATOMIC_DEC(val) __sync_sub_and_fetch((val), 1)
//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
#define __COMPILER_BARRIER() asm volatile("dmb ish" ::: "memory")
#define PAUSE() asm("YIELD\n")
#else
//add support for arm platform by wangd 202106:e
#define __COMPILER_BARRIER() asm volatile("" ::: "memory")
#define PAUSE() asm("pause\n")
#endif //add support for arm platform by wangd 202106

#define CACHE_ALIGN_SIZE 64
#define CACHE_ALIGNED __attribute__((aligned(CACHE_ALIGN_SIZE)))
#define DIO_ALIGN_SIZE 512
const int64_t OB_SERVER_VERSION_LENGTH = 64;

#define OB_SET_DISK_ERROR(disk_no) \
  do{\
    ObDiskCheckerSingleton::get_instance().set_check_stat(disk_no, CHECK_ERROR); \
  }while(0)


#define OB_STAT_INC(module, args...) \
  do { \
    ObStatManager *mgr = ObStatSingleton::get_instance(); \
    if (NULL != mgr) { mgr->inc(OB_STAT_##module, 0/*table id*/, ##args); } \
  }while(0)


#define OB_STAT_SET(module, args...) \
  do { \
    ObStatManager *mgr = ObStatSingleton::get_instance(); \
    if (NULL != mgr) { mgr->set_value(OB_STAT_##module, 0, /* table id */ ##args); } \
  }while(0)


#define OB_STAT_GET(module, args...) \
  do { \
    ObStatManager *mgr = ObStatSingleton::get_instance(); \
    if (NULL != mgr) { mgr->get_stat(OB_STAT_##module, 0, ##args); } \
  }while(0)

#define OB_STAT_GET_VALUE(module, type) \
  ({ \
    int64_t ret = 0; \
    ObStatManager *mgr = ObStatSingleton::get_instance(); \
    ObStat *stat = NULL; \
    if (NULL != mgr) { mgr->get_stat(OB_STAT_##module, 0, stat); } \
    if (NULL != stat) { ret = stat->get_value(type);} \
    ret; \
  })

#define OB_STAT_TABLE_INC(module, table_id, args...) \
  do { \
    ObStatManager *mgr = ObStatSingleton::get_instance(); \
    if (NULL != mgr) { mgr->inc(OB_STAT_##module,(table_id), ##args); } \
  }while(0)



#define OB_STAT_TABLE_SET(module, table_id, args...) \
  do { \
    ObStatManager *mgr = ObStatSingleton::get_instance(); \
    if (NULL != mgr) { mgr->set_value(OB_STAT_##module, (table_id), ##args); } \
  }while(0)


#define OB_STAT_TABLE_GET(module, table_id, args...) \
  do { \
    ObStatManager *mgr = ObStatSingleton::get_instance(); \
    if (NULL != mgr) { mgr->get_stat(OB_STAT_##module, (table_id), ##args); } \
  }while(0)


#ifdef __ENABLE_PRELOAD__
#include "ob_preload.h"
#endif

#include "ob_thread_name.h"

#endif // OCEANBASE_COMMON_DEFINE_H_

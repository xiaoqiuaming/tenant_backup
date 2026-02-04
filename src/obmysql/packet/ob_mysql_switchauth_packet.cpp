#include "ob_mysql_switchauth_packet.h"
#include "../ob_mysql_util.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace obmysql
  {
    ObMySQLSwitchAuthPacket::ObMySQLSwitchAuthPacket()
    {
      status_tag_ = 0xFE;
    }

    ObMySQLSwitchAuthPacket::~ObMySQLSwitchAuthPacket()
    {

    }

    void ObMySQLSwitchAuthPacket::set_auth_plugin_name(const char* plugin_name)
    {
      auth_plugin_name_.assign_ptr(const_cast<char *>(plugin_name), static_cast<int32_t>(strlen(plugin_name)));
    }

    void ObMySQLSwitchAuthPacket::set_auth_plugin_data(const char *plugin_data)
    {
      auth_plugin_data_.assign_ptr(const_cast<char *>(plugin_data), static_cast<int32_t>(strlen(plugin_data)));
    }

    uint64_t ObMySQLSwitchAuthPacket::get_serialize_size()
    {
      uint64_t len = 0;
      len += 4; //packet header
      len += 1; //status_flag_
      len += auth_plugin_name_.length();
      len += auth_plugin_data_.length();
      return len;
    }

    int ObMySQLSwitchAuthPacket::serialize(char *buffer, int64_t len, int64_t &pos)
    {
      int ret = OB_SUCCESS;
      int64_t start_pos = pos;
      if (NULL == buffer || 0 >= len || pos < 0)
      {
        YYSYS_LOG(ERROR, "invalid argument buffer=%p, length=%ld, pos=%ld",
                  buffer, len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        pos += OB_MYSQL_PACKET_HEADER_SIZE;
        if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, len, status_tag_, pos)))
        {
          YYSYS_LOG(ERROR, "serialize packet status_tag failed, buffer=%p, length=%ld,"
                    "status_tag=%d, pos=%ld", buffer, len, status_tag_, pos);
        }
        else if(OB_SUCCESS != (ret = ObMySQLUtil::store_obstr_zt(buffer, len, auth_plugin_name_, pos)))
        {
          YYSYS_LOG(ERROR, "serialize packet auth_plugin_name failed, buffer=%p, length=%ld,"
                    "auth_plugin_name len=%d, pos=%ld", buffer, len, auth_plugin_name_.length(), pos);
        }
        else if(OB_SUCCESS != (ret = ObMySQLUtil::store_obstr_zt(buffer, len, auth_plugin_data_, pos)))
        {
          YYSYS_LOG(ERROR, "serialize packet auth_plugin_data_ failed, buffer=%p, length=%ld,"
                    "auth_plugin_data len=%d, pos=%ld", buffer, len, auth_plugin_data_.length(), pos);
        }
        uint32_t pkt_len = static_cast<uint32_t>(pos - start_pos - OB_MYSQL_PACKET_HEADER_SIZE);
        if(OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = ObMySQLUtil::store_int3(buffer, len, pkt_len, start_pos)))
          {
            YYSYS_LOG(ERROR, "serialize packet header size failed, buffer=%p, length=%ld, value=%d, pos=%ld",
                      buffer, len, pkt_len, start_pos);
          }
          else if(OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, len, 2, start_pos)))
          {
            YYSYS_LOG(ERROR, "serialize packet header seq failed, buffer=%p, length=%ld, value=%d, pos=%ld",
                      buffer, len, 2, start_pos);
          }
        }
      }
      return ret;
    }
  }
}

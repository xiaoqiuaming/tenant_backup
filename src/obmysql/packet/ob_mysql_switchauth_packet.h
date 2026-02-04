#ifndef OB_MYSQL_SWITCHAUTH_PACKET_H_
#define OB_MYSQL_SWITCHAUTH_PACKET_H_

#include "ob_mysql_packet_header.h"
#include "ob_mysql_packet.h"
#include "common/ob_string.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLSwitchAuthPacket : public ObMySQLPacket
    {
      public:
        ObMySQLSwitchAuthPacket();
        ~ObMySQLSwitchAuthPacket();

        int serialize(char *buffer, int64_t length, int64_t& pos);

        uint64_t get_serialize_size();

        void set_auth_plugin_name(const char* plugin_name);
        void set_auth_plugin_data(const char* auth_plugin_data);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObMySQLSwitchAuthPacket);

        uint8_t status_tag_;
        common::ObString auth_plugin_name_;
        common::ObString auth_plugin_data_;
    };
  }
}

#endif // OB_MYSQL_SWITCHAUTH_PACKET_H_

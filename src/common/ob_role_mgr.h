/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_COMMON_OB_ROLE_MGR_H_
#define OCEANBASE_COMMON_OB_ROLE_MGR_H_

#include "ob_atomic.h"
#include <yysys.h>

namespace oceanbase
{
  namespace common
  {
    /// @brief ObRoleMgr�����˽��̵Ľ�ɫ��״̬
    class ObRoleMgr
    {
      public:
        enum Role
        {
          MASTER = 1,
          SLAVE = 2,
          STANDALONE = 3, // used for test
        };

        /// @brief Master��Slave��״̬λ
        /// ERROR״̬: ����״̬
        /// INIT״̬: ��ʼ��
        /// ACTIVE״̬: �ṩ��������
        /// SWITCHING: Slave�л�ΪMaster�����е�״̬
        /// STOP:
        /// Slave�л�ΪMaster���̵�״̬ת��˳���ǣ�
        ///     (SLAVE, ACTIVE) -> (MASTER, SWITCHING) -> (MASTER, ACTIVE)
        enum State
        {
          ERROR = 1,
          INIT = 2,
          NOTSYNC = 3,
          ACTIVE = 4,
          SWITCHING = 5,
          STOP = 6,
          HOLD = 7,
        };

      public:
        ObRoleMgr()
        {
          role_ = MASTER;
          state_ = INIT;
          //add lbzhong [Paxos Cluster.Balance] 201607018:b
          new_master_flag_ = 0;
          //add:e
        }

        virtual ~ObRoleMgr()
        {}

        /// @brief ��ȡRole
        inline Role get_role() const
        { return role_; }

        /// @brief �޸�Role
        inline void set_role(const Role role)
        {
          YYSYS_LOG(INFO, "before set_role=%s state=%s", get_role_str(), get_state_str());
          atomic_exchange(reinterpret_cast<uint32_t*>(&role_), static_cast<uint32_t>(role));//mod support for arm platform by wangd 202106
          YYSYS_LOG(INFO, "after set_role=%s state=%s", get_role_str(), get_state_str());
        }

        /// ��ȡState
        inline State get_state() const
        { return state_; }

        /// �޸�State
        inline void set_state(const State state)
        {
          YYSYS_LOG(INFO, "before set_state=%s role=%s", get_state_str(), get_role_str());
          atomic_exchange(reinterpret_cast<uint32_t*>(&state_), static_cast<uint32_t>(state));//mod support for arm platform by wangd 202106
          YYSYS_LOG(INFO, "after set_state=%s role=%s", get_state_str(), get_role_str());
        }

        //add lbzhong [Paxos Cluster.Balance] 201607018:b
        inline bool get_new_master_flag() const
        { return new_master_flag_ == 1; }
        inline void set_new_master_flag(const bool new_master_flag)
        {
          uint8_t tmp = new_master_flag ? 1 : 0;
          atomic_exchange(reinterpret_cast<uint8_t*>(&new_master_flag_), tmp);
        }
        //add:e

        inline const char* get_role_str() const
        {
          switch (role_)
          {
            case MASTER:
              return "MASTER";
            case SLAVE:
              return "SLAVE";
            case STANDALONE:
              return "STANDALONE";
            default:
              return "UNKNOWN";
          }
        }

        inline const char* get_state_str() const
        {
          switch (state_)
          {
            case ERROR:
              return "ERROR";
            case INIT:
              return "INIT";
            case ACTIVE:
              return "ACTIVE";
            case SWITCHING:
              return "SWITCHING";
            case STOP:
              return "STOP";
            case HOLD:
              return "HOLD";
            default:
              return "UNKNOWN";
          }
        }

        inline bool is_master() const
        {
          return (role_ == ObRoleMgr::MASTER) && (state_ == ObRoleMgr::ACTIVE);
        }
        //add pangtianze [Paxos]20170628:b
        inline bool is_slave() const
        {
          return (role_ == ObRoleMgr::SLAVE) && (state_ == ObRoleMgr::ACTIVE);
        }
        //add:e
      private:
        Role role_;
        State state_;
        //add lbzhong [Paxos Cluster.Balance] 201607018:b
        uint8_t new_master_flag_; //0,1
        //add:e
    };
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_ROLE_MGR_H_

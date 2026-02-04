/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_scan_helper.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_scan_helper_impl.h"
#include "common/utility.h"
using namespace oceanbase::common;

ObScanHelperImpl::ObScanHelperImpl():scan_timeout_us_(0), mutate_timeout_us_(0),
   scan_retry_times_(OB_TABLET_MAX_REPLICA_COUNT), ms_provider_(NULL),
   ups_provider_(NULL), rpc_stub_(NULL)
{
}

ObScanHelperImpl::~ObScanHelperImpl()
{
}

//add hongchen [UNLIMIT_TABLE] 20161031:b
int ObScanHelperImpl::scan_for_next_packet(const common::ServerSession& ssession, ObScanner& scanner) const
{
    int ret = OB_SUCCESS;

    ret = rpc_stub_->scan_for_next_packet(ssession, scan_timeout_us_ * 10, scanner);

    return ret;
}
//mod hongchen [UNLIMIT_TABLE] 20161031:e

//mod hongchen [UNLIMIT_TABLE] 20161031:b
//int ObScanHelperImpl::scan(const ObScanParam& scan_param, ObScanner &out) const
int ObScanHelperImpl::scan(const ObScanParam& scan_param, ObScanner &out, ServerSession* ssession) const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "scan help not init");
  }
  else
  {
    ObServer ms;
    for (int64_t i = 0; i < scan_retry_times_; ++i)
    {
      if (OB_SUCCESS != (ret = ms_provider_->get_ms(scan_param, i, ms)))
      {
        YYSYS_LOG(WARN, "failed to get one mergeserver, err=%d", ret);
      }
      else if (0 == ms.get_port() || 0 == ms.get_ipv4())
      {
        YYSYS_LOG(WARN, "invalid merge server address, i=%ld", i);
        ret = OB_INVALID_ARGUMENT;
      }
      /*
      else if (OB_SUCCESS == (ret = rpc_stub_->scan(ms, scan_param, out, scan_timeout_us_)))
      {
        YYSYS_LOG(DEBUG, "scan from ms=%s", to_cstring(ms));
        break;
      }
      */
      else if (OB_SUCCESS == (ret = rpc_stub_->scan(ms, scan_param, out, scan_timeout_us_, NULL == ssession ? NULL : &(ssession->session_id_))))
      {
        YYSYS_LOG(DEBUG, "scan from ms=%s", to_cstring(ms));
        if (NULL != ssession)
        {
          ssession->server_ = ms;
        }
        break;
      }
      else
      {
        YYSYS_LOG(WARN, "scan ms timeout, scan_timeout_us_=%ld, ms=%s, retry=%ld",
            scan_timeout_us_, to_cstring(ms), i);
      }
    } // end for
  }
  return ret;
}
//mod hongchen [UNLIMIT_TABLE] 20161031:e

int ObScanHelperImpl::mutate(ObMutator& mutator)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "scan help not init");
  }
  else
  {
    ObServer ups;
    if (OB_SUCCESS != (ret = ups_provider_->get_ups(ups)))
    {
      YYSYS_LOG(WARN, "failed to get ups, err=%d", ret);
    }
    else if (0 == ups.get_port() || 0 == ups.get_ipv4())
    {
      ret = OB_INVALID_ARGUMENT;
      YYSYS_LOG(WARN, "invalid update server address");
    }
    else if (OB_SUCCESS != (ret = rpc_stub_->mutate(ups, mutator, mutate_timeout_us_)))
    {
      YYSYS_LOG(WARN, "failed to mutate, mutate_timeout_us_=%ld, ups=%s, err=%d",
          mutate_timeout_us_, to_cstring(ups), ret);
    }
    else
    {
      YYSYS_LOG(DEBUG, "ups mutate succ, ups=%s", to_cstring(ups));
    }
  }
  return ret;
}

//add zhaoqiong [Schema Manager] 20150327:b
ObLocalMs::ObLocalMs()
{
}

ObLocalMs::~ObLocalMs()
{
}

int ObLocalMs::set_ms(const ObServer &ms)
{
  ms_ = ms;
  return OB_SUCCESS;
}

int ObLocalMs::get_ms(ObServer &ms)
{
  ms = ms_;
  return OB_SUCCESS;
}

int ObLocalMs::get_ms(const ObScanParam &param, const int64_t retry_num, ObServer &server)
{
  UNUSED(param);
  UNUSED(retry_num);
  server = ms_;
  return OB_SUCCESS;
}

ObLocalMsList::ObLocalMsList()
{
  ms_list_ = NULL;
}

ObLocalMsList::~ObLocalMsList()
{
}

int ObLocalMsList::init(MsList* ms_list)
{
  ms_list_ = ms_list;
  return OB_SUCCESS;
}

int ObLocalMsList::get_ms(ObServer & ms)
{
  UNUSED(ms);
  return OB_NOT_IMPLEMENT;
}

int ObLocalMsList::get_ms(const ObScanParam &param, const int64_t retry_num, ObServer &server)
{
  int ret = OB_SUCCESS;
  UNUSED(param);
  UNUSED(retry_num);
  if (NULL == ms_list_)
  {
    ret = OB_NOT_INIT;
  }
  else
  {
      // [595-feature_rs_get_local_ms]
//    server = ms_list_->get_one();
      server = ms_list_->get_cluster_one();
      // [595-feature_rs_get_local_ms] end
  }
  return ret;
}
 //add:e

// add [secondary index getold cchecksum opt]
ObLocalCsMsList::ObLocalCsMsList()
{
    ms_list_ = NULL;
    local_cs_= ObServer();
}

ObLocalCsMsList::~ObLocalCsMsList()
{

}

int ObLocalCsMsList::init(MsList *ms_list, const ObServer &server)
{
    ms_list_= ms_list;
    local_cs_ = server;
    return OB_SUCCESS;
}

int ObLocalCsMsList::get_ms(ObServer &ms)
{
    UNUSED(ms);
    return OB_NOT_IMPLEMENT;
}

int ObLocalCsMsList::get_ms(const ObScanParam &param, const int64_t retry_num, ObServer &server)
{
    int ret = OB_SUCCESS;
    UNUSED(param);
    if(NULL == ms_list_
       || 0 > retry_num
       || 0 == local_cs_.get_port()
       || 0 == local_cs_.get_ipv4())
    {
        ret = OB_NOT_INIT;
    }
    else if(0 == retry_num)
    {
        if(ms_list_->is_local_ms_alive(local_cs_,server))
        {
            //do nothing
        }
        else
        {
            server = ms_list_->get_cluster_one();
        }
    }
    else
    {
        server = ms_list_->get_cluster_one();
    }
    return ret;
}

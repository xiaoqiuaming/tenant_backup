/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_ups_check_runnable.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_ups_check_runnable.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObUpsCheckRunnable::ObUpsCheckRunnable(ObUpsManager& ups_manager)
  :ups_manager_(ups_manager)
{
}

ObUpsCheckRunnable::~ObUpsCheckRunnable()
{
}

void ObUpsCheckRunnable::run(yysys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);
  YYSYS_LOG(INFO, "[NOTICE] ups check thread start, tid=%ld", syscall(__NR_gettid));
  SET_THD_NAME_ONCE("rs-ups-check");
  while (!_stop)
  {
    //mod lqc [multiups][check master] 20170712 b
    if(ups_manager_.is_rs_master ())
    {
      ups_manager_.check_ups_master_exist();
      ups_manager_.check_lease();
      //add peiouya [MultiUPS] [UPS_Manage_Function] 20150527:b
      ups_manager_.set_new_frozen_version(ups_manager_.get_max_frozen_version());
      ups_manager_.check_and_set_last_frozen_version();
      //add 20150527:e
      ups_manager_.check_minor_frozen_and_set_stat();
    }//mod e
    usleep(CHECK_INTERVAL_US);
  }
  YYSYS_LOG(INFO, "[NOTICE] ups check thread exit");
}


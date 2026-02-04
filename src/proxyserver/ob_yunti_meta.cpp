/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 *  ob_yunti_meta.cpp is for 
 *
 *  Authors:
 *    zian <yunliang.shi@alipay.com>
 *
 */

#include "ob_yunti_meta.h"
#include "ob_yunti_proxy.h"
#include "common/ob_file.h"

namespace oceanbase
{
  namespace proxyserver
  {
    const char* YuntiMeta::TABLE_INFO_FILE_NAME = "_table_info";
    const char* YuntiMeta::PARTITION_FILE_NAME = "_partition_file";
    common::ObStringBuf name_pool_(ObModIds::TEST, 64*1024L);


    int transform_date_to_time(const char *str, common::ObDateTime& val, int &mem_sec)
    {
      int err = OB_SUCCESS;
      struct tm time;
      time_t tmp_time = 0;
      val = -1;
      if (NULL != str && *str != '\0')
      {
        if (strchr(str, '-') != NULL)
        {
          if (strchr(str, ':') != NULL)
          {
              if(strchr(str, '.') != NULL)
              {
                  if((sscanf(str, "%4d-%2d-%2d %2d:%2d:%2d.%6d",&time.tm_year,
                             &time.tm_mon,&time.tm_mday,&time.tm_hour,
                             &time.tm_min,&time.tm_sec, &mem_sec)) != 7)
                  {
                      err = OB_ERROR;
                  }
              }
              else
              {
                  if ((sscanf(str,"%4d-%2d-%2d %2d:%2d:%2d",&time.tm_year,
                          &time.tm_mon,&time.tm_mday,&time.tm_hour,
                          &time.tm_min,&time.tm_sec)) != 6)
                  {
                    err = OB_ERROR;
                  }
              }

          }
          else
          {
            if ((sscanf(str,"%4d-%2d-%2d",&time.tm_year,&time.tm_mon,
                    &time.tm_mday)) != 3)
            {
              err = OB_ERROR;
            }
            time.tm_hour = 0;
            time.tm_min = 0;
            time.tm_sec = 0;
          }
        }
        else
        {
          if (strchr(str, ':') != NULL)
          {
            if ((sscanf(str,"%4d%2d%2d %2d:%2d:%2d",&time.tm_year,
                    &time.tm_mon,&time.tm_mday,&time.tm_hour,
                    &time.tm_min,&time.tm_sec)) != 6)
            {
              err = OB_ERROR;
            }
          }
          else if (strlen(str) > 8)
          {
            if ((sscanf(str,"%4d%2d%2d%2d%2d%2d",&time.tm_year,
                    &time.tm_mon,&time.tm_mday,&time.tm_hour,
                    &time.tm_min,&time.tm_sec)) != 6)
            {
              err = OB_ERROR;
            }
          }
          else
          {
            if ((sscanf(str,"%4d%2d%2d",&time.tm_year,&time.tm_mon,
                    &time.tm_mday)) != 3)
            {
              err = OB_ERROR;
            }
            time.tm_hour = 0;
            time.tm_min = 0;
            time.tm_sec = 0;
          }
        }
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN,"sscanf failed : [%s] ",str);
        }
        else
        {
          time.tm_year -= 1900;
          time.tm_mon -= 1;
          time.tm_isdst = -1;

          if ((tmp_time = mktime(&time)) != -1)
          {
            val = tmp_time;
          }
          else
          {
            YYSYS_LOG(WARN, "failed to mktime, [%s]", str);
            err = OB_ERROR;
          }
        }
      }
      return err;
    }

    int drop_esc_char(char *buf,int32_t& len)
    {
      int ret = OB_SUCCESS;
      int32_t orig_len = len;
      char *dest = NULL;
      char *ptr = NULL;
      int32_t final_len = len;
      if (NULL == buf)
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        dest = buf;
        ptr = buf;
        for(int32_t i=0;i<orig_len-1;++i)
        {
          if ('\\' == *ptr)
          {
            switch(*(ptr+1))
            {
              case '\"':
                *dest++ = *(ptr + 1);
              ptr += 2;
              --final_len;
              break;
              case '\'':
              *dest++ = *(ptr + 1);
              ptr += 2;
              --final_len;
              break;
              case '\\':
              *dest++ = *(ptr + 1);
              ptr += 2;
              --final_len;
              break;
              default:
              {
                if (dest != ptr)
                  *dest = *ptr;
                ++dest;
                ++ptr;
              }
              break;
            }
          }
          else
          {
            if (dest != ptr)
              *dest = *ptr;
            ++dest;
            ++ptr;
          }
        }
        len = final_len;
      }
      return ret;
    }

    MetaInfo::MetaInfo()
      : mod_(ObModIds::OB_MEM_BUFFER), allocator_(ModuleArena::DEFAULT_PAGE_SIZE, mod_)
    {
      reset();
    }

    void MetaInfo::reset()
    {
      sstable_version_ = 0;
      memset(sstable_dir_, 0, sizeof(sstable_dir_));
      range_list_.clear();
      endkey_list_.clear();
      delimeter_ = '\1';
      memset(column_info_, 0, sizeof(column_info_));
      memset(table_name_, 0, sizeof(table_name_));
      column_num_ = 0;
      table_id_ = 0;
      ref_count_ = 0;
      allocator_.free();
    }

    int64_t MetaInfo::to_string(char* buffer, const int64_t length) const
    {
      int64_t pos = 0;
      databuff_printf(buffer, length, pos, "table_id=%lu, sstable_version=%d, sstable_dir=%s ref_count:%d\n", table_id_, sstable_version_, sstable_dir_, ref_count_);
      return pos;
    }

    YuntiMeta::~YuntiMeta()
    {
      MetaInfo* meta_info = NULL;
      for(int64_t i = 0; i < meta_infos_.count(); i++)
      {
        meta_info = meta_infos_.at(i);
        if(meta_info != NULL)
        {
          meta_info->reset();
          ob_free(meta_info, ObModIds::OB_MEM_BUFFER);
          meta_info = NULL;
        }
      }
    }

    int YuntiMeta::initialize(YuntiProxy* yunti_proxy, const int32_t mem_limit)
    {
      meta_map_.create(DEFAULT_META_NUM);
      mem_limit_ = mem_limit;
      yunti_proxy_ = yunti_proxy;
      return OB_SUCCESS;
    }

    int YuntiMeta::get_meta_info(const char* sstable_dir, MetaInfo* &meta_info)
    {
      int ret = OB_SUCCESS;
      MetaInfo* tmp_meta_info = NULL;
      int hash_ret = 0;
      ObString sstable_dir_key;
      sstable_dir_key.assign_ptr(const_cast<char*>(sstable_dir), static_cast<int32_t>(strlen(sstable_dir)));

      //find from the map first
      {
        yysys::CRLockGuard guard(lock_);
        hash_ret = meta_map_.get(sstable_dir_key, tmp_meta_info);
        if(hash::HASH_EXIST == hash_ret && NULL != tmp_meta_info)
        {
          tmp_meta_info->inc_ref();
        }
      }

      if(hash::HASH_EXIST == hash_ret && NULL != tmp_meta_info)
      {
        YYSYS_LOG(INFO, "get succ from hashmap, meta_info:%s", to_cstring(*tmp_meta_info));
      }
      else if(-1 == hash_ret)
      {
        YYSYS_LOG(WARN, "failed to find sstable_dir=%s in hash map, hash_ret=%d",
            sstable_dir, hash_ret);
        ret = OB_DATA_SOURCE_SYS_ERROR;
      }
      else
      {
        yysys::CWLockGuard guard(lock_);
        //find from the map again
        hash_ret = meta_map_.get(sstable_dir_key, tmp_meta_info);
        if(hash::HASH_EXIST == hash_ret && NULL != tmp_meta_info)
        {
          tmp_meta_info->inc_ref();
          YYSYS_LOG(INFO, "get succ from hashmap, meta_info:%s", to_cstring(*tmp_meta_info));
        }
        else if(OB_SUCCESS != (ret = create_meta_info(sstable_dir, tmp_meta_info)))
        {
          YYSYS_LOG(ERROR, "not exist sstable dir:%s", sstable_dir);
        }
        else
        {
          tmp_meta_info->inc_ref();
          YYSYS_LOG(INFO, "create meta info succ, meta_info:%s", to_cstring(*tmp_meta_info));
        }
      }

      if(OB_SUCCESS == ret)
      {
        meta_info = tmp_meta_info;
        //double check
        if(strncmp(sstable_dir, meta_info->sstable_dir_, strlen(meta_info->sstable_dir_)) != 0)
        {
          YYSYS_LOG(ERROR, "require key sstable_dir:%s => value:%s", sstable_dir, to_cstring(*tmp_meta_info));
          ret = OB_DATA_SOURCE_SYS_ERROR;
        }
      }

      return ret;
    }

    int YuntiMeta::get_sstable_version(const char* sstable_dir, int16_t &sstable_version)
    {
      int ret = OB_SUCCESS;
      MetaInfo* meta_info = NULL;
      if(OB_SUCCESS != (ret = get_meta_info(sstable_dir, meta_info)))
      {
        YYSYS_LOG(ERROR, "get meta info failed, ret:%d", ret);
      }
      else if(!meta_info)
      {
        YYSYS_LOG(ERROR, "get meta info null");
      }
      else
      {
        sstable_version = meta_info->sstable_version_;
      }

      if(NULL != meta_info)
      {
        meta_info->dec_ref();
        YYSYS_LOG(INFO, "return meta info succ, meta_info:%s", to_cstring(*meta_info));
      }
      
      return ret;
    }

    int YuntiMeta::fetch_range_list(const char* table_name, const char* sstable_dir, common::ObArray<common::ObNewRange*> &range_list)
    {
        UNUSED(table_name);
      int ret = OB_SUCCESS;
      MetaInfo* meta_info = NULL;
      if(OB_SUCCESS != (ret = get_meta_info(sstable_dir, meta_info)))
      {
        YYSYS_LOG(ERROR, "get meta info failed, ret:%d", ret);
      }
      else if(!meta_info)
      {
        YYSYS_LOG(ERROR, "get meta info null");
        ret = OB_DATA_SOURCE_SYS_ERROR;
      }
      /*else if(0 != strcmp(table_name, meta_info->table_name_))
      {
        YYSYS_LOG(ERROR, "meta table_name:%s not equal to query table_name:%s", meta_info->table_name_, table_name);
        ret = OB_DATA_SOURCE_RANGE_NOT_EXIST;
      }*/
      else
      {
        for(int32_t i = 0; i < meta_info->range_list_.size(); i++)
        {
          range_list.push_back(meta_info->range_list_.at(i));
        }
      }

      if(NULL != meta_info)
      {
        meta_info->dec_ref();
        YYSYS_LOG(INFO, "return meta info succ, meta_info:%s", to_cstring(*meta_info));
      }
      return ret;
    }

    int YuntiMeta::get_sstable_file_path(const char* sstable_dir, const ObNewRange &range, char* sstable_file_path, int len)
    {
      int ret = OB_SUCCESS;
      MetaInfo* meta_info = NULL;
      if(OB_SUCCESS != (ret = get_meta_info(sstable_dir, meta_info)))
      {
        YYSYS_LOG(ERROR, "get meta info failed, ret:%d", ret);
      }
      else if(!meta_info)
      {
        YYSYS_LOG(ERROR, "get meta info null");
        ret = OB_DATA_SOURCE_SYS_ERROR;
      }
      else
      {
        ObSortedVector<common::ObNewRange*>::iterator it = meta_info->range_list_.end();
        if(OB_SUCCESS != (ret = meta_info->range_list_.find(&range, it, compare_range, equal_range)))
        {
          ret = OB_DATA_SOURCE_RANGE_NOT_EXIST;
          YYSYS_LOG(ERROR, "cannot find range:%s", to_cstring(range));
        }
        else
        {
          long index = -1;
          index = it - meta_info->range_list_.begin();
          snprintf(sstable_file_path, len, "%s/%lu-%010ld", sstable_dir, range.table_id_, index);
          YYSYS_LOG(INFO, "read file path:%s", sstable_file_path);
        }
      }

      if(NULL != meta_info)
      {
        meta_info->dec_ref();
        YYSYS_LOG(INFO, "return meta info succ, meta_info:%s", to_cstring(*meta_info));
      }
      return ret;
    }


    bool compare_range(const ObNewRange* lhs, const ObNewRange* rhs)
    {
      return lhs->compare_with_endkey(*rhs) < 0;
    }

    bool equal_range(const ObNewRange* lhs, const ObNewRange* rhs)
    {
      return lhs->equal(*rhs);
    }

    bool unique_range(const ObNewRange* lhs, const ObNewRange* rhs)
    {
      return lhs->intersect(*rhs);
    }

    int YuntiMeta::parse_file(const char* sstable_dir, const char* file_name, parse_one_line parser, MetaInfo &meta_info)
    {
      int ret = OB_SUCCESS;
      int64_t read_size = 0;
      int64_t file_size = 0;
      char file_path[OB_MAX_FILE_NAME_LENGTH];
      char* buf = NULL;
      memset(file_path, 0, sizeof(file_path));
      FILE* fp = NULL;

      int size = snprintf(file_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s", sstable_dir, file_name);

      if(size + 1 > OB_MAX_FILE_NAME_LENGTH)
      {
        YYSYS_LOG(WARN, "file_path, cmd_len=%ld <= bufsiz=%d" ,OB_MAX_FILE_NAME_LENGTH, size);
        ret = OB_SIZE_OVERFLOW;
      }
      else if(!yunti_proxy_->is_file_exist(file_path))
      {
        YYSYS_LOG(ERROR, "file:%s not exist", file_path);
        ret = OB_DATA_SOURCE_TABLE_NOT_EXIST;
      }
      else if(OB_SUCCESS != (ret = yunti_proxy_->open_rfile(fp, file_path)) || fp == NULL)
      {
        YYSYS_LOG(ERROR, "open file:%s failed", file_path);
        ret = OB_DATA_SOURCE_READ_ERROR;
      }
      else if(OB_SUCCESS != (ret = yunti_proxy_->get_file_size(file_path, file_size)))
      {
        YYSYS_LOG(ERROR, "get file size:%s failed", file_path);
        ret = OB_DATA_SOURCE_READ_ERROR;
      }
      else if(NULL == (buf = static_cast<char*>(ob_malloc(file_size, ObModIds::OB_BUFFER))))
      {
        YYSYS_LOG(ERROR, "memory alloc failed");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if(OB_SUCCESS != (ret = yunti_proxy_->fetch_data(fp, buf, file_size, read_size)) || read_size != file_size)
      {
        YYSYS_LOG(ERROR, "read file size failed ret:%d file_size:%ld read_size:%ld", ret, file_size, read_size);
        ret = OB_DATA_SOURCE_READ_ERROR;
      }
      else
      {
        char* phead = buf;
        char* pend = buf + file_size;
        char* plast = phead;
        char* pcur = phead;
        do
        {
          while(pcur < pend && *pcur != '\n')
          {
            pcur++;
          }

          if(pcur == pend)
          {
            break;
          }

          if(OB_SUCCESS != (ret = (*parser)(plast, static_cast<int32_t>(pcur - plast), meta_info)))
          {
            YYSYS_LOG(ERROR, "parse one line failed, ret:%d", ret);
            break;
          }

          plast = pcur + 1;
          pcur += 1;

        }while(true);
      }

      if(NULL != buf)
      {
        ob_free(buf, ObModIds::OB_BUFFER);
      }

      if(NULL != fp)
      {
        if(OB_SUCCESS != yunti_proxy_->close_rfile(fp))
        {
          YYSYS_LOG(WARN, "close pipe fail");
        }
      }
      return ret;
    }

    MetaInfo* YuntiMeta::evict()
    {
      MetaInfo* tmp_meta_info = NULL;

      YYSYS_LOG(INFO, "used_mem_size:%ld mem_limit:%ld meta_info_count:%ld", used_mem_size_, mem_limit_, meta_infos_.count());

      if(used_mem_size_ > mem_limit_)
      {
        if(meta_infos_.count() > 0)
        {
          tmp_meta_info = meta_infos_.at(0);

          ObString sstable_dir_key;
          sstable_dir_key.assign_ptr(const_cast<char*>(tmp_meta_info->sstable_dir_), static_cast<int32_t>(strlen(tmp_meta_info->sstable_dir_)));
          if(0 == tmp_meta_info->get_ref())
          {
            meta_infos_.remove(0);
            YYSYS_LOG(INFO, "used_mem_size:%ld exceed mem_limit:%ld evict_sstable_dir:%s", used_mem_size_, mem_limit_, tmp_meta_info->sstable_dir_);
            //remove from the hash map
            meta_map_.erase(sstable_dir_key);
            used_mem_size_ -= tmp_meta_info->allocator_.used();
            tmp_meta_info->reset();
          }
          else
          {
            YYSYS_LOG(WARN, "ref count not equal to 0 sstable_dir:%s ref_count:%d", tmp_meta_info->sstable_dir_, tmp_meta_info->ref_count_);

            for(int64_t i = 0; i < meta_infos_.count(); i++)
            {
              tmp_meta_info = meta_infos_.at(i);
              if(NULL != tmp_meta_info)
              {
                YYSYS_LOG(WARN, "[%ld] sstable_dir:%s ref_count:%d", i, tmp_meta_info->sstable_dir_, tmp_meta_info->ref_count_);
              }
              else
              {
                YYSYS_LOG(ERROR, "[%ld] is NULL", i);
              }
            }
            tmp_meta_info = NULL;
          }
        }
      }

      return tmp_meta_info;
    }

    int YuntiMeta::create_meta_info(const char* sstable_dir, MetaInfo* &meta_info)
    {
      int ret = OB_SUCCESS;
      
      //check if can evict
      MetaInfo* tmp_meta_info = evict();

      if(NULL == tmp_meta_info)
      {
        char* ptr = (char*)ob_malloc(sizeof(MetaInfo), ObModIds::OB_MEM_BUFFER);
        if(ptr)
        {
          memset(ptr, 0, sizeof(MetaInfo));
          tmp_meta_info = new(ptr)MetaInfo();
        }
      }

      //check mem limit
      if(NULL == tmp_meta_info)
      {
        YYSYS_LOG(ERROR, "memory exhaust");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        if(OB_SUCCESS != (ret = parse_file(sstable_dir, TABLE_INFO_FILE_NAME, YuntiMeta::parse_table_info_one_line, *tmp_meta_info)))
        {
          YYSYS_LOG(ERROR, "failed parse the table info file, sstable_dir:%s", sstable_dir);
        }
        else if(OB_SUCCESS != (ret = parse_file(sstable_dir, PARTITION_FILE_NAME, YuntiMeta::parse_partition_one_line, *tmp_meta_info)))
        {
          YYSYS_LOG(ERROR, "failed parse the partition info file, sstable_dir:%s", sstable_dir);
        }
        else
        {
          //trans endkey to range
          common::ObNewRange* range = NULL;
          for(int64_t i = 0; i <= tmp_meta_info->endkey_list_.count(); i++)
          {
            range = (common::ObNewRange*)tmp_meta_info->allocator_.alloc_aligned(sizeof(common::ObNewRange));
            if(!range)
            {
              YYSYS_LOG(ERROR, "memory exhaust");
              ret = OB_ALLOCATE_MEMORY_FAILED;
              break;
            }

            memset(range, 0, sizeof(common::ObNewRange));
            range->table_id_ = tmp_meta_info->table_id_;

            if(i == 0)
            {
              range->start_key_.set_min_row();
            }
            else
            {
              range->start_key_ = *(tmp_meta_info->endkey_list_.at(i - 1));
            }

            range->border_flag_.unset_inclusive_start();

            if(i == tmp_meta_info->endkey_list_.count())
            {
              range->end_key_.set_max_row();
              range->border_flag_.unset_inclusive_end();
            }
            else
            {
              range->end_key_ = *(tmp_meta_info->endkey_list_.at(i));
              range->border_flag_.set_inclusive_end();
            }
            ObSortedVector<common::ObNewRange*>::iterator it = tmp_meta_info->range_list_.end();
            ret = tmp_meta_info->range_list_.insert_unique(range, it, compare_range, unique_range);
            if(OB_SUCCESS != ret)
            {
              char intersect_buf[OB_RANGE_STR_BUFSIZ];
              char input_buf[OB_RANGE_STR_BUFSIZ];
              range->to_string(intersect_buf, OB_RANGE_STR_BUFSIZ);
              if (it != tmp_meta_info->range_list_.end())
                (*it)->to_string(input_buf, OB_RANGE_STR_BUFSIZ);
              YYSYS_LOG(WARN, "cannot insert this tablet:%s, maybe intersect with exist tablet:%s",
                  intersect_buf, input_buf);
              break;
            }
          }

          ObString sstable_dir_key;
          strcpy(tmp_meta_info->sstable_dir_, sstable_dir);
          sstable_dir_key.assign_ptr(const_cast<char*>(tmp_meta_info->sstable_dir_),
              static_cast<int32_t>(strlen(tmp_meta_info->sstable_dir_)));

          if (OB_SUCCESS != ret)
          {
            YYSYS_LOG(ERROR, "parse range failed, ret:%d", ret);
          }
          else if (OB_SUCCESS != (ret = meta_infos_.push_back(tmp_meta_info)))
          {
            YYSYS_LOG(ERROR, "array push_back failed, ret:%d", ret);
          }
          else 
          {
            int hash_ret = meta_map_.set(sstable_dir_key, tmp_meta_info);
            if(-1 == hash_ret)
            {
              YYSYS_LOG(ERROR, "hash set failed, ret:%d", hash_ret);
              ret = hash_ret;
            }
            else if(hash::HASH_EXIST == hash_ret)
            {
              YYSYS_LOG(ERROR, "sstable_dir:%s already in hash, impossible, ret:%d", sstable_dir, hash_ret);
              ret = OB_DATA_SOURCE_SYS_ERROR;
            }
            else
            {
              YYSYS_LOG(INFO, "hash set succ, meta_info:%s hash_ret:%d", to_cstring(*tmp_meta_info), hash_ret);
              meta_info = tmp_meta_info;
              used_mem_size_ += tmp_meta_info->allocator_.used();
            }
          }
        }
      }

      if(OB_SUCCESS != ret && tmp_meta_info)
      {
        tmp_meta_info->reset();
        ob_free(tmp_meta_info, ObModIds::OB_MEM_BUFFER);
        tmp_meta_info = NULL;
      }

      return ret;
    }



    /*
     * the file content:
     * rowkey_desc=1,4,5,6-5 
     * sstable_version=2
     * delimeter=1
     */
    int YuntiMeta::parse_table_info_one_line(char* line, const int32_t len, MetaInfo &meta_info)
    {
      int ret = OB_SUCCESS;
      char* tmp_buf = (char*)ob_malloc(len + 1,  ObModIds::OB_BUFFER);
      if(NULL == tmp_buf)
      {
        YYSYS_LOG(ERROR, "memory alloc failed");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        memset(tmp_buf, 0, len + 1);
        memcpy(tmp_buf, line, len);
        char* split_pos = NULL;
        char *token = strtok_r(tmp_buf, "=", &split_pos);
        char* substr = NULL;
        if(0 == strcmp(token, "rowkey_desc"))
        {
          token = strtok_r(NULL, "=", &split_pos);
          char* sub_split_pos = NULL;
          token = strtok_r(token, ",", &sub_split_pos);
          while(token != NULL)
          {
            meta_info.column_info_[meta_info.column_num_].type = static_cast<common::ObObjType>(atoi(token));
            if(common::ObVarcharType == meta_info.column_info_[meta_info.column_num_].type)
            {
              char* sub2_split_pos = NULL;
              substr = token;
              token = strtok_r(substr, "-", &sub2_split_pos);
              token = strtok_r(NULL, "-", &sub2_split_pos);
              meta_info.column_info_[meta_info.column_num_].limit_len = atoi(token);

              token = strtok_r(NULL, "-", &sub2_split_pos);
              meta_info.column_info_[meta_info.column_num_].nullable = atoi(token);
            }
            else if(common::ObDecimalType == meta_info.column_info_[meta_info.column_num_].type)
            {
                char *sub2_split_pos = NULL;
                substr = token;
                token = strtok_r(substr, "-", &sub2_split_pos);
                token = strtok_r(NULL, "-", &sub2_split_pos);
                meta_info.column_info_[meta_info.column_num_].precision = atoi(token);
                token = strtok_r(NULL, "-", &sub2_split_pos);
                meta_info.column_info_[meta_info.column_num_].scale = atoi(token);
                token = strtok_r(NULL, "-", &sub2_split_pos);
                meta_info.column_info_[meta_info.column_num_].nullable = atoi(token);
            }
            else
            {
                char *sub2_split_pos = NULL;
                substr = token;
                token = strtok_r(substr, "-", &sub2_split_pos);
                token = strtok_r(NULL, "-", &sub2_split_pos);
                meta_info.column_info_[meta_info.column_num_].nullable = atoi(token);
            }
            token = strtok_r(NULL, ",", &sub_split_pos);
            meta_info.column_num_++;
          }
        }
        else if(0 == strcmp(token, "table_id"))
        {
          token = strtok_r(NULL, "=", &split_pos);
          meta_info.table_id_ = atoll(token);
          YYSYS_LOG(INFO, "table id:%lu", meta_info.table_id_);
        }
        else if(0 == strcmp(token, "sstable_version"))
        {
          token = strtok_r(NULL, "=", &split_pos);
          meta_info.sstable_version_ = static_cast<int16_t>(atoi(token));
          YYSYS_LOG(INFO, "sstable version:%d", meta_info.sstable_version_);
        }
        else if(0 == strcmp(token, "table_name"))
        {
          token = strtok_r(NULL, "=", &split_pos);
          int size = snprintf(meta_info.table_name_, OB_MAX_TABLE_NAME_LENGTH, "%s", token);
          if(size + 1 > OB_MAX_TABLE_NAME_LENGTH || size < 0)
          {
            YYSYS_LOG(ERROR, "table_name, table_name_len=%ld <= bufsiz=%d", OB_MAX_FILE_NAME_LENGTH, size);
            ret = OB_SIZE_OVERFLOW;
          }
        }
        else if(0 == strcmp(token, "delim"))
        {
          token = strtok_r(NULL, "=", &split_pos);
          int delimeter = atoi(token);
          meta_info.delimeter_ = static_cast<char>(delimeter);
          YYSYS_LOG(INFO, "delimeter:%c", meta_info.delimeter_);
        }

        ob_free(tmp_buf, ObModIds::OB_BUFFER);
        tmp_buf = NULL;
      }

      return ret;
    }

    int YuntiMeta::parse_partition_one_line(char* line, const int32_t len, MetaInfo &meta_info)
    {
      int ret = OB_SUCCESS;
      int64_t val = 0;
      int column_num = 0;
      char* ptr = NULL;
      ObObj* tmp_obj_array = NULL;
      if(NULL == (ptr = (char*)meta_info.allocator_.alloc_aligned(sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER)))
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        YYSYS_LOG(WARN, "allocate mem for obj array fail");
      }
      else
      {
        tmp_obj_array = new(ptr) ObObj[OB_MAX_ROWKEY_COLUMN_NUMBER];
      }

      char* tmp_buf = (char*)ob_malloc(len + 1,  ObModIds::OB_BUFFER);
      if(NULL == tmp_buf)
      {
        YYSYS_LOG(ERROR, "memory alloc failed");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        memset(tmp_buf, 0, len + 1);
        memcpy(tmp_buf, line, len);
      }

      if(OB_SUCCESS == ret)
      {
        char* phead = tmp_buf;
        char* pend = tmp_buf + len;
        char* plast = phead;
        char* pcur = phead;
        do
        {
          while(pcur < pend && *pcur != meta_info.delimeter_)
          {
            pcur++;
          }

          if (pcur < pend)
          {
            *pcur = '\0';
          }

          if(plast >= pend)
          {
            break;
          }

          if(column_num == meta_info.column_num_)
          {
            break;
          }

          int32_t length = static_cast<int32_t>(pcur - plast);

          switch(meta_info.column_info_[column_num].type)
          {
            case ObIntType:
              {

              if(1 == length && *plast == '\2')
              {
                  if(meta_info.column_info_[column_num].nullable == 1)
                  {
                      tmp_obj_array[column_num].set_null();
                  }
                  else
                  {
                      YYSYS_LOG(WARN, "the column is not allowed null");
                      ret = OB_SKIP_INVALID_ROW;
                  }
              }
              else
              {
                int64_t v = 0;
                if (strchr(plast,'.') != NULL) //float/double to int
                {
                  double a = atof(plast);
                  a *= (double)DOUBLE_MULTIPLE_VALUE;
                  v = static_cast<int64_t>(a);
                }
                else
                {
                  v = atol(plast);
                }
                tmp_obj_array[column_num].set_int(v);
              }
              }
              break;
            //add lijianqiang [INT_32] 20151008:b
            case ObInt32Type:
              {
              if(1 == length && *plast == '\2')
              {
                  if(meta_info.column_info_[column_num].nullable == 1)
                  {
                      tmp_obj_array[column_num].set_null();
                  }
                  else
                  {
                      YYSYS_LOG(WARN, "the column is not allowed null");
                      ret = OB_SKIP_INVALID_ROW;
                  }
              }
              else
              {
                int32_t v = 0;
                if (strchr(plast,'.') != NULL) //float/double to int
                {
                  double a = atof(plast);
                  a *= (double)DOUBLE_MULTIPLE_VALUE;
                  v = static_cast<int32_t>(a);
                }
                else
                {
                  v = atoi(plast);
                }
                tmp_obj_array[column_num].set_int32(v);
              }
              }
              break;
            //add 20151008:e
            case ObVarcharType:
              {
              if(1 == length && *plast == '\2')
              {
                  ObString varchar = ObString::make_string('\0');
                  tmp_obj_array[column_num].set_varchar(varchar);
              }
              else
              {
                ObString tmp_str;
                char* obuf = NULL;
                if (pcur - plast > 0)
                {
                  int32_t len = static_cast<int32_t>(pcur - plast);
                  if(len > meta_info.column_info_[column_num].limit_len)
                  {
                    YYSYS_LOG(WARN, "exceed the limit, column_num:%d len:%d limit_len:%d", column_num, len, meta_info.column_info_[column_num].limit_len);
                  }
                  drop_esc_char(plast, len);


                  if(NULL == (obuf = (char*)meta_info.allocator_.alloc_aligned(len)))
                  {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    YYSYS_LOG(WARN, "allocate mem for obj vchar fail");
                    break;
                  }
                  else
                  {
                    memcpy(obuf, plast, len);
                    tmp_str.assign(obuf,len);
                  }
                }
                tmp_obj_array[column_num].set_varchar(tmp_str);
              }
              }
              break;
            case ObDateTimeType:
          {
              if(1 == length && *plast == '\2')
              {
                  if(meta_info.column_info_[column_num].nullable == 1)
                  {
                      tmp_obj_array[column_num].set_null();
                  }
                  else
                  {
                      YYSYS_LOG(WARN, "the column is not allowed null");
                      ret = OB_SKIP_INVALID_ROW;
                  }
              }
              else
              {
                  int mem_sec = 0;
                  ret = transform_date_to_time(plast, val, mem_sec);
                  if (OB_SUCCESS == ret)
                  {
                      tmp_obj_array[column_num].set_datetime(val * 1000 * 1000L + mem_sec);
                  }
                  else
                  {
                      YYSYS_LOG(WARN, "failed to trans date time: %s",
                                plast);
                      ret = OB_SKIP_INVALID_ROW;
                  }
              }
          }
              break;
            //add peiouya [DATE_TIME] 20150906:b
            case ObDateType:
          {
              if(1 == length && *plast == '\2')
              {
                  if(meta_info.column_info_[column_num].nullable == 1)
                  {
                      tmp_obj_array[column_num].set_null();
                  }
                  else
                  {
                      YYSYS_LOG(WARN, "the column is not allowed null");
                      ret = OB_SKIP_INVALID_ROW;
                  }
              }
              else
              {
                  struct tm time;
                  time_t tmp_time = 0;
                  if ((sscanf(plast,"%4d-%2d-%2d",&time.tm_year,&time.tm_mon,
                              &time.tm_mday)) != 3)
                  {
                      YYSYS_LOG(WARN, "failed to trans date time: %s",
                                plast);
                      ret = OB_SKIP_INVALID_ROW;
                  }
                  else
                  {
                      time.tm_year -=1900;
                      time.tm_mon -= 1;
                      time.tm_isdst = -1;
                      time.tm_hour = 0;
                      time.tm_min = 0;
                      time.tm_sec = 0;
                      if((tmp_time = mktime(&time)) != -1)
                      {
                          val = tmp_time;
                      }
                      else
                      {
                          YYSYS_LOG(WARN, "failed to mktime: %s", plast);
                          ret = OB_SKIP_INVALID_ROW;
                      }
                      tmp_obj_array[column_num].set_date(val * 1000 * 1000L); //second ->ms
                  }
              }
          }
              break;
            case ObTimeType:
          {
              if(1 == length && *plast == '\2')
              {
                  if(meta_info.column_info_[column_num].nullable == 1)
                  {
                      tmp_obj_array[column_num].set_null();
                  }
                  else
                  {
                      YYSYS_LOG(WARN, "the column is not allowed null");
                      ret = OB_SKIP_INVALID_ROW;
                  }
              }
              else
              {
                  int hour, minute, second;
                  if ((sscanf(plast,"%2d:%2d:%2d",&hour, &minute,
                              &second)) != 3)
                  {
                      YYSYS_LOG(WARN, "failed to trans date time: %s",
                                plast);
                      ret = OB_SKIP_INVALID_ROW;
                  }

                      val = hour * 3600 + minute * 60 + second;
                      tmp_obj_array[column_num].set_time(val * 1000 * 1000L);

              }
          }
              break;
            //add 20150906:e
            case ObPreciseDateTimeType:
          {
              if(1 == length && *plast == '\2')
              {
                  if(meta_info.column_info_[column_num].nullable == 1)
                  {
                      tmp_obj_array[column_num].set_null();
                  }
                  else
                  {
                      YYSYS_LOG(WARN, "the column is not allowed null");
                      ret = OB_SKIP_INVALID_ROW;
                  }
              }
              else
              {
                  int mem_sec = 0;
                  ret = transform_date_to_time(plast, val, mem_sec);
                  if(OB_SUCCESS == ret)
                  {
                      tmp_obj_array[column_num].set_precise_datetime(val * 1000 * 1000L + mem_sec);
                  }
                  else
                  {
                      YYSYS_LOG(WARN, "failed to trans date time: %s",
                                plast);
                      ret = OB_SKIP_INVALID_ROW;
                  }
              }
          }
              break;
            case ObFloatType:
          {
              if(1 == length && *plast == '\2')
              {
                  if(meta_info.column_info_[column_num].nullable == 1)
                  {
                      tmp_obj_array[column_num].set_null();
                  }
                  else
                  {
                      YYSYS_LOG(WARN, "the column is not allowed null");
                      ret = OB_SKIP_INVALID_ROW;
                  }
              }
              else
              {
                  float val = (float) atof(plast);
                  tmp_obj_array[column_num].set_float(val);
              }

          }
               break;
          case ObDoubleType:
        {
            if(1 == length && *plast == '\2')
            {
                if(meta_info.column_info_[column_num].nullable == 1)
                {
                    tmp_obj_array[column_num].set_null();
                }
                else
                {
                    YYSYS_LOG(WARN, "the column is not allowed null");
                    ret = OB_SKIP_INVALID_ROW;
                }
            }
            else
            {
                double val = atof(plast);
                tmp_obj_array[column_num].set_double(val);
            }
        }
              break;
          case ObDecimalType:
          {
              if(1 == length && *plast == '\2')
              {
                  if(meta_info.column_info_[column_num].nullable == 1)
                  {
                      tmp_obj_array[column_num].set_null();
                  }
                  else
                  {
                      YYSYS_LOG(WARN, "the column is not allowed null");
                      ret = OB_SKIP_INVALID_ROW;
                  }
              }
              else
              {
                  int32_t p = meta_info.column_info_[column_num].precision;
                  int32_t s = meta_info.column_info_[column_num].scale;
                  ObDecimal od;
                  od.from(plast, length);
                  uint32_t data_p = od.get_precision();
                  uint32_t data_s = od.get_scale();
                  if((data_p - data_s) > (uint32_t)(meta_info.column_info_[column_num].precision - meta_info.column_info_[column_num].scale))
                  {
                      YYSYS_LOG(ERROR, "decimal value[%s] not satisfies decimal precision[%d, %d]", plast, p, s);
                      ret = OB_SKIP_INVALID_ROW;
                  }
                  else
                  {
                      ObString current_value;
                      ObDecimal dec_result;
                      int len;
                      uint64_t *new_result = NULL;
                      current_value.assign_ptr(plast, length);
                      if(OB_SUCCESS != (ret = dec_result.from(current_value.ptr(), current_value.length())))
                      {
                          YYSYS_LOG(ERROR, "cast current value to decimal failed!");
                      }
                      else
                      {
                          if(dec_result.get_words()->table[1] == 0)
                          {
                              len = 1;
                          }
                          else
                          {
                              len = 2;
                          }
                          if(OB_SUCCESS != (ret = name_pool_.write_decimal(dec_result.get_words()->ToUInt_v2(), new_result,len)))
                          {
                              YYSYS_LOG(ERROR, "write the new value to pool failed,ret::[%d]",ret);
                          }
                          else
                          {
                              tmp_obj_array[column_num].reset();
                              tmp_obj_array[column_num].set_precision(p);
                              tmp_obj_array[column_num].set_scale(s);
                              tmp_obj_array[column_num].set_vscale(dec_result.get_vscale());
                              tmp_obj_array[column_num].set_nwords(len);
                              tmp_obj_array[column_num].set_ttint(new_result);
                          }
                      }
                  }

              }
          }
              break;

          case ObBoolType:
          {
              if(1 == length && *plast == '\2')
              {
                  if(meta_info.column_info_[column_num].nullable == 1)
                  {
                      tmp_obj_array[column_num].set_null();
                  }
                  else
                  {
                      YYSYS_LOG(WARN, "the column is not allowed null");
                      ret = OB_SKIP_INVALID_ROW;
                  }
              }
              else
              {
                  int32_t val = atoi(plast);
                  tmp_obj_array[column_num].set_bool(val == 1 ? true : false);
              }
          }
              break;
            default:
              {
                ret = OB_ERROR;
                YYSYS_LOG(ERROR, "wrong type[%d] found in row key desc", meta_info.column_info_[column_num].type);
              }
              break;
          }

          plast = pcur + 1;
          pcur += 1;
          column_num++;
        }while(OB_SUCCESS == ret);
      }

      if(OB_SUCCESS == ret)
      {
        ObRowkey* endkey = NULL;
        char* ptr = NULL;
        if(NULL == (ptr = meta_info.allocator_.alloc_aligned(sizeof(ObRowkey))))
        {
          YYSYS_LOG(WARN, "allocate mem for endkey fail");
        }
        else
        {
          endkey = new(ptr)ObRowkey(tmp_obj_array, column_num);
          meta_info.endkey_list_.push_back(endkey);
        }
      }

      if(tmp_buf)
      {
        ob_free(tmp_buf, ObModIds::OB_MEM_BUFFER);
        tmp_buf = NULL;
      }

      if(column_num != meta_info.column_num_)
      {
        YYSYS_LOG(ERROR, "column num is not consistent, rowkey column num:%d schema column num:%d",
            column_num, meta_info.column_num_);
        ret = OB_ERROR;
      }
      return ret;
    }
  }
}

/**
 * skiplist.h
 *
 * this is a simple skiplist, every node has forward points,
 * but no backward points; it is not thread-safe
 *
 * add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701
 */
#ifndef SKIPLIST_H
#define SKIPLIST_H
#include "page_arena.h"
#include "utility.h"
#include "ob_define.h"

#define SKIPLIST_MAXLEVEL 8
namespace oceanbase
{
  namespace common
  {
    template<typename T>
    class SkipListNode
    {
      public:
        SkipListNode();
        virtual ~SkipListNode();
      private:
        T value_;
        SkipListNode<T> *forward_[];
        template<typename Type, typename AllocatorT> friend
        class SkipList;
    };

    template<typename T, typename AllocatorT = ModulePageAllocator>
    class SkipList
    {
      public:
        SkipList(const AllocatorT &alloc = AllocatorT(ObModIds::OB_SKIP_LIST));
        virtual ~SkipList();
        int init();
        void destory();
        int32_t random_level();
        int insert(const T& value);
        int delete_value(const T& value);
        int search(T& value);
        int64_t count() const;
        int process_every_node(int (*handle)(const void *));
        int64_t to_string(char* buffer, int64_t length) const;
      private:
        void delete_node(SkipListNode<T> *node, SkipListNode<T> **update);
      private:
        SkipListNode<T> *header_;
        int64_t length_;
        int32_t level_;
        AllocatorT allocator_;
    };

    template<typename T>
    SkipListNode<T>::SkipListNode()
    {
    }

    template<typename T>
    SkipListNode<T>::~SkipListNode()
    {
    }

    //**************
    template<typename T, typename AllocatorT>
    SkipList<T,AllocatorT>::SkipList(const AllocatorT &alloc)
      :header_(NULL),
        length_(0),level_(1),
        allocator_(alloc)
    {
    }

    template<typename T, typename AllocatorT>
    int SkipList<T,AllocatorT>:: init()
    {
      int ret = OB_SUCCESS;
      header_ = (SkipListNode<T>*)allocator_.alloc(sizeof(SkipListNode<T>) + SKIPLIST_MAXLEVEL*sizeof(SkipListNode<T>*));
      if(NULL != header_)
      {
        for(int i = 0; i < SKIPLIST_MAXLEVEL; i++)
        {
          header_->forward_[i] = NULL;
        }
        //header_->value_ = mini(T);
      }
      else
      {
        ret = OB_MEM_OVERFLOW;
      }
      return ret;
    }

    template<typename T, typename AllocatorT>
    SkipList<T,AllocatorT>::~SkipList()
    {
      destory();
    }

    template<typename T, typename AllocatorT>
    void SkipList<T,AllocatorT>::destory()
    {
      if(header_)
      {
        SkipListNode<T> *node = header_->forward_[0];
        SkipListNode<T> *next = NULL;
        allocator_.free(header_);

        while(NULL != node)
        {
          next = node->forward_[0];
          allocator_.free(node);
          node = next;
        }
        header_ = NULL;
        length_ = 0;
        level_ = 1;
      }
    }

    template<typename T, typename AllocatorT>
    int32_t SkipList<T,AllocatorT>::random_level()
    {
      int level = 1;
      while(level < SKIPLIST_MAXLEVEL && (random()&0xFFFF) < (0.5 * 0xFFFF))
      {
        level ++;
      }
      return level;
    }

    template<typename T, typename AllocatorT>
    int SkipList<T,AllocatorT>::insert(const T& value)
    {
      int ret = OB_SUCCESS;
      if(NULL == header_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        SkipListNode<T> *update[SKIPLIST_MAXLEVEL];
        SkipListNode<T> *node = header_;

        for(int i = level_ -1; i >= 0; i--)
        {
          while(node->forward_[i] && node->forward_[i]->value_ < value)
          {
            node = node->forward_[i];
          }

          //mod dyr [MultiUPS] [DISTRIBUTED_TRANS] 20150831:b
          //update[i] = node;
          if (NULL == node || value == node->value_)
          {
            ret = OB_ENTRY_EXIST;
            YYSYS_LOG(WARN,"value already exist or node is null,can't insert!"
                      "node[%p],ret=%d",node,ret);
            break;
          }
          else
          {
            update[i] = node;
          }
          //mod 20150831:e
        }

        //add dyr [MultiUPS] [DISTRIBUTED_TRANS] 20150831:b
        if (OB_SUCCESS == ret)
        {
          //add 20150831:e
          int level = random_level();

          if(level > level_)
          {
            for(int i = level_; i< level; i++)
            {
              update[i] = header_;
            }
            level_ = level;
          }

          node = (SkipListNode<T>*)allocator_.alloc(sizeof(SkipListNode<T>) + level*sizeof(SkipListNode<T>*));
          if(NULL != node)
          {
            node->value_ = value;
            for (int i = 0; i < level; i++)
            {
              node->forward_[i] = update[i]->forward_[i];
              update[i]->forward_[i] = node;
            }

            length_++;
          }
          else
          {
            ret = OB_MEM_OVERFLOW;
            YYSYS_LOG(ERROR, "no memory");
          }
          //add dyr [MultiUPS] [DISTRIBUTED_TRANS] 20150831:b
        }
        //add 20150831:e
      }
      return ret;
    }

    template<typename T, typename AllocatorT>
    void SkipList<T,AllocatorT>::delete_node(SkipListNode<T> *node, SkipListNode<T> **update)
    {
      for (int i = 0; i < level_; i++)
      {
        if (update[i]->forward_[i] == node)
        {
          update[i]->forward_[i] = node->forward_[i];
        }
      }

      while (level_ > 1 && header_->forward_[level_-1] == NULL)
      {
        level_--;
      }
      length_--;
    }

    template<typename T, typename AllocatorT>
    int SkipList<T,AllocatorT>::delete_value(const T& value)
    {
      int ret = OB_SUCCESS;
      if(NULL == header_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        SkipListNode<T> *update[SKIPLIST_MAXLEVEL];
        SkipListNode<T> *node = header_;

        for(int i = level_-1; i >= 0; i--)
        {
          while (node->forward_[i] && node->forward_[i]->value_ < value)
          {
            node = node->forward_[i];
          }
          update[i] = node;
        }

        node = node->forward_[0];
        if (node && value == node->value_)
        {
          delete_node(node,update);
          allocator_.free(node);
        }
        else
        {
          YYSYS_LOG(WARN,"Not found %s", to_cstring(value));
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
      return ret;
    }

    template<typename T, typename AllocatorT>
    int SkipList<T,AllocatorT>::search(T& value)
    {
      int ret = OB_SUCCESS;
      if(NULL == header_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        SkipListNode<T> *node = header_;

        for(int i = level_-1; i >= 0; i--)
        {
          while (node->forward_[i] && node->forward_[i]->value_ < value)
          {
            node = node->forward_[i];
          }
        }

        node = node->forward_[0];
        if (node && value == node->value_)
        {
          YYSYS_LOG(WARN,"found %s", to_cstring(value));
        }
        else
        {
          YYSYS_LOG(WARN,"Not found %s", to_cstring(value));
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
      return ret;
    }

    template<typename T, typename AllocatorT>
    int SkipList<T,AllocatorT>::process_every_node(int (*handle)(const void *))
    {
      int ret = OB_SUCCESS;
      if(NULL == header_)
      {
        YYSYS_LOG(ERROR, "not init");
        ret = OB_NOT_INIT;
      }
      else
      {
        SkipListNode<T> *node = header_->forward_[0];
        SkipListNode<T> *next_node = NULL;
        while(node)
        {
          YYSYS_LOG(INFO, "enter while");
          if(OB_ENTRY_NOT_EXIST == handle(&(node->value_)))
          {
            YYSYS_LOG(WARN,"drop node %s", to_cstring(node->value_));
            next_node = node->forward_[0];
            delete_value(node->value_);
            node = next_node;
          }
          else
          {
            node = node->forward_[0];
          }
        }
      }
      return ret;
    }

    template<typename T, typename AllocatorT>
    int64_t SkipList<T,AllocatorT>::count() const
    {
      return length_;
    }

    template<typename T, typename AllocatorT>
    int64_t SkipList<T,AllocatorT>::to_string(char* buffer, int64_t length) const
    {
      int64_t pos = 0;
      SkipListNode<T> *node = NULL;
      for (int i = 0; i < SKIPLIST_MAXLEVEL; i++)
      {
        databuff_printf(buffer, length, pos, "LEVEL[%d]: ", i);
        node = header_->forward_[i];
        while(node)
        {
          databuff_printf(buffer, length, pos, " %s", to_cstring(node->value_));
          node = node->forward_[i];
        }
        databuff_printf(buffer, length, pos, "NULL\n");
      }
      return pos;
    }

  }
}
#endif // SKIPLIST_H

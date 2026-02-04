#ifndef __COMSUMER_QUEUE_H__
#define  __COMSUMER_QUEUE_H__

#include <list>
#include <pthread.h>
#include <semaphore.h>
#include <limits.h>

#include "common/utility.h"
#include "yysys.h"
#include "oceanbase_db.h"

const int64_t l_lineno_sleep = 10;
//add by liyongfeng:20141020 
extern bool g_cluster_state;//rootserver״̬,��ʼֵΪtrue,���monitor�����쳣״̬,��Ϊfalse
const int64_t l_merge_sleep = 30;//monitor�߳�,ÿ���30s��ȡһ�κϲ�״̬
const int64_t l_ups_sleep = 20;//monitor�߳�,ÿ���20s��ȡһ��������UPS
//const int64_t l_gcd_sleep = 60;//��������ʱ������Լ��
//add:end

template <class T>
class QueueComsumer
{
  public:
    virtual ~QueueComsumer() { }
    virtual int init() { return 0; }
    virtual int comsume(T &obj) = 0;
    virtual int get_lineno() const = 0;
    virtual int get_bad_lineno() const = 0;
    virtual bool if_print_progress() const = 0;
};

template<class T>
class QueueProducer
{
  public:
    virtual ~QueueProducer() { }
    virtual int init() { return 0; }
    virtual int produce(T &obj) = 0;
};

template<class T>
class ComsumerQueue : public yysys::CDefaultRunnable
{
  public:
    static const int QUEUE_ERROR = -1;
    static const int QUEUE_SUCCESS = 0;
    static const int QUEUE_QUITING = 1;
  public:
    ComsumerQueue()
    {
      cap_ = 0;
      producer_ = NULL;
      comsumer_ = NULL;
      nr_producer_ = nr_comsumer_ = 0;
      running_ = false;
      producer_quiting_ = false;
      atomic_set(&producer_quiting_nr_, 0);
      atomic_set(&consumer_quiting_nr_, 0);//add by liyongfeng:20141016 ��ʼֵΪ0
      push_waiting_ = true;
      atomic_set(&queue_size_, 0);
      atomic_set(&import_state_, 0);//add by liyongfeng:20141017 ��ʼֵΪ0
      db_ = NULL;//add by liyongfeng:20141017 ��ʼֵΪNULL
      wait_time_sec_ = 0;
      wait_ups_mem_time_ = 0;
      no_enough_slave_ups_ = false;
    }

    //mod by liyongfeng:20141017 add oceanbase::api::OceanbaseDb *db
    ComsumerQueue(oceanbase::api::OceanbaseDb *db, QueueProducer<T> *producer, QueueComsumer<T> *comsumer, size_t cap = LONG_MAX) {
      cap_ = cap;
      comsumer_ = comsumer;
      producer_ = producer;
      nr_producer_ = nr_comsumer_ = 0;
      running_ = false;
      producer_quiting_ = false;
      atomic_set(&producer_quiting_nr_, 0);
      atomic_set(&consumer_quiting_nr_, 0);//add by liyongfeng:20141016 ��ʼֵΪ0
      push_waiting_ = true;
      atomic_set(&queue_size_, 0);
      atomic_set(&import_state_, 0);//add by liyongfeng:20141017 ��ʼֵΪ0
      db_ = db;//add by liyongfeng:20141017
      assert(db_ != NULL);//add by liyongfeng:20141017
      wait_time_sec_ = 0;
      wait_ups_mem_time_ = 0;
      no_enough_slave_ups_ = false;
    }
    //mod:end

    ~ComsumerQueue() { if (running_) dispose(); }

    void attach_comsumer(QueueComsumer<T> *comsumer)
    {
      comsumer_ = comsumer;
    }

    void attach_producer(QueueProducer<T> *producer)
    {
      producer_ = producer;
    }

    //mod by liyongfeng:20141017 add nr_monitor
    //start produce and comsume process
    int produce_and_comsume(int nr_producer, int nr_monitor, int nr_comsumer);
    //mod:end

    //(long)arg is thread id, id < nr_producer_ is producer thread 
    //or is comsume thread
    virtual void run(yysys::CThread *thread, void *arg);

    void produce(long id);

    void comsume(long id);

    void monitor(long id);//add by liyongfeng, 20141014, monitor the state of daily merge

    //maybe block
    void push(T &obj);

    //maybe sleep, ms
    int pop(T &obj);

    //dispose queue
    void dispose();

    //set queue size
    void set_capacity(int64_t cap) { cap_ = cap; }

    //return size
    size_t size() const { return queue_.size(); }

    //return capacity of the queue
    size_t capacity() const { return cap_; }

    void finalize_queue(long id);

    void sleep_when_full();

    void wakeup_push();

    int64_t get_waittime_sec();
    int64_t get_ups_time_sec();
  private:
    std::list<T> queue_;

    size_t cap_;
    yysys::CThreadCond queue_cond_;
    yysys::CThreadCond queue_cond_full_;
    QueueComsumer<T> *comsumer_;
    QueueProducer<T> *producer_;
    bool running_;
    long nr_comsumer_;
    long nr_producer_;
    long nr_monitor_;//add by liyongfeng:20141017 ����߳�����
    bool producer_quiting_;
    bool push_waiting_;
    atomic_t producer_quiting_nr_;
    atomic_t consumer_quiting_nr_;//add by liyongfeng 20141016: for notify monitor exit
    atomic_t queue_size_;
    atomic_t import_state_;//add by liyongfeng 20141016: ob_import state(1--continue;0--stop;-1--forbid)
    oceanbase::api::OceanbaseDb *db_;//add by liyongfeng:20141017
    int64_t wait_time_sec_;
    int64_t wait_ups_mem_time_;
    bool no_enough_slave_ups_;
};

template <class T>
void ComsumerQueue<T>::sleep_when_full()
{
  if (cap_ != 0) {                              /* cap_ == 0, no cap limit */
    queue_cond_full_.lock();

    while (static_cast<size_t>(atomic_read(&queue_size_)) >= cap_ && running_) {
      push_waiting_ = true;
      queue_cond_full_.wait(1000);
    }
    push_waiting_ = false;
    queue_cond_full_.unlock();
  }
}

template <class T>
void ComsumerQueue<T>::wakeup_push()
{
  if (push_waiting_)
    queue_cond_full_.signal();
}

template <class T>
void ComsumerQueue<T>::push(T &obj)
{
  sleep_when_full();                           /* sleep if queue cap reaches */

  if (running_ == false)                        /* no more obj, if quiting */
    return;

  atomic_inc(&queue_size_);

  queue_cond_.lock();
  queue_.push_back(obj);
  queue_cond_.unlock();

  queue_cond_.signal();
}

template <class T>
int ComsumerQueue<T>::pop(T &obj)
{
  int ret = 0;

  queue_cond_.lock();
  while (queue_.empty() && !producer_quiting_)
  {
    queue_cond_.wait(1000);
  }

  if (!queue_.empty())
  {
    obj = queue_.front();
    queue_.pop_front();
    atomic_dec(&queue_size_);
  }
  else if (producer_quiting_)
  {
    ret = QUEUE_QUITING;
  }

  queue_cond_.unlock();

  /* wake up sleeping push thread if needed */
  wakeup_push();

  return ret;
}

//mod by liyongfeng:20141017 add nr_monitor
template <class T>
int ComsumerQueue<T>::produce_and_comsume(int nr_producer, int nr_monitor, int nr_comsumer)
{
  int ret = QUEUE_SUCCESS;

  if (producer_ == NULL)
  {
    nr_producer_ = 0;
  }
  else
  {
    nr_producer_ = nr_producer;
  }

  if (comsumer_ == NULL)
  {
    nr_comsumer_ = 0;
  }
  else
  {
    nr_comsumer_ = nr_comsumer;
  }

  nr_monitor_ = nr_monitor;

  if (nr_monitor == 0)
  {
    atomic_set(&import_state_, 1);
  }

  if (producer_->init() || comsumer_->init())
  {
    YYSYS_LOG(ERROR, "can't init producer/comsumer, quiting");
    ret = QUEUE_ERROR;
  }
  else
  {
    running_ = true;
    YYSYS_LOG(INFO, "CQ:producer = %ld, monitor = %ld, comsumer = %ld, cap=%ld", nr_producer_, nr_monitor_, nr_comsumer_, cap_);//mod by liyongfeng:20141017 add nr_monitor_
    setThreadCount(static_cast<int32_t>(nr_comsumer_ + nr_monitor_ + nr_producer_));//mod by liyongfeng:20141017 add nr_monitor_
    start();
    wait();
  }

  return ret;
}
//mod:end

template <class T>
void ComsumerQueue<T>::run(yysys::CThread *thread, void *arg)
{
  long id = (long)arg;
  UNUSED(thread);
  UNUSED(arg);
  YYSYS_LOG(DEBUG, "CQ:in run, id=%ld", id);
  if (id < nr_producer_)
  {
    produce(id);
  }
  else if (id >= nr_producer_ && id < nr_producer_ + nr_comsumer_)
  {
    comsume(id);
  }
  else
  {
    monitor(id);
  }
}

template <class T>
void ComsumerQueue<T>::produce(long id)
{
  YYSYS_LOG(DEBUG, "producer id = %ld", id);

  int ret = 0;
  while (running_) {
      //mod by liyongfeng:20141016 ob_import��producer�߳���Բ�ͬimport״̬���в�ͬ����
      //import_state_=-1  RS�л���UPS�л�,ob_import��ֹ������������
      //����, ������������,�ṩ��ob_import��consumer��������
      if (0 != atomic_read(&import_state_) && 1 != atomic_read(&import_state_))
      {
          YYSYS_LOG(ERROR, "error import state, quit producer id=%ld", id);
          atomic_add(1, &producer_quiting_nr_);
          if (atomic_read(&producer_quiting_nr_) == nr_producer_)
          {
              YYSYS_LOG(INFO, "all producer quit");
              producer_quiting_ = true;
          }
          break;
      }
      else
      {
          T obj;
          ret = producer_->produce(obj);
          if (ret == QUEUE_ERROR)
          {
              //YYSYS_LOG(WARN, "can't produce object, producer id=%ld", id);
            producer_quiting_ = true;
            break;
          }
          else if (ret == QUEUE_QUITING)
          {
              atomic_add(1, &producer_quiting_nr_);
              if (atomic_read(&producer_quiting_nr_) == nr_producer_)
              {
                  YYSYS_LOG(INFO, "all producer quit");
                  producer_quiting_ = true;
              }
              break;
          }
          push(obj);
      }
      //mod:end
  }
}

template <class T>
void ComsumerQueue<T>::comsume(long id)
{
  int ret = 0;

  YYSYS_LOG(INFO, "in comsume thread, id=%ld", id);
  while (running_)
  {
      //mod by liyongfeng:20141016 ob_import��consumer�߳���Բ�ͬ��import״̬���в�ͬ����
      //import_state_=1  ob_import����consumer������������
      //import_state_=0  ob_importֹͣconsumer��������,�ȴ��ϲ�����
      //import_state_=-1  RS�л���UPS�л�,ob_import��producer��ֹ��������,��������consumer�������µ�������������
      if (0 == atomic_read(&import_state_))
      {
          //YYSYS_LOG(WARN, "merge: DOING or merge: TIMEOUT, pause comsumer id=%ld", id);
          sleep(1);
          continue;
      }
      else if (no_enough_slave_ups_)
      {
        sleep(10);
        if (id != 1)
        {
          continue;
        }
      }
      {
          T obj;
          ret = pop(obj);
          if (ret == 0)
          {
              ret = comsumer_->comsume(obj);
              if (ret != 0)
              {
                if ((-OB_NOT_ENOUGH_SLAVE) == ret)
                {
                  no_enough_slave_ups_ = true;
                  YYSYS_LOG(WARN, "there is no enough slave ups!");
                }
                  YYSYS_LOG(WARN, "can't comsume object, comsumer id=%ld, ret = %d", id, ret);
              }
              else if (ret == 0 && no_enough_slave_ups_)
              {
                no_enough_slave_ups_ = false;
                YYSYS_LOG(WARN, "the slave ups recover!");
              }
          }
          else if (ret == QUEUE_QUITING)
          {
              YYSYS_LOG(INFO, "producer quiting, quit comsumer id=%ld", id);
              break;
          }
          else if (ret == QUEUE_ERROR)
          {
              YYSYS_LOG(WARN, "can't pop queue err");
          }
      }//mod:end
  }

  /* comsume the remaining objs */
  finalize_queue(id);

  atomic_add(1, &consumer_quiting_nr_);//add by liyongfeng:20141016
}

//add by liyongfeng, 20141014, monitor the state of daily merge
template <class T>
void ComsumerQueue<T>::monitor(long id)
{
    int ret = 0;
    int32_t state = 0;//��¼ÿ�λ�ȡ��merge״̬
    int32_t ups_memory_state = 0;//��¼UPS�ڴ�ʹ�������0Ϊ����������ͣ����
//  int32_t ups_switch = 0;//��¼ÿ���ж�UPS�Ƿ��л�

    int64_t count = 0;//��¼sleep(1)�Ĵ���

    YYSYS_LOG(INFO, "in monitor thread, id=%ld", id);
    while (running_)
    {
        if (atomic_read(&consumer_quiting_nr_) == nr_comsumer_)
        {
            YYSYS_LOG(INFO, "all consumer quit, quit monitor id=%ld", id);
            break;
        }
        if (0 == (count % l_merge_sleep))
        {//��Ҫ��ȡÿ�պϲ�״̬
            //send request to get the state of daily merge
            RPC_WITH_RETRIES_SLEEP(db_->get_daily_merge_state(state), 3, ret);//����3��,ÿ�μ��5s
            if(ret != 0)
            {
                //send request failed
//              YYSYS_LOG(ERROR, "failed to get the state of daily merge, all producer forbid, err=%d", ret);
                //��import_state_��Ϊ-1,��ֹob_import��������
                /*
                if (1 == atomic_read(&is_merge_)) {
                    atomic_sub(2, &is_merge_);
                } else if (0 == atomic_read(&is_merge_)) {
                    atomic_sub(1, &is_merge_);
                } else {
                    //YYSYS_LOG(INFO, "forbid producer running, is_merge_=%d", atomic_read(&is_merge_));
                }
                */
//              atomic_set(&import_state_, -1);
//              YYSYS_LOG(INFO, "quit monitor id=%ld", id);
                //�޸�ȫ��״̬,֪ͨ���߳��쳣״̬,������쳣�˳�
//            g_cluster_state = false;
//            break;
              YYSYS_LOG(ERROR, "failed to get the state of daily merge, all consumer pause, err=%d", ret);
              atomic_set(&import_state_, 0);
              g_cluster_state = false;
            }
            else
            {
                //���ݻ�ȡ����ǰ״̬,�޸�import_state_
                if (1 == state)
                {//�Ѿ��ϲ����
                    YYSYS_LOG(INFO, "daily merge has done, all consumer continue, state=%d", state);
                    atomic_set(&import_state_, 1);
                }
                else if (0 == state)
                {//���ںϲ�
                    YYSYS_LOG(INFO, "daily merge is doing, all consumer pause, state=%d", state);
                    fprintf(stdout, "daily merge is doing, all consumer pause for 30s...\n");
                    wait_time_sec_ += l_merge_sleep;
                    atomic_set(&import_state_, 0);
                }
                else if (-1 == state)
                {//��ȡ�ϲ�ʧ��
//                    YYSYS_LOG(WARN, "error merge state, all producer forbid, state=%d", state);
//                    atomic_set(&import_state_, -1);
//                    YYSYS_LOG(INFO, "quit monitor id=%ld", id);
                    YYSYS_LOG(WARN, "error merge state, all consumer pause, state=%d", state);
                    atomic_set(&import_state_, 0);
                    //�޸�ȫ��״̬,֪ͨ���߳��쳣״̬,������쳣�˳�
                    g_cluster_state = false;
//                  break;
                }
                else
                {//����״̬��
//                  YYSYS_LOG(ERROR, "invalid merge state, all producer forbid, state=%d", state);
//                  atomic_set(&import_state_, -1);
//                  YYSYS_LOG(INFO, "quit monitor id=%ld", id);
                    YYSYS_LOG(WARN, "invalid merge state, all consumer pause, state=%d", state);
                    atomic_set(&import_state_, 0);
                    //�޸�ȫ��״̬,֪ͨ���߳��쳣״̬,������쳣�˳�
                    g_cluster_state = false;
//                  break;
                }
            }
            if (!g_cluster_state)
            {
              fprintf(stdout, "reset master rs, all consumer pause!\n");
              int64_t begin_time = yysys::CTimeUtil::getTime();
              while (true)
              {
                int64_t current_time = yysys::CTimeUtil::getTime();
                if (current_time - begin_time > 600000000)
                {
                  YYSYS_LOG(ERROR, "reset master rs failed!");
                  atomic_set(&import_state_, -1);
                  break;
                }
                else if (OB_SUCCESS != (ret = db_->set_master_rs()))
                {
                  sleep(1);
                }
                else
                {
                  YYSYS_LOG(INFO, "reset master rs success!");
                  break;
                }
              }

              if (OB_SUCCESS != ret)
              {
                YYSYS_LOG(ERROR, "reset master rs failed!");
                fprintf(stdout, "reset master rs failed!\n");
                YYSYS_LOG(INFO, "quit monitor id=%ld", id);
                break;
              }
              else
              {
                fprintf(stdout, "reset master rs success!\n");
                RPC_WITH_RETRIES_SLEEP(db_->get_daily_merge_state(state), 3, ret);
                if (ret == 0 && (state == 0 || state == 1))
                {
                  atomic_set(&import_state_, state);
                  g_cluster_state = true;
                }
                else
                {
                  YYSYS_LOG(WARN, "get merge state failed!");
                  atomic_set(&import_state_, -1);
                  YYSYS_LOG(INFO, "quit monitor id=%ld", id);
                  break;
                }
              }
            }
        }
        if (0 == (count % l_ups_sleep) && 1 == state)
        {//��Ҫ��ȡUPS�ڴ����
          RPC_WITH_RETRIES_SLEEP(db_->get_merge_ups_memory(ups_memory_state), 3, ret);
            if (ret != 0) {
                YYSYS_LOG(ERROR, "failed to get the memory of ups, all consumer pause, err=%d", ret);
                atomic_set(&import_state_, 0);
                g_cluster_state = false;
            }
            else
            {
                if (1 == ups_memory_state)
                {
                  YYSYS_LOG(INFO, "regular ups memory, all consumer continue, state=%d", ups_memory_state);
                  atomic_set(&import_state_, 1);
                }
                else if (0 == ups_memory_state)
                {
                  YYSYS_LOG(INFO, "invalid ups memory, all consumer pause, state=%d", ups_memory_state);
                  fprintf(stdout, "invalid ups memory, all consumer pause for 20s...\n");
                  wait_ups_mem_time_ += l_ups_sleep;
                  atomic_set(&import_state_, 0);
                }
            }
        }

        if (state == 1 && ups_memory_state == 1 && 0 == (count % l_lineno_sleep) && comsumer_->if_print_progress())
        {
          int lo = comsumer_->get_lineno();
          int blo = comsumer_->get_bad_lineno();
          fprintf(stdout, "%d %d %d\n", lo, lo-blo, blo);
          fflush(stdout);

        }
        count++;
        count = count % l_merge_sleep;
        sleep(1);
    }
}
//add:end

template <class T>
void ComsumerQueue<T>::finalize_queue(long id)
{
  queue_cond_.lock();
  while (!queue_.empty()) {
    T &obj = queue_.front();
    int ret = comsumer_->comsume(obj);
    if (ret != 0) {
      YYSYS_LOG(WARN, "can't comsume object, comsumer id=%ld", id);
    }
    queue_.pop_front();
  }
  queue_cond_.unlock();
}

template <class T>
void ComsumerQueue<T>::dispose()
{
  running_ = false;
  stop();
}

template <class T>
long ComsumerQueue<T>::get_waittime_sec()
{
  return wait_time_sec_;
}

template <class T>
long ComsumerQueue<T>::get_ups_time_sec()
{
  return wait_ups_mem_time_;
}

#endif

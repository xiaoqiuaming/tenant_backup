/*******************************************************************************


   Copyright (C) 2011-2014 SequoiaDB Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the term of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warrenty of
   MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.

   Source File Name = ossUtil.hpp

   Descriptive Name = Operating System Services Utility Header

   When/how to use: this program may be used on binary and text-formatted
   versions of OSS component. This file contains wrappers for utilities like
   memcpy, strcmp, etc...

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef OSSUTIL_HPP_
#define OSSUTIL_HPP_
#include "core.h"
#include "oss.hpp"
#include <ctime>
#include <time.h>
#include <sys/types.h>
#include "ossUtil.h"
#include <string>
#include <map>

#if defined( SDB_ENGINE )
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread/xtime.hpp>
#include <boost/thread/thread.hpp>
#endif // SDB_ENGINE

unit32_t ossRand () ;
inline unit32_t ossHash ( const char *str )
{
   unit32_t hash = 5381 ;
   char c ;
   while ( (c = *(str++)) )
      hash = ((hash << 5) + hash) + c;
   return hash ;
}

inline unit32_t ossHash( const char *value, unit32_t size, unit32_t bit = 5)
{
   unit32_t hash = 5381 ;
   const char *end = value + size ;
   while ( value < end )
      hash = ( (hash << bit) + hash + *value++ ) ;
   return hash ;
}

inline unit32_t ossHash( const unsigned char *v1, unit32_t s1,
                           const unsigned char *v2, unit32_t s2 )
{
   unit32_t hash = 5381 ;
   for ( unit32_t i = 0; i < s1; ++i )
   {
      hash = ( (hash << 5) + hash + *v1++ ) ;
   }

   for ( unit32_t i = 0; i < s2; ++i )
   {
      hash = ( (hash << 5) + hash + *v2++ ) ;
   }

   return hash ;
}

int32_t ossIsPowerOf2( unit32_t num, unit32_t *pSquare = NULL ) ;

inline void ossSleepmicros(uint64_t s)
{
   struct timespec t;
   t.tv_sec = (time_t)(s / 1000000);
   t.tv_nsec = 1000 * ( s % 1000000 );
   while(nanosleep( &t , &t )==-1);// && ossGetLastError()==EINTR);
}
inline void ossSleepmillis(uint64_t s)
{
   ossSleepmicros( s * 1000 );
}

inline void ossSleepsecs(unit32_t s)
{
   ossSleepmicros((uint64_t)s * 1000000);
}

class ossTime : public CBObject
{
public :
   unit32_t seconds ;
   unit32_t microsec ;
} ;
typedef class ossTime ossTime ;

class ossTimestamp : public CBObject
{
public :
   time_t time ;     // tv_sec ,  seconds
   unit32_t microtm ;  // tv_usec,  microseconds

   ossTimestamp ()
   : time( 0 ),
     microtm( 0 )
   {
   }

   ossTimestamp ( uint64_t curTime )
   : time( curTime / 1000 ),
     microtm( ( curTime % 1000 ) * 1000 )
   {
   }

   ossTimestamp ( const ossTimestamp & timestamp )
   : time( timestamp.time ),
     microtm( timestamp.microtm )
   {
   }

   ossTimestamp &operator= ( const ossTimestamp &rhs )
   {
      time    = rhs.time ;
      microtm = rhs.microtm ;
      return *this ;
   }

   void clear ()
   {
      time = 0 ;
      microtm = 0 ;
   }
} ;
typedef class ossTimestamp ossTimestamp ;


#define OSS_TIMESTAMP_STRING_LEN 26

void ossTimestampToString( ossTimestamp &Tm, char * pStr ) ;

void ossStringToTimestamp( const char * pStr, ossTimestamp &Tm ) ;

void ossLocalTime ( time_t &Time, struct tm &TM ) ;

void ossGmtime ( time_t &Time, struct tm &TM ) ;

void ossGetCurrentTime( ossTimestamp &TM ) ;

uint64_t ossGetCurrentMicroseconds() ;
uint64_t ossGetCurrentMilliseconds() ;

int32_t ossGetCPUUsage
(
   ossTime &usrTime,
   ossTime &sysTime
) ;

int32_t ossGetCPUUsage
(
   OSSTID tid,
   ossTime &usrTime,
   ossTime &sysTime
) ;

/*
   Get Operator System Infomation
*/
struct _ossOSInfo
{
   char _distributor[ OSS_MAX_PATHSIZE + 1 ] ;
   char _release[ OSS_MAX_PATHSIZE + 1 ] ;
   char _desp[ OSS_MAX_PATHSIZE + 1 ] ;
   int32_t _bit ;
} ;
typedef _ossOSInfo ossOSInfo ;

int32_t ossGetOSInfo( ossOSInfo &info ) ;

inline uint64_t ossPack32To64( unit32_t hi, unit32_t lo )
{
   return ((uint64_t)hi << 32) | (uint64_t)lo ;
}

inline void ossUnpack32From64 ( uint64_t u64, unit32_t & hi, unit32_t & lo )
{
   hi = (unit32_t)( u64 >> 32 ) ;
   lo = (unit32_t)( u64 & 0xFFFFFFFF ) ;
}



inline uint64_t ossRdtsc()
{
   unit32_t lo, hi;
   __asm__ __volatile__ (
     " xorl %%eax,%%eax \n"
     " cpuid"
     ::: "%rax", "%rbx", "%rcx", "%rdx" ) ;
   __asm__ __volatile__ ( "rdtsc" : "=a" (lo), "=d" (hi) ) ;
   return (uint64_t)( ((uint64_t) lo) | ( ((uint64_t) hi) << 32 ) );
}

#if defined (_LINUX) || defined (_AIX)
inline void ossGetTimeOfDay( struct timeval * pTV )
{
   if ( pTV )
   {
      if ( -1 == gettimeofday( pTV, NULL ) )
      {
         pTV->tv_sec = 0 ;
         pTV->tv_usec = 0 ;
      }
   }
   return ;
}

inline uint64_t ossTimeValToUint64( struct timeval & tv )
{
   return ossPack32To64( (unit32_t)tv.tv_sec, (unit32_t)tv.tv_usec ) ;
}
#endif

#define OSS_TICKS_OP_ADD 1
#define OSS_TICKS_OP_SUB 2
#define OSS_TICKS_OP_NO_SCALE 4

class ossTickCore : public CBObject
{
public :
   ossTickCore()
   {
      _value = 0 ;
   }

   ossTickCore ( const ossTickCore & tick )
   {
      _value = tick._value ;
   }

   uint64_t _value ;

   inline uint64_t peek(void) const
   {
      return _value ;
   } ;

   inline void poke(const uint64_t val)
   {
      _value = val ;
   } ;

   static uint64_t addOrSub
   (
      const ossTickCore op1, const ossTickCore op2, const unit32_t flags
   )
   {
      uint64_t val ;
      val = addOrSub( op1._value, op2._value, flags ) ;
      return val ;
   } ;


   static uint64_t addOrSub
   (
      const uint64_t op1, const uint64_t op2, const unit32_t flags
   )
   {
      int64_t resultHi, resultLo ;

      if ( OSS_TICKS_OP_ADD & flags )
      {
         resultHi = (int64_t)(op1 >> 32) + (int64_t)(op2 >> 32) ;
         resultLo = (int64_t)(op1 & 0xFFFFFFFF) + (int64_t)(op2 & 0xFFFFFFFF) ;
         if ( resultLo >= OSS_ONE_MILLION )
         {
            resultHi++ ;
            resultLo -= OSS_ONE_MILLION ;
         }
      }
      else
      {
         resultHi = (int64_t)(op1 >> 32) - (int64_t)(op2 >> 32) ;
         resultLo = (int64_t)(op1 & 0xFFFFFFFF) - (int64_t)(op2 & 0xFFFFFFFF ) ;

         if ( resultLo < 0 )
         {
            resultHi-- ;
            resultLo += OSS_ONE_MILLION ;
         }
         if ( ( OSS_TICKS_OP_SUB & flags ) && resultHi < 0 )
         {
            resultHi = 0 ;
            resultLo = 0 ;
         }
      }
      return (uint64_t)( ((uint64_t)resultHi << 32) | (uint64_t)resultLo  ) ;
   } ;
} ;

class ossTickDelta ;
class ossTick ;

class ossTickConversionFactor : public CBObject
{
protected :
   friend class ossTickDelta ;
   uint64_t factor ;
public :
   ossTickConversionFactor() ;
} ;

ossTickDelta operator - (const ossTick      &x, const ossTick      &y) ;
ossTickDelta operator - (const ossTickDelta &x, const ossTickDelta &y) ;

ossTickDelta operator + (const ossTickDelta &x, const ossTickDelta &y) ;
ossTick operator + (const ossTick &x, const ossTickDelta &y) ;
ossTick operator + (const ossTickDelta &x, const ossTick &y) ;

int32_t operator >  (const ossTickDelta &x, const ossTickDelta &y) ;
int32_t operator >= (const ossTickDelta &x, const ossTickDelta &y) ;
int32_t operator <  (const ossTickDelta &x, const ossTickDelta &y) ;
int32_t operator <= (const ossTickDelta &x, const ossTickDelta &y) ;
int32_t operator == (const ossTickDelta &x, const ossTickDelta &y) ;

int32_t operator >  (const ossTick &x, const ossTick &y) ;
int32_t operator >= (const ossTick &x, const ossTick &y) ;
int32_t operator <  (const ossTick &x, const ossTick &y) ;
int32_t operator <= (const ossTick &x, const ossTick &y) ;
int32_t operator == (const ossTick &x, const ossTick &y) ;

class ossTickDelta : protected ossTickCore
{
   friend ossTickDelta operator- (const ossTick      &x, const ossTick      &y);
   friend ossTickDelta operator- (const ossTickDelta &x, const ossTickDelta &y);
   friend ossTickDelta operator+ (const ossTickDelta &x, const ossTickDelta &y);

   friend int32_t operator >  (const ossTickDelta &x, const ossTickDelta &y) ;
   friend int32_t operator >= (const ossTickDelta &x, const ossTickDelta &y) ;
   friend int32_t operator <  (const ossTickDelta &x, const ossTickDelta &y) ;
   friend int32_t operator <= (const ossTickDelta &x, const ossTickDelta &y) ;
   friend int32_t operator == (const ossTickDelta &x, const ossTickDelta &y) ;

   friend ossTick operator +  (const ossTick      &x, const ossTickDelta &y) ;
   friend ossTick operator +  (const ossTickDelta &x, const ossTick      &y) ;
public :
   ossTickDelta ()
   : ossTickCore()
   {
   }

   ossTickDelta ( const ossTickDelta & delta )
   : ossTickCore( delta )
   {
   }

   inline void clear(void)
   {
      poke( 0 ) ;
   } ;

   inline operator int32_t() const
   {
      return ( 0 != peek() ) ;
   } ;

   inline uint64_t peek(void) const
   {
      return ossTickCore::peek() ;
   } ;

   inline void poke(const uint64_t val)
   {
      ossTickCore::poke( val ) ;
   } ;

   inline uint64_t toUint64_t() const
   {
      uint64_t val = peek() ;
   #if defined (_LINUX) || defined (_AIX)
      unit32_t hi, lo ;

      hi = (unit32_t)( val >> 32 ) ;
      lo = (unit32_t)( val & 0xFFFFFFFF ) ;

      val = ( uint64_t )hi * OSS_ONE_MILLION + lo ;
   #endif
      return val ;
   } ;

   inline operator uint64_t() const
   {
      return toUINT64() ;
   } ;

   inline void fromUINT64( const uint64_t v )
   {
      unit32_t hi, lo ;
      uint64_t val ;

      hi = v / OSS_ONE_MILLION ;
      lo = v % OSS_ONE_MILLION ;
      val= ((uint64_t)hi << 32) | (uint64_t)lo ;

      poke( val ) ;
   } ;

   inline ossTickDelta & operator += ( const ossTickDelta & x )
   {
      poke( ossTickCore::addOrSub( *this, x, OSS_TICKS_OP_ADD ) ) ;
      return *this ;
   } ;

   inline ossTickDelta & operator -= ( const ossTickDelta & x )
   {
      poke( ossTickCore::addOrSub( *this, x, OSS_TICKS_OP_SUB ) ) ;
      return *this ;
   } ;

   inline ossTickDelta & operator= ( const ossTickDelta &rhs )
   {
      poke( rhs.peek() ) ;
      return *this ;
   }

   inline void convertToTime
   (
      const ossTickConversionFactor &  cFactor,
      unit32_t &                         seconds,
      unit32_t &                         microseconds
   ) const
   {
      uint64_t ticks = peek() ;
      ossUnpack32From64( ticks, seconds, microseconds ) ;
   } ;

   inline int32_t initFromTimeValue
   (
      const ossTickConversionFactor &  cFactor,
      const uint64_t                     timeValueInMicroseconds
   )
   {
      int32_t rc = SDB_OK ;
      uint64_t numTicksForInterval ;
      unit32_t hi, lo ;

      hi = timeValueInMicroseconds / OSS_ONE_MILLION ;
      lo = timeValueInMicroseconds % OSS_ONE_MILLION ;
      numTicksForInterval = ossPack32To64( hi, lo ) ;
      poke( numTicksForInterval ) ;
      return rc ;
   } ;
} ;


class ossTick : protected ossTickCore
{
   friend int32_t operator >  (const ossTick &x, const ossTick &y ) ;
   friend int32_t operator >= (const ossTick &x, const ossTick &y ) ;
   friend int32_t operator <  (const ossTick &x, const ossTick &y ) ;
   friend int32_t operator <= (const ossTick &x, const ossTick &y ) ;
   friend int32_t operator == (const ossTick &x, const ossTick &y ) ;
   friend ossTick operator +  (const ossTick &x,      const ossTickDelta &y ) ;
   friend ossTick operator +  (const ossTickDelta &x, const ossTick &y ) ;
   friend ossTickDelta operator - (const ossTick &x, const ossTick &y ) ;
public :
   ossTick ()
   : ossTickCore()
   {
   }

   ossTick ( const ossTick & tick )
   : ossTickCore( tick )
   {
   }

   inline void sample(void)
   {
      struct timeval tv ;

      tv.tv_sec = 0 ;
      tv.tv_usec = 0 ;
      gettimeofday( &tv, NULL ) ;
      _value = ((uint64_t)tv.tv_sec << 32) | (uint64_t)tv.tv_usec ;
   } ;

   inline void clear(void)
   {
      poke( 0 ) ;
   } ;

   inline operator int32_t() const
   {
      return (0 != peek()) ;
   } ;


   inline ossTick & operator= ( const ossTick &rhs )
   {
      poke( rhs.peek() ) ;
      return *this ;
   }

   int32_t initFromTimeValue
   (
      const ossTickConversionFactor &  cFactor,
      const uint64_t                     timeValueInMicroseconds
   )
   {
      return ((ossTickDelta*)this)->initFromTimeValue( cFactor,
                                                       timeValueInMicroseconds);
   } ;

   void convertToTime
   (
      const ossTickConversionFactor &  cFactor,
      unit32_t &                         seconds,
      unit32_t &                         microseconds
   ) const
   {
      ((ossTickDelta*)this)->convertToTime( cFactor, seconds, microseconds ) ;
   } ;

   void convertToTimestamp( ossTimestamp &Tm ) const
   {
      uint64_t ticks = peek() ;
      Tm.time = (time_t)( ticks >> 32 ) ;
      Tm.microtm = ticks & 0x00000000ffffffff ;
   }

   static uint64_t addOrSub
   (
      const uint64_t op1, const uint64_t op2, const unit32_t flags
   )
   {
      return ossTickCore::addOrSub( op1, op2, flags ) ;
   } ;
} ;


inline int32_t operator > (const ossTickDelta &x, const ossTickDelta &y )
{
   return (int64_t)(ossTickCore::addOrSub(x, y, OSS_TICKS_OP_NO_SCALE)) > 0 ;
}

inline int32_t operator >= (const ossTickDelta &x, const ossTickDelta &y )
{
   return (int64_t)(ossTickCore::addOrSub(x, y, OSS_TICKS_OP_NO_SCALE)) >= 0 ;
}

inline int32_t operator < (const ossTickDelta &x, const ossTickDelta &y )
{
   return (int64_t)(ossTickCore::addOrSub(x, y, OSS_TICKS_OP_NO_SCALE)) < 0 ;
}

inline int32_t operator <= (const ossTickDelta &x, const ossTickDelta &y )
{
   return (int64_t)(ossTickCore::addOrSub(x, y, OSS_TICKS_OP_NO_SCALE)) <= 0 ;
}

inline int32_t operator == (const ossTickDelta &x, const ossTickDelta &y )
{
   return (int64_t)(ossTickCore::addOrSub(x, y, OSS_TICKS_OP_NO_SCALE)) == 0 ;
}

inline int32_t operator > (const ossTick &x, const ossTick &y)
{
   return (int64_t)(ossTickCore::addOrSub(x, y, OSS_TICKS_OP_NO_SCALE)) > 0 ;
}

inline int32_t operator >= (const ossTick &x, const ossTick &y)
{
   return (int64_t)(ossTickCore::addOrSub(x, y, OSS_TICKS_OP_NO_SCALE)) >= 0 ;
}

inline int32_t operator < (const ossTick &x, const ossTick &y)
{
   return (int64_t)(ossTickCore::addOrSub(x, y, OSS_TICKS_OP_NO_SCALE)) < 0 ;
}

inline int32_t operator <= (const ossTick &x, const ossTick &y)
{
   return (int64_t)(ossTickCore::addOrSub(x, y, OSS_TICKS_OP_NO_SCALE)) <= 0 ;
}

inline int32_t operator == (const ossTick &x, const ossTick &y)
{
   return (int64_t)(ossTickCore::addOrSub(x, y, OSS_TICKS_OP_NO_SCALE)) == 0 ;
}

inline ossTickDelta operator + (const ossTickDelta &x, const ossTickDelta &y )
{
   ossTickDelta result ;

   result.poke(  ossTickCore::addOrSub( x, y, OSS_TICKS_OP_ADD ) ) ;
   return result ;
}

inline ossTick operator + (const ossTick &x, const ossTickDelta &y)
{
   ossTick result ;

   result.poke( ossTickCore::addOrSub( x,y, OSS_TICKS_OP_ADD ) ) ;
   return result ;
}

inline ossTick operator + (const ossTickDelta &x, const ossTick &y)
{
   ossTick result ;

   result.poke( ossTickCore::addOrSub( x,y, OSS_TICKS_OP_ADD ) ) ;
   return result ;
}

inline ossTickDelta operator - (const ossTickDelta &x, const ossTickDelta &y )
{
   ossTickDelta result ;

   result.poke( ossTickCore::addOrSub( x, y, OSS_TICKS_OP_SUB ) ) ;
   return result ;
}

inline ossTickDelta operator - (const ossTick &x, const ossTick &y )
{
   ossTickDelta result ;

   result.poke( ossTickCore::addOrSub( x, y, OSS_TICKS_OP_SUB ) ) ;
   return result ;
}


typedef unsigned int UintPtr ;

#if defined OSS_ARCH_64
   #define OSS_PRIxPTR "%016lx"
   #define OSS_PRIXPTR "%016lX"
#elif defined ( OSS_ARCH_32 )
   #define OSS_PRIxPTR "%08lx"
   #define OSS_PRIXPTR "%08lX"
#endif

#define OSS_INT8_MAX_HEX_STRING   "0xFF"
#define OSS_INT16_MAX_HEX_STRING  "0xFFFF"
#define OSS_INT32_MAX_HEX_STRING  "0xFFFFFFFF"
#define OSS_INT64_MAX_HEX_STRING  "0xFFFFFFFFFFFFFFFF"

#if defined OSS_ARCH_64
   #define OSS_INTPTR_MAX_HEX_STRING   OSS_INT64_MAX_HEX_STRING
#elif defined ( OSS_ARCH_32 )
   #define OSS_INTPTR_MAX_HEX_STRING   OSS_INT32_MAX_HEX_STRING
#endif

#define OSS_HEXDUMP_SPLITER " : "
#define OSS_HEXDUMP_BYTES_PER_LINE 16
#define OSS_HEXDUMP_ADDRESS_SIZE  (   sizeof( OSS_INTPTR_MAX_HEX_STRING \
                                              OSS_HEXDUMP_SPLITER )     \
                                    - sizeof( '\0' ) )
#define OSS_HEXDUMP_HEX_LEN       (   ( OSS_HEXDUMP_BYTES_PER_LINE << 1 )   \
                                    + ( OSS_HEXDUMP_BYTES_PER_LINE >> 1 ) )
#define OSS_HEXDUMP_SPACES_IN_BETWEEN  2
#define OSS_HEXDUMP_START_OF_DATA_DISP (   OSS_HEXDUMP_HEX_LEN              \
                                         + OSS_HEXDUMP_SPACES_IN_BETWEEN )
#define OSS_HEXDUMP_LINEBUFFER_SIZE  (   OSS_HEXDUMP_ADDRESS_SIZE       \
                                       + OSS_HEXDUMP_START_OF_DATA_DISP \
                                       + OSS_HEXDUMP_BYTES_PER_LINE     \
                                       + sizeof(OSS_NEWLINE) )
#define OSS_HEXDUMP_NULL_PREFIX    ((char *) NULL)

#define OSS_HEXDUMP_INCLUDE_ADDR    1
#define OSS_HEXDUMP_RAW_HEX_ONLY    2
#define OSS_HEXDUMP_PREFIX_AS_ADDR  4

unit32_t ossHexDumpLine
(
   const void *   inPtr,
   unit32_t         len,
   char *         szOutBuf,
   unit32_t         flags
) ;



unit32_t ossHexDumpBuffer
(
   const void *   inPtr,
   unit32_t         len,
   char *         szOutBuf,
   unit32_t         outBufSz,
   const void *   szPrefix,
   unit32_t         flags,
   unit32_t     *   pBytesProcessed = NULL
) ;

int32_t ossGetMemoryInfo ( int32_t &loadPercent,
                         int64_t &totalPhys,   int64_t &availPhys,
                         int64_t &totalPF,     int64_t &availPF,
                         int64_t &totalVirtual, int64_t &availVirtual,
                         int32_t &overCommitMode,
                         int64_t &commitLimit,  int64_t &committedAS ) ;

int32_t ossGetMemoryInfo ( int32_t &loadPercent,
                         int64_t &totalPhys,   int64_t &availPhys,
                         int64_t &totalPF,     int64_t &availPF,
                         int64_t &totalVirtual, int64_t &availVirtual ) ;

int32_t ossGetDiskInfo ( const char *pPath, int64_t &totalBytes, int64_t &freeBytes,
                       char* fsName = NULL, int32_t fsNameSize = 0 ) ;

int32_t ossGetFileDesp ( int64_t &usedNum ) ;

int32_t ossGetProcessMemory( OSSPID pid, int64_t &vmRss, int64_t &vmSize ) ;

typedef struct _ossDiskIOStat
{
   uint64_t rdSectors ;
   uint64_t wrSectors ;
   uint64_t rdIos ;
   uint64_t rdMerges ;
   uint64_t wrIos ;
   uint64_t wrMerges ;
   unit32_t rdTicks ;
   unit32_t wrTicks ;
   unit32_t iosPgr ;
   unit32_t totTicks ;
   unit32_t rqTicks ;
} ossDiskIOStat ;

int32_t ossReadlink ( const char *pPath, char *pLinkedPath, int32_t maxLen ) ;

int32_t ossGetDiskIOStat ( const char *pDriverName, ossDiskIOStat &ioStat ) ;

int32_t ossGetCPUInfo ( int64_t &user, int64_t &sys,
                      int64_t &idle, int64_t &other ) ;

typedef struct _ossProcMemInfo
{
   int64_t    vSize;         // used virtual memory size(MB)
   int64_t    rss;           // resident size(MB)
   int64_t    fault;
}ossProcMemInfo;
int32_t ossGetProcMemInfo( ossProcMemInfo &memInfo,
                        OSSPID pid = ossGetCurrentProcessID() );
#if defined (_LINUX) || defined (_AIX)
class ossProcStatInfo
{
public:
   ossProcStatInfo( OSSPID pid );
   ~ossProcStatInfo(){}

public:
   int32_t    _pid;
   char     _comm[ OSS_MAX_PATHSIZE + 1 ];
   char     _state;
   int32_t    _ppid;
   int32_t    _pgrp;
   int32_t    _session;
   int32_t    _tty;
   int32_t    _tpgid;
   unit32_t   _flags;
   unit32_t   _minFlt;
   unit32_t   _cMinFlt;
   unit32_t   _majFlt;
   unit32_t   _cMajFlt;
   unit32_t   _uTime;
   unit32_t   _sTime;
   int32_t    _cuTime;
   int32_t    _csTime;
   int32_t    _priority;
   int32_t    _nice;
   int32_t    _nlwp;
   unit32_t   _alarm;
   unit32_t   _startTime;
   unit32_t   _vSize;
   int32_t    _rss;
   unit32_t   _rssRlim;
   unit32_t   _startCode;
   unit32_t   _endCode;
   unit32_t   _startStack;
   unit32_t   _kstkEsp;
   unit32_t   _kstkEip;
};
#endif   //#if defined (_LINUX)

#define OSS_MAX_IP_NAME 15
#define OSS_MAX_IP_ADDR 15
#define OSS_LOOPBACK_IP "127.0.0.1"
#define OSS_LOCALHOST   "localhost"

typedef struct _ossIP
{
   char  ipName[OSS_MAX_IP_NAME + 1];
   char  ipAddr[OSS_MAX_IP_ADDR + 1];
} ossIP;

class ossIPInfo
{
private:
   int32_t   _ipNum;
   ossIP*  _ips;

public:
   ossIPInfo();
   ~ossIPInfo();
   inline int32_t getIPNum() const { return _ipNum; }
   inline ossIP* getIPs() const { return _ips; }

private:
   int32_t _init();
};

#define OSS_LIMIT_VIRTUAL_MEM "virtual memory"
#define OSS_LIMIT_CORE_SZ "core file size"
#define OSS_LIMIT_DATA_SEG_SZ "data seg size"
#define OSS_LIMIT_FILE_SZ "file size"
#define OSS_LIMIT_CPU_TIME "cpu time"
#define OSS_LIMIT_FILE_LOCK "file locks"
#define OSS_LIMIT_MEM_LOCK "max locked memory"
#define OSS_LIMIT_MSG_QUEUE "POSIX message queues"
#define OSS_LIMIT_OPEN_FILE "open files"
#define OSS_LIMIT_SCHE_PRIO "scheduling priority"
#define OSS_LIMIT_STACK_SIZE "stack size"
#define OSS_LIMIT_PROC_NUM "process num"

class ossProcLimits
{
public:
   ossProcLimits() ;

   std::string str() const ;

   int32_t init() ;

   int32_t getLimit( const char *str,
                     int64_t &soft,
                     int64_t &hard ) const ;

   int32_t setLimit( const char *str, int64_t soft, int64_t hard ) ;

private:
   void _initRLimit( int32_t resource, const char *str ) ;

private:
   struct cmp
   {
      int32_t operator()( const char *l, const char *r ) const
      {
         return ossStrcmp( l, r ) < 0 ;
      }
   } ;
   std::map<const char *, std::pair<int64_t, int64_t>, cmp > _desc ;
} ;

int32_t ossNetIpIsValid( const char *ip, int32_t len ) ;

int32_t& ossGetSignalShieldFlag() ;
int32_t& ossGetPendingSignal() ;

/*
   ossSignalShield define
*/
class ossSignalShield
{
   public:
      ossSignalShield() ;
      ~ossSignalShield() ;
      void close() ;
      void doNothing() {}
} ;

#endif  //OSSUTIL_HPP_


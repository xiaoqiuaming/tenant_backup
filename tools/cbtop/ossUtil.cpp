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

   Source File Name = ossUtil.cpp

   Descriptive Name = Operating System Services Utilities

   When/how to use: this program may be used on binary and text-formatted
   versions of OSS component. This file contains wrappers for basic System Calls
   or C APIs.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#include <stdarg.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <locale.h>

#include "ossUtil.h"
#include "ossMem.h"
#include "oss.h"
#include "ob_define.h"

using namespace oceanbase::common;

#ifdef TRUE
#undef TRUE
#endif
#define TRUE 1

#ifdef FALSE
#undef FALSE
#endif
#define FALSE 0

#define OSS_TIMESTAMP_MIN (-2147483648LL)
#define OSS_TIMESTAMP_MAX (2147483647LL)
#define OSS_FD_SETSIZE  65528
#define CB_DEV_NULL     "/dev/null"

const char *OSSTRUELIST[]={
   "YES",
   "yes",
   "Y",
   "y",
   "TRUE",
   "true",
   "T",
   "t",
   "1"};

const char *OSSFALSELIST[]={
   "NO",
   "no",
   "N",
   "n",
   "FALSE",
   "false",
   "F",
   "f",
   "0"};

char *ossStrdup ( const char * str )
{
   size_t siz ;
   char *copy ;
   siz = ossStrlen ( str ) + 1 ;
   if ( ( copy = (char*)CB_OSS_MALLOC ( siz ) ) == NULL )
      return NULL ;
   ossMemcpy ( copy, str, siz ) ;
   return copy ;
}

int32_t ossStrToInt ( const char *pBuffer, int32_t *num )
{
   int32_t rc = OB_SUCCESS ;
   const char *pTempBuf = pBuffer ;
   int32_t number = 0 ;
   if ( !num )
   {
      rc = OB_MEM_OVERFLOW ;
      goto error ;
   }
   while ( pBuffer &&
           *pBuffer )
   {
      if ( *pBuffer >= '0' &&
           *pBuffer <= '9' )
      {
         number *= 10 ;
         number += ( *pBuffer - '0' ) ;
         ++pBuffer ;
      }
      else if ( '.' == *pBuffer )
      {
         if ( pBuffer - pTempBuf <= 0 )
         {
            rc = OB_INVALID_ARGUMENT ;
            goto error ;
         }
         break ;
      }
      else
      {
         rc = OB_INVALID_ARGUMENT ;
         goto error ;
      }
   }
   *num = number ;
done:
   return rc ;
error:
   goto done ;
}

size_t ossSnprintf(char* pBuffer, size_t iLength, const char* pFormat, ...)
{
   va_list ap;
   size_t n;
   va_start(ap, pFormat);
   n=vsnprintf(pBuffer, iLength, pFormat, ap);
   va_end(ap);
   if(n >= iLength)
      n=iLength-1;
   pBuffer[n]='\0';
   return n;
}

int32_t ossIsInteger( const char *pStr )
{
   uint32_t i = 0 ;
   while( pStr[i] )
   {
      if ( pStr[i] < '0' || pStr[i] > '9' )
      {
         if ( 0 != i || ( '-' != pStr[i] && '+' != pStr[i] ) )
         {
            return FALSE ;
         }
      }
      ++i ;
   }
   return TRUE ;
}

int32_t ossIsUTF8 ( char *pzInfo )
{
   size_t size = 0 ;
   setlocale ( LC_ALL, "" ) ;
   size = mbstowcs ( NULL, pzInfo, 0 ) ;
   if ( (size_t)-1 == size )
      return FALSE ;
   else
      return TRUE ;
}

void ossCloseAllOpenFileHandles ( int32_t closeSTD )
{
   int32_t i = 3 ;
   int32_t max = OSS_FD_SETSIZE ;
   if ( closeSTD )
   {
      i = 0 ;
   }
   while ( i < max )
   {
      close ( i ) ;
      ++i ;
   }
   if ( closeSTD )
   {
      int32_t fd = 0 ;
      close ( STDIN_FILENO ) ;
      fd = open ( CB_DEV_NULL, O_RDWR ) ;
      if ( -1 != fd )
      {
         dup2 ( fd, STDOUT_FILENO ) ;
         dup2 ( fd, STDERR_FILENO ) ;
      }
   }
}

void ossCloseStdFds()
{
   int32_t fd = 0 ;

   fd = open ( CB_DEV_NULL, O_RDWR ) ;
   if ( -1 != fd )
   {
      dup2 ( fd, STDIN_FILENO ) ;
      dup2 ( fd, STDOUT_FILENO ) ;
      dup2 ( fd, STDERR_FILENO ) ;
      close( fd ) ;
   }
}

int32_t ossStrncasecmp ( const char *pString1, const char *pString2,
                       size_t iLength)
{
   return strncasecmp(pString1, pString2, iLength);
}

char *ossStrnchr(const char *pString, uint32_t c, uint32_t n)
{
   const char* p = pString;
   while(n--)
      if(*p==(char)c) return (char*)p; else ++p;
   return NULL;
}

int32_t ossStrToBoolean(const char* pString, int32_t* pBoolean)
{
   int32_t  rc           = OB_SUCCESS ;
   uint32_t i            = 0 ;
   size_t len          = ossStrlen(pString) ;

   if (0 == len)
   {
      *pBoolean=FALSE ;
      rc = OB_INVALID_ARGUMENT ;
      goto error ;
   }
   for(; i < sizeof(OSSTRUELIST)/sizeof(uint32_t); i++)
   {
      if(ossStrncasecmp(pString, OSSTRUELIST[i], len) == 0)
      {
         *pBoolean = TRUE ;
         goto done ;
      }
   }
   for(i = 0; i < sizeof(OSSFALSELIST)/sizeof(uint32_t); i++)
   {
      if(ossStrncasecmp(pString, OSSFALSELIST[i], len) == 0)
      {
         *pBoolean = FALSE ;
         goto done ;
      }
   }

   *pBoolean = FALSE ;
   rc = OB_INVALID_ARGUMENT ;
   goto error ;

done :
   return rc ;
error :
   goto done ;
}

size_t ossVsnprintf
(
   char * buf, size_t size, const char * fmt, va_list ap
)
{
   size_t n ;
   size_t terminator ;

   n = vsnprintf( buf, size, fmt, ap ) ;
   if ( n < size )
   {
      terminator = n ;
   }
   else
   {
      terminator = size - 1 ;
   }
   buf[terminator] = '\0' ;

   return terminator ;
}

uint32_t ossHashFileName ( const char *fileName )
{
   const char *pathSep = OSS_FILE_SEP ;
   const char *pFileName = ossStrrchr ( fileName, pathSep[0] ) ;
   if ( !pFileName )
      pFileName = fileName ;
   else
      pFileName++ ;
   return ossHash ( pFileName, (int32_t)ossStrlen ( pFileName ) ) ;
}
#undef get16bits
#if (defined(__GNUC__) && defined(__i386__)) || defined(__WATCOMC__) \
  || defined(_MSC_VER) || defined (__BORLANDC__) || defined (__TURBOC__)
#define get16bits(d) (*((const uint16_t *) (d)))
#endif

#if !defined (get16bits)
#define get16bits(d) ((((uint32_t)(((const uint8_t *)(d))[1])) << 8)\
                       +(uint32_t)(((const uint8_t *)(d))[0]) )
#endif
uint32_t ossHash ( const char *data, int32_t len )
{
   uint32_t hash = len, tmp ;
   int32_t rem ;
   if ( len <= 0 || data == NULL ) return 0 ;
   rem = len&3 ;
   len >>= 2 ;
   for (; len > 0 ; --len )
   {
      hash += get16bits (data) ;
      tmp   = (get16bits (data+2) << 11) ^ hash;
      hash  = (hash<<16)^tmp ;
      data += 2*sizeof(uint16_t) ;
      hash += hash>>11 ;
   }
   switch ( rem )
   {
   case 3:
      hash += get16bits (data) ;
      hash ^= hash<<16 ;
      hash ^= ((int8_t)data[sizeof (uint16_t)])<<18 ;
      hash += hash>>11 ;
      break ;
   case 2:
      hash += get16bits(data) ;
      hash ^= hash <<11 ;
      hash += hash >>17 ;
      break ;
   case 1:
      hash += (int8_t)*data ;
      hash ^= hash<<10 ;
      hash += hash>>1 ;
   }
   hash ^= hash<<3 ;
   hash += hash>>5 ;
   hash ^= hash<<4 ;
   hash += hash>>17 ;
   hash ^= hash<<25 ;
   hash += hash>>6 ;
   return hash ;
}
#undef get16bits

int32_t ossDup2( int oldFd, int newFd )
{
   if ( dup2( oldFd, newFd ) < 0 )
   {
      return OB_ERR_SYS ;
   }
   return OB_SUCCESS ;
}

int32_t ossResetTty()
{
   int32_t rc     = OB_SUCCESS ;
   FILE *stream = NULL ;
   stream = freopen( "/dev/tty", "w", stdout ) ;
   if ( NULL == stream )
   {
      rc = OB_ERR_SYS ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}


int32_t ossIsTimestampValid( int64_t tm )
{
   if( tm > OSS_TIMESTAMP_MAX || tm < OSS_TIMESTAMP_MIN )
   {
      return FALSE ;
   }

   return TRUE ;
}

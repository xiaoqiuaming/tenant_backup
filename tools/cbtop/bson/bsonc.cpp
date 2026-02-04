/* bson.c */
/*    Copyright 2012 SequoiaDB Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
/*    Copyright 2009, 2010 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include "oss.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <limits.h>
#include <pthread.h>

#include "bsonc.h"
#include "encoding.h"
#include "base64c.h"

//#if defined (__linux__) || defined (_AIX)
#include <sys/types.h>
#include <unistd.h>
//#elif defined (_WIN32)
//#include <Windows.h>
//#include <WinBase.h>
//#endif

using namespace std;

#define OSS_SINT64_JS_MAX  (9007199254740991LL)
#define OSS_SINT64_JS_MIN (-9007199254740991LL)

static int jsCompatibility = 0;
const int initialBufferSize = 0;

/* only need one of these */
static const int zero = 0;

/* Custom standard function pointers. */
void *( *bson_malloc_func )( size_t ) = malloc;
void *( *bson_realloc_func )( void *, size_t ) = realloc;
void  ( *bson_free_func )( void * ) = free;
#ifdef R_SAFETY_NET
bson_printf_func bson_printf;
#else
bson_printf_func bson_printf = printf;
#endif
bson_fprintf_func bson_fprintf = fprintf;
bson_sprintf_func bson_sprintf = sprintf;

static int _bson_errprintf( const char *, ... );
bson_printf_func bson_errprintf = _bson_errprintf;

/* ObjectId fuzz functions. */
static int ( *oid_fuzz_func )( void ) = NULL;
static int ( *oid_inc_func )( void )  = NULL;
void LocalTime ( time_t *Time, struct tm *TM );
/* ----------------------------
   READING
   ------------------------------ */

static int bson_sprint_decimal_length_iterator ( const bson_iterator *i ) ;


CB_EXPORT bson* bson_create( void ) {
   bson *obj = (bson*)bson_malloc(sizeof(bson));
   if ( obj )
   {
      obj->data = NULL ;
      obj->ownmem = 0 ;
      bson_init ( obj ) ;
   }
   return obj ;
}

CB_EXPORT void bson_dispose(bson* b) {
   if ( b )
   {
      if ( b->ownmem )
         bson_free ( b->data ) ;
      bson_free(b);
   }
}

CB_EXPORT bson *bson_empty( bson *obj ) {
#if defined (CB_BIG_ENDIAN)
    static char *data = "\0\0\0\005\0" ;
#else
  static char data[] = "\005\0\0\0\0" ;
#endif
    bson_init_data( obj, data );
    obj->finished = 1;
    obj->err = 0;
    obj->errstr = NULL;
    obj->stackPos = 0;
    obj->ownmem = 0 ;
    return obj;
}

CB_EXPORT bson_bool_t bson_is_empty( bson *obj ) {
   bson_iterator it ;
   if ( !obj )
   {
      return 1 ;
   }

   bson_iterator_init( &it, obj );
   return !bson_iterator_more( &it ) ;
}

CB_EXPORT int bson_copy( bson *out, const bson *in ) {
    if ( !out || !in ) return BSON_ERROR;
    if ( !in->finished ) return BSON_ERROR;
    bson_destroy ( out ) ;
    bson_init_size( out, bson_size( in ) );
    memcpy( out->data, in->data, bson_size( in ) );
    out->finished = 1;

    return BSON_OK;
}

int bson_init_data( bson *b, const char *data ) {
    if ( b->ownmem && b->data )
    {
       bson_free ( b->data ) ;
    }
    b->data = (char*)data;
    b->ownmem = 0 ;
    return BSON_OK;
}

int bson_init_finished_data( bson *b, const char *data ) {
    bson_init_data( b, data );
    b->finished = 1;
    return BSON_OK;
}

static void _bson_reset( bson *b ) {
    b->finished = 0;
    b->stackPos = 0;
    b->err = 0;
    b->errstr = NULL;
}

CB_EXPORT int bson_size( const bson *b ) {
    int i;
    if ( ! b || ! b->data )
        return 0;
    bson_little_endian32( &i, b->data );
    return i;
}

CB_EXPORT int bson_buffer_size( const bson *b ) {
    return static_cast<int>(b->cur - b->data + 1);
}


CB_EXPORT const char *bson_data( const bson *b ) {
    return (const char *)b->data;
}

static char hexbyte( char hex ) {
    if (hex >= '0' && hex <= '9')
        return static_cast<char>(hex - '0');
    else if (hex >= 'A' && hex <= 'F')
        return static_cast<char>(hex - 'A' + 10);
    else if (hex >= 'a' && hex <= 'f')
        return static_cast<char>(hex - 'a' + 10);
    else
        return 0x0;
}

CB_EXPORT void bson_oid_from_string( bson_oid_t *oid, const char *str ) {
    int i;
    for ( i=0; i<12; i++ ) {
        oid->bytes[i] = static_cast<char>(( hexbyte( str[2*i] ) << 4 ) | hexbyte( str[2*i + 1] ));
    }
}

CB_EXPORT void bson_oid_to_string( const bson_oid_t *oid, char *str ) {
    static const char hex[16] = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
    int i;
    for ( i=0; i<12; i++ ) {
        str[2*i]     = hex[( oid->bytes[i] & 0xf0 ) >> 4];
        str[2*i + 1] = hex[ oid->bytes[i] & 0x0f      ];
    }
    str[24] = '\0';
}

CB_EXPORT void bson_set_oid_fuzz( int ( *func )( void ) ) {
    oid_fuzz_func = func;
}

CB_EXPORT void bson_set_oid_inc( int ( *func )( void ) ) {
    oid_inc_func = func;
}
#pragma pack(1)
struct bson_machine_pid {
   unsigned char _machineNumber[3] ;
   unsigned short _pid ;
} ;
static struct bson_machine_pid bson_ourMachineAndPid ;
#pragma pack()

static void _initOid( void )
{
    unsigned long long n = 0 ;
    unsigned short pid = 0 ;
    srand ( (unsigned int)time(NULL) ) ;
#if defined (_WIN32)
    {
       unsigned int a=0, b=0 ;
       pid = (unsigned short) GetCurrentProcessId () ;
       rand_s ( &a ) ;
       rand_s ( &b ) ;
       n = (((unsigned long long)a)<<32) | b ;
    }
#elif defined (__linux__) || defined (_AIX)
    pid = (unsigned short) getpid () ;
    n = (((unsigned long long)random())<<32) | random() ;
#endif
    memcpy ( &bson_ourMachineAndPid, &n, sizeof(struct bson_machine_pid) ) ;
    bson_ourMachineAndPid._pid = pid ;
}

CB_EXPORT void bson_oid_gen( bson_oid_t *oid ) {
    static int incr = 0;
    int i;
    int t = static_cast<int>(time( NULL ));
    static pthread_once_t initOnce = 0 ;
    pthread_once( &initOnce, _initOid );
    if( oid_inc_func )
        i = oid_inc_func();
#if defined(_WIN32)
    else
        i = InterlockedIncrement((volatile long*)&incr)-1;
#elif defined (__linux__) || defined (_AIX)
    else
        i = __sync_fetch_and_add(&incr, 1);
#endif
    memset ( oid, 0, sizeof(bson_oid_t) ) ;
    {
       unsigned char *source = (unsigned char *)&t ;
       unsigned char *dest = (unsigned char *)&oid->ints[0] ;
#if defined (CB_BIG_ENDIAN)
       dest[0] = source[0] ;
       dest[1] = source[1] ;
       dest[2] = source[2] ;
       dest[3] = source[3] ;
#else
       dest[0] = source[3] ;
       dest[1] = source[2] ;
       dest[2] = source[1] ;
       dest[3] = source[0] ;
#endif
       memcpy ( &oid->ints[1], &bson_ourMachineAndPid,
                sizeof(bson_ourMachineAndPid) ) ;
       source = (unsigned char *)&i ;
       dest = (unsigned char*)&oid->ints[2] ;
#if defined (CB_BIG_ENDIAN)
       dest[1] = source[1] ;
       dest[2] = source[2] ;
       dest[3] = source[3] ;
#else
       dest[3] = source[0] ;
       dest[2] = source[1] ;
       dest[1] = source[2] ;
#endif
   }
}

CB_EXPORT time_t bson_oid_generated_time( bson_oid_t *oid ) {
    time_t out;
    unsigned char *source = (unsigned char *)&oid->ints[0] ;
    unsigned char *dest = (unsigned char *)&out ;
#if defined (CB_BIG_ENDIAN)
    dest[0] = source[0] ;
    dest[1] = source[1] ;
    dest[2] = source[2] ;
    dest[3] = source[3] ;
#else
    dest[0] = source[3] ;
    dest[1] = source[2] ;
    dest[2] = source[1] ;
    dest[3] = source[0] ;
#endif

    return out;
}

CB_EXPORT int bson_sprint ( char *buffer, int bufsize, const bson *b ) {
    char *pbuf = buffer ;
    int leftsize = bufsize ;
    if ( bufsize <= 0 || !buffer || !b )
       return 0 ;
    memset ( pbuf, 0, bufsize ) ;
    return bson_sprint_raw ( &pbuf, &leftsize, b->data, 1 ) ;
}

#define CHECK_LEFT(x) \
{ \
   if ( (*x) <= 0 ) \
      return 0 ; \
}

static int strlen_a ( const char *data )
{
   int len = 0 ;
   if ( !data )
   {
      return 0 ;
   }
   while ( data && *data )
   {
      if ( data[0] == '\"' ||
           data[0] == '\\' ||
           data[0] == '\b' ||
           data[0] == '\f' ||
           data[0] == '\n' ||
           data[0] == '\r' ||
           data[0] == '\t' )
      {
         ++len ;
      }
      ++len ;
      ++data ;
   }
   return len ;
}

static void bson_sprint_raw_concat ( char **pbuf, int *left, const char *data, int isString )
{
    unsigned int tempsize = 0 ;
    char *pTempBuffer = *pbuf ;

    if( isString )
    {
       tempsize = strlen_a( data ) ;
    }
    else
    {
       tempsize = static_cast<int>(strlen( data )) ;
    }
    tempsize = tempsize > (unsigned int)(*left) ? (unsigned int)(*left) : tempsize ;

    if( isString )
    {
       unsigned int i = 0 ;
       for ( i = 0; i < tempsize; ++i )
       {
          switch( *data )
          {
          case '\"':
             pTempBuffer[i] = '\\' ;
             ++i ;
             pTempBuffer[i] = '"' ;
             break ;
          case '\\':
             pTempBuffer[i] = '\\' ;
             ++i ;
             pTempBuffer[i] = '\\' ;
             break ;
          case '\b':
             pTempBuffer[i] = '\\' ;
             ++i ;
             pTempBuffer[i] = 'b' ;
             break ;
          case '\f':
             pTempBuffer[i] = '\\' ;
             ++i ;
             pTempBuffer[i] = 'f' ;
             break ;
          case '\n':
             pTempBuffer[i] = '\\' ;
             ++i ;
             pTempBuffer[i] = 'n' ;
             break ;
          case '\r':
             pTempBuffer[i] = '\\' ;
             ++i ;
             pTempBuffer[i] = 'r' ;
             break ;
          case '\t':
             pTempBuffer[i] = '\\' ;
             ++i ;
             pTempBuffer[i] = 't' ;
             break ;
          default:
             pTempBuffer[i] = *data ;
             break ;
          }
          ++data ;
       }
    }
    else
    {
       memcpy( *pbuf, data, tempsize ) ;
    }

    *left -= tempsize ;
    *pbuf += tempsize ;
}

/* Comment unused function to clean compile warning. If you want to use it, just
 * remove this comment.
static void bson_sprint_hex_concat ( char **pbuf, int *left, const char *data, unsigned int size )
{
   unsigned int tempsize = size * 2 ;
   tempsize = tempsize > (unsigned int)(*left) ? (unsigned int)(*left) : tempsize ;
   while ( tempsize > 0 )
   {
      sprintf ( *pbuf, "%02x", *data ) ;
      *pbuf += 2 ;
      ++data ;
      tempsize -= 2 ;
   }
}
*/

CB_EXPORT int bson_sprint_iterator ( char **pbuf, int *left, bson_iterator *i,
                                      char delChar )
{

   char delCharStr[2] ;
   bson_type t ;
   delCharStr[0] = delChar ;
   delCharStr[1] = 0 ;
   if ( !left || *left <= 0 || !pbuf || !i )
      return 0 ;
   t = bson_iterator_type ( i ) ;
   if ( t == 0 )
      return 0 ;
   switch ( t )
   {
      case BSON_DOUBLE:
      {
         double valNum = bson_iterator_double( i ) ;
         int sign = 0 ;
         char temp[64] = {0} ;
         if( bson_is_inf( valNum, &sign ) == 0 )
         {
            sprintf ( temp, "%.16g", valNum ) ;
            bson_sprint_raw_concat ( pbuf, left, temp, 0 ) ;
         }
         else
         {
            if( sign == 1 )
            {
               bson_sprint_raw_concat( pbuf, left, "Infinity", 0 ) ;
            }
            else
            {
               bson_sprint_raw_concat( pbuf, left, "-Infinity", 0 ) ;
            }
         }
         CHECK_LEFT ( left )
         break;
      }
      case BSON_STRING:
      case BSON_SYMBOL:
      {
         const char *temp = bson_iterator_string( i ) ;
         bson_sprint_raw_concat ( pbuf, left, delCharStr, 0 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, temp, 1 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, delCharStr, 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_OID:
      {
         char oidhex[25];
         bson_oid_to_string( bson_iterator_oid( i ), oidhex );
         bson_sprint_raw_concat ( pbuf, left, "{ \"$oid\": \"", 0 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, oidhex, 0 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, "\" }", 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_BOOL:
      {
         char temp[6] = {0} ;
         sprintf ( temp, "%s", (bson_iterator_bool( i ) ? "true" : "false") ) ;
         bson_sprint_raw_concat ( pbuf, left, temp, 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_DATE:
      {
         char temp[64] = {0} ;
         time_t timer = bson_iterator_date( i ) / 1000 ;
         struct tm psr;
         LocalTime ( &timer, &psr ) ;
         if ( (psr.tm_year + 1900) >= 0 && (psr.tm_year + 1900) <= 9999 )
         {
            sprintf ( temp, "{ \"$date\": \"%04d-%02d-%02d\" }", psr.tm_year + 1900, psr.tm_mon + 1, psr.tm_mday ) ;
         }
         else
         {
            sprintf ( temp, "{ \"$date\": %lld }", (unsigned long long)bson_iterator_date( i ) ) ;
         }
         bson_sprint_raw_concat ( pbuf, left, temp, 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_BINDATA:
      {
         char temp[64] = {0} ;
         int bin_size = 0 ;
         int base64_size = 0 ;
         char *pBase64Buf = NULL ;
         char *pBin_data = NULL ;
         sprintf ( temp, "\", \"$type\": \"%u\" }", (unsigned char)bson_iterator_bin_type ( i ) ) ;
         bson_sprint_raw_concat ( pbuf, left, "{ \"$binary\": \"", 0 ) ;
         CHECK_LEFT ( left )
         bin_size = bson_iterator_bin_len ( i ) ;
         if( bin_size > 0 )
         {
            base64_size = getEnBase64Size( bin_size ) ;
            pBase64Buf = (char *)malloc( base64_size + 1 ) ;
            if ( !pBase64Buf )
            {
               return 0 ;
            }
            memset( pBase64Buf, 0, base64_size + 1 ) ;
            pBin_data = (char *)bson_iterator_bin_data ( i ) ;
            if ( base64Encode( pBin_data, bin_size, pBase64Buf, base64_size ) < 0 )
            {
               free( pBase64Buf ) ;
               pBase64Buf = NULL ;
               return 0 ;
            }
            bson_sprint_raw_concat ( pbuf, left, pBase64Buf, 0 ) ;
            free( pBase64Buf ) ;
            CHECK_LEFT ( left )
         }
         bson_sprint_raw_concat ( pbuf, left, temp, 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_UNDEFINED:
      {
       char temp[] = "{ \"$undefined\": 1 }" ;
         bson_sprint_raw_concat ( pbuf, left, temp, 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_NULL:
      {
       char temp[] = "null" ;
         bson_sprint_raw_concat ( pbuf, left, temp, 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_MINKEY:
      {
       char temp[] = "{ \"$minKey\": 1 }" ;
         bson_sprint_raw_concat ( pbuf, left, temp, 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_MAXKEY:
      {
       char temp[] = "{ \"$maxKey\": 1 }" ;
         bson_sprint_raw_concat ( pbuf, left, temp, 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_REGEX:
      {
         bson_sprint_raw_concat ( pbuf, left, "{ \"$regex\": \"", 0 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, bson_iterator_regex( i ), 1 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, "\", \"$options\": \"", 0 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, bson_iterator_regex_opts( i ), 0 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, "\" }", 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_CODE:
      {
         bson_sprint_raw_concat ( pbuf, left, "{ \"$code\": \"", 0 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, delCharStr, 0 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, bson_iterator_code( i ), 1 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, delCharStr, 0 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, "\" }", 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_INT:
      {
         char temp[32] = {0} ;
         sprintf ( temp, "%d", bson_iterator_int( i ) ) ;
         bson_sprint_raw_concat ( pbuf, left, temp, 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_LONG:
      {
         char temp[64] = {0} ;
         const char *pFormat = 0 ;
         long long num = ( long long )bson_iterator_long( i );
         if (!bson_get_js_compatibility())
         {
            pFormat = "%lld" ;
         }
         else
         {
            if ( num >= OSS_SINT64_JS_MIN && num <= OSS_SINT64_JS_MAX )
            {
               pFormat = "%lld" ;
            }
            else
            {
               pFormat = "{ \"$numberLong\": \"%lld\" }" ;
            }
         }
         sprintf ( temp, pFormat, num ) ;
         bson_sprint_raw_concat ( pbuf, left, temp, 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_DECIMAL:
      {
         bson_decimal decimal = DECIMAL_DEFAULT_VALUE ;
         char *temp  = NULL ;
         int tmpSize = 0 ;
         int tmpRC   = 0 ;

         bson_iterator_decimal( i, &decimal ) ;
         decimal_to_jsonstr_len( decimal.sign, decimal.weight, decimal.dscale,
                                 decimal.typemod, &tmpSize ) ;

         temp = (char *)malloc( tmpSize ) ;
         if ( !temp )
         {
            decimal_free( &decimal ) ;
            return 0 ;
         }

         memset( temp, 0, tmpSize ) ;
         tmpRC = decimal_to_jsonstr( &decimal, temp, tmpSize ) ;
         if ( 0 != tmpRC )
         {
            decimal_free( &decimal ) ;
            free( temp ) ;
            return 0 ;
         }

         bson_sprint_raw_concat ( pbuf, left, temp, 0 ) ;
         decimal_free( &decimal ) ;
         free( temp ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_TIMESTAMP:
      {
         bson_timestamp_t ts;
         char temp[64] = {0} ;
         time_t timer ;
         struct tm psr;
         ts = bson_iterator_timestamp( i );
         timer = (time_t)ts.t;
         LocalTime ( &timer, &psr ) ;
         sprintf ( temp, "{ \"$timestamp\": \"%04d-%02d-%02d-%02d.%02d.%02d.%06d\" }",
                   psr.tm_year + 1900, psr.tm_mon + 1, psr.tm_mday, psr.tm_hour,
                   psr.tm_min, psr.tm_sec, ts.i ) ;
         bson_sprint_raw_concat ( pbuf, left, temp, 0 ) ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_OBJECT:
      {
         if ( !bson_sprint_raw( pbuf, left, bson_iterator_value( i ) , 1) )
            return  0 ;
         CHECK_LEFT ( left )
         break ;
      }
      case BSON_ARRAY:
      {
         if ( !bson_sprint_raw( pbuf, left, bson_iterator_value( i ) , 0 ) )
            return  0 ;
         CHECK_LEFT ( left )
         break;
      }
      case BSON_DBREF:
      {
         char oidhex[25] ;
         bson_oid_to_string( bson_iterator_dbref_oid( i ), oidhex ) ;

         bson_sprint_raw_concat ( pbuf, left, "{ \"$db\" : \"", 0 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, bson_iterator_dbref( i ), 1 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, "\", \"$id\" : \"", 0 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, oidhex, 0 ) ;
         CHECK_LEFT ( left )
         bson_sprint_raw_concat ( pbuf, left, "\" }", 0 ) ;
         CHECK_LEFT ( left )

         break ;
      }
      default:
         return 0 ;
   }
   return 1 ;
}
CB_EXPORT int bson_sprint_raw ( char **pbuf, int *left, const char *data, int isobj )
{
    bson_iterator i;
    const char *key;
    int first = 1 ;
    if ( !left || *left <= 0 || !pbuf || !data )
       return 0 ;

    bson_iterator_from_buffer( &i, data );
    if ( isobj )
    {
       bson_sprint_raw_concat ( pbuf, left, "{ ", 0 ) ;
       CHECK_LEFT ( left )
    }
    else
    {
        bson_sprint_raw_concat ( pbuf, left, "[ ", 0 ) ;
       CHECK_LEFT ( left )
    }
    while ( bson_iterator_next( &i ) ) {
        bson_type t = bson_iterator_type( &i );
        if ( t == 0 )
            break;
        if ( !first )
        {
           bson_sprint_raw_concat ( pbuf, left, ", ", 0 ) ;
            CHECK_LEFT ( left )
        }
        else
           first = 0 ;
        key = bson_iterator_key( &i );
        if ( isobj )
        {
           bson_sprint_raw_concat ( pbuf, left, "\"", 0 ) ;
           CHECK_LEFT ( left )
           bson_sprint_raw_concat ( pbuf, left, key, 1 ) ;
           CHECK_LEFT ( left )
           bson_sprint_raw_concat ( pbuf, left, "\"", 0 ) ;
           CHECK_LEFT ( left )
           bson_sprint_raw_concat ( pbuf, left, ": ", 0 ) ;
           CHECK_LEFT ( left )
        }
        if ( !bson_sprint_iterator ( pbuf, left, &i, '"' ) )
           return 0 ;
    }
    if ( isobj )
    {
       bson_sprint_raw_concat ( pbuf, left, " }", 0 ) ;
       CHECK_LEFT ( left )
    }
    else
    {
       bson_sprint_raw_concat ( pbuf, left, " ]", 0 ) ;
       CHECK_LEFT ( left )
    }
    return 1 ;
}

int bson_sprint_decimal_length_iterator ( const bson_iterator *i )
{
   int sign     = CB_DECIMAL_POS ;
   int weight   = 0 ;
   int scale    = 0 ;
   int typemod  = 0 ;
   int size     = 0 ;
   if ( bson_iterator_type ( i ) != BSON_DECIMAL )
   {
      return 0 ;
   }

   bson_iterator_decimal_scale( i, &sign, &scale ) ;
   bson_iterator_decimal_weight( i, &weight ) ;
   bson_iterator_decimal_typemod( i, &typemod ) ;

   decimal_to_jsonstr_len( sign, weight, scale, typemod, &size ) ;

   return size ;
}

CB_EXPORT int bson_sprint_length_iterator ( bson_iterator *i )
{
   int total = 0 ;
   bson_type t ;
   if ( !i )
      return 0 ;
   t = bson_iterator_type ( i ) ;
   switch ( t )
   {
   case BSON_MAXKEY :
   case BSON_MINKEY :
      /* { "$minKey": 1 }
         { "$maxKey": 1 }
      */
      total += 17 ;
      break ;
   case BSON_DOUBLE :
      total += 64 ;
      break ;
   case BSON_STRING :
   case BSON_SYMBOL :
      /* "<string>" */
      total += static_cast<int>(2 * ( 2 + strlen ( bson_iterator_string ( i ) ) ) );
      break ;
   case BSON_OID :
      /* { "$oid": "<24-digits-string>" } */
      total += 39 ;
      break ;
   case BSON_BOOL :
      /* true or false */
      total += 5 ;
      break ;
   case BSON_DATE :
      /* { "$date": "YYYY-MM-DD" } or */
      /* { "$date": +/-number } +/-number max length 20 */
      total += 34 ;
      break ;
   case BSON_BINDATA :
      /* { "$binary" : "<bin data>", "$type" : "<type>" } */
      total += bson_iterator_bin_len(i) * 2 + 50 ;
      break ;
   case BSON_UNDEFINED :
      /* { "$undefined": 1 } */
      total += 19 ;
      break ;
   case BSON_NULL :
      total += 4 ;
      break ;
   case BSON_REGEX :
      /* "{ "$regex" : "<regexstr>", "$options" : "<optionsstr>" }" */
      total += static_cast<int>(34 + strlen ( bson_iterator_regex ( i ) ) * 2 +
                    strlen ( bson_iterator_regex_opts ( i ) )) ;
      break ;
   case BSON_CODE :
      total += static_cast<int>(2 + strlen ( bson_iterator_code ( i ) )) ;
      break ;
   case BSON_INT :
      total += 16 ;
      break ;
   case BSON_LONG :
   {
      if (!bson_get_js_compatibility())
      {
         total += 32 ;
      }
      else
      {
         total += 64 ;
      }
      break ;
   }
   case BSON_DECIMAL :
      total += bson_sprint_decimal_length_iterator( i ) ;
      break ;
   case BSON_TIMESTAMP :
      /* { "$timestamp": "YYYY-MM-DD-HH.MM.SS.mmmmmm" } */
      total += 47 ;
      break ;
   case BSON_OBJECT :
   {
      int len = bson_sprint_length_raw ( bson_iterator_value ( i ), 1 ) ;
      if ( 0 == len ) return 0 ;
      total += len ;
      break ;
   }
   case BSON_ARRAY :
   {
      int len = bson_sprint_length_raw ( bson_iterator_value ( i ), 0 ) ;
      if ( 0 == len ) return 0 ;
      total += len ;
      break ;
   }
   case BSON_DBREF :
   {
      /* { "$db" : "xxxx", "$id" : "<24-digits oid>" } */
      total += static_cast<int>( 64 + strlen ( bson_iterator_dbref ( i ) ) * 2 ) ;
      break ;
   }
   default :
      return 0 ;
   }
   return total ;
}


CB_EXPORT int bson_sprint_length_raw ( const char *data, int isobj ) {
    bson_iterator i;
    const char *key;
    int first = 1 ;
    int total = 0 ;

    if ( !data ) return 0 ;

    bson_iterator_from_buffer( &i, data );

    total += 2 ; // the leading '{<SPC>' or '[<SPC>'

    while ( bson_iterator_next( &i ) ) {
        bson_type t = bson_iterator_type( &i );
        int k = 0 ;
        if ( BSON_EOO == t )
            break ;
        if ( ! first )
            total += 2 ; // ",<SPC>"
        else
            first = 0 ;
        key = bson_iterator_key( &i );
        if ( isobj )
        {
            total += static_cast<int>(5 + strlen( key ));
        }
        k = bson_sprint_length_iterator ( &i ) ;
        if ( 0 == k )
           return 0 ;
        total += k ;
    }
    total += 2 ; // <SPC>} or <SPC>]
    total += 1 ; // ending '\0'
    return total ;
}

CB_EXPORT int bson_sprint_length( const bson *b ) {
    if ( ! b ) return 0 ;
    return bson_sprint_length_raw( b->data, 1 ) ;
}

CB_EXPORT void bson_print( const bson *b )
{
   char *p = NULL ;
   int bufferSize = bson_sprint_length ( b ) ;
   p = (char*)malloc(bufferSize) ;
   if ( !p )
      return ;
   if ( bson_sprint ( p, bufferSize, b ) )
   {
      printf ( "%s\n", p ) ;
   }
   free ( p ) ;
}

/*CB_EXPORT void bson_print( const bson *b ) {
    bson_print_raw( b->data , 0 );
}

CB_EXPORT void bson_print_raw( const char *data , int depth ) {
    bson_iterator i;
    const char *key;
    int temp;
    bson_timestamp_t ts;
    char oidhex[25];
    bson scope;
    bson_iterator_from_buffer( &i, data );

    while ( bson_iterator_next( &i ) ) {
        bson_type t = bson_iterator_type( &i );
        if ( t == 0 )
            break;
        key = bson_iterator_key( &i );

        for ( temp=0; temp<=depth; temp++ )
            bson_printf( "\t" );
        bson_printf( "%s : %d \t " , key , t );
        switch ( t ) {
        case BSON_DOUBLE:
            bson_printf( "%f" , bson_iterator_double( &i ) );
            break;
        case BSON_STRING:
            bson_printf( "%s" , bson_iterator_string( &i ) );
            break;
        case BSON_SYMBOL:
            bson_printf( "SYMBOL: %s" , bson_iterator_string( &i ) );
            break;
        case BSON_OID:
            bson_oid_to_string( bson_iterator_oid( &i ), oidhex );
            bson_printf( "%s" , oidhex );
            break;
        case BSON_BOOL:
            bson_printf( "%s" , bson_iterator_bool( &i ) ? "true" : "false" );
            break;
        case BSON_DATE:
            bson_printf( "%ld" , ( long int )bson_iterator_date( &i ) );
            break;
        case BSON_BINDATA:
            bson_printf( "BSON_BINDATA" );
            break;
        case BSON_UNDEFINED:
            bson_printf( "BSON_UNDEFINED" );
            break;
        case BSON_NULL:
            bson_printf( "BSON_NULL" );
            break;
        case BSON_REGEX:
            bson_printf( "BSON_REGEX: %s", bson_iterator_regex( &i ) );
            break;
        case BSON_CODE:
            bson_printf( "BSON_CODE: %s", bson_iterator_code( &i ) );
            break;
        case BSON_CODEWSCOPE:
            bson_printf( "BSON_CODE_W_SCOPE: %s", bson_iterator_code( &i ) );
            bson_iterator_code_scope( &i, &scope );
            bson_printf( "\n\t SCOPE: " );
            bson_print( &scope );
            break;
        case BSON_INT:
            bson_printf( "%d" , bson_iterator_int( &i ) );
            break;
        case BSON_LONG:
            bson_printf( "%lld" , ( uint64_t )bson_iterator_long( &i ) );
            break;
        case BSON_TIMESTAMP:
            ts = bson_iterator_timestamp( &i );
            bson_printf( "i: %d, t: %d", ts.i, ts.t );
            break;
        case BSON_OBJECT:
        case BSON_ARRAY:
            bson_printf( "\n" );
            bson_print_raw( bson_iterator_value( &i ) , depth + 1 );
            break;
        default:
            bson_errprintf( "can't print type : %d\n" , t );
        }
        bson_printf( "\n" );
    }
}*/

/* ----------------------------
   ITERATOR
   ------------------------------ */

CB_EXPORT bson_iterator* bson_iterator_create( void ) {
    return ( bson_iterator* )malloc( sizeof( bson_iterator ) );
}

CB_EXPORT void bson_iterator_dispose(bson_iterator* i) {
    free(i);
}

CB_EXPORT void bson_iterator_init( bson_iterator *i, const bson *b ) {
    i->cur = b->data + 4;
    i->first = 1;
}

CB_EXPORT void bson_iterator_from_buffer( bson_iterator *i, const char *buffer ) {
    i->cur = buffer + 4;
    i->first = 1;
}

CB_EXPORT bson_type bson_find( bson_iterator *it, const bson *obj, const char *name ) {
    int namelen = static_cast<int>(strlen ( name )) ;
    char *pTempChar = (char*)bson_malloc ( namelen + 1 ) ;
    char *pDot = NULL ;
    char *pNextField = NULL ;
    if ( !pTempChar )
        return BSON_EOO ;
    memcpy ( pTempChar, name, namelen ) ;
    pTempChar[namelen] = 0 ;
    pDot = strchr ( pTempChar, '.' ) ;
    if ( pDot )
    {
       *pDot = 0 ;
       pNextField = pDot + 1 ;
    }
    bson_iterator_init( it, (bson *)obj );
    /* loop for all elements */
    while( bson_iterator_next( it ) ) {
        /* compare the field name and the one we want */
        if ( strcmp( pTempChar, bson_iterator_key( it ) ) == 0 )
        {
            /* if it match and there's subfield we are looking for */
            if ( pNextField )
            {
               /* let's make sure it's object or array type */
               if ( bson_iterator_type ( it ) == BSON_OBJECT ||
                    bson_iterator_type ( it ) == BSON_ARRAY )
               {
                  /* create temp object and use iterator's current location */
                  bson tempobj ;
                  bson_init ( &tempobj ) ;
                  bson_init_finished_data ( &tempobj,
                                           (char*) bson_iterator_value ( it )) ;
                  /* push iterator into subobject find */
                  bson_find ( it, &tempobj, pNextField ) ;
                  /* go to end for result */
                  break ;
               }
            }
            else
            {
               /* if we don't want to find any subfield, let's return */
               break ;
            }
        }
    }
    bson_free ( pTempChar ) ;
    return bson_iterator_type( it );
}

CB_EXPORT bson_bool_t bson_iterator_more( const bson_iterator *i ) {
    return *( i->cur );
}

CB_EXPORT bson_type bson_iterator_next( bson_iterator *i ) {
    int ds;

    if ( i->first ) {
        i->first = 0;
        return ( bson_type )(signed char)( *i->cur );
    }

    switch ( bson_iterator_type( i ) ) {
    case BSON_EOO:
        return BSON_EOO; /* don't advance */
    case BSON_UNDEFINED:
    case BSON_NULL:
    case BSON_MINKEY:
    case BSON_MAXKEY:
        ds = 0;
        break;
    case BSON_BOOL:
        ds = 1;
        break;
    case BSON_INT:
        ds = 4;
        break;
    case BSON_DECIMAL:
        bson_iterator_decimal_size( i, &ds ) ;
        break;
    case BSON_LONG:
    case BSON_DOUBLE:
    case BSON_TIMESTAMP:
    case BSON_DATE:
        ds = 8;
        break;
    case BSON_OID:
        ds = 12;
        break;
    case BSON_STRING:
    case BSON_SYMBOL:
    case BSON_CODE:
        ds = 4 + bson_iterator_int_raw( i );
        break;
    case BSON_BINDATA:
        ds = 5 + bson_iterator_int_raw( i );
        break;
    case BSON_OBJECT:
    case BSON_ARRAY:
    case BSON_CODEWSCOPE:
        ds = bson_iterator_int_raw( i );
        break;
    case BSON_DBREF:
        ds = 4+12 + bson_iterator_int_raw( i );
        break;
    case BSON_REGEX: {
        const char *s = bson_iterator_value( i );
        const char *p = s;
        p += strlen( p )+1;
        p += strlen( p )+1;
        ds = static_cast<int>(p-s);
        break;
    }

    default: {
        char msg[] = "unknown type: 000000000000";
        bson_numstr( msg+14, ( unsigned )( i->cur[0] ) );
        bson_fatal_msg( 0, msg );
        return (bson_type)0;
    }
    }

    i->cur += 1 + strlen( i->cur + 1 ) + 1 + ds;

    return ( bson_type )(signed char)( *i->cur );
}

CB_EXPORT bson_type bson_iterator_type( const bson_iterator *i ) {
    return ( bson_type )(signed char)i->cur[0];
}

CB_EXPORT const char *bson_iterator_key( const bson_iterator *i ) {
    return i->cur + 1;
}

CB_EXPORT const char *bson_iterator_value( const bson_iterator *i ) {
    const char *t = i->cur + 1;
    t += strlen( t ) + 1;
    return t;
}

/* types */
short bson_iterator_int16_raw( const bson_iterator *i ) {
    short out;
    bson_little_endian16( &out, bson_iterator_value( i ) );
    return out;
}

int bson_iterator_int_raw( const bson_iterator *i ) {
    int out;
    bson_little_endian32( &out, bson_iterator_value( i ) );
    return out;
}

double bson_iterator_double_raw( const bson_iterator *i ) {
    double out;
    bson_little_endian64( &out, bson_iterator_value( i ) );
    return out;
}

int64_t bson_iterator_long_raw( const bson_iterator *i ) {
    int64_t out;
    bson_little_endian64( &out, bson_iterator_value( i ) );
    return out;
}

bson_bool_t bson_iterator_bool_raw( const bson_iterator *i ) {
    return bson_iterator_value( i )[0];
}

CB_EXPORT bson_oid_t *bson_iterator_oid( const bson_iterator *i ) {
    return ( bson_oid_t * )bson_iterator_value( i );
}

CB_EXPORT int bson_iterator_int( const bson_iterator *i ) {
    switch ( bson_iterator_type( i ) ) {
    case BSON_INT:
        return bson_iterator_int_raw( i );
    case BSON_LONG:
        return static_cast<int>(bson_iterator_long_raw( i ));
    case BSON_DOUBLE:
        return static_cast<int>(bson_iterator_double_raw( i ));
    case BSON_DECIMAL:
    {
        int result = 0 ;
        bson_decimal decimal = DECIMAL_DEFAULT_VALUE ;
        result = decimal_from_bsonvalue( bson_iterator_value( i ), &decimal ) ;
        if ( 0 != result )
        {
            decimal_free( &decimal ) ;
            return BSON_ERROR ;
        }

        result = decimal_to_int( &decimal ) ;
        if ( 0 != result )
        {
            decimal_free( &decimal ) ;
            return BSON_ERROR ;
        }

        decimal_free( &decimal ) ;
        return BSON_OK ;
    }
    default:
        return 0;
    }
}

CB_EXPORT double bson_iterator_double( const bson_iterator *i ) {
    switch ( bson_iterator_type( i ) ) {
    case BSON_INT:
        return bson_iterator_int_raw( i );
    case BSON_LONG:
        return static_cast<int>(bson_iterator_long_raw( i ));
    case BSON_DOUBLE:
        return bson_iterator_double_raw( i );
    case BSON_DECIMAL:
    {
        double result = 0.0 ;
        bson_decimal decimal = DECIMAL_DEFAULT_VALUE ;
        decimal_from_bsonvalue( bson_iterator_value( i ), &decimal ) ;
        result = decimal_to_double( &decimal ) ;
        decimal_free( &decimal ) ;
        return result ;
    }
    default:
        return 0;
    }
}

CB_EXPORT int bson_iterator_decimal_weight( const bson_iterator *i,
                                             int *weight )
{
   const char *value = NULL ;
   short tmpWeight   = 0 ;
   if ( bson_iterator_type( i ) != BSON_DECIMAL )
   {
      return BSON_ERROR ;
   }

   value = bson_iterator_value( i ) ;

   value += 4 ;   // size
   value += 4 ;   // typemod
   value += 2 ;   // scale
   bson_little_endian16( &tmpWeight, value ) ;

   *weight = tmpWeight ;

   return BSON_OK ;
}

CB_EXPORT int bson_iterator_decimal_size( const bson_iterator *i,
                                           int *size )
{
   const char *value = NULL ;
   if ( bson_iterator_type( i ) != BSON_DECIMAL )
   {
      return BSON_ERROR ;
   }

   value = bson_iterator_value( i ) ;
   bson_little_endian32( size, value ) ;

   return BSON_OK ;
}

CB_EXPORT int bson_iterator_decimal_typemod( const bson_iterator *i,
                                              int *typemod )
{
   const char *value = NULL ;
   if ( bson_iterator_type( i ) != BSON_DECIMAL )
   {
      return BSON_ERROR ;
   }

   value = bson_iterator_value( i ) ;
   value += 4 ;   // size

   bson_little_endian32( typemod, value ) ;

   return BSON_OK ;
}


CB_EXPORT int bson_iterator_decimal_scale( const bson_iterator *i,
                                            int *sign, int *scale )
{
   const char *value = NULL ;
   short tmpScale    = 0 ;
   if ( bson_iterator_type( i ) != BSON_DECIMAL )
   {
      return BSON_ERROR ;
   }

   value = bson_iterator_value( i ) ;

   value += 4 ;   // size
   value += 4 ;   // typemod
   bson_little_endian16( &tmpScale, value ) ;

   *sign  = tmpScale & DECIMAL_SIGN_MASK ;
   *scale = tmpScale & DECIMAL_DSCALE_MASK ;

   return BSON_OK ;
}

CB_EXPORT int bson_iterator_decimal( const bson_iterator *i,
                                      bson_decimal *decimal )
{
   bson_type type ;
   int rc = 0 ;

   decimal_free( decimal ) ;
   type = bson_iterator_type( i ) ;
   if ( BSON_INT == type )
   {
      int tmp = bson_iterator_int_raw( i ) ;
      rc = decimal_from_int( tmp, decimal ) ;
   }
   else if ( BSON_LONG == type )
   {
      int64_t tmp = bson_iterator_long_raw( i ) ;
      rc = decimal_from_long( tmp, decimal ) ;
   }
   else if ( BSON_DOUBLE == type )
   {
      double tmp = bson_iterator_double_raw( i ) ;
      rc = decimal_from_double( tmp , decimal ) ;
   }
   else if ( BSON_DECIMAL == type )
   {
      rc = decimal_from_bsonvalue( bson_iterator_value( i ), decimal ) ;
   }
   else
   {
      rc = -1 ;
   }

   if ( 0 == rc )
   {
      return BSON_OK ;
   }
   else
   {
      return BSON_ERROR ;
   }
}

CB_EXPORT int64_t bson_iterator_long( const bson_iterator *i ) {
    switch ( bson_iterator_type( i ) ) {
    case BSON_INT:
        return bson_iterator_int_raw( i );
    case BSON_LONG:
        return bson_iterator_long_raw( i );
    case BSON_DOUBLE:
        return static_cast<int>(bson_iterator_double_raw( i ));
    case BSON_DECIMAL:
    {
        int64_t result = 0 ;
        bson_decimal decimal = DECIMAL_DEFAULT_VALUE ;
        decimal_from_bsonvalue( bson_iterator_value( i ), &decimal ) ;
        result = decimal_to_long( &decimal ) ;
        decimal_free( &decimal ) ;
        return result ;
    }
    default:
        return 0;
    }
}

CB_EXPORT bson_timestamp_t bson_iterator_timestamp( const bson_iterator *i ) {
    bson_timestamp_t ts;
    bson_little_endian32( &( ts.i ), bson_iterator_value( i ) );
    bson_little_endian32( &( ts.t ), bson_iterator_value( i ) + 4 );
    return ts;
}


CB_EXPORT int bson_iterator_timestamp_time( const bson_iterator *i ) {
    int time;
    bson_little_endian32( &time, bson_iterator_value( i ) + 4 );
    return time;
}


CB_EXPORT int bson_iterator_timestamp_increment( const bson_iterator *i ) {
    int increment;
    bson_little_endian32( &increment, bson_iterator_value( i ) );
    return increment;
}


CB_EXPORT bson_bool_t bson_iterator_bool( const bson_iterator *i ) {
    switch ( bson_iterator_type( i ) ) {
    case BSON_BOOL:
        return bson_iterator_bool_raw( i );
    case BSON_INT:
        return bson_iterator_int_raw( i ) != 0;
    case BSON_LONG:
        return bson_iterator_long_raw( i ) != 0;
    case BSON_DOUBLE:
        return bson_iterator_double_raw( i ) != 0;
    case BSON_DECIMAL:
    {
        bson_bool_t result = 0 ;
        bson_decimal decimal = DECIMAL_DEFAULT_VALUE ;
        decimal_from_bsonvalue( bson_iterator_value( i ), &decimal ) ;
        result = decimal_is_zero( &decimal ) ;
        decimal_free( &decimal ) ;
        return !result ;
    }
    case BSON_EOO:
    case BSON_NULL:
        return 0;
    default:
        return 1;
    }
}

CB_EXPORT const char *bson_iterator_string( const bson_iterator *i ) {
    switch ( bson_iterator_type( i ) ) {
    case BSON_STRING:
    case BSON_SYMBOL:
        return bson_iterator_value( i ) + 4;
    default:
        return "";
    }
}

int bson_iterator_string_len( const bson_iterator *i ) {
    return bson_iterator_int_raw( i );
}

CB_EXPORT const char *bson_iterator_code( const bson_iterator *i ) {
    switch ( bson_iterator_type( i ) ) {
    case BSON_STRING:
    case BSON_CODE:
        return bson_iterator_value( i ) + 4;
    case BSON_CODEWSCOPE:
        return bson_iterator_value( i ) + 8;
    default:
        return NULL;
    }
}

CB_EXPORT void bson_iterator_code_scope( const bson_iterator *i, bson *scope ) {
    if ( bson_iterator_type( i ) == BSON_CODEWSCOPE ) {
        int code_len;
        bson_little_endian32( &code_len, bson_iterator_value( i )+4 );
        bson_init_data( scope, (char*)(( void * )( bson_iterator_value( i )+8+code_len )) );
        _bson_reset( scope );
        scope->finished = 1;
    } else {
        bson_empty( scope );
    }
}

CB_EXPORT bson_date_t bson_iterator_date( const bson_iterator *i ) {
    return bson_iterator_long_raw( i );
}

CB_EXPORT time_t bson_iterator_time_t( const bson_iterator *i ) {
    return bson_iterator_date( i ) / 1000;
}

CB_EXPORT int bson_iterator_bin_len( const bson_iterator *i ) {
    return ( bson_iterator_bin_type( i ) == BSON_BIN_BINARY_OLD )
           ? bson_iterator_int_raw( i ) - 4
           : bson_iterator_int_raw( i );
}

CB_EXPORT char bson_iterator_bin_type( const bson_iterator *i ) {
    return bson_iterator_value( i )[4];
}

CB_EXPORT const char *bson_iterator_bin_data( const bson_iterator *i ) {
    return ( bson_iterator_bin_type( i ) == BSON_BIN_BINARY_OLD )
           ? bson_iterator_value( i ) + 9
           : bson_iterator_value( i ) + 5;
}

CB_EXPORT const char *bson_iterator_regex( const bson_iterator *i ) {
    return bson_iterator_value( i );
}

CB_EXPORT const char *bson_iterator_regex_opts( const bson_iterator *i ) {
    const char *p = bson_iterator_value( i );
    return p + strlen( p ) + 1;

}

CB_EXPORT const char *bson_iterator_dbref( const bson_iterator *i ) {
    return bson_iterator_value( i ) + 4;
}

CB_EXPORT bson_oid_t *bson_iterator_dbref_oid( const bson_iterator *i ) {
    const char *p = bson_iterator_dbref( i );
    return ( bson_oid_t * )( p + strlen( p ) + 1 );
}

CB_EXPORT void bson_iterator_subobject( const bson_iterator *i, bson *sub ) {
    bson_init_data( sub, ( char * )bson_iterator_value( i ) );
    _bson_reset( sub );
    sub->finished = 1;
}

CB_EXPORT void bson_iterator_subiterator( const bson_iterator *i, bson_iterator *sub ) {
    bson_iterator_from_buffer( sub, bson_iterator_value( i ) );
}

/* ----------------------------
   BUILDING
   ------------------------------ */

static void _bson_init_size( bson *b, int size ) {
    if ( b->ownmem && b->data )
    {
       bson_free ( b->data ) ;
       b->ownmem = 0 ;
       b->data = NULL ;
    }
    if( size == 0 )
        b->data = NULL;
    else
        b->data = ( char * )bson_malloc( size );
    b->dataSize = size;
    b->cur = b->data + 4;
    _bson_reset( b );
    b->ownmem = 1 ;
}

CB_EXPORT void bson_init( bson *b ) {
    b->ownmem = 0 ;
    b->data = NULL ;
    _bson_init_size( b, initialBufferSize );
}

void bson_init_size( bson *b, int size ) {
    _bson_init_size( b, size );
}

void bson_append_byte( bson *b, char c ) {
    b->cur[0] = c;
    b->cur++;
}

void bson_append( bson *b, const void *data, int len ) {
    memcpy( b->cur , data , len );
    b->cur += len;
}

void bson_append16( bson *b, const void *data ) {
    bson_little_endian16( b->cur, data );
    b->cur += 2;
}


void bson_append32( bson *b, const void *data ) {
    bson_little_endian32( b->cur, data );
    b->cur += 4;
}

void bson_append64( bson *b, const void *data ) {
    bson_little_endian64( b->cur, data );
    b->cur += 8;
}

int bson_ensure_space( bson *b, const int bytesNeeded ) {
    int pos = static_cast<int>(b->cur - b->data);
    char *orig = b->data;
    int new_size;
    if ( b->data && !b->ownmem )
    {
       /* if the data is pointing to somewhere but it's not owned buffer */
       return BSON_ERROR ;
    }
    if ( pos + bytesNeeded <= b->dataSize )
        return BSON_OK;

    if ( !b->dataSize )
    {
       new_size = static_cast<int>(1.5 * ( static_cast<double>(sizeof(int)) + bytesNeeded )) ;
    }
    else
    {
       new_size = static_cast<int>(1.5 * ( b->dataSize + bytesNeeded ));
    }
    if( new_size < b->dataSize ) {
        if( ( b->dataSize + bytesNeeded ) < INT_MAX )
            new_size = INT_MAX;
        else {
            b->err = BSON_SIZE_OVERFLOW;
            return BSON_ERROR;
        }
    }

    b->data = (char*)bson_realloc( b->data, new_size );
    if ( !b->data )
        bson_fatal_msg( !!b->data, "realloc() failed" );
    b->ownmem = 1 ;
    b->dataSize = new_size;
    b->cur += b->data - orig;

    return BSON_OK;
}

CB_EXPORT int bson_finish( bson *b ) {
    int i;

    if( b->err & BSON_NOT_UTF8 )
        return BSON_ERROR;

    if ( ! b->finished ) {
        if ( bson_ensure_space( b, 1 ) == BSON_ERROR ) return BSON_ERROR;
        bson_append_byte( b, 0 );
        i = static_cast<int>(b->cur - b->data);
        bson_little_endian32( b->data, &i );
        b->finished = 1;
    }

    return BSON_OK;
}

CB_EXPORT void bson_destroy( bson *b ) {
    if (b) {
        if ( b->ownmem )
           bson_free( b->data );
        b->err = 0;
        b->data = 0;
        b->cur = 0;
        b->finished = 1;
    }
}

static bson_bool_t _bson_is_nested_array( const bson *b ) {
   if ( ( b->stackPos > 0 ) &&
        ( BSON_ARRAY == (bson_type)(b->stackType[ b->stackPos - 1 ]) ) )
      return 1 ;
   else
      return 0 ;
}

static int bson_append_estart( bson *b, int type, const char *name, const int dataSize ) {
    const int len = static_cast<int>(strlen( name ) + 1);

    if ( b->finished ) {
        b->err |= BSON_ALREADY_FINISHED;
        return BSON_ERROR;
    }

    if ( bson_ensure_space( b, 1 + len + dataSize ) == BSON_ERROR ) {
        return BSON_ERROR;
    }

    if( bson_check_field_name( b, ( const char * )name, len - 1 ) == BSON_ERROR ) {
        bson_builder_error( b );
        return BSON_ERROR;
    }
    if ( _bson_is_nested_array( b ) ) {
       char c = '0' ;
       if ( len == 2 ) {
          c = (char)(name[0]) ;
          if ( c < '0' || c > '9' ) {
             bson_builder_error( b );
             return BSON_ERROR ;
          }
       } else if ( len > 2 ) {
          int i = 0 ;
          c = (char)(name[0]) ;
          if ( c < '1' || c > '9' ) {
             bson_builder_error( b );
             return BSON_ERROR ;
          }
          for ( i = 1; i < len - 1; i++ ) {
             c = (char)(name[i]) ;
             if ( c < '0' || c > '9' ) {
                bson_builder_error( b );
                return BSON_ERROR ;
             }
          }
       } else {
          bson_builder_error( b );
          return BSON_ERROR ;
       }
    }

    bson_append_byte( b, ( char )type );
    bson_append( b, name, len );
    return BSON_OK;
}

/* ----------------------------
   BUILDING TYPES
   ------------------------------ */

CB_EXPORT int bson_append_int( bson *b, const char *name, const int i ) {
    if ( bson_append_estart( b, BSON_INT, name, 4 ) == BSON_ERROR )
        return BSON_ERROR;
    bson_append32( b , &i );
    return BSON_OK;
}

CB_EXPORT int bson_append_long( bson *b, const char *name, const int64_t i ) {
    if ( bson_append_estart( b , BSON_LONG, name, 8 ) == BSON_ERROR )
        return BSON_ERROR;
    bson_append64( b , &i );
    return BSON_OK;
}

CB_EXPORT int bson_append_decimal( bson *b, const char *name,
                                    const bson_decimal *decimal )
{
   int i = 0 ;
   int size = static_cast<int>(DECIMAL_HEADER_SIZE + decimal->ndigits * sizeof( short )) ;
   int typemod  = decimal->typemod ;
   short dscale = static_cast<short>(( decimal->dscale & DECIMAL_DSCALE_MASK ) | decimal->sign) ;
   short weight = static_cast<short>(decimal->weight) ;
   if ( bson_append_estart( b , BSON_DECIMAL, name, size ) == BSON_ERROR )
        return BSON_ERROR;

   bson_append32( b, &size ) ;
   bson_append32( b, &typemod ) ;
   bson_append16( b, &dscale ) ;
   bson_append16( b, &weight ) ;
   for ( i = 0 ; i < decimal->ndigits; i++ )
   {
      bson_append16( b, &decimal->digits[i] ) ;
   }

   return BSON_OK ;
}

CB_EXPORT int bson_append_decimal3( bson *b, const char *name,
                                     const char *value )
{
   int rc = 0 ;
   bson_decimal decimal = DECIMAL_DEFAULT_VALUE ;

   rc = decimal_from_str( value, &decimal ) ;
   if ( 0 != rc )
   {
      return BSON_ERROR ;
   }

   rc = bson_append_decimal( b, name, &decimal ) ;
   decimal_free( &decimal ) ;

   return rc ;
}


CB_EXPORT int bson_append_decimal2( bson *b, const char *name,
                                     const char *value, int precision,
                                     int scale )
{
   int rc = 0 ;
   bson_decimal decimal = DECIMAL_DEFAULT_VALUE ;
   rc = decimal_init1( &decimal, precision, scale ) ;
   if ( 0 != rc )
   {
      return BSON_ERROR ;
   }

   rc = decimal_from_str( value, &decimal ) ;
   if ( 0 != rc )
   {
      return BSON_ERROR ;
   }

   rc = bson_append_decimal( b, name, &decimal ) ;
   decimal_free( &decimal ) ;

   return rc ;
}

CB_EXPORT int bson_append_double( bson *b, const char *name, const double d ) {
    if ( bson_append_estart( b, BSON_DOUBLE, name, 8 ) == BSON_ERROR )
        return BSON_ERROR;
    bson_append64( b , &d );
    return BSON_OK;
}

CB_EXPORT int bson_append_bool( bson *b, const char *name, const bson_bool_t i ) {
    if ( bson_append_estart( b, BSON_BOOL, name, 1 ) == BSON_ERROR )
        return BSON_ERROR;
    bson_append_byte( b , i != 0 );
    return BSON_OK;
}

CB_EXPORT int bson_append_null( bson *b, const char *name ) {
    if ( bson_append_estart( b , BSON_NULL, name, 0 ) == BSON_ERROR )
        return BSON_ERROR;
    return BSON_OK;
}

CB_EXPORT int bson_append_minkey( bson *b, const char *name ){
    if ( bson_append_estart( b , BSON_MINKEY, name, 0 ) == BSON_ERROR )
        return BSON_ERROR;
    return BSON_OK;
}

CB_EXPORT int bson_append_maxkey( bson *b, const char *name ){
    if ( bson_append_estart( b , BSON_MAXKEY, name, 0 ) == BSON_ERROR )
        return BSON_ERROR;
    return BSON_OK;
}

CB_EXPORT int bson_append_undefined( bson *b, const char *name ) {
    if ( bson_append_estart( b, BSON_UNDEFINED, name, 0 ) == BSON_ERROR )
        return BSON_ERROR;
    return BSON_OK;
}

int bson_append_string_base( bson *b, const char *name,
                             const char *value, int len, bson_type type ) {

    int sl = len + 1;
    if ( bson_check_string( b, ( const char * )value, sl - 1 ) == BSON_ERROR )
        return BSON_ERROR;
    if ( bson_append_estart( b, type, name, 4 + sl ) == BSON_ERROR ) {
        return BSON_ERROR;
    }
    bson_append32( b , &sl );
    bson_append( b , value , sl - 1 );
    bson_append( b , "\0" , 1 );
    return BSON_OK;
}

int bson_append_string_not_utf8( bson *b, const char *name, const char *value, int len ) {

    int sl = len + 1;

    if ( bson_check_string( b, ( const char * )value, sl - 1 ) == BSON_ERROR ) {
        int i;

        if ( !( b->err & BSON_NOT_UTF8 ) ) {
            return BSON_ERROR;
        }

        for ( i = 0; i < len; i++ ) {
            if ( '\0' == value[i] ) {
                return BSON_ERROR;
            }
        }

        b->err &= ~BSON_NOT_UTF8;
    }

    if ( bson_append_estart( b, BSON_STRING, name, 4 + sl ) == BSON_ERROR ) {
        return BSON_ERROR;
    }
    bson_append32( b , &sl );
    bson_append( b , value , sl - 1 );
    bson_append( b , "\0" , 1 );
    return BSON_OK;
}

CB_EXPORT int bson_append_string( bson *b, const char *name, const char *value ) {
    return bson_append_string_base( b, name, value, static_cast<int>(strlen ( value )), BSON_STRING );
}

CB_EXPORT int bson_append_symbol( bson *b, const char *name, const char *value ) {
    return bson_append_string_base( b, name, value, static_cast<int>(strlen ( value )), BSON_SYMBOL );
}

CB_EXPORT int bson_append_code( bson *b, const char *name, const char *value ) {
    return bson_append_string_base( b, name, value, static_cast<int>(strlen ( value )), BSON_CODE );
}

CB_EXPORT int bson_append_string_n( bson *b, const char *name, const char *value, int len ) {
    return bson_append_string_base( b, name, value, len, BSON_STRING );
}

CB_EXPORT int bson_append_symbol_n( bson *b, const char *name, const char *value, int len ) {
    return bson_append_string_base( b, name, value, len, BSON_SYMBOL );
}

CB_EXPORT int bson_append_code_n( bson *b, const char *name, const char *value, int len ) {
    return bson_append_string_base( b, name, value, len, BSON_CODE );
}

CB_EXPORT int bson_append_code_w_scope_n( bson *b, const char *name,
                                const char *code, int len, const bson *scope ) {

    int sl, size;
    if ( !scope ) return BSON_ERROR;
    sl = len + 1;
    size = 4 + 4 + sl + bson_size( scope );
    if ( bson_append_estart( b, BSON_CODEWSCOPE, name, size ) == BSON_ERROR )
        return BSON_ERROR;
    bson_append32( b, &size );
    bson_append32( b, &sl );
    bson_append( b, code, sl );
    bson_append( b, scope->data, bson_size( scope ) );
    return BSON_OK;
}

CB_EXPORT int bson_append_code_w_scope( bson *b, const char *name, const char *code, const bson *scope ) {
    return bson_append_code_w_scope_n( b, name, code, static_cast<int>(strlen ( code )), scope );
}

CB_EXPORT int bson_append_binary( bson *b, const char *name, char type, const char *str, int len ) {
    if ( type == BSON_BIN_BINARY_OLD ) {
        int subtwolen = len + 4;
        if ( bson_append_estart( b, BSON_BINDATA, name, 4+1+4+len ) == BSON_ERROR )
            return BSON_ERROR;
        bson_append32( b, &subtwolen );
        bson_append_byte( b, type );
        bson_append32( b, &len );
        bson_append( b, str, len );
    } else {
        if ( bson_append_estart( b, BSON_BINDATA, name, 4+1+len ) == BSON_ERROR )
            return BSON_ERROR;
        bson_append32( b, &len );
        bson_append_byte( b, type );
        bson_append( b, str, len );
    }
    return BSON_OK;
}

CB_EXPORT int bson_append_oid( bson *b, const char *name, const bson_oid_t *oid ) {
    if ( bson_append_estart( b, BSON_OID, name, 12 ) == BSON_ERROR )
        return BSON_ERROR;
    bson_append( b , oid , 12 );
    return BSON_OK;
}

CB_EXPORT int bson_append_new_oid( bson *b, const char *name ) {
    bson_oid_t oid;
    bson_oid_gen( &oid );
    return bson_append_oid( b, name, &oid );
}

CB_EXPORT int bson_append_regex( bson *b, const char *name, const char *pattern, const char *opts ) {
    const int plen = static_cast<int>(strlen( pattern )+1);
    const int olen = static_cast<int>(strlen( opts )+1);
    if ( bson_append_estart( b, BSON_REGEX, name, plen + olen ) == BSON_ERROR )
        return BSON_ERROR;
    if ( bson_check_string( b, pattern, plen - 1 ) == BSON_ERROR )
        return BSON_ERROR;
    bson_append( b , pattern , plen );
    bson_append( b , opts , olen );
    return BSON_OK;
}

CB_EXPORT int bson_append_bson( bson *b, const char *name, const bson *bson ) {
    if ( !bson ) return BSON_ERROR;
    if ( bson_append_estart( b, BSON_OBJECT, name, bson_size( bson ) ) == BSON_ERROR )
        return BSON_ERROR;
    bson_append( b , bson->data , bson_size( bson ) );
    return BSON_OK;
}

CB_EXPORT int bson_append_array( bson *b, const char *name, const bson *bson ){
    if ( !bson ) return BSON_ERROR;
    if ( bson_append_estart( b, BSON_ARRAY, name, bson_size( bson ) ) == BSON_ERROR )
        return BSON_ERROR;
    bson_append( b , bson->data , bson_size( bson ) );
    return BSON_OK;
}

CB_EXPORT int bson_append_element( bson *b, const char *name_or_null, const bson_iterator *elem ) {
    bson_iterator next = *elem;
    int size;

    bson_iterator_next( &next );
    size = static_cast<int>(next.cur - elem->cur);

    if ( name_or_null == NULL ) {
        if( bson_ensure_space( b, size ) == BSON_ERROR )
            return BSON_ERROR;
        bson_append( b, elem->cur, size );
    } else {
        int data_size = static_cast<int>(size - 2 - strlen( bson_iterator_key( elem ) ));
        bson_append_estart( b, elem->cur[0], name_or_null, data_size );
        bson_append( b, bson_iterator_value( elem ), data_size );
    }

    return BSON_OK;
}

CB_EXPORT int bson_append_elements( bson *dst, const bson *src ) {
   bson_iterator iter ;

   if ( !dst || !src ) return BSON_ERROR;
   if ( !src->finished ) return BSON_ERROR;

   bson_iterator_init( &iter, src );
   while ( bson_iterator_more( &iter ) ) {
      bson_iterator_next( &iter );
      if ( bson_append_element( dst, NULL, &iter ) == BSON_ERROR ) return BSON_ERROR;
   }

   return BSON_OK;
}

CB_EXPORT int bson_append_timestamp( bson *b, const char *name, bson_timestamp_t *ts ) {
    if ( bson_append_estart( b, BSON_TIMESTAMP, name, 8 ) == BSON_ERROR ) return BSON_ERROR;

    bson_append32( b , &( ts->i ) );
    bson_append32( b , &( ts->t ) );

    return BSON_OK;
}

CB_EXPORT int bson_append_timestamp2( bson *b, const char *name, int time, int increment ) {
    if ( bson_append_estart( b, BSON_TIMESTAMP, name, 8 ) == BSON_ERROR ) return BSON_ERROR;

    bson_append32( b , &increment );
    bson_append32( b , &time );
    return BSON_OK;
}

CB_EXPORT int bson_append_date( bson *b, const char *name, bson_date_t millis ) {
    if ( bson_append_estart( b, BSON_DATE, name, 8 ) == BSON_ERROR ) return BSON_ERROR;
    bson_append64( b , &millis );
    return BSON_OK;
}

CB_EXPORT int bson_append_time_t( bson *b, const char *name, time_t secs ) {
    return bson_append_date( b, name, ( bson_date_t )secs * 1000 );
}

CB_EXPORT int bson_append_start_object( bson *b, const char *name ) {
    if ( bson_append_estart( b, BSON_OBJECT, name, 5 ) == BSON_ERROR ) return BSON_ERROR;
    if ( b->stackPos >= BSON_MAX_STACK_SIZE-1 ) return BSON_ERROR ;
    b->stack[ b->stackPos ] = static_cast<int>(b->cur - b->data);
    b->stackType[ b->stackPos ] = (char)BSON_OBJECT ;
    b->stackPos++ ;
    bson_append32( b , &zero );
    return BSON_OK;
}

CB_EXPORT int bson_append_start_array( bson *b, const char *name ) {
    if ( bson_append_estart( b, BSON_ARRAY, name, 5 ) == BSON_ERROR ) return BSON_ERROR;
    if ( b->stackPos >= BSON_MAX_STACK_SIZE-1 ) return BSON_ERROR ;
    b->stack[ b->stackPos ] = static_cast<int>(b->cur - b->data);
    b->stackType[ b->stackPos ] = (char)BSON_ARRAY ;
    b->stackPos++ ;
    bson_append32( b , &zero );
    return BSON_OK;
}

CB_EXPORT int bson_append_finish_object( bson *b ) {
    char *start;
    int i;
    if ( bson_ensure_space( b, 1 ) == BSON_ERROR ) return BSON_ERROR;
    bson_append_byte( b , 0 );

    --b->stackPos ;
    b->stackType[ b->stackPos ] = (char)(-1) ;
    start = b->data + b->stack[ b->stackPos ];
    i = static_cast<int>(b->cur - start);
    bson_little_endian32( start, &i );

    return BSON_OK;
}

CB_EXPORT double bson_int64_to_double( int64_t i64 ) {
  return (double)i64;
}

CB_EXPORT int bson_append_finish_array( bson *b ) {
    return bson_append_finish_object( b );
}

/* Error handling and allocators. */

static bson_err_handler err_handler = NULL;

CB_EXPORT bson_err_handler set_bson_err_handler( bson_err_handler func ) {
    bson_err_handler old = err_handler;
    err_handler = func;
    return old;
}

CB_EXPORT void bson_free( void *ptr ) {
     if ( ptr )
       bson_free_func( ptr );
}

CB_EXPORT void *bson_malloc( int size ) {
    void *p;
    p = bson_malloc_func( size );
    bson_fatal_msg( !!p, "malloc() failed" );
    return p;
}

void *bson_realloc( void *ptr, int size ) {
    void *p;
    p = bson_realloc_func( ptr, size );
    bson_fatal_msg( !!p, "realloc() failed" );
    return p;
}

int _bson_errprintf( const char *format, ... ) {
    va_list ap;
    int ret;
    va_start( ap, format );
#ifndef R_SAFETY_NET
    ret = vfprintf( stderr, format, ap );
#endif
    va_end( ap );

    return ret;
}

/**
 * This method is invoked when a non-fatal bson error is encountered.
 * Calls the error handler if available.
 *
 *  @param
 */
void bson_builder_error( bson *b ) {
  (void)b;
  if( err_handler )
        err_handler( "BSON error." );
}

void bson_fatal( int ok ) {
    bson_fatal_msg( ok, "" );
}

void bson_fatal_msg( int ok , const char *msg ) {
    if ( ok )
        return;

    if ( err_handler ) {
        err_handler( msg );
    }
#ifndef R_SAFETY_NET
    bson_errprintf( "error: %s\n" , msg );
    exit( -5 );
#endif
}


/* all the numbers that fit in a 4 byte string */
char bson_numstrs[1000][4] = {
    "0",  "1",  "2",  "3",  "4",  "5",  "6",  "7",  "8",  "9",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
    "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
    "90", "91", "92", "93", "94", "95", "96", "97", "98", "99",

    "100", "101", "102", "103", "104", "105", "106", "107", "108", "109",
    "110", "111", "112", "113", "114", "115", "116", "117", "118", "119",
    "120", "121", "122", "123", "124", "125", "126", "127", "128", "129",
    "130", "131", "132", "133", "134", "135", "136", "137", "138", "139",
    "140", "141", "142", "143", "144", "145", "146", "147", "148", "149",
    "150", "151", "152", "153", "154", "155", "156", "157", "158", "159",
    "160", "161", "162", "163", "164", "165", "166", "167", "168", "169",
    "170", "171", "172", "173", "174", "175", "176", "177", "178", "179",
    "180", "181", "182", "183", "184", "185", "186", "187", "188", "189",
    "190", "191", "192", "193", "194", "195", "196", "197", "198", "199",

    "200", "201", "202", "203", "204", "205", "206", "207", "208", "209",
    "210", "211", "212", "213", "214", "215", "216", "217", "218", "219",
    "220", "221", "222", "223", "224", "225", "226", "227", "228", "229",
    "230", "231", "232", "233", "234", "235", "236", "237", "238", "239",
    "240", "241", "242", "243", "244", "245", "246", "247", "248", "249",
    "250", "251", "252", "253", "254", "255", "256", "257", "258", "259",
    "260", "261", "262", "263", "264", "265", "266", "267", "268", "269",
    "270", "271", "272", "273", "274", "275", "276", "277", "278", "279",
    "280", "281", "282", "283", "284", "285", "286", "287", "288", "289",
    "290", "291", "292", "293", "294", "295", "296", "297", "298", "299",

    "300", "301", "302", "303", "304", "305", "306", "307", "308", "309",
    "310", "311", "312", "313", "314", "315", "316", "317", "318", "319",
    "320", "321", "322", "323", "324", "325", "326", "327", "328", "329",
    "330", "331", "332", "333", "334", "335", "336", "337", "338", "339",
    "340", "341", "342", "343", "344", "345", "346", "347", "348", "349",
    "350", "351", "352", "353", "354", "355", "356", "357", "358", "359",
    "360", "361", "362", "363", "364", "365", "366", "367", "368", "369",
    "370", "371", "372", "373", "374", "375", "376", "377", "378", "379",
    "380", "381", "382", "383", "384", "385", "386", "387", "388", "389",
    "390", "391", "392", "393", "394", "395", "396", "397", "398", "399",

    "400", "401", "402", "403", "404", "405", "406", "407", "408", "409",
    "410", "411", "412", "413", "414", "415", "416", "417", "418", "419",
    "420", "421", "422", "423", "424", "425", "426", "427", "428", "429",
    "430", "431", "432", "433", "434", "435", "436", "437", "438", "439",
    "440", "441", "442", "443", "444", "445", "446", "447", "448", "449",
    "450", "451", "452", "453", "454", "455", "456", "457", "458", "459",
    "460", "461", "462", "463", "464", "465", "466", "467", "468", "469",
    "470", "471", "472", "473", "474", "475", "476", "477", "478", "479",
    "480", "481", "482", "483", "484", "485", "486", "487", "488", "489",
    "490", "491", "492", "493", "494", "495", "496", "497", "498", "499",

    "500", "501", "502", "503", "504", "505", "506", "507", "508", "509",
    "510", "511", "512", "513", "514", "515", "516", "517", "518", "519",
    "520", "521", "522", "523", "524", "525", "526", "527", "528", "529",
    "530", "531", "532", "533", "534", "535", "536", "537", "538", "539",
    "540", "541", "542", "543", "544", "545", "546", "547", "548", "549",
    "550", "551", "552", "553", "554", "555", "556", "557", "558", "559",
    "560", "561", "562", "563", "564", "565", "566", "567", "568", "569",
    "570", "571", "572", "573", "574", "575", "576", "577", "578", "579",
    "580", "581", "582", "583", "584", "585", "586", "587", "588", "589",
    "590", "591", "592", "593", "594", "595", "596", "597", "598", "599",

    "600", "601", "602", "603", "604", "605", "606", "607", "608", "609",
    "610", "611", "612", "613", "614", "615", "616", "617", "618", "619",
    "620", "621", "622", "623", "624", "625", "626", "627", "628", "629",
    "630", "631", "632", "633", "634", "635", "636", "637", "638", "639",
    "640", "641", "642", "643", "644", "645", "646", "647", "648", "649",
    "650", "651", "652", "653", "654", "655", "656", "657", "658", "659",
    "660", "661", "662", "663", "664", "665", "666", "667", "668", "669",
    "670", "671", "672", "673", "674", "675", "676", "677", "678", "679",
    "680", "681", "682", "683", "684", "685", "686", "687", "688", "689",
    "690", "691", "692", "693", "694", "695", "696", "697", "698", "699",

    "700", "701", "702", "703", "704", "705", "706", "707", "708", "709",
    "710", "711", "712", "713", "714", "715", "716", "717", "718", "719",
    "720", "721", "722", "723", "724", "725", "726", "727", "728", "729",
    "730", "731", "732", "733", "734", "735", "736", "737", "738", "739",
    "740", "741", "742", "743", "744", "745", "746", "747", "748", "749",
    "750", "751", "752", "753", "754", "755", "756", "757", "758", "759",
    "760", "761", "762", "763", "764", "765", "766", "767", "768", "769",
    "770", "771", "772", "773", "774", "775", "776", "777", "778", "779",
    "780", "781", "782", "783", "784", "785", "786", "787", "788", "789",
    "790", "791", "792", "793", "794", "795", "796", "797", "798", "799",

    "800", "801", "802", "803", "804", "805", "806", "807", "808", "809",
    "810", "811", "812", "813", "814", "815", "816", "817", "818", "819",
    "820", "821", "822", "823", "824", "825", "826", "827", "828", "829",
    "830", "831", "832", "833", "834", "835", "836", "837", "838", "839",
    "840", "841", "842", "843", "844", "845", "846", "847", "848", "849",
    "850", "851", "852", "853", "854", "855", "856", "857", "858", "859",
    "860", "861", "862", "863", "864", "865", "866", "867", "868", "869",
    "870", "871", "872", "873", "874", "875", "876", "877", "878", "879",
    "880", "881", "882", "883", "884", "885", "886", "887", "888", "889",
    "890", "891", "892", "893", "894", "895", "896", "897", "898", "899",

    "900", "901", "902", "903", "904", "905", "906", "907", "908", "909",
    "910", "911", "912", "913", "914", "915", "916", "917", "918", "919",
    "920", "921", "922", "923", "924", "925", "926", "927", "928", "929",
    "930", "931", "932", "933", "934", "935", "936", "937", "938", "939",
    "940", "941", "942", "943", "944", "945", "946", "947", "948", "949",
    "950", "951", "952", "953", "954", "955", "956", "957", "958", "959",
    "960", "961", "962", "963", "964", "965", "966", "967", "968", "969",
    "970", "971", "972", "973", "974", "975", "976", "977", "978", "979",
    "980", "981", "982", "983", "984", "985", "986", "987", "988", "989",
    "990", "991", "992", "993", "994", "995", "996", "997", "998", "999",
};

void bson_numstr( char *str, int i ) {
    if( i < 1000 )
        memcpy( str, bson_numstrs[i], 4 );
    else
        bson_sprintf( str,"%d", i );
}

CB_EXPORT void bson_swap_endian64( void *outp, const void *inp ) {
    const char *in = ( const char * )inp;
    char *out = ( char * )outp;

    out[0] = in[7];
    out[1] = in[6];
    out[2] = in[5];
    out[3] = in[4];
    out[4] = in[3];
    out[5] = in[2];
    out[6] = in[1];
    out[7] = in[0];

}

CB_EXPORT void bson_swap_endian32( void *outp, const void *inp ) {
    const char *in = ( const char * )inp;
    char *out = ( char * )outp;

    out[0] = in[3];
    out[1] = in[2];
    out[2] = in[1];
    out[3] = in[0];
}

CB_EXPORT void bson_swap_endian16( void *outp, const void *inp ) {
    const char *in = ( const char * )inp;
    char *out = ( char * )outp;

    out[0] = in[1];
    out[1] = in[0];
}

CB_EXPORT bson_bool_t bson_is_inf( double d, int *pSign )
{
    volatile double tmp = d ;
    if( ( tmp == d ) && ( ( tmp - d ) != 0.0 ) )
    {
        if( pSign )
        {
            *pSign = ( d < 0.0 ? -1 : 1 ) ;
        }
        return 1 ;
    }
    if( pSign )
    {
        *pSign = 0 ;
    }
    return 0 ;
}

CB_EXPORT void bson_set_js_compatibility(int compatible)
{
    jsCompatibility = compatible;
}

CB_EXPORT int bson_get_js_compatibility()
{
    return jsCompatibility;
}


void LocalTime ( time_t *Time, struct tm *TM )
{
   if ( !Time || !TM )
      return ;
#if defined (__linux__ ) || defined (_AIX)
   localtime_r( Time, TM ) ;
#elif defined (_WIN32)
   localtime_s( TM, Time ) ;
#endif
}


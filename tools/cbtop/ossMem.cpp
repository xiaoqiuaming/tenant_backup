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

   Source File Name = ossMem.c

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          12/1/2014  ly  Initial Draft

   Last Changed =

*******************************************************************************/

#include "core.h"
#include "ossMem.h"
#include "ossUtil.h"
#include <stdlib.h>

#define CB__OSSMEMALLOC
#define CB__OSSMEMREALLOC
#define CB__OSSMEMFREE
#define PD_TRACE_ENTRY(x)
#define PD_TRACE_EXIT(x)


int32_t ossMemDebugEnabled = 0 ;
uint32_t ossMemDebugSize = 0 ;

#define OSS_MEM_HEADSZ 32

#define OSS_MEM_HEAD_EYECATCHER1SIZE  sizeof(uint32_t)
#define OSS_MEM_HEAD_FREEDSIZE sizeof(uint32_t)
#define OSS_MEM_HEAD_SIZESIZE    sizeof(uint64_t)
#define OSS_MEM_HEAD_DEBUGSIZE   sizeof(uint32_t)
#define OSS_MEM_HEAD_FILESIZE    sizeof(uint32_t)
#define OSS_MEM_HEAD_LINESIZE    sizeof(uint32_t)
#define OSS_MEM_HEAD_KEYECATCHER2SIZE sizeof(uint32_t)

#define OSS_MEM_HEAD_EYECATCHER1OFFSET 0
#define OSS_MEM_HEAD_FREEDOFFSET \
  OSS_MEM_HEAD_EYECATCHER1OFFSET + OSS_MEM_HEAD_EYECATCHER1SIZE
#define OSS_MEM_HEAD_SIZEOFFSET \
  OSS_MEM_HEAD_FREEDOFFSET + OSS_MEM_HEAD_FREEDSIZE
#define OSS_MEM_HEAD_DEBUGOFFSET   \
  OSS_MEM_HEAD_SIZEOFFSET + OSS_MEM_HEAD_SIZESIZE
#define OSS_MEM_HEAD_FILEOFFSET    \
  OSS_MEM_HEAD_DEBUGOFFSET + OSS_MEM_HEAD_DEBUGSIZE
#define OSS_MEM_HEAD_LINEOFFSET    \
  OSS_MEM_HEAD_FILEOFFSET + OSS_MEM_HEAD_FILESIZE
#define OSS_MEM_HEAD_EYECATCHER2OFFSET \
  OSS_MEM_HEAD_LINEOFFSET + OSS_MEM_HEAD_LINESIZE

#define CB_MEMDEBUG_ENDPOS     sizeof(char)

#if defined (_LINUX) || defined (_AIX)
#define OSS_MEM_MAX_SZ 4294967295ll
#elif defined (_WINDOWS)
#define OSS_MEM_MAX_SZ 4294967295LL
#endif

#if !defined (__cplusplus) || !defined (CB_ENGINE)
void ossMemTrack ( void *p ) {
  (void)p;
}
void ossMemUnTrack ( void *p ) {
  (void)p;
}
void ossMemTrace ( const char *pPath ) {
  (void)pPath;
}
#endif

#define OSS_MIN(a, b) (((a) < (b)) ? (a) : (b))
#define OSS_MAX(a, b) (((a) > (b)) ? (a) : (b))

void ossPanic()
{
   int32_t *p = NULL ;
   *p = 10 ;
}

static int32_t ossMemSanityCheck ( void *p )
{
  char *headerMem = NULL ;
  if ( !p )
    return 1 ;
  headerMem = ((char*)p) - OSS_MEM_HEADSZ ;
  return ( *(uint32_t*)(headerMem+OSS_MEM_HEAD_EYECATCHER1OFFSET) ==
           CB_MEMHEAD_EYECATCHER1 ) &&
      ( *(uint32_t*)(headerMem+OSS_MEM_HEAD_EYECATCHER2OFFSET) ==
        CB_MEMHEAD_EYECATCHER2 ) ;
}
static int32_t ossMemVerify ( void *p )
{
  char *headerMem  = NULL ;
  uint32_t debugSize = 0 ;
  char *pStart     = NULL ;
  uint32_t i         = 0 ;
  uint64_t size      = 0 ;
  char *pEnd       = NULL ;
  if ( !p )
    return 1 ;
  headerMem = ((char*)p) - OSS_MEM_HEADSZ ;
  if ( *(uint32_t*)(headerMem+OSS_MEM_HEAD_EYECATCHER1OFFSET) !=
       CB_MEMHEAD_EYECATCHER1 )
  {
    return 0 ;
  }
  if ( *(uint32_t*)(headerMem+OSS_MEM_HEAD_EYECATCHER2OFFSET) !=
       CB_MEMHEAD_EYECATCHER2 )
  {
    return 0 ;
  }
  if ( *(uint32_t*)(headerMem+OSS_MEM_HEAD_FREEDOFFSET) != 0 )
  {
    return 0 ;
  }
  debugSize = *(uint32_t*)(headerMem+OSS_MEM_HEAD_DEBUGOFFSET) ;
  if ( 0 == debugSize )
    return 1 ;
  if ( debugSize > CB_MEMDEBUG_MAXGUARDSIZE ||
       debugSize < CB_MEMDEBUG_MINGUARDSIZE ||
       debugSize > ossMemDebugSize )
  {
    return 0 ;
  }
  pStart = headerMem - debugSize ;
  for ( i = 0; i < debugSize; ++i )
  {
    if ( *(pStart + i) != CB_MEMDEBUG_GUARDSTART )
    {
      return 0 ;
    }
  }
  size = *(uint64_t*)(headerMem+OSS_MEM_HEAD_SIZEOFFSET) ;
  pEnd = ((char*)p)+size ;
  if ( *pEnd != CB_MEMDEBUG_GUARDEND )
  {
    return 0 ;
  }
  for ( i = CB_MEMDEBUG_ENDPOS; i < debugSize; ++i )
  {
    if ( *(pEnd+i) != CB_MEMDEBUG_GUARDSTOP )
    {
      return 0 ;
    }
  }
  return 1 ;
}

static void ossMemFixHead ( char *p,
                            size_t datasize, uint32_t debugSize,
                            const char *file, uint32_t line )
{
  *(uint32_t*)(p+OSS_MEM_HEAD_EYECATCHER1OFFSET) = CB_MEMHEAD_EYECATCHER1 ;
  *(uint64_t*)(p+OSS_MEM_HEAD_SIZEOFFSET)    = datasize ;
  *(uint32_t*)(p+OSS_MEM_HEAD_DEBUGOFFSET)   = debugSize ;
  *(uint32_t*)(p+OSS_MEM_HEAD_FILEOFFSET)    = ossHashFileName ( file ) ;
  *(uint32_t*)(p+OSS_MEM_HEAD_LINEOFFSET)    = line ;
  *(uint32_t*)(p+OSS_MEM_HEAD_EYECATCHER2OFFSET) = CB_MEMHEAD_EYECATCHER2 ;
  *(uint32_t*)(p+OSS_MEM_HEAD_FREEDOFFSET)   = 0 ;
  if ( ossMemDebugEnabled )
    ossMemTrack ( p ) ;
}

static void *ossMemAlloc1 ( size_t size, const char* file, uint32_t line )
{
  char *p = NULL ;
  uint64_t totalSize = size + OSS_MEM_HEADSZ ;
  p = (char*)malloc ( totalSize ) ;
  if ( !p )
    return NULL ;
  ossMemFixHead ( p, size, 0, file, line ) ;
  return ((char*)p)+OSS_MEM_HEADSZ ;
}

static void *ossMemAlloc2 ( size_t size, const char* file, uint32_t line )
{
  char *p          = NULL ;
  char *expMem     = NULL ;
  uint32_t debugSize = ossMemDebugSize ;
  uint32_t endSize   = 0 ;
  uint64_t totalSize = 0 ;
  debugSize = OSS_MIN ( debugSize, CB_MEMDEBUG_MAXGUARDSIZE ) ;
  debugSize = OSS_MAX ( debugSize, CB_MEMDEBUG_MINGUARDSIZE ) ;
  endSize = debugSize - static_cast<uint32_t>(CB_MEMDEBUG_ENDPOS);
  totalSize = size + OSS_MEM_HEADSZ + ( debugSize<<1 ) ;

  p = (char*)malloc ( totalSize ) ;
  if ( !p )
    return NULL ;
  ossMemFixHead ( p + debugSize, size, debugSize, file, line ) ;
  expMem = p + debugSize + OSS_MEM_HEADSZ ;
  ossMemset ( p, CB_MEMDEBUG_GUARDSTART, debugSize ) ;

  *(expMem + size) = CB_MEMDEBUG_GUARDEND ;
  ossMemset ( expMem+size+CB_MEMDEBUG_ENDPOS,
              CB_MEMDEBUG_GUARDSTOP,
              endSize ) ;
  ossMemset ( expMem, 0, size ) ;
  return expMem ;
}

void ossEnableMemDebug( int32_t debugEnable, uint32_t memDebugSize )
{
  ossMemDebugEnabled   = debugEnable ;
  ossMemDebugSize      = memDebugSize ;
}

// PD_TRACE_DECLARE_FUNCTION ( CB__OSSMEMALLOC, "ossMemAlloc" )
void* ossMemAlloc ( size_t size, const char* file, uint32_t line )
{
  void *p = NULL ;
  if ( size == 0 )
    p = NULL ;
  else if ( !ossMemDebugEnabled || !ossMemDebugSize )
    p = ossMemAlloc1 ( size, file, line ) ;
  else
    p = ossMemAlloc2 ( size, file, line ) ;
  return p ;
}

static void *ossMemRealloc2 ( void* pOld, size_t size,
                              const char* file, uint32_t line )
{
  char *p          = NULL ;
  char *expMem     = NULL ;
  char *headerMem  = NULL ;
  uint64_t oldSize   = 0 ;
  uint32_t debugSize = ossMemDebugSize ;
  uint32_t endSize   = 0 ;
  uint64_t totalSize = 0 ;
  uint64_t diffSize = 0 ;
  debugSize = OSS_MIN ( debugSize, CB_MEMDEBUG_MAXGUARDSIZE ) ;
  debugSize = OSS_MAX ( debugSize, CB_MEMDEBUG_MINGUARDSIZE ) ;

  if ( pOld )
  {
    int32_t checkHead = ossMemVerify ( pOld ) ;
    if ( !checkHead )
    {
      ossPanic () ;
    }
    headerMem = ((char*)pOld) - OSS_MEM_HEADSZ ;
    debugSize = *(uint32_t*)(headerMem+OSS_MEM_HEAD_DEBUGOFFSET) ;
    oldSize = *(uint64_t*)(headerMem+OSS_MEM_HEAD_SIZEOFFSET) ;
    *(uint32_t*)(headerMem+OSS_MEM_HEAD_FREEDOFFSET) = 1 ;
    p = headerMem - debugSize ;
  }
  if ( size > oldSize )
    diffSize = size - oldSize ;
  endSize = debugSize - static_cast<uint32_t>(CB_MEMDEBUG_ENDPOS);
  totalSize = size + OSS_MEM_HEADSZ + ( debugSize<<1 ) ;
  p = (char*)realloc ( p, totalSize ) ;
  if ( !p )
    return NULL ;

  ossMemFixHead ( p + debugSize, size, debugSize, file, line ) ;
  expMem = p + OSS_MEM_HEADSZ + debugSize ;
  ossMemset ( p, CB_MEMDEBUG_GUARDSTART, debugSize ) ;

  *(expMem + size) = CB_MEMDEBUG_GUARDEND ;
  ossMemset ( expMem+size+CB_MEMDEBUG_ENDPOS,
              CB_MEMDEBUG_GUARDSTOP,
              endSize ) ;
  if ( diffSize )
    ossMemset ( expMem + oldSize, 0, diffSize ) ;
  return expMem ;
}

static void *ossMemRealloc1 ( void* pOld, size_t size,
                              const char* file, uint32_t line )
{
  char *p = NULL ;
  uint64_t totalSize = size + OSS_MEM_HEADSZ ;
  if ( pOld )
  {
    int32_t checkHead = ossMemSanityCheck ( pOld ) ;
    if ( !checkHead )
    {
      ossPanic () ;
    }
    p = ((char*)pOld - OSS_MEM_HEADSZ ) ;
    if ( *(uint32_t*)( p + OSS_MEM_HEAD_DEBUGOFFSET ) != 0 )
      return ossMemRealloc2 ( pOld, size, file, line ) ;
    *(uint32_t*)(p+OSS_MEM_HEAD_FREEDOFFSET) = 1 ;
  }
  p = (char*)realloc ( p, totalSize ) ;
  if ( !p )
    return NULL ;
  ossMemFixHead ( p, size, 0, file, line ) ;
  return ((char*)p)+OSS_MEM_HEADSZ ;
}

// PD_TRACE_DECLARE_FUNCTION ( CB__OSSMEMREALLOC, "ossMemRealloc" )
void* ossMemRealloc ( void* pOld, size_t size,
                      const char* file, uint32_t line )
{
  void *p = NULL ;
  if ( size == 0 )
    p = NULL ;
  else if ( !ossMemDebugEnabled || !ossMemDebugSize )
    p = ossMemRealloc1 ( pOld, size, file, line ) ;
  else
    p = ossMemRealloc2 ( pOld, size, file, line ) ;
  return p ;
}

void ossMemFree2 ( void *p )
{
  if ( p )
  {
    char *headerMem   = NULL ;
    uint32_t debugSize  = 0 ;
    char *pStart      = NULL ;
    int32_t checkHead = ossMemVerify ( p ) ;
    if ( !checkHead )
    {
      ossPanic () ;
    }
    headerMem = ((char*)p) - OSS_MEM_HEADSZ ;
    debugSize = *(uint32_t*)(headerMem+OSS_MEM_HEAD_DEBUGOFFSET) ;
    *(uint32_t*)(headerMem+OSS_MEM_HEAD_FREEDOFFSET)   = 1 ;
    pStart = headerMem - debugSize ;
    free ( pStart ) ;
    if ( ossMemDebugEnabled )
      ossMemUnTrack ( pStart ) ;
  }
}

void ossMemFree1 ( void *p )
{
  if ( p )
  {
    char *pStart = NULL ;
    int32_t checkHead = ossMemSanityCheck ( p ) ;
    if ( !checkHead )
    {
      ossPanic () ;
    }
    pStart = ((char*)p) - OSS_MEM_HEADSZ ;
    if ( *(uint32_t*)(pStart + OSS_MEM_HEAD_DEBUGOFFSET ) != 0 )
      ossMemFree2 ( p ) ;
    else
    {
      *(uint32_t*)((char*)pStart+OSS_MEM_HEAD_FREEDOFFSET)   = 1 ;
      free ( pStart ) ;
      if ( ossMemDebugEnabled )
        ossMemUnTrack ( pStart ) ;
    }
  }
}

// PD_TRACE_DECLARE_FUNCTION ( CB__OSSMEMFREE, "ossMemFree" )
void ossMemFree ( void *p )
{
  if ( !ossMemDebugEnabled || !ossMemDebugSize )
    ossMemFree1 ( p ) ;
  else
    ossMemFree2 ( p ) ;
}

void *ossAlignedAlloc( uint32_t alignment, uint32_t size )
{
#if defined (_LINUX) || defined (_AIX)
  void *ptr = NULL ;
  INT32 rc = CB_OK ;
  rc = posix_memalign( &ptr, alignment, size ) ;
  if ( CB_OK != rc )
  {
    goto error ;
  }
done:
  return ptr ;
error:
  if ( NULL != ptr )
  {
    CB_OSS_ORIGINAL_FREE( ptr ) ;
    ptr = NULL ;
  }
  goto done ;
#elif defined (_WINDOWS)
  return _aligned_malloc( size, alignment ) ;
#else
  (void)alignment;
  (void)size;
  return NULL ;
#endif
}


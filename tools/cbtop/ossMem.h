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

   Source File Name = ossMem.h

   Descriptive Name = Operating System Services Memory Header

   When/how to use: this program may be used on binary and text-formatted
   versions of OSS component. This file contains declares for all memory
   allocation/free operations.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  TW  Initial Draft

   Last Changed =

*******************************************************************************/
#ifndef OSSMEM_H_
#define OSSMEM_H_
#include "core.h"
#include <new>
#include <stdint.h>
#include <cstring>

/*
 * [x bytes start][8 bytes data size][4 bytes guard size][4 bytes file hash][4
 * bytes line num][data][1 byte end][x-1 bytes stop]*/
#define CB_MEMDEBUG_MINGUARDSIZE 256
#define CB_MEMDEBUG_MAXGUARDSIZE 4194304
#define CB_MEMDEBUG_GUARDSTART ((char)0xBE)
#define CB_MEMDEBUG_GUARDSTOP  ((char)0xBF)
#define CB_MEMDEBUG_GUARDEND   ((char)0xBD)
#define CB_MEMHEAD_EYECATCHER1 0xFABD0538
#define CB_MEMHEAD_EYECATCHER2 0xFACE7352

#define CB_OSS_MALLOC(x)       ossMemAlloc(x,__FILE__,__LINE__)
#define CB_OSS_FREE(x)         ossMemFree(x)
#define CB_OSS_ORIGINAL_FREE(x) free(x)
#define CB_OSS_REALLOC(x,y)    ossMemRealloc(x,y,__FILE__,__LINE__)

#define CB_OSS_MALLOC3(x,y,z)  ossMemAlloc(x,y,z)

#define CB_OSS_MEMDUMPNAME      "memdump.info"
#define CB_OSS_NEW              new//(__FILE__,__LINE__,std::nothrow)
#define CB_OSS_DEL              delete

#define SAFE_OSS_FREE(p)      \
   do {                       \
      if (p) {                \
         CB_OSS_FREE(p) ;    \
         p = NULL ;           \
      }                       \
   } while (0)

CB_EXTERN_C_START

void  ossEnableMemDebug( int32_t debugEnable, uint32_t memDebugSize ) ;

void* ossMemAlloc ( size_t size, const char* file, uint32_t line ) ;

void* ossMemRealloc ( void* pOld, size_t size,
                      const char* file, uint32_t line ) ;

void ossMemFree ( void *p ) ;

void ossMemTrack ( void *p ) ;

void ossMemUnTrack ( void *p ) ;

void ossMemTrace ( const char *pPath ) ;

void *ossAlignedAlloc( uint32_t alignment, uint32_t size ) ;

CB_EXTERN_C_END
#endif

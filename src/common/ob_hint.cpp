#include "ob_hint.h"

using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151224:b
    const char* get_read_atomic_level_str(ObReadAtomicLevel level)
    {
      return (level == READ_ATOMIC_WEAK ? "READ_ATOMIC_WEAK" : (level == READ_ATOMIC_STRONG ? "READ_ATOMIC_STRONG" : "NO_READ_ATOMIC_LEVEL"));
    }
    //add duyr 20151224:e
    const char* get_consistency_level_str(ObConsistencyLevel level)
    {
      return (level == WEAK ? "WEAK" : (level == STRONG ? "STRONG": (level == STATIC ? "STATIC": ((level == FROZEN) ? "FROZEN" : "NO_CONSISTENCY" ))));
    }
  }
}

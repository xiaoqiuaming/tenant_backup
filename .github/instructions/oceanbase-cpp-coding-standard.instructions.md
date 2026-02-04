---
description: 'YaoBase C/C++ coding standards for kernel development with strict safety rules, naming conventions, and formatting guidelines'
applyTo: '**/*.cpp, **/*.h, **/*.c, **/*.hpp, **/*.ipp, **/*.cc'
---

# YaoBase C/C++ Coding Standard

Coding standards for YaoBase database kernel development. Emphasizes safety, clarity, and maintainability.

## Core Principles

- Write clear, understandable code using common patterns
- Avoid obscure syntax and clever tricks
- Single entry, single exit for all functions
- Check all parameters and return values
- Manual resource management with clear ownership

## File Organization

- Files: `lowercase_with_underscores.cpp/h`, templates: `.ipp`
- Header guard: `#ifndef YAOBASE_MODULE_FILENAME_`
- Include order: own header → system C → system C++ → third-party → project
- Prefer forward declarations in headers: `class ObFoo;`

## Naming Conventions

- **Classes**: `ObClassName` (prefix `Ob`), interfaces: `ObIInterface`, nested: no prefix
- **Members**: `member_var_` (trailing `_`), constants: `UPPERCASE`
- **Locals**: `lowercase_with_underscores`
- **Globals**: `g_name` (avoid new ones)
- **Functions**: `verb_noun()`, getters: `get_xxx()`, setters: `set_xxx()`

## Namespace Rules

```cpp
// Namespace matches directory: src/common → yaobase::common
namespace yaobase {
namespace common {  // No indentation

class ObFoo {
  int func();
};

}  // namespace common
}  // namespace yaobase
```

**Rules:**
- ❌ Anonymous namespaces (breaks debugging)
- ❌ `using` directives in headers
- ✅ `using` declarations in headers: `using yaobase::common::ObFoo;`
- ✅ `using` directives in .cpp: `using namespace common;`

## Variables & Resources

- **Locals**: Declare at block start, initialize when declared
- ❌ Complex objects in loops (use `reuse()` instead)
- **Static**: ❌ In headers (except class `static const` integral/`constexpr`)
- **Global**: ❌ New globals (use singletons, constants in `ob_define.h`)
- **Resources**: "Who allocates, releases" - cleanup at block end, set pointer to `nullptr` after freeool is_inited() const { return is_inited_; }
  
private:
  void *ptr_;
  int64_t count_;
  bool is_inited_;
  
  DISALLOW_COPY_AND_ASSIGN(ObFoo);  // No copy constructor
};
```

### Explicit Constructors
```cpp
class ObFoo {
public:
  explicit ObFoo(int value);  // Prevent implicit conversion
};
```

### Common Functions
```cpp
class ObFoo {
public:
  int init();                 // Initialize object
  void destroy();             // Cleanup resources
  void reset();               // Reset to initial state
  void reuse();               // Reuse (may keep memory)
  bool is_valid() const;      // Validation
  int deep_copy(const ObFoo &src);
  int shallow_copy(const ObFoo &src);
  int64_t to_string(char *buf, const int64_t len) const;
  
  NEED_SERIALIZE_AND_DESERIALIZE;  // Serialization macro
};
```

### Member Initialization
```cpp
class ObFoo {
public:
  ObFoo() 
    : member1_(0),      // Order matches declaration
      member2_(nullptr),
      member3_(false) { }
      
- **Constructor**: Trivial init only, use `init()` for complex setup
- **Always define**: Constructor, virtual destructor, `DISALLOW_COPY_AND_ASSIGN`
- **Single-arg constructors**: Use `explicit`
- **Init order**: Match declaration order
- **Common functions**: `init()`, `destroy()`, `reset()`, `reuse()`, `is_valid()`, `to_string()`
- **Inheritance**: Public only, max one implementation base + interface bases
- **Order**: public → private; typedefs → constructors → methods → members

// Multi-line step: use block
if (OB_FAIL(step1())) {
  // error
} else {
  if (OB_FAIL(step2_part1())) {
    // error
  } else if (OB_FAIL(step2_part2())) {
    // error
  }
}
```

### Loops
```cpp
// Include ret check in condition
for (int64_t i = 0; OB_SUCCESS == ret && i < count; ++i) {
  ret = process(i);
}

while (OB_SUCCESS == ret && has_more()) {
  ret = process_next();
}

// Empty loop: comment required
while (condition) {
  // empty
}
```

### Conditionals (MECE Principle)
```cpp
// ✅ Mutually Exclusive, Completely Exhaustive
if (cond) {
  // handle cond
} else {
  // handle !cond
}

// Exception: error-only check (else optional)
if (OB_SUCCESS != ret) {
  // error handling only
}

// ✅ Nested for two conditions
if (cond1) {
  if (cond2) {
    // cond1 && cond2
  } else {
    // cond1 && !cond2
  }
} else {
  // !cond1
}

// ❌ Prohibited: Flat compound conditions
if (cond1 && cond2) { }
else if (cond1 && !cond2) { }  // Hard to verify MECE
```

### Parameter Validation
```cpp
// ✅ Check all parameters (public and private functions)
int process(const char *data, int64_t len, ObResult &result) {
  int ret = OB_SUCCESS;
  
  // Check parameters
  if (nullptr == data || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(data), K(len));
  } else if (!result.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid result", K(ret));
  } else {
    // Business logic
  }
  
  return ret;
}

// ❌ Prohibited: assert, OB_ASSERT (use if checks)
```

### Function Limits
- Max 7 parameters
- Max 120 lines per function
- Order: input parameters first, output parameters last

## C++ Features Guide

### Prohibited Features
```cpp
// ❌ Smart pointers
std::shared_ptr<ObFoo> ptr;  // No
std::unique_ptr<ObFoo> ptr;  // No

- **Single exit**: ❌ Multiple returns/goto/exit; ✅ One return at end
- **Return `int ret`**: Always (except getters/setters, operators, `reset()`/`is_valid()`)
- **Error handling**: Use `OB_FAIL()` macro, check all return values
- **Loops**: Include `ret` check: `for (i = 0; OB_SUCCESS == ret && i < n; ++i)`
- **MECE conditionals**: Nested structure, not flat compound conditions
- **Validate all params**: Check nullptr, bounds, `is_valid()` - ❌ assert/OB_ASSERT
- **Limits**: Max 7 params, 120 lines; order: inputs first, output
}
```

### Line Length
- Max 100 characters (can extend to 120 for URLs, long includes)

### Spacing
```cpp
// Spaces around operators
a = b;
x = y + z;
result = (a > b) ? a : b;

// No space after ( or before )
func(arg1, arg2);
if (condition) { }

// Space after keywords
if (cond) { }
for (int i = 0; i < n; ++i) { }
while (cond) { }

// Pointers/references: * and & next to variable
int64_t *ptr = nullptr;
const ObString &name = get_name();
```

### Variable Declaration
```cpp
// One per line, initialize when declared
int64_t *ptr1 = nullptr;
int64_t *ptr2 = nullptr;
const int64_t MAX = 100;

// ❌ Multiple per line, uninitialized
int64_t *ptr1 = nullptr, ptr2 = nullptr;
int64_t *ptr3;
```

### Switch Statements
```cpp
switch (var) {
case TYPE_ONE: {
    // 4-space indent from case
    do_something();
    break;
  }
case TYPE_TWO: {
    do_other();
    break;
  }
default: {
    // Always include default
    handle_error();
    break;
  }
}
```

### Class Formatting
```cpp
class ObMyClass : public ObBase  // Space before/after :
{                                 // Brace on new line
public:                           // No indent for access specifiers
  ObMyClass();                    // 2-space indent
  ~ObMyClass();
  
  int func();
  
  void set_x(int v) { x_ = v; }  // One-line inline OK
  
private:
  int64_t x_;
  
  DISALLOW_COPY_AND_ASSIGN(ObMyClass);
};
```

### Initializat

### Prohibited
❌ Smart pointers, `std::string`, exceptions, RTTI, C casts, copy constructors, new macros, STL (except `<algorithm>`), `auto`, lambdas, range-for, move semantics

### Allowed
✅ `override`/`final` (required), `enum class`, `nullptr`, `constexpr`, `= default/delete`, `static_assert`

### Guidelines
- **Casts**: Use `static_cast<>`, avoid `const_cast` on input params
- **Memory**: `ob_malloc/ob_free` with module ID, nullify after free, RAII guards
- **Strings**: `ObString`, length-limited C functions (`snprintf`, not `sprintf`)
- **Integers**: `int` for ret, `int64_t` for counters, avoid unsigned
- **sizeof**: Prefer `sizeof(var)` over `sizeof(Type)WARN  | DBA | Degraded service, throttling |
| INFO  | DBA | State changes (default level) |
| EDIAG | Dev | Unexpected logic errors (bugs) |
| WDIAG | Dev | Expected errors |
| TRACE | Dev | Request-level debugging |
| DEBUG | Dev | Detailed debugging |

### Log Format
```cpp
// Use key=value format
LOG_WARN("operation failed", 
         K(ret),           // Error code
         K(table_id),      // Variables
         KP(ptr),          // Pointers
         K(count));

// Output: [timestamp] WARN file.cpp:123 operation failed(ret=-1234, table_id=5000, ptr=0x7fff..., count=42)
```

### Log Configuration
```sql
-- Statement level (hint)
/*+ log_level='SQL:DEBUG' */

-- Session level
SET @@session.log_level = 'SQL:DEBUG';

-- System level
ALTER SYSTEM SET log_level = 'DEBUG';
```

## Comments

- English only, `//` style
- Classes: purpose, thread-safety, usage
- Functions: `@param [in/out]`, `@return`
- Implementation: why, not what
- TODO: `TODO(owner): task, deadline`clude default
- **Classes**: Access specifiers no indent, members 2-space indent
- **Init lists**: Same line if fits, else 4-space indent aligned
- **Namespaces**: No indentation for contentMacros

`OB_SUCC/FAIL(func())`, `OB_ISNULL/NOT_NULL(ptr)`, `IS_INIT/NOT_INIT`, `OZ(func())`, `K(var)`, `KP(ptr)Levels: ERROR/WARN/INFO (DBA), EDIAG/WDIAG/TRACE/DEBUG (Dev)  
Format: `LOG_WARN("msg", K(ret), K(var), KP(ptr))`  
Config: Statement hint, session, system levels
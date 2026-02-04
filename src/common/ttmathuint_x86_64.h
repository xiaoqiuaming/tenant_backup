/*
 * This file is a part of TTMath Bignum Library
 * and is distributed under the (new) BSD licence.
 * Author: Tomasz Sowa <t.sowa@ttmath.org>
 */

/*
 * Copyright (c) 2006-2009, Tomasz Sowa
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name Tomasz Sowa nor the names of contributors to this
 *    project may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */


#ifndef headerfilettmathuint_x86_64
#define headerfilettmathuint_x86_64


#ifndef TTMATH_NOASM
#ifdef TTMATH_PLATFORM64


/*!
    \file ttmathuint_x86_64.h
    \brief template class UInt<uint> with assembler code for 64bit x86_64 processors

    this file is included at the end of ttmathuint.h
*/

#ifdef _MSC_VER
#include <intrin.h>
#endif


namespace ttmath
{

#ifdef _MSC_VER

  extern "C"
  {
  uint __fastcall ttmath_adc_x64(uint* p1, const uint* p2, uint nSize, uint c);
  uint __fastcall ttmath_addindexed_x64(uint* p1, uint nSize, uint nPos, uint nValue);
  uint __fastcall ttmath_addindexed2_x64(uint* p1, uint nSize, uint nPos, uint nValue1, uint nValue2);
  uint __fastcall ttmath_addvector_x64(const uint * ss1, const uint * ss2, uint ss1_size, uint ss2_size, uint * result);
  uint __fastcall ttmath_sbb_x64(uint* p1, const uint* p2, uint nSize, uint c);
  uint __fastcall ttmath_subindexed_x64(uint* p1, uint nSize, uint nPos, uint nValue);
  uint __fastcall ttmath_subvector_x64(const uint * ss1, const uint * ss2, uint ss1_size, uint ss2_size, uint * result);
  uint __fastcall ttmath_rcl_x64(uint* p1, uint nSize, uint nLowestBit);
  uint __fastcall ttmath_rcr_x64(uint* p1, uint nSize, uint nLowestBit);
  uint __fastcall ttmath_div_x64(uint* pnValHi, uint* pnValLo, uint nDiv);
  uint __fastcall ttmath_rcl2_x64(uint* p1, uint nSize, uint nBits, uint c);
  uint __fastcall ttmath_rcr2_x64(uint* p1, uint nSize, uint nBits, uint c);
  };
#endif


  /*!
        returning the string represents the currect type of the library
        we have following types:
          asm_vc_32   - with asm code designed for Microsoft Visual C++ (32 bits)
          asm_gcc_32  - with asm code designed for GCC (32 bits)
          asm_vc_64   - with asm for VC (64 bit)
          asm_gcc_64  - with asm for GCC (64 bit)
          no_asm_32   - pure C++ version (32 bit) - without any asm code
          no_asm_64   - pure C++ version (64 bit) - without any asm code
    */
  template<uint value_size>
  const char * UInt<value_size>::LibTypeStr()
  {
#ifdef _MSC_VER
    static const char info[] = "asm_vc_64";
#endif

#ifdef __GNUC__
    static const char info[] = "asm_gcc_64";
#endif

    return info;
  }


  /*!
        returning the currect type of the library
    */
  template<uint value_size>
  LibTypeCode UInt<value_size>::LibType()
  {
#ifdef _MSC_VER
    LibTypeCode info = asm_vc_64;
#endif

#ifdef __GNUC__
    LibTypeCode info = asm_gcc_64;
#endif

    return info;
  }


  /*!
    *
    *    basic mathematic functions
    *
    */



  /*!
        this method adding ss2 to the this and adding carry if it's defined
        (this = this + ss2 + c)

        ***this method is created only on a 64bit platform***

        c must be zero or one (might be a bigger value than 1)
        function returns carry (1) (if it was)
    */
  template<uint value_size>
  uint UInt<value_size>::Add(const UInt<value_size> & ss2, uint c)
  {
    uint b = value_size;
    uint * p1 = table;
    const uint * p2 = ss2.table;

    // we don't have to use TTMATH_REFERENCE_ASSERT here
    // this algorithm doesn't require it

#if !defined(__GNUC__) && !defined(_MSC_VER)
#error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
#endif

#ifdef _MSC_VER
    c = ttmath_adc_x64(p1,p2,b,c);
#endif

#ifdef __x86_64__ //add support for arm platform by wangd 202106
#ifdef __GNUC__
    uint dummy, dummy2;

    /*
                this part should be compiled with gcc
            */
    __asm__ __volatile__(

          "xorq %%rdx, %%rdx                \n"
          "negq %%rax                        \n"     // CF=1 if rax!=0 , CF=0 if rax==0

          "1:                                    \n"
          "movq (%%rsi,%%rdx,8), %%rax    \n"
          "adcq %%rax, (%%rbx,%%rdx,8)    \n"

          "incq %%rdx                        \n"
          "decq %%rcx                        \n"
          "jnz 1b                                \n"

          "adcq %%rcx, %%rcx                \n"

          : "=c" (c), "=a" (dummy), "=d" (dummy2)
          : "0" (b),  "1" (c), "b" (p1), "S" (p2)
          : "cc", "memory" );
#endif

//add support for arm platform by wangd 202106:b
#elif defined(__aarch64__)
#ifdef __GNUC__
    uint dummy, dummy2;
    __asm__ __volatile__(
                "eor %2, %2, %2 \n"  //异或%2变量对应寄存器，即初始化该寄存器
                "neg %1, %1 \n" //对%1寄存器的值取负
                "cmp %1, #0\n"  //比较%1寄存器和0的值得大小
                "beq 2f\n" //如果相等跳转到循环2（目的为了将CF标志位设为0）
                "mov x15, #0\n" //这两句为假指令，目的为将CF标志位设为1
                "cmp x15, #0\n"

                "1:\n"
                "ldr %1, [%5] \n" //取得%5寄存器地址对应内存中的值赋给%1寄存器
                "ldr x20, [%6], #8\n"
                //取得%6寄存器地址对应内存中的值赋给x20寄存器，并将%6的地址加8
                "adcs %1, %1, x20\n" //将%1和x20寄存器和CF标识位的值相加
                "str %1, [%5], #8\n"
                //将上面得到的最终结果放入%5寄存器的内存中，并地址加8

                "add %2, %2, #1\n"
                "sub %3, %3, #1\n"
                "cbnz %3, 1b\n" //如果%3寄存器不为0则跳转回循环1，否则跳出循环

                "adc %0, %0, %0 \n" //将%0寄存器与CF标识位值相加，目的为记录CF标识
                "b 3f \n"

                "2: \n"
                "mov x15, #0 \n"
                "cmp x15, #1 \n"
                "b 1b\n"

                "3:\n"

                : "=&r"(c), "=&r"(dummy), "=&r"(dummy2)
                : "0"(b), "1"(c), "r"(p1), "r"(p2)
                : "x15", "x20", "cc", "memory");
#endif
#endif
//add support for arm platform by wangd 202106:e
    TTMATH_LOGC("UInt::Add", c)

        return c;
  }



  /*!
        this method adds one word (at a specific position)
        and returns a carry (if it was)

        ***this method is created only on a 64bit platform***


        if we've got (value_size=3):
            table[0] = 10;
            table[1] = 30;
            table[2] = 5;
        and we call:
            AddInt(2,1)
        then it'll be:
            table[0] = 10;
            table[1] = 30 + 2;
            table[2] = 5;

        of course if there was a carry from table[2] it would be returned
    */
  template<uint value_size>
  uint UInt<value_size>::AddInt(uint value, uint index)
  {
    uint b = value_size;
    uint * p1 = table;
    uint c;

    TTMATH_ASSERT( index < value_size )

    #if !defined(__GNUC__) && !defined(_MSC_VER)
    #error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
    #endif


    #ifdef _MSC_VER
        c = ttmath_addindexed_x64(p1,b,index,value);
#endif

//add support for arm platform by wangd 202106:b
#if defined(__x86_64__)
#ifdef __GNUC__
    uint dummy, dummy2;

    __asm__ __volatile__(

          "subq %%rdx, %%rcx                 \n"

          "1:                                    \n"
          "addq %%rax, (%%rbx,%%rdx,8)    \n"
          "jnc 2f                                \n"

          "movq $1, %%rax                    \n"
          "incq %%rdx                        \n"
          "decq %%rcx                        \n"
          "jnz 1b                                \n"

          "2:                                    \n"
          "setc %%al                        \n"
          "movzx %%al, %%rdx                \n"

          : "=d" (c),    "=a" (dummy), "=c" (dummy2)
          : "0" (index), "1" (value),  "2" (b), "b" (p1)
          : "cc", "memory" );

#endif

//add support for arm platform by wangd 202106
#elif defined(__aarch64__)
#ifdef __GNUC__
    uint dummy, dummy2;
    __asm__ __volatile__(
        "sub %2, %2, %0\n"  //将寄存器%0的值与%2的值相减

        "1:\n"
        "mov x15, #8\n"  //给x15寄存器赋值为8
        "mul x20, %0, x15\n" //将%0寄存器值乘x15的值，即乘8
        "add x19, %6, x20\n" //将%6寄存器值与上面的结果相加放入x19寄存器
        "ldr x16, [x19]\n"   //从上述计算出的地址对应内存中取值，放入x16寄存器
        "adds x16, %1, x16\n" //将x16与%1寄存器相加，并设置对应标识位
        "str x16, [x19]\n"    //将结果放入x19寄存器对应的内存中
        "mrs x17, nzcv \n"    //从CPSR寄存器读取数据放入x17寄存器中
        "ubfx x18, x17, #29, #1\n" //取出其中的第29位，即CF标识位的值
        "cbz x18, 2f\n"      //判断CF标识位是否为0，为0则跳转到循环2

        "mov %1, #1\n"  //给%1寄存器赋值1
        "add %0, %0, #1\n"  //将%0寄存器加1，记录循环次数
        "sub %2, %2, #1\n"  //将%2寄存器减1，控制循环次数
        "cbnz %2, 1b\n"  //如果%2寄存器非0则继续循环1，否则跳出

        "2:\n"
        "mrs x17, nzcv\n"
        "ubfx x18, x17, #29, #1\n" //同上取出标识位放入x18
        "mov %1, x18\n"  //将CF标识位赋值给%1
        "eor %0, %0, %0\n" //将%0寄存器置0
        "add %0, %0, %1\n" //将CF标识位放入%0寄存器中


        : "=&r" (c), "=&r" (dummy), "=&r" (dummy2)
        : "0" (index), "1" (value), "2" (b), "r" (p1)
        : "x15", "x16", "x17", "x18", "x19", "cc", "memory" );
#endif
#endif
//add support for arm platform by wangd 202106:e

    TTMATH_LOGC("UInt::AddInt", c)

        return c;
  }



  /*!
        this method adds only two unsigned words to the existing value
        and these words begin on the 'index' position
        (it's used in the multiplication algorithm 2)

        ***this method is created only on a 64bit platform***

        index should be equal or smaller than value_size-2 (index <= value_size-2)
        x1 - lower word, x2 - higher word

        for example if we've got value_size equal 4 and:
            table[0] = 3
            table[1] = 4
            table[2] = 5
            table[3] = 6
        then let
            x1 = 10
            x2 = 20
        and
            index = 1

        the result of this method will be:
            table[0] = 3
            table[1] = 4 + x1 = 14
            table[2] = 5 + x2 = 25
            table[3] = 6

        and no carry at the end of table[3]

        (of course if there was a carry in table[2](5+20) then
        this carry would be passed to the table[3] etc.)
    */
  template<uint value_size>
  uint UInt<value_size>::AddTwoInts(uint x2, uint x1, uint index)
  {
    uint b = value_size;
    uint * p1 = table;
    uint c;

    TTMATH_ASSERT( index < value_size - 1 )

    #if !defined(__GNUC__) && !defined(_MSC_VER)
    #error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
    #endif


    #ifdef _MSC_VER
        c = ttmath_addindexed2_x64(p1,b,index,x1,x2);
#endif

#ifdef __x86_64__ //add support for arm platform by wangd 202106
#ifdef __GNUC__
    uint dummy, dummy2;

    __asm__ __volatile__(

          "subq %%rdx, %%rcx                 \n"

          "addq %%rsi, (%%rbx,%%rdx,8)     \n"
          "incq %%rdx                        \n"
          "decq %%rcx                        \n"

          "1:                                    \n"
          "adcq %%rax, (%%rbx,%%rdx,8)    \n"
          "jnc 2f                                \n"

          "mov $0, %%rax                    \n"
          "incq %%rdx                        \n"
          "decq %%rcx                        \n"
          "jnz 1b                                \n"

          "2:                                    \n"
          "setc %%al                        \n"
          "movzx %%al, %%rax                \n"

          : "=a" (c), "=c" (dummy), "=d" (dummy2)
          : "0" (x2), "1" (b),      "2" (index), "b" (p1), "S" (x1)
          : "cc", "memory" );

#endif

//add support for arm platform by wangd 202106:b
#elif defined(__aarch64__)
#ifdef __GNUC__
    uint dummy, dummy2;

    __asm__ __volatile__(
        "sub %1, %1, %2\n"  //将%1和%2寄存器的值相减存入%1寄存器

        "mov x15, #8\n"
        "mul x20, %2, x15\n"
        "add x19, %6, x20\n"  //计算（%6+%2*8）寄存器中的值
        "ldr x16, [x19]\n"    //将计算出的地址中内存值取出放入x16寄存器
        "adds x16, %7, x16\n" //%7和x16和CF标识位的值相加
        "str x16, [x19]\n"    //将最终结果x16放入x19寄存器地址对应的内存中
        "add %2, %2, #1\n"    //将%2寄存器加1，记录循环次数
        "sub %1, %1, #1\n"    //将%1寄存器减1，控制循环次数

        "1:\n"
        "mov x15, #8\n"
        "mul x20, %2, x15\n"
        "add x19, %6, x20\n"
        "ldr x16, [x19]\n"  //计算（%6+%2*8）寄存器地址对应的内存值放入x16

        "adcs x16, %3, x16\n" //将x16，x2和CF标识位的值相加
        "str x16, [x19]\n"

        "mrs x17, nzcv\n"
        "ubfx x18, x17, #29, #1\n" //获取CF标识位
        "cbz x18, 2f\n" //如果CF值为0跳转到2，否则继续下面操作

        "mov x2, #0\n"
        "add %2, %2, #1\n"
        "sub %1, %1, #1\n"
        "cbnz %1, 1b\n"  //如果%1寄存器值非0则跳回循环1，否则退出

        "2:\n"
        "mrs x17, nzcv\n"
        "ubfx x18, x17, #29, #1\n"  //读取CF标识位
        "mov %0, x18\n"   //将CF标识位值存入%0寄存器


        : "=&r" (c), "=&r" (dummy), "=&r" (dummy2)
        : "0" (x2), "1" (b), "2" (index), "r" (p1), "r" (x1)
        : "x15", "x16", "x17", "x18", "x19", "x20", "cc", "memory" );
#endif
#endif
//add support for arm platform by wangd 202106:e

    TTMATH_LOGC("UInt::AddTwoInts", c)

        return c;
  }



  /*!
        this static method addes one vector to the other
        'ss1' is larger in size or equal to 'ss2'

        ss1 points to the first (larger) vector
        ss2 points to the second vector
        ss1_size - size of the ss1 (and size of the result too)
        ss2_size - size of the ss2
        result - is the result vector (which has size the same as ss1: ss1_size)

        Example:  ss1_size is 5, ss2_size is 3
        ss1:      ss2:   result (output):
          5        1         5+1
          4        3         4+3
          2        7         2+7
          6                  6
          9                  9
      of course the carry is propagated and will be returned from the last item
      (this method is used by the Karatsuba multiplication algorithm)
    */
  template<uint value_size>
  uint UInt<value_size>::AddVector(const uint * ss1, const uint * ss2, uint ss1_size, uint ss2_size, uint * result)
  {
    TTMATH_ASSERT( ss1_size >= ss2_size )

        uint c;

#if !defined(__GNUC__) && !defined(_MSC_VER)
#error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
#endif


#ifdef _MSC_VER
    c = ttmath_addvector_x64(ss1, ss2, ss1_size, ss2_size, result);
#endif

#ifdef __x86_64__ //add support for arm platform by wangd 202106
#ifdef __GNUC__
    uint dummy1, dummy2, dummy3;
    uint rest = ss1_size - ss2_size;

    //    this part should be compiled with gcc

    __asm__ __volatile__(
          "mov %%rdx, %%r8                    \n"
          "xor %%rdx, %%rdx                    \n"   // rdx = 0, cf = 0
          "1:                                        \n"
          "mov (%%rsi,%%rdx,8), %%rax            \n"
          "adc (%%rbx,%%rdx,8), %%rax            \n"
          "mov %%rax, (%%rdi,%%rdx,8)            \n"

          "inc %%rdx                            \n"
          "dec %%rcx                            \n"
          "jnz 1b                                    \n"

          "adc %%rcx, %%rcx                    \n"   // rcx has the cf state

          "or %%r8, %%r8                        \n"
          "jz 3f                                \n"

          "xor %%rbx, %%rbx                    \n"   // ebx = 0
          "neg %%rcx                            \n"   // setting cf from rcx
          "mov %%r8, %%rcx                    \n"   // rcx=rest and is != 0
          "2:                                        \n"
          "mov (%%rsi, %%rdx, 8), %%rax        \n"
          "adc %%rbx, %%rax                     \n"
          "mov %%rax, (%%rdi, %%rdx, 8)        \n"

          "inc %%rdx                            \n"
          "dec %%rcx                            \n"
          "jnz 2b                                    \n"

          "adc %%rcx, %%rcx                    \n"
          "3:                                        \n"

          : "=a" (dummy1), "=b" (dummy2), "=c" (c),       "=d" (dummy3)
          :                "1" (ss2),     "2" (ss2_size), "3" (rest),   "S" (ss1),  "D" (result)
          : "%r8", "cc", "memory" );

#endif

//add support for arm platform by wangd 202106:b
#elif defined(__aarch64__)
#ifdef __GNUC__
    uint dummy1, dummy2, dummy3;
    uint rest = ss1_size - ss2_size;

    //    this part should be compiled with gcc
    __asm__ __volatile__(
            "mov x8, %3\n"
            "eor %3, %3, %3 \n"
            "cmp %3, #0\n"  //比较%3寄存器值是否为0
            "beq 4f\n"    //如果为0则跳转到4f, 将CF标识位置0
            "mov x16, #0\n"
            "cmp x16, #0\n"   //假指令， 将CF标识位置1
          "1:   \n"
            "mov x9, #8\n"
            "mul x10, %3, x9\n"
            "add x11, %7, x10\n"
            "ldr x12, [x11]\n"  //计算（%7+%3*8）地址对应内存的值放入x12
            "mov x15, x12\n"
            "add x11, %1, x10\n"
            "ldr x12, [x11]\n" //计算（%1+%3*8）地址对应内存的值放入x12

            "adcs x15, x12, x15\n"  //计算x12和x15和CF的相加的值，并设置CF标识位
            "add x11, %8, x10\n"
            "str x15, [x11]\n" //将结果放入（%8+%3*8）地址对应的内存中

            "add %3, %3, #1\n"  //将%3的寄存器值加1， 记录循环次数
            "sub %2, %2, #1\n"  //将%2的寄存器值减1， 控制循环次数
            "cbnz %2, 1b\n"     //如果%2寄存器值非0，则跳转循环1

            "adcs %2, %2, %2\n"  //将%2寄存器的值拥有CF标识位的值
            "orr x8, x8, x8\n"   //将x8进行同或操作
            "cbz x8, 5f\n"       //如果为0则跳转至5

            "eor %1, %1, %1\n"
            "neg %2, %2\n"  //对%2寄存器取负
            "cmp %2, #0\n"  //比较%2寄存器与0的值
            "mov %2, x8\n"
            "beq 3f\n"  //如果为0则置CF为0
            "mov x16, #0\n"
            "cmp x16, #0\n"  //否则将CF置1
          "2:   \n"
            "mov x9, #8\n"
            "mul x10, %3, x9\n"
            "add x11, %7, x10\n"
            "ldr x12, [x11]\n" //计算（%7+%3*8）地址内存值放入x12
            "mov x15, x12\n"   //将x12寄存器值放入x15寄存器中
            "adcs x15, %1, x15\n"  //计算x15和%1和CF相加的值，并设置标识位
            "add x11, %8, x10\n"
            "str x15, [x11]\n"

            "add %3, %3, #1\n"
            "sub %2, %2, #1\n"
          "cbnz %2, 2b \n"

            "adcs %2, %2, %2 \n"
            "b 5f\n"
          "3:  \n"
            "mov x16, #0\n"
            "cmp x16, #1\n"
            "b 2b\n"
          "4:  \n"
            "mov x16, #0\n"
            "cmp x16, #1\n"
            "b 1b\n"
          "5:  \n"
            "mov %0, x15\n"

            : "=r"(dummy1), "=r"(dummy2), "=r"(c), "=r"(dummy3)
            : "1"(ss2), "2"(ss2_size), "3"(rest), "r"(ss1), "r"(result)
            : "x8", "x9", "x10", "x11", "x12", "16", "x15", "cc", "memory" );
#endif
#endif
//add support for arm platform by wangd 202106:e

    TTMATH_VECTOR_LOGC("UInt::AddVector", c, result, ss1_size)

        return c;
  }



  /*!
        this method's subtracting ss2 from the 'this' and subtracting
        carry if it has been defined
        (this = this - ss2 - c)

        ***this method is created only on a 64bit platform***

        c must be zero or one (might be a bigger value than 1)
        function returns carry (1) (if it was)
    */
  template<uint value_size>
  uint UInt<value_size>::Sub(const UInt<value_size> & ss2, uint c)
  {
    uint b = value_size;
    uint * p1 = table;
    const uint * p2 = ss2.table;


    // we don't have to use TTMATH_REFERENCE_ASSERT here
    // this algorithm doesn't require it

#if !defined(__GNUC__) && !defined(_MSC_VER)
#error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
#endif


#ifdef _MSC_VER
    c = ttmath_sbb_x64(p1,p2,b,c);
#endif

#ifdef __x86_64__ //add support for arm platform by wangd 202106
#ifdef __GNUC__
    uint dummy, dummy2;

    __asm__  __volatile__(

          "xorq %%rdx, %%rdx                \n"
          "negq %%rax                        \n"     // CF=1 if rax!=0 , CF=0 if rax==0

          "1:                                    \n"
          "movq (%%rsi,%%rdx,8), %%rax    \n"
          "sbbq %%rax, (%%rbx,%%rdx,8)    \n"

          "incq %%rdx                        \n"
          "decq %%rcx                        \n"
          "jnz 1b                                \n"

          "adcq %%rcx, %%rcx                \n"

          : "=c" (c), "=a" (dummy), "=d" (dummy2)
          : "0" (b),  "1" (c), "b" (p1), "S" (p2)
          : "cc", "memory" );

#endif

//add support for arm platform by wangd 202106:b
#elif defined(__aarch64__)
#ifdef __GNUC__
    uint dummy, dummy2;
    __asm__  __volatile__(
        "eor %2, %2, %2\n"   //异或%2变量对应寄存器，即初始化该寄存器
        "negs %1, %1\n"      //对%1寄存器的值取负

        "1:  \n"
        "ldr %1, [%5]\n"
        "ldr x20, [%6], #8\n"
        "sbcs %1, %1, x20\n"  //将%1和x20寄存器和CF标识位的值相减
        "str %1, [%5], #8\n"

        "add %2, %2, #1\n"
        "sub %3, %3, #1\n"
        "cbnz %3, 1b\n"

        "mrs x17, nzcv\n"
        "ubfx x18, x17, #29, #1\n"
        "cmp x18, #1\n"
        "beq 3f\n"
        "add x18, x18, #1\n"
        "2:  \n"
        "add %0, %0, x18\n"
        "b 4f\n"

        "3:  \n"
        "sub x18, x18, #1\n"
        "b 2b\n"
        "4:  \n"

        : "=&r"(c), "=&r"(dummy), "=&r"(dummy2)
        : "0" (b), "1" (c), "r" (p1), "r" (p2)
        : "x17", "x18", "x20", "cc", "memory" );
#endif
#endif
//add support for arm platform by wangd 202106:e

    TTMATH_LOGC("UInt::Sub", c)

        return c;
  }



  /*!
        this method subtracts one word (at a specific position)
        and returns a carry (if it was)

        ***this method is created only on a 64bit platform***

        if we've got (value_size=3):
            table[0] = 10;
            table[1] = 30;
            table[2] = 5;
        and we call:
            SubInt(2,1)
        then it'll be:
            table[0] = 10;
            table[1] = 30 - 2;
            table[2] = 5;

        of course if there was a carry from table[2] it would be returned
    */
  template<uint value_size>
  uint UInt<value_size>::SubInt(uint value, uint index)
  {
    uint b = value_size;
    uint * p1 = table;
    uint c;

    TTMATH_ASSERT( index < value_size )

    #if !defined(__GNUC__) && !defined(_MSC_VER)
    #error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
    #endif


    #ifdef _MSC_VER
        c = ttmath_subindexed_x64(p1,b,index,value);
#endif

#ifdef __x86_64__ //add support for arm platform by wangd 202106
#ifdef __GNUC__
    uint dummy, dummy2;

    __asm__ __volatile__(

          "subq %%rdx, %%rcx                 \n"

          "1:                                    \n"
          "subq %%rax, (%%rbx,%%rdx,8)    \n"
          "jnc 2f                                \n"

          "movq $1, %%rax                    \n"
          "incq %%rdx                        \n"
          "decq %%rcx                        \n"
          "jnz 1b                                \n"

          "2:                                    \n"
          "setc %%al                        \n"
          "movzx %%al, %%rdx                \n"

          : "=d" (c),    "=a" (dummy), "=c" (dummy2)
          : "0" (index), "1" (value),  "2" (b), "b" (p1)
          : "cc", "memory" );

#endif

//add support for arm platform by wangd 202106:b
#elif defined(__aarch64__)
#ifdef __GNUC__
    uint dummy, dummy2;
    __asm__ __volatile__(
        "sub %2, %2, %0\n"

        "1:  \n"
        "mov x15, #8\n"
        "mul x20, %0, x15\n"
        "add x19, %6, x20\n"
        "ldr x16, [x19]\n"
        "subs x16, x16, %1\n"
        "str x16, [x19]\n"
        "mrs x17, nzcv\n"
        "ubfx x18, x17, #29, #1\n"
        "cbnz x18, 2f\n"

        "mov %1, #1\n"
        "add %0, %0, #1\n"
        "sub %2, %2, #1\n"
        "cbnz %2, 1b\n"

        "2:  \n"
        "mrs x17, nzcv\n"
        "ubfx x18, x17, #29, #1\n"
        "cmp x18, #1\n"
        "beq 3f\n"
        "add x18, x18, #1\n"
        "b 4f\n"

        "3:  \n"
        "sub x18, x18, #1\n"
        "4:  \n"
        "mov %1, x18\n"
        "mov %0, %1\n"


        : "=&r" (c), "=&r" (dummy), "=&r" (dummy2)
        : "0" (index), "1" (value), "2" (b), "r" (p1)
        : "x16", "x17", "x18", "x19", "x20", "cc", "memory" );

#endif
#endif
//add support for arm platform by wangd 202106:e
    TTMATH_LOGC("UInt::SubInt", c)

        return c;
  }


  /*!
        this static method subtractes one vector from the other
        'ss1' is larger in size or equal to 'ss2'

        ss1 points to the first (larger) vector
        ss2 points to the second vector
        ss1_size - size of the ss1 (and size of the result too)
        ss2_size - size of the ss2
        result - is the result vector (which has size the same as ss1: ss1_size)

        Example:  ss1_size is 5, ss2_size is 3
        ss1:      ss2:   result (output):
          5        1         5-1
          4        3         4-3
          2        7         2-7
          6                  6-1  (the borrow from previous item)
          9                  9
                       return (carry): 0
      of course the carry (borrow) is propagated and will be returned from the last item
      (this method is used by the Karatsuba multiplication algorithm)
    */
  template<uint value_size>
  uint UInt<value_size>::SubVector(const uint * ss1, const uint * ss2, uint ss1_size, uint ss2_size, uint * result)
  {
    TTMATH_ASSERT( ss1_size >= ss2_size )

        uint c;

#if !defined(__GNUC__) && !defined(_MSC_VER)
#error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
#endif


#ifdef _MSC_VER
    c = ttmath_subvector_x64(ss1, ss2, ss1_size, ss2_size, result);
#endif

#ifdef __x86_64__ //add support for arm platform by wangd 202106
#ifdef __GNUC__

    //    the asm code is nearly the same as in AddVector
    //    only two instructions 'adc' are changed to 'sbb'

    uint dummy1, dummy2, dummy3;
    uint rest = ss1_size - ss2_size;

    __asm__ __volatile__(
          "mov %%rdx, %%r8                    \n"
          "xor %%rdx, %%rdx                    \n"   // rdx = 0, cf = 0
          "1:                                        \n"
          "mov (%%rsi,%%rdx,8), %%rax            \n"
          "sbb (%%rbx,%%rdx,8), %%rax            \n"
          "mov %%rax, (%%rdi,%%rdx,8)            \n"

          "inc %%rdx                            \n"
          "dec %%rcx                            \n"
          "jnz 1b                                    \n"

          "adc %%rcx, %%rcx                    \n"   // rcx has the cf state

          "or %%r8, %%r8                        \n"
          "jz 3f                                \n"

          "xor %%rbx, %%rbx                    \n"   // ebx = 0
          "neg %%rcx                            \n"   // setting cf from rcx
          "mov %%r8, %%rcx                    \n"   // rcx=rest and is != 0
          "2:                                        \n"
          "mov (%%rsi, %%rdx, 8), %%rax        \n"
          "sbb %%rbx, %%rax                     \n"
          "mov %%rax, (%%rdi, %%rdx, 8)        \n"

          "inc %%rdx                            \n"
          "dec %%rcx                            \n"
          "jnz 2b                                    \n"

          "adc %%rcx, %%rcx                    \n"
          "3:                                        \n"

          : "=a" (dummy1), "=b" (dummy2), "=c" (c),       "=d" (dummy3)
          :                "1" (ss2),     "2" (ss2_size), "3" (rest),   "S" (ss1),  "D" (result)
          : "%r8", "cc", "memory" );

#endif

//add support for arm platform by wangd 202106:b
#elif defined(__aarch64__)
#ifdef __GNUC__

    //    the asm code is nearly the same as in AddVector
    //    only two instructions 'adc' are changed to 'sbb'

    uint dummy1, dummy2, dummy3;
    uint rest = ss1_size - ss2_size;
    __asm__ __volatile__(
            "mov x8, %3\n"
            "eor %3, %3, %3\n"
            "cmp %3, #0\n"
            "bne 6f\n"  //如果为0则跳转到4f，将CF标志位置0
            "mov x16, #0\n"
            "cmp x16, #0\n"  //假指令，将CF标识位置1
          "1:  \n"
            "mov x9, #8\n"
            "mul x10, %3, x9\n"
            "add x11, %7, x10\n"
            "ldr x12, [x11]\n"
            "mov x15, x12\n"
            "add x11, %1, x10\n"
            "ldr x12, [x11]\n"

            "sbcs x15, x15, x12\n"
            "add x11, %8, x10\n"
            "str x15, [x11]\n"

            "add %3, %3, #1\n"
            "sub %2, %2, #1\n"
            "cbnz %2, 1b\n"

            "mrs x17, nzcv\n"
            "ubfx x18, x17, #29, #1\n"
            "cmp x18, #1\n"
            "beq 7f\n"
            "add x18, x18, #1\n"
          "2:  \n"
            "adds %2, %2, %2\n"
            "add %2, %2, x18\n"
            "orr x8, x8, x8\n"
            "cbz x8, 9f\n"

            "eor %1, %1, %1\n"
            "negs %2, %2\n"
            "mov %2, x8\n"
          "3:  \n"
            "mov x9, #8\n"
            "mul x10, %3, x9\n"
            "add x11, %7, x10\n"
            "ldr x12, [x11]\n"
            "mov x15, x12\n"
            "sbcs x15, x15, %1\n"
            "add x11, %8, x10\n"
            "str x15, [x11]\n"

            "mrs x17, nzcv\n"
            "ubfx x18, x17, #29, #1\n"
            "cmp x18, #1\n"
            "beq 8f\n"
            "add x18, x18, #1\n"

          "4:  \n"
            "add %3, %3, #1\n"
            "sub %2, %2, #1\n"
            "cbnz %2, 3b\n"

            "adds %2, %2, %2\n"
            "add %2, %2, x18\n"
            "b 9f\n"
          "5:  \n"
            "mov x16, #0\n"
            "cmp x16, #1\n"
            "b 3b\n"
          "6:  \n"
            "mov x16, #0\n"
            "cmp x16, #1\n"
            "b 1b\n"
          "7:  \n"
            "sub x18, x18, #1\n"
            "b 2b\n"
          "8:  \n"
            "sub x18, x18, #1\n"
            "b 4b\n"
          "9:  \n"
            "mov %0, x15\n"

            : "=r" (dummy1), "=r" (dummy2), "=r" (c), "=r" (dummy3)
            : "1" (ss2), "2" (ss2_size), "3" (rest), "r" (ss1), "r" (result)
            : "x8", "x15", "x16", "x17", "x18", "x10", "x11", "x12", "cc", "memory" );
#endif
#endif
//add support for arm platform by wangd 202106:e

    TTMATH_VECTOR_LOGC("UInt::SubVector", c, result, ss1_size)

        return c;
  }


  /*!
        this method moves all bits into the left hand side
        return value <- this <- c

        the lowest *bit* will be held the 'c' and
        the state of one additional bit (on the left hand side)
        will be returned

        for example:
        let this is 001010000
        after Rcl2_one(1) there'll be 010100001 and Rcl2_one returns 0

        ***this method is created only on a 64bit platform***
    */
  template<uint value_size>
  uint UInt<value_size>::Rcl2_one(uint c)
  {
    sint b = value_size;
    uint * p1 = table;


#if !defined(__GNUC__) && !defined(_MSC_VER)
#error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
#endif


#ifdef _MSC_VER
    c = ttmath_rcl_x64(p1,b,c);
#endif


#ifdef __GNUC__
#ifdef __x86_64__ //add support for arm platform by wangd 202106
    uint dummy, dummy2;

    __asm__  __volatile__(

          "xorq %%rdx, %%rdx            \n"   // rdx=0
          "negq %%rax                    \n"   // CF=1 if rax!=0 , CF=0 if rax==0

          "1:                                \n"
          "rclq $1, (%%rbx, %%rdx, 8)    \n"

          "incq %%rdx                    \n"
          "decq %%rcx                    \n"
          "jnz 1b                            \n"

          "adcq %%rcx, %%rcx            \n"

          : "=c" (c), "=a" (dummy), "=d" (dummy2)
          : "0" (b),  "1" (c), "b" (p1)
          : "cc", "memory" );
//add support for arm platform by wangd 202106:b
#elif defined(__aarch64__)
    uint dummy, dummy2;
    __asm__  __volatile__(
        "eor %2, %2, %2   \n"
        "neg %1, %1       \n"
        "cmp %1, #0       \n"
        "beq 1f           \n"
        "mov x10, #0      \n"
        "cmp x10, #0      \n"
        "b 2f             \n"
        "1:               \n"
        "mov x10, #0      \n"
        "cmp x10, #1      \n"
        "b 2f             \n"
        "2:               \n"
        "add x21, %5, %2, lsl #3  \n"
        "ldr x22, [x21]   \n"
        "ubfx x23, x22, #63, #1   \n"
        "mrs x24, nzcv    \n"
        "ubfx x25, x24, #29, #1   \n"

        "add x26, x25, x22, lsl #1 \n"

        "str x26, [x21]   \n"
        "cmp x23, #0      \n"
        "beq 3f           \n"
        "mov x10, #0      \n"
        "cmp x10, #0      \n"
        "b 4f             \n"
        "3:               \n"
        "mov x10, #0      \n"
        "cmp x10, #1      \n"
        "b 4f             \n"
        "4:               \n"
        "add %2, %2, #1   \n"
        "sub %3, %3, #1   \n"
        "cbnz %3, 2b      \n"
        "b 5f             \n"
        "5:               \n"
        "adc %0, %0, %0   \n"


        : "=&r" (c), "=&r" (dummy), "=&r"(dummy2)
        : "0" (b), "1" (c), "r" (p1)
        : "x10", "x21", "x22", "x23", "x24", "x25", "x26", "cc", "memory" );
#endif
//add support for arm platform by wangd 202106:e
#endif

    TTMATH_LOGC("UInt::Rcl2_one", c)

        return c;
  }


  /*!
        this method moves all bits into the right hand side
        c -> this -> return value

        the highest *bit* will be held the 'c' and
        the state of one additional bit (on the right hand side)
        will be returned

        for example:
        let this is 000000010
        after Rcr2_one(1) there'll be 100000001 and Rcr2_one returns 0

        ***this method is created only on a 64bit platform***
    */
  template<uint value_size>
  uint UInt<value_size>::Rcr2_one(uint c)
  {
    sint b = value_size;
    uint * p1 = table;


#if !defined(__GNUC__) && !defined(_MSC_VER)
#error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
#endif


#ifdef _MSC_VER
    c = ttmath_rcr_x64(p1,b,c);
#endif


#ifdef __GNUC__
#ifdef __x86_64__ //add support for arm platform by wangd 202106
    uint dummy;

    __asm__  __volatile__(

          "negq %%rax                        \n"   // CF=1 if rax!=0 , CF=0 if rax==0

          "1:                                    \n"
          "rcrq $1, -8(%%rbx, %%rcx, 8)    \n"

          "decq %%rcx                        \n"
          "jnz 1b                                \n"

          "adcq %%rcx, %%rcx                \n"

          : "=c" (c), "=a" (dummy)
          : "0" (b),  "1" (c), "b" (p1)
          : "cc", "memory" );
//add support for arm platform by wangd 202106:b
#elif defined(__aarch64__)
    uint dummy;
    __asm__  __volatile__(
        /* %0: c, %1: dummy, %2: b, %3: c, %4: p1 */
        //CF=1 if rax !=0, CF=0 if rax==0
        "neg %1, %1     \n" //取反%1 neg命令只取反，不置标志位
        "cmp %1, #0     \n" //置标志位CF，比较%1和0的大小
        "beq 1f         \n" //相等则跳转到循环1
        "mov x10, #0    \n"
        "cmp x10, #0    \n"
        "b 2f           \n"
      "1:  \n"
        "mov x10, #0    \n"
        "cmp x10, #1    \n"
        "b 2f           \n"
      "2:  \n"
        //对应指令 rcrq $1, -8(%%rbx, %%rcx, 8)
        // 1. 保存最低位 2. 保存CF位 3. 右移一位 4. 将CF位放到最高位 5. 将最低位放到CF位
        "add x21, %4, %0, lsl #3  \n" //x21=%4+%0 << 3
        "sub x21, x21, #8         \n" //x21=x21-8
        "ldr x22, [x21]           \n" //将x21地址存放的内容加载到x22中
        //<1.保存最低位>
        "ubfx x23, x22, #0, #1    \n" //将x22寄存器从第0位开始，宽度为1的数放到x23寄存器中
        //<2.保存CF位>
        "mrs x24, nzcv            \n" //读取系统寄存器数据到x24寄存器
        "ubfx x25, x24, #29, #1   \n" //将x25寄存器从第29位开始，宽度为1的数[CF标志位]放入x25寄存器
        //<3.右移一位>
        "lsr x22, x22, #1         \n" //将x22寄存器右移1位
        //<4.将CF位放到最高位>
        "add x26, x22, x25, lsl #63  \n" //x26 = x22 +x25 << 63
        "str x26, [x21]           \n"  //将修改后的值存储到x21寄存器对应地址
        //<5.将最低位放到CF位>
        "cmp x23, #0              \n"
        "beq 3f                   \n"
        "mov x10, #0              \n"
        "cmp x10, #0              \n"
        "b 4f                     \n"
      "3:  \n"
        "mov x10, #0              \n"
        "cmp x10, #1              \n"
        "b 4f                     \n"
      "4:  \n"
        //对应指令 decq %%rcx
        "sub %2, %2, #1           \n"
        //对应指令 jnz
        "cbnz %2, 2b              \n"
        "b  5f                    \n"
      "5:  \n"
        //对应指令adcq %%rcx, %%rcx
        "adc %0, %0, %0           \n"


        : "=&r" (c), "=&r" (dummy)
        : "0" (b), "1" (c), "r" (p1)
        : "x10", "x21", "x22", "x23", "x24", "x25", "x26", "cc", "memory" );
#endif
//add support for arm platform by wangd 202106:e
#endif

    TTMATH_LOGC("UInt::Rcr2_one", c)

        return c;
  }



  /*!
        this method moves all bits into the left hand side
        return value <- this <- c

        the lowest *bits* will be held the 'c' and
        the state of one additional bit (on the left hand side)
        will be returned

        for example:
        let this is 001010000
        after Rcl2(3, 1) there'll be 010000111 and Rcl2 returns 1

        ***this method is created only on a 64bit platform***
    */
  template<uint value_size>
  uint UInt<value_size>::Rcl2(uint bits, uint c)
  {
    TTMATH_ASSERT( bits>0 && bits<TTMATH_BITS_PER_UINT )

        uint b = value_size;
    uint * p1 = table;


#if !defined(__GNUC__) && !defined(_MSC_VER)
#error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
#endif


#ifdef _MSC_VER
    c = ttmath_rcl2_x64(p1,b,bits,c);
#endif


#ifdef __GNUC__
    uint dummy, dummy2, dummy3;

//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
    __asm__  __volatile__(
            "mov %2, %7              \n" // %2 = bits
            "mov x15, #64            \n"
            "sub x15, x15, %2        \n" // x22 = 64-bits
            "mov %3, #-1             \n" // %3=-1
            "ubfx x19, x15, #0, #16  \n" // x15:lower 16 bits
            "lsr %3, %3, x19         \n" // left shift
            "mov x20, %3             \n" // x20: number of 1(bits)
            "mov x15, %2             \n" // x15=bits

            "eor %3, %3, %3          \n" // %3 = 0
            "mov %2, %3              \n" // %2 = 0

            //"orr %0, %0, %0        \n"
            "cbz %0, 0f              \n" // mov dummy2 to x20 when c==1
            "mov %2, x20             \n" // x86: "cmovnz %%r8, %%rsi"

          "0:  \n"
            "mov x22, #64            \n"
            "sub x19, x22, x15       \n" // x19:use for ror

          "1:  \n"
            "ldr x21, [%6]           \n" // x21=p1[%3]
            "ror x21, x21, x19       \n" // x21 = x21<<bits
            "mov %0, x21             \n"
            "and %0, %0, x20         \n" // %0=%0+x20
            "eor x21, x21, %0        \n" // x21=0
            "orr x21, x21, %2        \n" // x21=x21 + %2(1bit)
            "str x21, [%6], #8       \n"
            "mov %2, %0              \n"

            "add %3, %3, #1          \n"
            "sub %1, %1, #1          \n"
            "cmn %1, #0              \n"
          "bne 1b                    \n"

            "and %0, %0, #1          \n"

            : "=&r" (c), "=&r" (dummy), "=&r"(dummy2), "=&r" (dummy3)
            : "0" (c), "1" (b), "r" (p1), "r" (bits)
            : "x15", "x19", "x20", "x21", "x22", "cc", "memory" );
#else
//add support for arm platform by wangd 202106:e
    __asm__  __volatile__(

          "movq %%rcx, %%rsi                \n"
          "movq $64, %%rcx                \n"
          "subq %%rsi, %%rcx                \n"
          "movq $-1, %%rdx                \n"
          "shrq %%cl, %%rdx                \n"
          "movq %%rdx, %%r8                 \n"
          "movq %%rsi, %%rcx                \n"

          "xorq %%rdx, %%rdx                \n"
          "movq %%rdx, %%rsi                \n"
          "orq %%rax, %%rax                \n"
          "cmovnz %%r8, %%rsi                \n"

          "1:                                    \n"
          "rolq %%cl, (%%rbx,%%rdx,8)        \n"

          "movq (%%rbx,%%rdx,8), %%rax    \n"
          "andq %%r8, %%rax                \n"
          "xorq %%rax, (%%rbx,%%rdx,8)    \n"
          "orq  %%rsi, (%%rbx,%%rdx,8)    \n"
          "movq %%rax, %%rsi                \n"

          "incq %%rdx                        \n"
          "decq %%rdi                        \n"
          "jnz 1b                                \n"

          "and $1, %%rax                    \n"

          : "=a" (c), "=D" (dummy), "=S" (dummy2), "=d" (dummy3)
          : "0" (c),  "1" (b), "b" (p1), "c" (bits)
          : "%r8", "cc", "memory" );
#endif //add support for arm platform by wangd 202106

#endif

    TTMATH_LOGC("UInt::Rcl2", c)

        return c;
  }


  /*!
        this method moves all bits into the right hand side
        C -> this -> return value

        the highest *bits* will be held the 'c' and
        the state of one additional bit (on the right hand side)
        will be returned

        for example:
        let this is 000000010
        after Rcr2(2, 1) there'll be 110000000 and Rcr2 returns 1

        ***this method is created only on a 64bit platform***
    */
  template<uint value_size>
  uint UInt<value_size>::Rcr2(uint bits, uint c)
  {
    TTMATH_ASSERT( bits>0 && bits<TTMATH_BITS_PER_UINT )

        sint b = value_size;
    uint * p1 = table;

#if !defined(__GNUC__) && !defined(_MSC_VER)
#error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
#endif


#ifdef _MSC_VER
    c = ttmath_rcr2_x64(p1,b,bits,c);
#endif


#ifdef __GNUC__
    uint dummy, dummy2, dummy3;

//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
    __asm__  __volatile__(
                "mov %2, %7              \n"  //%2=bits
                "mov x22, #64            \n"
                "sub x22, x22, %2        \n"   //x22=64-bits
                "mov %3, #-1             \n"   //%3=-1
                "ubfx x19, x22, #0, #16  \n"   //x19:lower 16 bits
                "lsl %3, %3, x19         \n"   //left shift
                "mov x20, %3             \n"   //x20:number of 1(bits)
                "mov x22, %2             \n"   //x22=bits

                "eor %3, %3, %3          \n"  //%3=0
                "mov %2, %3              \n"  //%2=0
                "add %3, %3, %1          \n"
                "sub %3, %3, #1          \n"

                "orr %0, %0, %0          \n"
                "cbz %0, 1f              \n"  // mov dummy2 to x20 when c==1
                "mov %2, x20             \n"  //x86: "cmovnz %%r8, %%rsi"
              "1:  \n"
                "ldr x21, [%6, %3, lsl #3]  \n" // x21=p1[%3]
                "ror x21, x21, x22       \n"    // x21 = x21 << bits
                "mov %0, x21             \n"
                "and %0, %0, x20         \n"    // %0 = %0 +x20
                "eor x21, x21, %0        \n"
                "orr x21, x21, %2        \n"
                "str x21, [%6, %3, lsl #3]  \n"
                "mov %2, %0              \n"

                "sub %3, %3, #1          \n"
                "sub %1, %1, #1          \n"
                "cmn %1, #0              \n"
                "bne 1b                  \n"

                "ror %0, %0, #63         \n"
                "and %0, %0, #1          \n"

                : "=&r" (c), "=&r" (dummy), "=&r" (dummy2), "=&r" (dummy3)
                : "0" (c), "1" (b), "r" (p1), "r" (bits)
                : "x15", "x19", "x20", "x21", "x22", "cc", "memory" );

#else
//add support for arm platform by wangd 202106:e
    __asm__  __volatile__(

          "movq %%rcx, %%rsi                \n"
          "movq $64, %%rcx                \n"
          "subq %%rsi, %%rcx                \n"
          "movq $-1, %%rdx                \n"
          "shlq %%cl, %%rdx                \n"
          "movq %%rdx, %%R8                \n"
          "movq %%rsi, %%rcx                \n"

          "xorq %%rdx, %%rdx                \n"
          "movq %%rdx, %%rsi                \n"
          "addq %%rdi, %%rdx                \n"
          "decq %%rdx                        \n"
          "orq %%rax, %%rax                \n"
          "cmovnz %%R8, %%rsi                \n"

          "1:                                    \n"
          "rorq %%cl, (%%rbx,%%rdx,8)        \n"

          "movq (%%rbx,%%rdx,8), %%rax    \n"
          "andq %%R8, %%rax                \n"
          "xorq %%rax, (%%rbx,%%rdx,8)    \n"
          "orq  %%rsi, (%%rbx,%%rdx,8)    \n"
          "movq %%rax, %%rsi                \n"

          "decq %%rdx                        \n"
          "decq %%rdi                        \n"
          "jnz 1b                                \n"

          "rolq $1, %%rax                    \n"
          "andq $1, %%rax                    \n"

          : "=a" (c), "=D" (dummy), "=S" (dummy2), "=d" (dummy3)
          : "0" (c), "1" (b), "b" (p1), "c" (bits)
          : "%r8", "cc", "memory" );
#endif //add support for arm platform by wangd 202106

#endif

    TTMATH_LOGC("UInt::Rcr2", c)

        return c;
  }


  /*
        this method returns the number of the highest set bit in one 64-bit word
        if the 'x' is zero this method returns '-1'

        ***this method is created only on a 64bit platform***
    */
  template<uint value_size>
  sint UInt<value_size>::FindLeadingBitInWord(uint x)
  {
    sint result;


#if !defined(__GNUC__) && !defined(_MSC_VER)
#error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
#endif


#ifdef _MSC_VER

    unsigned long nIndex = 0;

    if( _BitScanReverse64(&nIndex,x) == 0 )
      result = -1;
    else
      result = nIndex;

#endif


#ifdef __GNUC__
//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
    result = 63 - __builtin_clzll(x);
#else
//add support for arm platform by wangd 202106:e
    uint dummy;

    __asm__ (

    "movq $-1, %1          \n"
    "bsrq %2, %0           \n"
    "cmovz %1, %0          \n"

    : "=r" (result), "=&r" (dummy)
      : "r" (x)
      : "cc" );
//add support for arm platform by wangd 202106

#endif


    return result;
  }


  /*!
        this method sets a special bit in the 'value'
        and returns the last state of the bit (zero or one)

        ***this method is created only on a 64bit platform***

        bit is from <0,63>

        e.g.
         uint x = 100;
         uint bit = SetBitInWord(x, 3);
         now: x = 108 and bit = 0
    */
  template<uint value_size>
  uint UInt<value_size>::SetBitInWord(uint & value, uint bit)
  {
    TTMATH_ASSERT( bit < TTMATH_BITS_PER_UINT )

        uint old_bit;
    uint v = value;

#if !defined(__GNUC__) && !defined(_MSC_VER)
#error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
#endif


#ifdef _MSC_VER
    old_bit = _bittestandset64((__int64*)&value,bit) != 0;
#endif


#ifdef __GNUC__
//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
    __asm__ __volatile__(

/*      "mov x11, %2            \n"
        "mov x12, %2            \n"
        "mov x20, #1            \n"
        "lsr x11, x11, %3       \n"
        "ubfx x22, x11, #0, #1  \n"
        "bfi x11, x20, #0, #1   \n"
        "lsl x11, x11, %3       \n"
        "orr x11, x11, x12      \n"
        "mov x13, #0            \n"
        "ubfx x13, x22, #0, #8  \n"
        "mov %0, x11            \n"
        "mov %1, x13            \n"*/

        "mov x16, #1            \n" // x16用来存储第几位需要置1
        "lsl x16, x16, %3       \n" // x16 左移bit位
        "orr %0, %2, x16        \n" // 通过或操作输出v
        "AND x16, %2, x16       \n" // 提取old bit数值
        "lsr x16, x16, %3       \n"
        "mov %1, x16            \n" // 输出old bit

        "MRS x11, nzcv          \n"
        "BFI x11, x16, #29, #1  \n"
        "MSR nzcv, x11          \n"
        : "=&r" (v), "=&r" (old_bit)
        : "r" (v), "r" (bit)
        : "x11", "x16" );
#else
//add support for arm platform by wangd 202106:e
    __asm__ (

    "btsq %%rbx, %%rax        \n"
    "setc %%bl                \n"
    "movzx %%bl, %%rbx        \n"

    : "=a" (v), "=b" (old_bit)
      : "0" (v),  "1" (bit)
      : "cc" );
#endif //add support for arm platform by wangd 202106
#endif

    value = v;

    return old_bit;
  }


  /*!
     *
     * Multiplication
     *
     *
    */


  /*!
        multiplication: result_high:result_low = a * b
        result_high - higher word of the result
        result_low  - lower word of the result

        this methos never returns a carry
        this method is used in the second version of the multiplication algorithms

        ***this method is created only on a 64bit platform***
    */
  template<uint value_size>
  void UInt<value_size>::MulTwoWords(uint a, uint b, uint * result_high, uint * result_low)
  {
    /*
        we must use these temporary variables in order to inform the compilator
        that value pointed with result1 and result2 has changed

        this has no effect in visual studio but it's usefull when
        using gcc and options like -O
    */
    uint result1_;
    uint result2_;

#if !defined(__GNUC__) && !defined(_MSC_VER)
#error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
#endif


#ifdef _MSC_VER
    result1_ = _umul128(a,b,&result2_);
#endif


#ifdef __GNUC__
//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
    __asm__ (
    "MUL %0, %3, %2    \n"
    "UMULH %1, %3, %2  \n"
    : "=&r" (result1_), "=&r" (result2_)
    : "r"(a), "r"(b)
    : "cc");

#else
//add support for arm platform by wangd 202106:e
    __asm__ (

    "mulq %%rdx            \n"

    : "=a" (result1_), "=d" (result2_)
      : "0" (a),         "1" (b)
      : "cc" );
#endif //add support for arm platform by wangd 202106

#endif


    *result_low  = result1_;
    *result_high = result2_;
  }




  /*!
     *
     * Division
     *
     *
    */


  /*!
        this method calculates 64bits word a:b / 32bits c (a higher, b lower word)
        r = a:b / c and rest - remainder

        ***this method is created only on a 64bit platform***

        *
        * WARNING:
        * if r (one word) is too small for the result or c is equal zero
        * there'll be a hardware interruption (0)
        * and probably the end of your program
        *
    */
  template<uint value_size>
  void UInt<value_size>::DivTwoWords(uint a,uint b, uint c, uint * r, uint * rest)
  {
    uint r_;
    uint rest_;
    /*
            these variables have similar meaning like those in
            the multiplication algorithm MulTwoWords
        */

    TTMATH_ASSERT( c != 0 )

    #if !defined(__GNUC__) && !defined(_MSC_VER)
    #error "another compiler than GCC or Microsoft VC is currently not supported in 64bit mode, you can compile with TTMATH_NOASM macro"
    #endif


    #ifdef _MSC_VER

        ttmath_div_x64(&a,&b,c);
    r_    = a;
    rest_ = b;

#endif


#ifdef __GNUC__
//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
    if (a > 0) {  //a>0: 128bits division
        __uint128_t a_128bit = a;
        __uint128_t value = (a_128bit << 64) + b;
        __uint128_t r_tmp = value / c;
        __uint128_t r_rest = value % c;

        r_ = static_cast<uint>(r_tmp);
        rest_ = static_cast<uint>(r_rest);
    } else {  // ARM can only support 64bit division
        __asm__ (
        // r_:%0, rest_:%1, a:%2, b:%3, c:%4, d:%5, e:%6
        "mov x11, %2, lsl #32   \n"  // d= a<<32
        "add x11, x11, %3       \n"  // d =d+b
        "udiv %0, x11, %4       \n"  // r_ = d/c
        "mul x12, %0, %4        \n"  // e =r_*c
        "sub %1, x11, x12       \n"  // rest_ = d-e
        : "=&r" (r_), "=&r" (rest_)
        : "r"(a), "r"(b), "r"(c)
        : "x11", "x12", "cc");
    }
#else
//add support for arm platform by wangd 202106:e
    __asm__ (

    "divq %%rcx                \n"

    : "=a" (r_), "=d" (rest_)
      : "d" (a), "a" (b), "c" (c)
      : "cc" );
#endif //add support for arm platform by wangd 202106

#endif


    *r = r_;
    *rest = rest_;
  }

} //namespace


#endif //ifdef TTMATH_PLATFORM64
#endif //ifndef TTMATH_NOASM
#endif
#endif



/**
 * @file bsonDecimal.h
 * @brief CPP BSONObjBuilder and BSONArrayBuilder Declarations
 */

/*    Copyright 2009 10gen Inc.
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

#pragma once

//#include "ossTypes.h"
#include <string>
#include <cstring>
#include "common_decimal_type.h"

using namespace std;
/** \namespace bson
    \brief Include files for C++ BSON module
*/
namespace bson {

   #define BSONDECIMAL_TOSTRING_FULL   ( 0 )
   #define BSONDECIMAL_TOSTRING_NICE   ( 1 )
   #define BSONDECIMAL_TOSTRING_SIMPLE ( 2 )

   class CB_EXPORT bsonDecimal
   {
   public:
      bsonDecimal() ;
      bsonDecimal( const bsonDecimal &right ) ;
      ~bsonDecimal() ;

      bsonDecimal& operator= ( const bsonDecimal &right ) ;

   public:
      int32_t          init() ;
      int32_t          init( int32_t precision, int32_t scale ) ;

      void           setZero() ;
      int32_t        isZero() ;

      void           setMin() ;
      int32_t        isMin() ;

      void           setMax() ;
      int32_t        isMax() ;
      
      int32_t          fromInt( int32_t value ) ;
      int32_t          toInt( int32_t *value ) const ;

      int32_t          fromLong( int64_t value ) ;
      int32_t          toLong( int64_t *value ) const ;

      int32_t          fromDouble( double value ) ;
      int32_t          toDouble( double *value ) const ;

      int32_t          fromString( const char *value ) ;
      string         toString() const ;

      string         toJsonString() ;

      int32_t          fromBsonValue( const char *bsonValue ) ;

      int32_t          compare( const bsonDecimal &right ) const ;
      int32_t          compare( int right ) const ;

   public:
      int32_t          add( const bsonDecimal &right, bsonDecimal &result ) ;
      int32_t          add( const bsonDecimal &right ) ;
      int32_t          sub( const bsonDecimal &right, bsonDecimal &result ) ;
      int32_t          mul( const bsonDecimal &right, bsonDecimal &result ) ;
      int32_t          div( const bsonDecimal &right, bsonDecimal &result ) ;
      int32_t          div( int64_t right, bsonDecimal &result ) ;
      int32_t          abs() ;
      int32_t          ceil( bsonDecimal &result ) ;
      int32_t          floor( bsonDecimal &result ) ;
      int32_t          mod( bsonDecimal &right, bsonDecimal &result ) ;
      int32_t          updateTypemod( int32_t typemod ) ;

   public:
      /* getter */
      int16_t          getWeight() const ;
      int32_t          getTypemod() const ;
      int32_t          getPrecision( int32_t *precision, int32_t *scale ) const ;

      int32_t          getPrecision() const ;

      int16_t          getStorageScale() const ;

      int16_t          getScale() const ;
      int16_t          getSign() const ;

      int32_t          getNdigit() const ;
      const int16_t*   getDigits() const ;
      int32_t          getSize() const ;

   private:
      bson_decimal   _decimal ;
   } ;

}



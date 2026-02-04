/** @file bsonDecimal.cpp - BSON DECIMAL implementation
    http://www.mongodb.org/display/DOCS/BSON
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

#include "ob_define.h"
#include "ossMem.h"
#include "bsonDecimal.h"
#include "common_decimal_fun.h"
using namespace oceanbase::common;


#ifdef _DEBUG
#include <assert.h>
#define CB_ASSERT(cond,str)  assert(cond)
#else
#define CB_ASSERT(cond,str)
#endif // _DEBUG


namespace bson {

  bsonDecimal::bsonDecimal()
  {
    init() ;
  }

  bsonDecimal::bsonDecimal( const bsonDecimal &right )
  {
    int32_t rc = OB_SUCCESS ;
    init() ;
    rc = decimal_copy( &( right._decimal ), &_decimal ) ;
    CB_ASSERT( OB_SUCCESS == rc , "out of memory" ) ;
    (void)rc;
  }

  bsonDecimal::~bsonDecimal()
  {
    decimal_free( &_decimal ) ;
  }

  bsonDecimal& bsonDecimal::operator= ( const bsonDecimal &right )
  {
    int32_t rc = OB_SUCCESS ;
    decimal_free( &_decimal ) ;
    rc = decimal_copy( &( right._decimal ), &_decimal ) ;
    CB_ASSERT( OB_SUCCESS == rc , "out of memory" ) ;
    (void)rc;
    return *this ;
  }

  int32_t bsonDecimal::init( int32_t precision, int32_t scale )
  {
    return decimal_init1( &_decimal, precision, scale ) ;
  }

  int32_t bsonDecimal::init()
  {
    decimal_init( &_decimal ) ;
    return OB_SUCCESS ;
  }

  void bsonDecimal::setZero()
  {
    decimal_set_zero( &_decimal ) ;
  }

  int32_t bsonDecimal::isZero()
  {
    return decimal_is_zero( &_decimal ) ;
  }

  void bsonDecimal::setMin()
  {
    decimal_set_min( &_decimal ) ;
  }

  int32_t bsonDecimal::isMin()
  {
    return decimal_is_min( &_decimal ) ;
  }

  void bsonDecimal::setMax()
  {
    decimal_set_max( &_decimal ) ;
  }

  int32_t bsonDecimal::isMax()
  {
    return decimal_is_max( &_decimal ) ;
  }

  int32_t bsonDecimal::fromInt( int32_t value )
  {
    return decimal_from_int( value, &_decimal ) ;
  }

  int32_t bsonDecimal::toInt( int32_t *value ) const
  {
    if ( NULL == value )
    {
      return OB_INVALID_ARGUMENT ;
    }

    *value = decimal_to_int( &_decimal ) ;
    return OB_SUCCESS ;
  }

  int32_t bsonDecimal::fromLong( int64_t value )
  {
    return decimal_from_long( value, &_decimal ) ;
  }

  int32_t bsonDecimal::toLong( int64_t *value ) const
  {
    if ( NULL == value )
    {
      return OB_INVALID_ARGUMENT ;
    }

    *value = decimal_to_long( &_decimal ) ;
    return OB_SUCCESS ;
  }

  int32_t bsonDecimal::fromDouble( double value )
  {
    return decimal_from_double( value, &_decimal ) ;
  }

  int32_t bsonDecimal::toDouble( double *value ) const
  {
    if ( NULL == value )
    {
      return OB_INVALID_ARGUMENT ;
    }

    *value = decimal_to_double( &_decimal ) ;
    return OB_SUCCESS ;
  }

  int32_t bsonDecimal::fromString( const char *value )
  {
    return decimal_from_str( value, &_decimal ) ;
  }

  string bsonDecimal::toString() const
  {
    int32_t rc       = OB_SUCCESS ;
    char *temp     = NULL ;
    int32_t size     = 0 ;
    string result  = "" ;

    rc = decimal_to_str_get_len( &_decimal, &size ) ;
    if ( OB_SUCCESS != rc )
    {
      goto error ;
    }

    temp = (char *)CB_OSS_MALLOC( size ) ;
    if ( NULL == temp )
    {
      goto error ;
    }

    rc = decimal_to_str( &_decimal, temp, size ) ;
    if ( OB_SUCCESS != rc )
    {
      goto error ;

    }

    result = temp ;

done:
    if ( NULL != temp )
    {
      CB_OSS_FREE( temp ) ;
    }
    return result ;
error:
    goto done ;
  }

  string bsonDecimal::toJsonString()
  {
    int32_t rc       = OB_SUCCESS ;
    char *temp     = NULL ;
    int32_t size     = 0 ;
    string result  = "" ;

    rc = decimal_to_jsonstr_len( _decimal.sign, _decimal.weight,
                                 _decimal.dscale, _decimal.typemod, &size ) ;
    if ( OB_SUCCESS != rc )
    {
      goto error ;
    }

    temp = (char *)CB_OSS_MALLOC( size ) ;
    if ( NULL == temp )
    {
      goto error ;
    }

    rc = decimal_to_jsonstr( &_decimal, temp, size ) ;
    if ( OB_SUCCESS != rc )
    {
      goto error ;

    }

    result = temp ;

done:
    if ( NULL != temp )
    {
      CB_OSS_FREE( temp ) ;
    }
    return result ;
error:
    goto done ;
  }

  int32_t bsonDecimal::fromBsonValue( const char *bsonValue )
  {
    return decimal_from_bsonvalue( bsonValue, &_decimal ) ;
  }

  int32_t bsonDecimal::compare( const bsonDecimal &right ) const
  {
    return decimal_cmp( &_decimal, &( right._decimal ) ) ;
  }

  int32_t bsonDecimal::compare( int right ) const
  {
    int32_t rc = OB_SUCCESS ;
    bsonDecimal decimal ;

    rc = decimal.fromInt( right ) ;
    if ( OB_SUCCESS != rc )
    {
      return 1 ;
    }

    return compare( decimal ) ;
  }

  int32_t bsonDecimal::add( const bsonDecimal &right, bsonDecimal &result )
  {
    return decimal_add( &_decimal, &right._decimal, &result._decimal ) ;
  }

  int32_t bsonDecimal::add( const bsonDecimal &right )
  {
    int32_t rc      = OB_SUCCESS ;
    int32_t typemod = -1 ;
    bsonDecimal result ;
    rc = add( right, result ) ;
    if ( OB_SUCCESS != rc )
    {
      return rc ;
    }

    typemod = decimal_get_typemod2( &_decimal );

    rc = decimal_copy( &result._decimal, &_decimal ) ;
    if ( OB_SUCCESS != rc )
    {
      return rc ;
    }

    return decimal_update_typemod( &_decimal, typemod ) ;
  }

  int32_t bsonDecimal::sub( const bsonDecimal &right, bsonDecimal &result )
  {
    return decimal_sub( &_decimal, &right._decimal, &result._decimal ) ;
  }

  int32_t bsonDecimal::mul( const bsonDecimal &right, bsonDecimal &result )
  {
    return decimal_mul( &_decimal, &right._decimal, &result._decimal ) ;
  }

  int32_t bsonDecimal::div( const bsonDecimal &right, bsonDecimal &result )
  {
    return decimal_div( &_decimal, &right._decimal, &result._decimal ) ;
  }

  int32_t bsonDecimal::div( int64_t right, bsonDecimal &result )
  {
    int32_t rc = OB_SUCCESS ;
    bsonDecimal tmpRight ;
    rc = tmpRight.fromLong( right ) ;
    if ( OB_SUCCESS != rc )
    {
      return rc ;
    }

    return div( tmpRight, result ) ;
  }

  int32_t bsonDecimal::abs()
  {
    return decimal_abs( &_decimal ) ;
  }

  int32_t bsonDecimal::ceil( bsonDecimal &result )
  {
    int32_t rc = OB_SUCCESS ;
    rc = decimal_ceil( &_decimal, &(result._decimal) ) ;
    if ( OB_SUCCESS != rc )
    {
      return rc ;
    }

    return decimal_update_typemod( &(result._decimal), -1 ) ;
  }

  int32_t bsonDecimal::floor( bsonDecimal &result )
  {
    int32_t rc = OB_SUCCESS ;
    rc = decimal_floor( &_decimal, &(result._decimal) ) ;
    if ( OB_SUCCESS != rc )
    {
      return rc ;
    }

    return decimal_update_typemod( &(result._decimal), -1 ) ;
  }

  int32_t bsonDecimal::mod( bsonDecimal &right, bsonDecimal &result )
  {
    int32_t rc = OB_SUCCESS ;
    rc = decimal_mod( &_decimal, &(right._decimal), &(result._decimal) ) ;
    if ( OB_SUCCESS != rc )
    {
      return rc ;
    }

    return decimal_update_typemod( &(result._decimal), -1 ) ;
  }

  int32_t bsonDecimal::updateTypemod( int32_t typemod )
  {
    return decimal_update_typemod( &_decimal, typemod ) ;
  }

  int16_t bsonDecimal::getWeight() const
  {
    return static_cast<int16_t>(_decimal.weight);
  }

  int32_t bsonDecimal::getTypemod() const
  {
    return _decimal.typemod ;
  }

  int32_t bsonDecimal::getPrecision( int32_t *precision, int32_t *scale ) const
  {
    return decimal_get_typemod( &_decimal, precision, scale ) ;
  }

  int32_t bsonDecimal::getPrecision() const
  {
    int32_t rc        = OB_SUCCESS ;
    int32_t precision = -1 ;
    int32_t scale     = -1 ;

    rc = decimal_get_typemod( &_decimal, &precision, &scale ) ;
    if ( OB_SUCCESS != rc )
    {
      return -1 ;
    }

    return precision ;
  }

  int16_t bsonDecimal::getStorageScale() const
  {
    return static_cast<int16_t>(( _decimal.dscale & DECIMAL_DSCALE_MASK ) | _decimal.sign );
  }

  int16_t bsonDecimal::getScale() const
  {
    int32_t rc        = OB_SUCCESS ;
    int32_t precision = -1 ;
    int32_t scale     = -1 ;

    rc = decimal_get_typemod( &_decimal, &precision, &scale ) ;
    if ( OB_SUCCESS != rc )
    {
      return -1 ;
    }

    return static_cast<int16_t>(scale) ;
  }

  int16_t bsonDecimal::getSign() const
  {
    return static_cast<int16_t>(_decimal.sign) ;
  }

  int32_t bsonDecimal::getNdigit() const
  {
    return _decimal.ndigits ;
  }

  const int16_t* bsonDecimal::getDigits() const
  {
    return _decimal.digits ;
  }

  int32_t bsonDecimal::getSize() const
  {
    return static_cast<int32_t>( DECIMAL_HEADER_SIZE + _decimal.ndigits * sizeof( short ) ) ;
  }
}


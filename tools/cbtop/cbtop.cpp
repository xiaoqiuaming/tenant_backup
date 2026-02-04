#include "cbtop.h"
#include "bson/bsonobj.h"
#include "bson/bsonelement.h"
#include "bson/bson-inl.h"
#include "bson/bsonobjbuilder.h"
#include "ob_buffer.h"
#include "data_buffer.h"
#include "ob_result.h"
#include "ob_server.h"
#include "ob_malloc.h"
#include "updateserver/ob_sstable_mgr.h"
#include "updateserver/clog_status.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

using namespace bson;
using namespace std;
using namespace oceanbase::updateserver;


Event::Event(): buff( OB_MAX_PACKET_LENGTH )
{
  root.input.activatedPanel                 = NULL ;
  root.input.displayModeChooser             = 0 ;
  root.input.sortingWay                     = NULLSTRING;
  root.input.sortingField                   = NULLSTRING ;
  root.input.filterCondition                = NULLSTRING ;
  root.input.filterNumber                   = 0 ;
  root.input.snapshotModeChooser            = GLOBAL ;
  root.input.forcedToRefresh_Global         = NOTREFRESH ;
  root.input.forcedToRefresh_Global         = NOTREFRESH ;
  root.input.fieldPosition                  = 0 ;
  root.input.groupName                      = NULLSTRING ;
  root.input.nodeName                       = NULLSTRING ;
  root.input.confPath                       = CBTOP_DEFAULT_CONFPATH ;
  root.input.refreshInterval                = 0;
  root.input.isFirstGetSnapshot             = TRUE ;
  root.input.colourOfTheMax.foreGroundColor = COLOR_BLACK ;
  root.input.colourOfTheMax.backGroundColor = COLOR_WHITE ;
  root.input.colourOfTheMin.foreGroundColor = COLOR_BLACK ;
  root.input.colourOfTheMin.backGroundColor = COLOR_WHITE ;
  root.keySuiteLength                       = 0 ;
  root.headerLength                         = 0 ;
  root.bodyLength                           = 0 ;
  root.footerLength                         = 0;
  root.input.last_Snapshot.clear() ;
  root.input.cur_Snapshot.clear() ;
  root.input.last_absoluteMap.clear() ;
  root.input.last_averageMap.clear() ;
  root.input.last_deltaMap.clear() ;
  root.input.cur_absoluteMap.clear() ;
  root.input.cur_averageMap.clear() ;
  root.input.cur_deltaMap.clear() ;
}

Event::~Event()
{
  int32_t headerLength   = root.headerLength ;
  int32_t bodyLength     = root.bodyLength ;
  int32_t footerLength   = root.footerLength ;
  int32_t keySuiteLength = root.keySuiteLength;
  int32_t numOfSubWindow = 0 ;
  HeadTailMap *header  = NULL ;
  BodyMap *body        = NULL ;
  HeadTailMap *footer  = NULL ;
  NodeWindow *window   = NULL ;

  while( keySuiteLength ) // free the memory root.keySuite
  {
    CBTOP_SAFE_DELETE( root.keySuite[keySuiteLength -1].hotKey ) ;
    --keySuiteLength ;
  }
  if( root.keySuiteLength )
    CB_OSS_DEL []root.keySuite ;

  while( headerLength ) // free the memory root.header
  {
    header = &root.header[headerLength - 1] ;
    numOfSubWindow = header->value.numOfSubWindow;
    while( numOfSubWindow )
    {
      window = &header->value.subWindow[numOfSubWindow - 1] ;
      if( DISPLAYTYPE_DYNAMIC_EXPRESSION ==
          window->displayType )
      {
        CBTOP_SAFE_DELETE( window->displayContent.dyExOutPut.content ) ;
      }
      else if( DISPLAYTYPE_DYNAMIC_SNAPSHOT ==
               window->displayType )
      {
        CBTOP_SAFE_DELETE(
              window->displayContent.dySnapshotOutPut.fixedField ) ;
        CBTOP_SAFE_DELETE(
              window->displayContent.dySnapshotOutPut.mobileField ) ;
      }
      --numOfSubWindow ;
    }
    CBTOP_SAFE_DELETE( header->value.subWindow ) ;
    --headerLength ;
  }
  if( root.headerLength )
    CB_OSS_DEL []root.header;

  while( bodyLength ) // free the memory root.body
  {
    body = &root.body[bodyLength - 1] ;
    numOfSubWindow = body->value.numOfSubWindow ;
    while( numOfSubWindow )
    {
      window = &body->value.subWindow[numOfSubWindow - 1] ;
      if( DISPLAYTYPE_DYNAMIC_EXPRESSION ==
          window->displayType )
      {
        CBTOP_SAFE_DELETE( window->displayContent.dyExOutPut.content ) ;
      }
      else if( DISPLAYTYPE_DYNAMIC_SNAPSHOT ==
               window->displayType )
      {
        CBTOP_SAFE_DELETE(
              window->displayContent.dySnapshotOutPut.fixedField ) ;
        CBTOP_SAFE_DELETE(
              window->displayContent.dySnapshotOutPut.mobileField ) ;
      }
      --numOfSubWindow ;
    }
    CBTOP_SAFE_DELETE( body->value.subWindow ) ;
    --bodyLength ;
  }
  if( root.bodyLength )
    CBTOP_SAFE_DELETE( root.body ) ;

  while( footerLength ) // free the memory root.footer
  {
    footer = &root.footer[footerLength - 1] ;
    numOfSubWindow = footer->value.numOfSubWindow ;
    while( numOfSubWindow )
    {
      window = &footer->value.subWindow[numOfSubWindow - 1] ;
      if( DISPLAYTYPE_DYNAMIC_EXPRESSION ==
          window->displayType )
      {
        CBTOP_SAFE_DELETE( window->displayContent.dyExOutPut.content ) ;
      }
      else if( DISPLAYTYPE_DYNAMIC_SNAPSHOT ==
               window->displayType )
      {
        CBTOP_SAFE_DELETE(
              window->displayContent.dySnapshotOutPut.fixedField ) ;
        CBTOP_SAFE_DELETE(
              window->displayContent.dySnapshotOutPut.mobileField ) ;
      }
      --numOfSubWindow ;
    }
    CBTOP_SAFE_DELETE( footer->value.subWindow ) ;
    --footerLength ;
  }
  if( root.footerLength )
    CBTOP_SAFE_DELETE( root.footer ) ;

  client.destroy();
  chunkServerList.clear();
  updateServerInfoList.clear();
  snapshotResult.clear();
}

int32_t Event::assignActivatedPanel( BodyMap **activatedPanel,
                                     string bodyPanelType )
{
  int32_t rc        = OB_SUCCESS ;
  int32_t i         = 0 ;
  *activatedPanel = NULL ;
  for( i = 0; i < root.bodyLength; ++i )
  {
    if( root.body[i].bodyPanelType == bodyPanelType )
    {
      *activatedPanel = &root.body[i] ;
      break ;
    }
  }
  if( NULL == *activatedPanel )
  {
    rc = OB_ERROR ;
    goto error ;
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::assignActivatedPanelByLabelName( BodyMap **activatedPanel,
                                                string labelName )
{
  int32_t rc = OB_SUCCESS ;
  int32_t i  = 0 ;
  for( i = 0; i < root.bodyLength; ++i )
  {
    if( root.body[i].labelName== labelName )
    {
      *activatedPanel = &root.body[i] ;
      break ;
    }
  }
  if( NULL == *activatedPanel )
  {
    rc = OB_ERROR ;
    goto error ;
  }
done :
  root.input.fieldPosition = 0 ;
  return rc ;
error :
  goto done ;
}

int32_t Event::getActivatedHeadTailMap( BodyMap *activatedPanel,
                                        HeadTailMap **header,
                                        HeadTailMap **footer )
{
  int32_t rc           = OB_SUCCESS ;
  *header            = NULL ;
  *footer            = NULL ;
  int32_t index_header = 0 ;
  int32_t index_footer = 0 ;
  for( index_header = 0; index_header < root.headerLength; ++index_header )
  {
    if( root.header[index_header].key == activatedPanel->headerKey )
    {
      *header = &root.header[index_header] ;
      break ;
    }
  }
  for( index_footer = 0; index_footer < root.footerLength; ++index_footer )
  {
    if( root.footer[index_footer].key == activatedPanel->footerKey )
    {
      *footer = &root.footer[index_footer] ;
      break ;
    }
  }
  if( NULL == *header )
  {
    ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
    ossSnprintf( errStr, errStrLength,
                 "%s getActivatedHeadTailMap failed,"
                 "SDB_HEADER_NULL"OSS_NEWLINE,
                 errStrBuf ) ;
    rc = OB_ERROR;
    goto error ;
  }
  if( NULL == *footer )
  {
    if( FOOTER_NULL != activatedPanel->footerKey )
    {
      if( NULL == *header )
      {
        ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
        ossSnprintf( errStr, errStrLength,
                     "%s getActivatedHeadTailMap failed, "
                     "SDB_HEADER_FOOTER_NULL\n", errStrBuf ) ;
        rc = OB_ERROR;
        goto error ;
      }
      else
      {
        ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
        ossSnprintf( errStr, errStrLength,
                     "%s getActivatedHeadTailMap failed, "
                     "SDB_FOOTER_NULL\n", errStrBuf ) ;
        rc = OB_ERROR;
        goto error ;
      }
    }
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::getActualPosition( Position &actualPosition,
                                  Position &referPosition,
                                  const string zoomMode,
                                  const string occupyMode )
{
  int32_t rc          = OB_SUCCESS ;
  int32_t row         = 0 ;
  int32_t col         = 0 ;
  float SCALE_ROW = 0.0f ;
  getmaxyx( stdscr, row, col ) ;
  if( row < root.actualWindowMinRow || col < root.actualWindowMinColumn )
  {
    rc = OB_ERROR ;
    ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
    ossSnprintf( errStr, errStrLength,
                 "%s Minimum window size:"
                 "%dx%d, found %dx%d"OSS_NEWLINE,
                 errStrBuf, root.actualWindowMinRow,
                 root.actualWindowMinColumn, row, col ) ;
    rc = OB_ERROR ;
    goto error ;
  }
  SCALE_ROW = (float)row / (float)root.referWindowRow ;
  if( zoomMode == ZOOM_MODE_ALL )
  {
    actualPosition.length_X =
        ( int32_t )(static_cast<float>
                    (referPosition.length_X) * (float)col /
                     ( float )root.referWindowColumn ) ;
    actualPosition.length_Y =
        ( int32_t )( static_cast<float>(referPosition.length_Y) * (float)row /
                     ( float )root.referWindowRow ) ;
    actualPosition.referUpperLeft_X =
        ( int32_t )( static_cast<float>(referPosition.referUpperLeft_X) * ( float )col /
                     ( float )root.referWindowColumn) ;
    actualPosition.referUpperLeft_Y =
        ( int32_t )( static_cast<float>(referPosition.referUpperLeft_Y) * ( float )row /
                     ( float)root.referWindowRow ) ;
  }
  else if( zoomMode == ZOOM_MODE_NONE )
  {
    actualPosition.length_X = referPosition.length_X ;
    actualPosition.length_Y = referPosition.length_Y ;
    actualPosition.referUpperLeft_X = referPosition.referUpperLeft_X ;
    actualPosition.referUpperLeft_Y = referPosition.referUpperLeft_Y ;
  }
  else if( zoomMode == ZOOM_MODE_POS )
  {
    actualPosition.length_X = referPosition.length_X ;
    actualPosition.length_Y = referPosition.length_Y ;
    actualPosition.referUpperLeft_X =
        ( int32_t )(static_cast<float>( referPosition.referUpperLeft_X) * ( float )col /
                                         ( float )root.referWindowColumn ) ;
    actualPosition.referUpperLeft_Y =
        ( int32_t )(static_cast<float>( referPosition.referUpperLeft_Y) * ( float )row /
                                         ( float )root.referWindowRow ) ;
  }
  else if( zoomMode == ZOOM_MODE_ROW_POS )
  {
    actualPosition.length_X = referPosition.length_X ;
    actualPosition.length_Y =
        ( int32_t )(static_cast<float>( referPosition.length_Y) * SCALE_ROW ) ;
    actualPosition.referUpperLeft_X =
        ( int32_t )(static_cast<float>( referPosition.referUpperLeft_X) * ( float )col /
                                         ( float )root.referWindowColumn ) ;
    actualPosition.referUpperLeft_Y =
        ( int32_t )(static_cast<float>( referPosition.referUpperLeft_Y) * ( float )row /
                                         ( float)root.referWindowRow ) ;
  }
  else if( zoomMode == ZOOM_MODE_COL_POS )
  {
    actualPosition.length_X =
        ( int32_t )(static_cast<float>( referPosition.length_X) * ( float )col /
                                         ( float )root.referWindowColumn ) ;
    actualPosition.length_Y = referPosition.length_Y ;
    actualPosition.referUpperLeft_X=
        ( int32_t )(static_cast<float>( referPosition.referUpperLeft_X) * ( float )col /
                                         ( float )root.referWindowColumn ) ;
    actualPosition.referUpperLeft_Y=
        ( int32_t )(static_cast<float>( referPosition.referUpperLeft_Y) * ( float )row /
                                         ( float )root.referWindowRow ) ;
  }
  else if( zoomMode == ZOOM_MODE_POS_X )
  {
    actualPosition.length_X = referPosition.length_X ;
    actualPosition.length_Y = referPosition.length_Y ;
    actualPosition.referUpperLeft_X=
        ( int32_t )(static_cast<float>( referPosition.referUpperLeft_X) * ( float )col /
                                         (float)root.referWindowColumn ) ;
    actualPosition.referUpperLeft_Y= referPosition.referUpperLeft_Y ;
  }
  else if( zoomMode == ZOOM_MODE_ROW_POS_X )
  {
    actualPosition.length_X = referPosition.length_X ;
    actualPosition.length_Y =
        ( int32_t )(static_cast<float>( referPosition.length_Y) * SCALE_ROW ) ;
    actualPosition.referUpperLeft_X=
        ( int32_t )(static_cast<float>( referPosition.referUpperLeft_X) * ( float )col /
                                         ( float )root.referWindowColumn ) ;
    actualPosition.referUpperLeft_Y= referPosition.referUpperLeft_Y ;
  }
  else if( zoomMode == ZOOM_MODE_COL_POS_X )
  {
    actualPosition.length_X =
        ( int32_t )(static_cast<float>( referPosition.length_X) * ( float )col /
                                         ( float )root.referWindowColumn ) ;
    actualPosition.length_Y = referPosition.length_Y ;
    actualPosition.referUpperLeft_X =
        ( int32_t )(static_cast<float>( referPosition.referUpperLeft_X) * ( float )col /
                                         ( float )root.referWindowColumn ) ;
    actualPosition.referUpperLeft_Y = referPosition.referUpperLeft_Y ;
  }
  else if( zoomMode == ZOOM_MODE_POS_Y )
  {
    actualPosition.length_X = referPosition.length_X ;
    actualPosition.length_Y = referPosition.length_Y ;
    actualPosition.referUpperLeft_X= referPosition.referUpperLeft_X ;
    actualPosition.referUpperLeft_Y=
        ( int32_t )(static_cast<float>( referPosition.referUpperLeft_Y) * ( float )row /
                                         ( float )root.referWindowRow ) ;
  }
  else if( zoomMode == ZOOM_MODE_ROW_POS_Y )
  {
    actualPosition.length_X = referPosition.length_X ;
    actualPosition.length_Y =
        ( int32_t )(static_cast<float>( referPosition.length_Y) * SCALE_ROW ) ;
    actualPosition.referUpperLeft_X= referPosition.referUpperLeft_X ;
    actualPosition.referUpperLeft_Y=
        ( int32_t )(static_cast<float>( referPosition.referUpperLeft_Y) * ( float )row /
                                         ( float )root.referWindowRow ) ;
  }
  else if( zoomMode == ZOOM_MODE_COL_POS_Y )
  {
    actualPosition.length_X =
        ( int32_t )(static_cast<float>( referPosition.length_X) * (float)col /
                                         ( float )root.referWindowColumn ) ;
    actualPosition.length_Y = referPosition.length_Y ;
    actualPosition.referUpperLeft_X = referPosition.referUpperLeft_X ;
    actualPosition.referUpperLeft_Y =
        ( int32_t )(static_cast<float>( referPosition.referUpperLeft_Y) * ( float )row /
                                         ( float )root.referWindowRow ) ;
  }
  else if( zoomMode == ZOOM_MODE_ROW_COL )
  {
    actualPosition.length_X =
        ( int32_t )(static_cast<float>( referPosition.length_X) * ( float )col /
                                         ( float )root.referWindowColumn ) ;
    actualPosition.length_Y =
        ( int32_t )(static_cast<float>( referPosition.length_Y) * ( float )row /
                                         ( float )root.referWindowRow ) ;
    actualPosition.referUpperLeft_X = referPosition.referUpperLeft_X ;
    actualPosition.referUpperLeft_Y = referPosition.referUpperLeft_Y ;
  }
  else if( zoomMode == ZOOM_MODE_COL )
  {
    actualPosition.length_X =
        ( int32_t )(static_cast<float>( referPosition.length_X) * ( float )col /
                                         ( float )root.referWindowColumn ) ;
    actualPosition.length_Y = referPosition.length_Y ;
    actualPosition.referUpperLeft_X = referPosition.referUpperLeft_X ;
    actualPosition.referUpperLeft_Y = referPosition.referUpperLeft_Y ;
  }
  else if( zoomMode == ZOOM_MODE_ROW )
  {
    actualPosition.length_X = referPosition.length_X ;
    actualPosition.length_Y =
        ( int32_t )(static_cast<float>( referPosition.length_Y) * ( float )row /
                                         ( float )root.referWindowRow ) ;
    actualPosition.referUpperLeft_X = referPosition.referUpperLeft_X ;
    actualPosition.referUpperLeft_Y = referPosition.referUpperLeft_Y ;
  }
  else
  {
    rc = OB_ERROR ;
    ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
    ossSnprintf( errStr, errStrLength,
                 "%s getActualPosition failed:"
                 "wrong zoomMode:%s"OSS_NEWLINE,
                 errStrBuf, zoomMode.c_str() ) ;
    rc = OB_ERROR ;
    goto error ;
  }
  if( occupyMode != OCCUPY_MODE_NONE )
  {
    if( occupyMode == OCCUPY_MODE_WINDOW_BELOW )
    {
      actualPosition.length_Y = row - actualPosition.referUpperLeft_Y ;
    }
    else
    {
      rc = OB_ERROR ;
      ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,
                   "%s getActualPosition failed:"
                   "wrong occupyMode:%s"OSS_NEWLINE,
                   errStrBuf, occupyMode.c_str() ) ;
      rc = OB_ERROR ;
      goto error ;
    }
  }
done :
  return rc ;
error :
  goto done ;
}


int32_t Event::getActivatedKeySuite( KeySuite **keySuite )
{
  int32_t rc = OB_SUCCESS ;
  int32_t i  = 0 ;
  try
  {
    for( i = 0; i < root.keySuiteLength; ++i )
    {
      if( root.keySuite[i].mark ==
          root.input.activatedPanel->hotKeySuiteType )
      {
        break ;
      }
    }
    if( i != root.keySuiteLength )
    {
      keySuite[0] = root.keySuite ;
      goto done ;
    }
    else
    {
      *keySuite = NULL ;
      rc = OB_ERROR ;
      goto error ;
    }
  }
  catch( std::exception &e )
  {
    ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
    ossSnprintf( errStr, errStrLength,
                 "%s getActivatedKeySuite failed,"
                 "e.what():%s"OSS_NEWLINE,
                 errStrBuf, e.what() ) ;
    rc = OB_ERROR ;
    goto error ;
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::mvprintw_CBTOP( const string &expression, int32_t expressionLength,
                               const string &alignment, int32_t start_row,
                               int32_t start_col )
{
  int32_t rc           = OB_SUCCESS ;

  rc = formattingOutput( cbtopBuffer, expressionLength, expression.c_str() ) ;
  if( rc )
  {
    ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
    ossSnprintf( errStr, errStrLength,
                 "%s MVPRINTW_TOP failed,"
                 "SNPRINTF_TOP failed"OSS_NEWLINE,
                 errStrBuf ) ;
    goto error ;
  }
  rc = MVPRINTW( start_row, start_col, expressionLength, cbtopBuffer,
                 alignment ) ;
  if( rc )
  {
    rc = OB_ERROR ;
    goto error ;
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::mvprintw_CBTOP( const char *expression, int32_t expressionLength,
                               const string &alignment, int32_t start_row,
                               int32_t start_col )
{
  int32_t rc           = OB_SUCCESS ;

  rc = formattingOutput( cbtopBuffer, expressionLength, expression ) ;
  if( rc )
  {
    ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
    ossSnprintf( errStr, errStrLength,
                 "%s MVPRINTW_TOP failed,"
                 "SNPRINTF_TOP failed"OSS_NEWLINE,
                 errStrBuf ) ;
    goto error ;
  }
  rc = MVPRINTW( start_row, start_col, expressionLength, cbtopBuffer,
                 alignment ) ;
  if( rc )
  {
    rc = OB_ERROR ;
    goto error ;
  }
done :
  return rc ;
error :
  goto done ;
}

void Event::getColourPN( Colours colour, int32_t &colourPairNumber )
{
  colourPairNumber = colour.foreGroundColor + colour.backGroundColor *
                     COLOR_MULTIPLE ;
}

int32_t Event::getResultFromBSONObj( const BSONObj &bsonobj,
                                     const string &sourceField,
                                     const string &displayMode,
                                     string &result, int32_t canSwitch,
                                     const string &baseField,
                                     const FiledWarningValue &waringValue,
                                     int32_t &colourPairNumber )
{
  int32_t rc                          = OB_SUCCESS ;
  uint32_t pos_last                   = 0 ;
  const char *sourceFieldbuf        = sourceField.c_str();
  const char *baseFieldbuf          = baseField.c_str();
  InputPanel &input                 = root.input ;
  const int64_t absoluteMaxLimitValue = waringValue.absoluteMaxLimitValue ;
  const int64_t absoluteMinLimitValue = waringValue.absoluteMinLimitValue ;
  const int64_t averageMaxLimitValue  = waringValue.averageMaxLimitValue ;
  const int64_t averageMinLimitValue  = waringValue.averageMinLimitValue ;
  const int64_t deltaMaxLimitValue    = waringValue.deltaMaxLimitValue ;
  const int64_t deltaMinLimitValue    = waringValue.deltaMinLimitValue ;
  double elementDouble             = 0.0f ;
  BSONElement element ;
  BSONElement last_element ;
  BSONElement baseElement  ;
  BSONElement baseElement_last ;
  string new_                       = NULLSTRING ;
  string old_                       = NULLSTRING ;
  int32_t maxPairNumber               = 0 ;
  int32_t minPairNumber               = 0 ;
  int32_t changePairNumber            = 0 ;
  getColourPN( input.colourOfTheMax, maxPairNumber ) ;
  getColourPN( input.colourOfTheMin, minPairNumber ) ;
  getColourPN( input.colourOfTheChange, changePairNumber ) ;
  try
  {
    element = bsonobj.getFieldDotted( sourceFieldbuf ) ;
    if( !canSwitch )
    {
      if( element.isNumber() )
      {
        elementDouble = element.Number() ;
        if( NumberLong == element.type() || NumberInt == element.type() )
        {
          result = element.toString( FALSE ) ;
        }
        else
        {
          ossSnprintf( cbtopBuffer, BUFFERSIZE,
                       OUTPUT_FORMATTING, elementDouble ) ;
          result = cbtopBuffer ;
        }
        if( absoluteMaxLimitValue != 0 &&
            elementDouble > absoluteMaxLimitValue )
        {
          colourPairNumber = maxPairNumber ;
        }
        else if( absoluteMinLimitValue != 0 &&
                 elementDouble < absoluteMinLimitValue )
        {
          colourPairNumber = minPairNumber ;
        }
      }
      else
      {
        result = element.toString( FALSE ) ;
      }
      input.cur_absoluteMap[new_+sourceField] = result ;
    }
    else
    {
      baseElement = bsonobj.getFieldDotted( baseFieldbuf ) ;
      new_ = baseElement.toString( FALSE ) ;
      while( pos_last < input.last_Snapshot.size() )
      {
        baseElement_last =
            input.last_Snapshot[pos_last].getFieldDotted(
              baseFieldbuf ) ;
        old_ = baseElement_last.toString( FALSE ) ;
        if( new_ == old_ )
          break ;
        ++pos_last ;
      }
      if( pos_last == input.last_Snapshot.size() )
      {
        if( DELTA == displayMode || AVERAGE == displayMode )
        {
          ossSnprintf( cbtopBuffer, BUFFERSIZE, "%d", 0 ) ;
          result = cbtopBuffer ;
          input.cur_deltaMap[new_+sourceField] = cbtopBuffer ;
          input.cur_averageMap[new_+sourceField] = cbtopBuffer ;
        }
        else if( ABSOLUTE == displayMode )
        {
          if( element.isNumber() )
          {
            elementDouble = element.Number() ;
            if( NumberLong == element.type() ||
                NumberInt == element.type() )
            {
              result = element.toString( FALSE ) ;
            }
            else
            {
              ossSnprintf( cbtopBuffer, BUFFERSIZE,
                           OUTPUT_FORMATTING, elementDouble ) ;
              result = cbtopBuffer ;
            }
            if( absoluteMaxLimitValue != 0 &&
                elementDouble > absoluteMaxLimitValue )
            {
              colourPairNumber = maxPairNumber ;
            }
            else if( absoluteMinLimitValue != 0 &&
                     elementDouble < absoluteMinLimitValue )
            {
              colourPairNumber = minPairNumber ;
            }
          }
          else
          {
            result = element.toString( FALSE ) ;
          }
          input.cur_absoluteMap[new_+sourceField] = result ;
        }
        else
        {
          ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
          ossSnprintf( errStr, errStrLength,
                       "%s getResultFromBSONobj failed,"
                       "displayMode = %s"OSS_NEWLINE,
                       errStrBuf, displayMode.c_str() ) ;
          rc = OB_ERROR ;
          goto error ;
        }
      }
      else
      {
        last_element =
            input.last_Snapshot[pos_last].getFieldDotted(
              sourceFieldbuf ) ;
        if( DELTA == displayMode )
        {
          if( element.isNumber() )
          {
            elementDouble = element.Number() - last_element.Number() ;
            if( element.type() == NumberInt ||
                element.type() == NumberLong )
            {
              ossSnprintf( cbtopBuffer, BUFFERSIZE, "%d",
                           element.Long() - last_element.Long() ) ;
            }
            else
            {
              ossSnprintf( cbtopBuffer, BUFFERSIZE,
                           OUTPUT_FORMATTING, elementDouble ) ;
            }
            result = cbtopBuffer ;
            if( deltaMaxLimitValue != 0 &&
                elementDouble > deltaMaxLimitValue )
            {
              colourPairNumber = maxPairNumber ;
            }
            else if( deltaMinLimitValue!= 0 &&
                     elementDouble < deltaMinLimitValue )
            {
              colourPairNumber = minPairNumber ;
            }
            else if( isExist( input.last_deltaMap, new_+sourceField ) &&
                     result != input.last_deltaMap[new_+sourceField] )
            {
              colourPairNumber = changePairNumber ;
            }
          }
          else
          {
            result = element.toString( FALSE ) ;
          }
          input.cur_deltaMap[new_+sourceField] = result ;
        }
        else if( ABSOLUTE == displayMode )
        {
          if( element.isNumber() )
          {
            elementDouble = element.Number() ;
            if( NumberLong == element.type() ||
                NumberInt == element.type() )
            {
              result = element.toString( FALSE ) ;
            }
            else
            {
              ossSnprintf( cbtopBuffer, BUFFERSIZE,
                           OUTPUT_FORMATTING, elementDouble ) ;
              result = cbtopBuffer ;
            }
            if( absoluteMaxLimitValue != 0 &&
                elementDouble > absoluteMaxLimitValue )
            {
              colourPairNumber = maxPairNumber ;
            }
            else if( absoluteMinLimitValue != 0 &&
                     elementDouble < absoluteMinLimitValue )
            {
              colourPairNumber = minPairNumber ;
            }
            else if(
                    isExist( input.last_absoluteMap, new_+sourceField ) &&
                    result != input.last_absoluteMap[new_+sourceField] )
            {
              colourPairNumber = changePairNumber ;
            }
          }
          else
          {
            result = element.toString( FALSE ) ;
          }
          input.cur_absoluteMap[new_+sourceField] = result ;
        }
        else if( AVERAGE == displayMode )
        {
          if( element.isNumber() )
          {
            elementDouble =
                ( element.Number() - last_element.Number() ) /
                root.input.refreshInterval ;
            ossSnprintf( cbtopBuffer, BUFFERSIZE,
                         OUTPUT_FORMATTING, elementDouble ) ;
            result = cbtopBuffer ;
            if( averageMaxLimitValue!= 0 &&
                elementDouble > averageMaxLimitValue )
            {
              colourPairNumber = maxPairNumber ;
            }
            else if( averageMinLimitValue!= 0 &&
                     elementDouble < averageMinLimitValue )
            {
              colourPairNumber = minPairNumber ;
            }
            else if(
                    isExist( input.last_averageMap, new_+sourceField ) &&
                    result != input.last_averageMap[new_+sourceField] )
            {
              colourPairNumber = changePairNumber ;
            }
          }
          else
          {
            result = element.toString( FALSE ) ;
          }
          input.cur_averageMap[new_+sourceField] = result ;
        }
        else
        {
          ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
          ossSnprintf( errStr, errStrLength,
                       "%s getResultFromBSONobj failed,"
                       "displayMode = %s"OSS_NEWLINE,
                       errStrBuf, displayMode.c_str() ) ;
          rc = OB_ERROR ;
          goto error ;
        }
      }
    }
  }
  catch( std::exception &e )
  {
    ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
    ossSnprintf( errStr, errStrLength,
                 "%s getResultFromBSONobj failed, e.what():%s ,"
                 "sourceField = %s"OSS_NEWLINE,
                 errStrBuf, e.what(), sourceField.c_str() ) ;
    rc = OB_ERROR ;
    goto error ;
  }

  doubleQuotesTrim( result ) ;
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::getExpression( string& expression, string& result )
{
  int32_t rc  = OB_SUCCESS ;
  if( EXPRESSION_BODY_LABELNAME == expression )
  {
    result = root.input.activatedPanel->labelName ;
  }
  else if( EXPRESSION_REFRESH_QUIT_HELP == expression )
  {
    result = CBTOP_REFRESH_QUIT_HELP ;
  }
  else if( EXPRESSION_REFRESH_TIME == expression )
  {
    ossSnprintf( cbtopBuffer, BUFFERSIZE, "%d", root.input.refreshInterval ) ;
    result = cbtopBuffer ;
  }
  else if(EXPRESSION_REFRESH_STATE == expression)
  {
    if (root.input.refreshState == 0)
    {
      ossSnprintf(cbtopBuffer, BUFFERSIZE, "%s", "PAUSE     ");
    }
    else
    {
      ossSnprintf(cbtopBuffer, BUFFERSIZE, "%s", "REFRESHING");
    }
    result = cbtopBuffer;
  }
  else if(EXPRESSION_LOCAL_TIME == expression)
  {
    time_t t;
    struct tm *local;
    int32_t microSecond = 0;
    int64_t rsLocalTime = 0;
    char timeBuffer[20] = {'\0'};
    if(OB_SUCCESS != (rc = getRootServerLocalTime(rsLocalTime)))
    {
      YYSYS_LOG(WARN, "getRootServerLocalTime() error, rc=%d", rc);
      goto error;
    }
    t = rsLocalTime / 1000000;
    local = localtime(&t);
    microSecond = static_cast<int32_t>(rsLocalTime % 1000000);
    strftime(timeBuffer, 20, "%Y-%m-%d %H:%M:%S", local);
    ossSnprintf(cbtopBuffer, BUFFERSIZE, "%s.%d", timeBuffer, microSecond);
    result = cbtopBuffer;
  }
  else if( EXPRESSION_HOSTNAME == expression )
  {
    result = hostname ;
  }
  else if( EXPRESSION_SERVICENAME == expression )
  {
    result = serviceName ;
  }
  else if( EXPRESSION_FILTER_NUMBER == expression )
  {
    ossSnprintf( cbtopBuffer, BUFFERSIZE, "%d", root.input.filterNumber ) ;
    result = cbtopBuffer ;
  }
  else if( EXPRESSION_SORTINGWAY == expression )
  {
    result = root.input.sortingWay ;
  }
  else if( EXPRESSION_SORTINGFIELD == expression )
  {
    result = root.input.sortingField ;
  }
  else if( EXPRESSION_SNAPSHOTMODE_INPUTNAME == expression )
  {
    if( GLOBAL == root.input.snapshotModeChooser )
      result = STRING_NULL ;
    else if( GROUP == root.input.snapshotModeChooser )
      result = root.input.groupName ;
    else if( NODE == root.input.snapshotModeChooser )
      result = root.input.nodeName ;
    else
      result = STRING_NULL ;
  }
  else if( EXPRESSION_DISPLAYMODE == expression )
  {
    result = DISPLAYMODECHOOSER[root.input.displayModeChooser] ;
  }
  else if( EXPRESSION_SNAPSHOTMODE == expression )
  {
    result = root.input.snapshotModeChooser ;
  }
  else
  {
    ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
    ossSnprintf( errStr, errStrLength,
                 "%s getExpression failed,"
                 "wrong expression:%s"OSS_NEWLINE,
                 errStrBuf, expression.c_str() ) ;
    rc = OB_ERROR ;
    goto error ;
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::getRootServerLocalTime(int64_t &rsLocalTime)
{
  int32_t rc = OB_SUCCESS;
  ObResultCode resultCode;
  ObDataBuffer dataBuffer(buff.ptr(), buff.capacity());
  ObClientManager clientManager = client.get_client_mgr();

  char rsAddress[30] = {'\0'};
  snprintf(rsAddress, 30, "%d.%d.%d.%d:%d@%d",
           rootServerMaster.get_ipv4()        & 0xFF,
           (rootServerMaster.get_ipv4() >> 8 ) & 0xFF,
           (rootServerMaster.get_ipv4() >> 16) & 0xFF,
           (rootServerMaster.get_ipv4() >> 24) & 0xFF,
           rootServerMaster.get_port(),
           rootServerMaster.cluster_id_);

  if (OB_SUCCESS == rc)
  {
    rc = clientManager.send_request(rootServerMaster, OB_RS_GET_LOCAL_TIMESTAMP,
                                    DEFAULT_VERSION, DEFAULT_TIMEOUT, dataBuffer);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "send request to root server to get local time failed, root server:%s, ret[%d]",
                rsAddress, rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  if(OB_SUCCESS == rc)
  {
    dataBuffer.get_position() = 0;
    rc = resultCode.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                dataBuffer.get_position());
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "ObResultCode deserialize error, rc=%d", rc);
      rc = OB_ERROR;
      goto error;
    }
  }
  if(OB_SUCCESS == rc)
  {
    dataBuffer.get_position() = 0;
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &rsLocalTime);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode root server local time fail, rc=%d", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "root server local time is %ld", rsLocalTime);
    }
  }
  goto done;

done:
  return rc;
error:
  goto done;
}

int32_t Event::getChunkServerList(int64_t &maxMergeDurationTimeout)
{
  int32_t rc = OB_SUCCESS;
  ObResultCode resultCode;
  ObDataBuffer dataBuffer(buff.ptr(), buff.capacity());
  ObClientManager clientManager = client.get_client_mgr();
  ObServer cs;
  int32_t serverInfoIndex = 0;
  int32_t csNumber = 0;

  if (OB_SUCCESS == rc)
  {
    rc = clientManager.send_request(rootServerMaster, OB_RS_FETCH_CS_INFO,
                                    DEFAULT_VERSION, DEFAULT_TIMEOUT, dataBuffer);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "send request to root server to get chunk server list failed! ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
  }
  if(OB_SUCCESS == rc)
  {
    dataBuffer.get_position() = 0;
    rc = resultCode.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                dataBuffer.get_position());
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "ObResultCode deserialize error, rc=%d", rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi32(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &csNumber);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode cs num fail:ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "cs server number[%d]", csNumber);
    }
  }

  for(int32_t i = 0; (i < csNumber) && (OB_SUCCESS == rc); i++)
  {
    rc = cs.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                        dataBuffer.get_position());
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "deserialize chunk server fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      rc = serialization::decode_vi32(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                      dataBuffer.get_position(), &serverInfoIndex);
      if(OB_SUCCESS != rc)
      {
        YYSYS_LOG(WARN, "decode chunk server index fail, ret[%d]", rc);
        rc = OB_ERROR;
        goto error;
      }
      else
      {
        chunkServerList.insert(map<oceanbase::common::ObServer, int32_t>::value_type(cs, serverInfoIndex));
      }
    }
  }
  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &maxMergeDurationTimeout);
    if(OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode max merge duration timeout fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "max merge duration timeout[%ld]", maxMergeDurationTimeout);
    }
  }

done:
  return rc;
error:
  goto done;
}

int32_t Event::getChunkServerMergeStatus(const ObServer &cs, const int32_t &index, const int64_t &maxMergeDurationTimeout)
{
  int32_t rc = OB_SUCCESS;
  BSONObj bsonObj;
  BSONObjBuilder bsonObjBuilder;

  ObResultCode resultCode;
  ObDataBuffer dataBuffer(buff.ptr(), buff.capacity());
  ObClientManager clientManager = client.get_client_mgr();

  int64_t mergeStartTime           = 0;
  int64_t totalTabletNumber        = 0;
  int64_t mergedTabletNumber       = 0;
  int64_t servingVersion           = 0;
  int64_t mergingVersion           = 0;
  int64_t latestFrozenVersion      = 0;
  int32_t versionDifference        = 0;
  int32_t totalProgressBarLength   = 20;
  int32_t currentProgressBarLength = 0;
  double mergeProgressPercent      = 0.0;

  char mergeStartTimeString[30] = {'\0'};
  char remainingTimeString[30]  = {'\0'};
  char timeBuffer[20]           = {'\0'};
  char mergeProgressBuffer[30]  = {'\0'};
  char mergeProgressBar[21]     = {'\0'};
  time_t t;
  struct tm *local;
  int64_t currentTime = yysys::CTimeUtil::getTime();
  int64_t remainingTime = 0;
  int32_t hour        = 0;
  int32_t minute      = 0;
  int32_t second      = 0;
  int32_t microSecond = 0;

  char csAddress[30] = {'\0'};
  snprintf(csAddress, 30, "%d.%d.%d.%d:%d@%d",
           cs.get_ipv4()        & 0xFF,
           (cs.get_ipv4() >> 8 ) & 0xFF,
           (cs.get_ipv4() >> 16) & 0xFF,
           (cs.get_ipv4() >> 24) & 0xFF,
           cs.get_port(),
           cs.cluster_id_);

  if (OB_SUCCESS == rc)
  {
    rc = clientManager.send_request(cs, OB_CS_FETCH_MERGE_STAT,
                                    DEFAULT_VERSION, DEFAULT_TIMEOUT, dataBuffer);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "send request to chunk server to get chunk server merge stat failed! ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
  }
  if(OB_SUCCESS == rc)
  {
    dataBuffer.get_position() = 0;
    rc = resultCode.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                dataBuffer.get_position());
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "ObResultCode deserialize error, rc=%d", rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &mergeStartTime);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode merge start time fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "merge start time[%ld]", mergeStartTime);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &totalTabletNumber);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode total tablet number fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "total tablet number[%ld]", totalTabletNumber);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &mergedTabletNumber);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode merged tablet number fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "mergef tablet number[%ld]", mergedTabletNumber);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &servingVersion);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode serving version fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "serving version [%ld]", servingVersion);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &mergingVersion);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode merging version fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "merging version [%ld]", mergingVersion);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &latestFrozenVersion);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode latest frozen version fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "latest frozen version [%ld]", latestFrozenVersion);
    }
  }

  t = mergeStartTime / 1000000;
  local = localtime(&t);
  microSecond = static_cast<int32_t>(mergeStartTime % 1000000);
  strftime(timeBuffer, 20, "%Y-%m-%d %H:%M:%S", local);
  snprintf(mergeStartTimeString, 30, "%s.%d", timeBuffer, microSecond);

  if ((currentTime - mergeStartTime <= maxMergeDurationTimeout) && (totalTabletNumber != mergedTabletNumber))
  {
    remainingTime = mergeStartTime + maxMergeDurationTimeout - currentTime;
  }
  else // tablet merge doesn't start or already finished
  {
    remainingTime = maxMergeDurationTimeout;
  }
  microSecond = static_cast<int32_t>(remainingTime % 1000000);
  remainingTime = remainingTime / 1000000;
  hour    = static_cast<int32_t>(remainingTime / 3600);
  minute  = static_cast<int32_t>((remainingTime - (hour * 3600)) / 60);
  second  = static_cast<int32_t>(remainingTime - (hour * 3600) - (minute * 60));
  YYSYS_LOG(DEBUG, "remaingTime:%ld, hour:%d, minute:%d, second:%d", remainingTime, hour, minute, second);
  if(hour > 0)
  {
    snprintf(remainingTimeString, 30, "%d:%d:%d.%d", hour, minute, second, microSecond);
  }
  else if(minute > 0)
  {
    snprintf(remainingTimeString, 30, "0:%d:%d.%d", minute, second, microSecond);
  }
  else
  {
    snprintf(remainingTimeString, 30, "0:0:%d.%d", second, microSecond);
  }

  versionDifference = static_cast<int32_t>(abs(latestFrozenVersion - servingVersion));
  if(totalTabletNumber != 0)
  {
    if(versionDifference != 0)
    {
      if(latestFrozenVersion != 0)
      {
        mergeProgressPercent = (static_cast<double>(mergedTabletNumber) * 1.0) / static_cast<double>(totalTabletNumber) / versionDifference;
      }
      else //cluster bootstrap
      {
        mergeProgressPercent = 1.0;
      }
    }
    else //latestFrozenVersion == servingVersion
    {
      mergeProgressPercent = 1.0;
    }
  }
  else
  {
    mergeProgressPercent = 0;
  }
  currentProgressBarLength = int(mergeProgressPercent * totalProgressBarLength);
  for (int i = 0; i < totalProgressBarLength; i++)
  {
    if (i < currentProgressBarLength)
    {
      mergeProgressBar[i]= '=';
    }
    else if(i == currentProgressBarLength)
    {
      mergeProgressBar[i]= '>';
    }
    else
    {
      mergeProgressBar[i]= ' ';
    }
  }
  mergeProgressBar[totalProgressBarLength] = '\0';
  snprintf(mergeProgressBuffer, 30, "[%s][%d%%]", mergeProgressBar, int(mergeProgressPercent * 100));
  bsonObjBuilder.append("ChunkServerID", csAddress);
  bsonObjBuilder.append("ServerIndex", (int)index);
  bsonObjBuilder.append("MergeProgress", mergeProgressBuffer);
  bsonObjBuilder.append("ServingVersion", servingVersion);
  bsonObjBuilder.append("MergingVersion", mergingVersion);
  bsonObjBuilder.append("LatestFrozenVersion", latestFrozenVersion);
  bsonObjBuilder.append("MergeStartTime", mergeStartTimeString);
  bsonObjBuilder.append("RemainingTime", remainingTimeString);
  bsonObjBuilder.append("TotalTabletNumber", totalTabletNumber);
  bsonObjBuilder.append("MergedTabletNumber", mergedTabletNumber);
  bsonObj = bsonObjBuilder.obj();

  snapshotResult.push_back(bsonObj);

done:
  return rc;
error:
  goto done;
}

int32_t Event::getWholeIndexBuildInfoFromRootServer(BSONObjBuilder &bsonObjBuilder)
{
  int32_t rc = OB_SUCCESS;

  ObResultCode resultCode;
  ObDataBuffer dataBuffer(buff.ptr(), buff.capacity());
  ObClientManager clientManager = client.get_client_mgr();

  int64_t indexMergeStartTime = 0;
  int64_t singalIndexMergeStartTime = 0;
  int64_t maxCreateSignalIndexTimeout = 0;
  int32_t totalIndexNumber = 0;
  int64_t buildSuccessNumber = 0;
  int64_t buildFailNumber = 0;
  int64_t currentBuildIndexTableID = OB_INVALID_ID;
  ObString currentBuildIndexTableName;
  int64_t indexBuildPhase = -1;

  char indexMergeStartTimeString[30] = {'\0'};
  char currentIndexStartTimeString[30] = {'\0'};
  char remainingTimeString[30] = {'\0'};
  char timeBuffer[20] = {'\0'};
  time_t t;
  struct tm *local;
  int64_t currentTime = yysys::CTimeUtil::getTime();
  int64_t remainingTime = 0;
  int32_t hour = 0;
  int32_t minute = 0;
  int32_t second = 0;
  int32_t microSecond = 0;

  char currentBuildIndexTableNameString[60] = {'\0'};
  char indexBuildPhaseString[10] = {'\0'};

  char rsAddress[30] = {'\0'};
  snprintf(rsAddress, 30, "%d.%d.%d.%d:%d@%d",
           rootServerMaster.get_ipv4()        & 0xFF,
           (rootServerMaster.get_ipv4() >> 8 ) & 0xFF,
           (rootServerMaster.get_ipv4() >> 16) & 0xFF,
           (rootServerMaster.get_ipv4() >> 24) & 0xFF,
           rootServerMaster.get_port(),
           rootServerMaster.cluster_id_);

  if (OB_SUCCESS == rc)
  {
    rc = clientManager.send_request(rootServerMaster, OB_RS_FETCH_INDEX_MERGE_STAT,
                                    DEFAULT_VERSION, DEFAULT_TIMEOUT, dataBuffer);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "send request to root server to get index build status failed, root server:%s, ret[%d]", rsAddress, rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  if(OB_SUCCESS == rc)
  {
    dataBuffer.get_position() = 0;
    rc = resultCode.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                dataBuffer.get_position());
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "ObResultCode deserialize error, rc=%d", rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &indexMergeStartTime);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode index merge start time fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "index merge start time[%ld]", indexMergeStartTime);
    }
  }
  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &singalIndexMergeStartTime);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode singal index merge start time fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "singal index merge start time[%ld]", singalIndexMergeStartTime);
    }
  }
  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &maxCreateSignalIndexTimeout);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode max create singal index timeout fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "max create singal index timeout[%ld]", maxCreateSignalIndexTimeout);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi32(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &totalIndexNumber);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode total index number fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "total index number[%d]", totalIndexNumber);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &buildSuccessNumber);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode build success index number fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "build success index number[%ld]", buildSuccessNumber);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &buildFailNumber);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode build fail index number fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "build fail index number[%ld]", buildFailNumber);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &currentBuildIndexTableID);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode current build index table ID fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "current build index table ID[%ld]", currentBuildIndexTableID);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = currentBuildIndexTableName.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                                dataBuffer.get_position());
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode current build index table name fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
  }
  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &indexBuildPhase);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode index build phase fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "index build phase[%ld]", indexBuildPhase);
    }
  }

  t = indexMergeStartTime / 1000000;
  local = localtime(&t);
  microSecond = static_cast<int32_t>(indexMergeStartTime % 1000000);
  strftime(timeBuffer, 20, "%Y-%m-%d %H:%M:%S", local);
  snprintf(indexMergeStartTimeString, 30, "%s.%d", timeBuffer, microSecond);

  t = singalIndexMergeStartTime / 1000000;
  local = localtime(&t);
  microSecond = static_cast<int32_t>(singalIndexMergeStartTime % 1000000);
  strftime(timeBuffer, 20, "%Y-%m-%d %H:%M:%S", local);
  snprintf(currentIndexStartTimeString, 30, "%s.%d", timeBuffer, microSecond);

  if ((currentTime - singalIndexMergeStartTime <= maxCreateSignalIndexTimeout) &&
      (totalIndexNumber != buildSuccessNumber + buildFailNumber))
  {
    remainingTime = singalIndexMergeStartTime + maxCreateSignalIndexTimeout - currentTime;
  }
  else
  {
    remainingTime = maxCreateSignalIndexTimeout;
  }
  microSecond = static_cast<int32_t>(remainingTime % 1000000);
  remainingTime = remainingTime / 1000000;
  hour    = static_cast<int32_t>(remainingTime / 3600);
  minute  = static_cast<int32_t>((remainingTime - (hour * 3600)) / 60);
  second  = static_cast<int32_t>(remainingTime - (hour * 3600) - (minute * 60));
  YYSYS_LOG(DEBUG, "remaingTime:%ld, hour:%d, minute:%d, second:%d", remainingTime, hour, minute, second);
  if(hour > 0)
  {
    snprintf(remainingTimeString, 30, "%d:%d:%d.%d", hour, minute, second, microSecond);
  }
  else if(minute > 0)
  {
    snprintf(remainingTimeString, 30, "0:%d:%d.%d", minute, second, microSecond);
  }
  else
  {
    snprintf(remainingTimeString, 30, "0:0:%d.%d", second, microSecond);
  }

  currentBuildIndexTableName.to_string(currentBuildIndexTableNameString, 60);

  switch(indexBuildPhase)
  {
    case 0: snprintf(indexBuildPhaseString, 10, "%s", "INITIAL");
      break;
    case 1: snprintf(indexBuildPhaseString, 10, "%s", "LOCAL");
      break;
    case 2: snprintf(indexBuildPhaseString, 10, "%s", "GLOBAL");
      break;
    default: snprintf(indexBuildPhaseString, 10, "%s", "UNKNOWN");
      break;
  }

  bsonObjBuilder.append("IndexMergeStartTime", indexMergeStartTimeString);
  bsonObjBuilder.append("CurrentIndexStartTime", currentIndexStartTimeString);
  bsonObjBuilder.append("RemainingTime", remainingTimeString);
  bsonObjBuilder.append("TotalIndexNumber", (int)totalIndexNumber);
  bsonObjBuilder.append("BuildSuccessNumber", buildSuccessNumber);
  bsonObjBuilder.append("BuildFailNumber", buildFailNumber);
  bsonObjBuilder.append("IndexBuildPhase", indexBuildPhaseString);
  bsonObjBuilder.append("CurrentIndexTableID", remainingTimeString);
  bsonObjBuilder.append("TotalTabletNumber", currentBuildIndexTableID);
  bsonObjBuilder.append("currentIndexTableName", currentIndexStartTimeString);
  goto done;

done:
  return rc;
error:
  goto done;
}

int32_t Event::getChunkServerIndexBuildStatus(const ObServer &cs, const int32_t &index, BSONObjBuilder &bsonObjBuilder)
{
  int32_t rc = OB_SUCCESS;

  ObResultCode resultCode;
  ObDataBuffer dataBuffer(buff.ptr(), buff.capacity());
  ObClientManager clientManager = client.get_client_mgr();

  int64_t indexBuildPhase = -1;
  int64_t localTotalIndexTabletNumber   = 0;
  int64_t localMergedIndexTabletNumber  = 0;
  int64_t globalTotalIndexTabletNumber  = 0;
  int64_t globalMergedIndexTabletNumber = 0;
  int32_t totalProgressBarLength   = 20;
  int32_t currentProgressBarLength = 0;
  double  indexBuildProgressPercent = 0.0;
  char indexBuildProgressBuffer[30] = {'\0'};
  char indexBuildProgressBar[21] = {'\0'};

  char csAddress[30] = {'\0'};
  snprintf(csAddress, 30, "%d.%d.%d.%d:%d@%d",
           cs.get_ipv4()        & 0xFF,
           (cs.get_ipv4() >> 8 ) & 0xFF,
           (cs.get_ipv4() >> 16) & 0xFF,
           (cs.get_ipv4() >> 24) & 0xFF,
           cs.get_port(),
           cs.cluster_id_);

  if (OB_SUCCESS == rc)
  {
    rc = clientManager.send_request(cs, OB_CS_FETCH_LOCAL_INDEX_STAT,
                                    DEFAULT_VERSION, DEFAULT_TIMEOUT, dataBuffer);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "send request to chunk server to get chunk server index build status failed, chunk server:%s, ret[%d]", csAddress, rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  if(OB_SUCCESS == rc)
  {
    dataBuffer.get_position() = 0;
    rc = resultCode.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                dataBuffer.get_position());
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "ObResultCode deserialize error, rc=%d", rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &localTotalIndexTabletNumber);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode local total index tablet number fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "local total index tablet number[%ld]", localTotalIndexTabletNumber);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &localMergedIndexTabletNumber);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode local merged index tablet number fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "local merged index tablet number[%ld]", localMergedIndexTabletNumber);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &globalTotalIndexTabletNumber);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode global total index  tablet number fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "global total index tablet number[%ld]", globalTotalIndexTabletNumber);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &globalMergedIndexTabletNumber);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode global merged index tablet number fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "global merged index tablet number[%ld]", globalMergedIndexTabletNumber);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &indexBuildPhase);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode index build phase fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "index build phase[%ld]", indexBuildPhase);
    }
  }

  switch(indexBuildPhase)
  {
    case 0: indexBuildProgressPercent = 0;
      break;
    case 1: if(localTotalIndexTabletNumber != 0)
      {
        indexBuildProgressPercent = (static_cast<double>(localMergedIndexTabletNumber) * 1.0) / static_cast<double>(localTotalIndexTabletNumber);
      }
      else
      {
        indexBuildProgressPercent = 0;
      }
      break;
    case 2: if(globalTotalIndexTabletNumber != 0)
      {
        indexBuildProgressPercent = (static_cast<double>(globalMergedIndexTabletNumber) * 1.0) / static_cast<double>(globalTotalIndexTabletNumber);
      }
      else
      {
        indexBuildProgressPercent = 0;
      }
      break;
    default: indexBuildProgressPercent = 0;
      break;
  }

  currentProgressBarLength = int(indexBuildProgressPercent * totalProgressBarLength);
  for(int i = 0; i < totalProgressBarLength; i++)
  {
    if(i < currentProgressBarLength)
    {
      indexBuildProgressBar[i] = '=';
    }
    else if(i == currentProgressBarLength)
    {
      indexBuildProgressBar[i] = '>';
    }
    else
    {
      indexBuildProgressBar[i] = ' ';
    }
  }
  indexBuildProgressBar[totalProgressBarLength] = '\0';
  snprintf(indexBuildProgressBuffer, 30, "[%s][%d%%]", indexBuildProgressBar, int(indexBuildProgressPercent * 100));

  bsonObjBuilder.append("ChunkServerID", csAddress);
  bsonObjBuilder.append("ServerIndex", index);
  bsonObjBuilder.append("IndexBuildProgress", indexBuildProgressBuffer);
  bsonObjBuilder.append("LocalTotalTabletTasks", localTotalIndexTabletNumber);
  bsonObjBuilder.append("GlobalTotalTabletTasks", globalTotalIndexTabletNumber);

done :
  return rc;
error :
  goto done;
}

int32_t Event::getUpdateServerInfoList()
{
  int32_t rc = OB_SUCCESS;

  ObResultCode resultCode;
  ObDataBuffer dataBuffer(buff.ptr(), buff.capacity());
  ObClientManager clientManager = client.get_client_mgr();
  ObUpsList upsList;

  if (OB_SUCCESS == rc)
  {
    rc = clientManager.send_request(rootServerMaster, OB_RS_GET_ALL_UPS_LIST,
                                    DEFAULT_VERSION, DEFAULT_TIMEOUT, dataBuffer);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "send request to root server to get all update server info list failed! ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  if(OB_SUCCESS == rc)
  {
    dataBuffer.get_position() = 0;
    rc = resultCode.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                dataBuffer.get_position());
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "ObResultCode deserialize error, rc=%d", rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = upsList.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                             dataBuffer.get_position());
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode update server list fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  YYSYS_LOG(DEBUG, "update server count:%d", upsList.ups_count_);
  for(int32_t i = 0; i < upsList.ups_count_; i++)
  {
    updateServerInfoList.insert(map<oceanbase::common::ObServer, oceanbase::common::ObUpsInfo>::value_type(upsList.ups_array_[i].addr_, upsList.ups_array_[i]));
    YYSYS_LOG(DEBUG, "update server address:%d, paxos id:%ld", upsList.ups_array_[i].addr_.get_ipv4(), upsList.ups_array_[i].paxos_id_);
  }

done :
  return rc;
error :
  goto done;
}

int32_t Event::getUpdateServerMemTableInfo(const ObServer &ups, const ObUpsInfo &upsInfo)
{
  int32_t rc = OB_SUCCESS;

  int32_t state = 0;
  SSTableID sstID;
  int64_t frozenTimeStamp = 0;
  int64_t dumpedTimeStamp = 0;
  int64_t memoryUsed = 0;
  int64_t memoryLimit = 0;
  int64_t startCommitFileID = 0;
  int64_t endCommitFileID = 0;
  ObString storeInfo;

  ObResultCode resultCode;
  ObDataBuffer dataBuffer(buff.ptr(), buff.capacity());
  ObClientManager clientManager = client.get_client_mgr();

  char upsAddress[30] = {'\0'};
  snprintf(upsAddress, 30, "%d.%d.%d.%d:%d@%d",
           ups.get_ipv4()        & 0xFF,
           (ups.get_ipv4() >> 8 ) & 0xFF,
           (ups.get_ipv4() >> 16) & 0xFF,
           (ups.get_ipv4() >> 24) & 0xFF,
           ups.get_port(),
           ups.cluster_id_);

  if (OB_SUCCESS == rc)
  {
    rc = clientManager.send_request(ups, OB_UPS_GET_MEM_TABLE_INFO,
                                    DEFAULT_VERSION, DEFAULT_TIMEOUT, dataBuffer);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "send request to update server to get update server memtabl info failed, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  if(OB_SUCCESS == rc)
  {
    dataBuffer.get_position() = 0;
    rc = resultCode.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                dataBuffer.get_position());
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "ObResultCode deserialize error, rc=%d", rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  while (true)
  {
    BSONObj bsonobj;
    BSONObjBuilder bsonObjBuilder;

    char memtableStateString[10] = {'\0'};
    char memtableName[20] = {'\0'};
    char CommitFileIDString[30] = {'\0'};
    char storeInfoString[20] = {'\0'};
    char SSTableName[50] = {'\0'};
    char frozenTimeString[30] = {'\0'};
    char dumpedTimeString[30] = {'\0'};
    char timeBuffer[20] = {'\0'};


    time_t t;
    struct tm *local;
    int32_t microSecond = 0;

    if(OB_SUCCESS == rc)
    {
      rc = serialization::decode_vi32(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                      dataBuffer.get_position(), &state);
      if (OB_SUCCESS != rc)
      {
        YYSYS_LOG(WARN, "decode memtable stat fail, ret[%d]", rc);
        rc = OB_ERROR;
        goto error;
      }
      else
      {
        YYSYS_LOG(DEBUG, "memtable state[%d]", state);
      }
    }
    if(state == 9) // all memtables have been visited
    {
      break;
    }

    if(OB_SUCCESS == rc)
    {
      rc = sstID.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                             dataBuffer.get_position());
      if (OB_SUCCESS != rc)
      {
        YYSYS_LOG(WARN, "decode SSTable ID fail, ret[%d]", rc);
        rc = OB_ERROR;
        goto error;
      }
    }

    if(OB_SUCCESS == rc)
    {
      rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                      dataBuffer.get_position(), &frozenTimeStamp);
      if (OB_SUCCESS != rc)
      {
        YYSYS_LOG(WARN, "decode frozen timestamp fail, ret[%d]", rc);
        rc = OB_ERROR;
        goto error;
      }
      else
      {
        YYSYS_LOG(DEBUG, "frozen timestamp[%ld]", frozenTimeStamp);
      }
    }

    if(OB_SUCCESS == rc)
    {
      rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                      dataBuffer.get_position(), &dumpedTimeStamp);
      if (OB_SUCCESS != rc)
      {
        YYSYS_LOG(WARN, "decode dumped timestamp fail, ret[%d]", rc);
        rc = OB_ERROR;
        goto error;
      }
      else
      {
        YYSYS_LOG(DEBUG, "dumped timestamp[%ld]", dumpedTimeStamp);
      }
    }

    if(OB_SUCCESS == rc)
    {
      rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                      dataBuffer.get_position(), &memoryUsed);
      if (OB_SUCCESS != rc)
      {
        YYSYS_LOG(WARN, "decode memory used fail, ret[%d]", rc);
        rc = OB_ERROR;
        goto error;
      }
      else
      {
        YYSYS_LOG(DEBUG, "memory used[%ld]", memoryUsed);
      }
    }

    if(OB_SUCCESS == rc)
    {
      rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                      dataBuffer.get_position(), &memoryLimit);
      if (OB_SUCCESS != rc)
      {
        YYSYS_LOG(WARN, "decode memory limit fail, ret[%d]", rc);
        rc = OB_ERROR;
        goto error;
      }
      else
      {
        YYSYS_LOG(DEBUG, "memory capacity[%ld]", memoryLimit);
      }
    }

    if(OB_SUCCESS == rc)
    {
      rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                      dataBuffer.get_position(), &startCommitFileID);
      if (OB_SUCCESS != rc)
      {
        YYSYS_LOG(WARN, "decode start commit File ID fail, ret[%d]", rc);
        rc = OB_ERROR;
        goto error;
      }
      else
      {
        YYSYS_LOG(DEBUG, "start commit file ID[%ld]", startCommitFileID);
      }
    }

    if(OB_SUCCESS == rc)
    {
      rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                      dataBuffer.get_position(), &endCommitFileID);
      if (OB_SUCCESS != rc)
      {
        YYSYS_LOG(WARN, "decode end commit File ID fail, ret[%d]", rc);
        rc = OB_ERROR;
        goto error;
      }
      else
      {
        YYSYS_LOG(DEBUG, "end commit file ID[%ld]", endCommitFileID);
      }
    }
    if(OB_SUCCESS == rc)
    {
      rc = storeInfo.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                 dataBuffer.get_position());
      if (OB_SUCCESS != rc)
      {
        YYSYS_LOG(WARN, "decode store info fail, ret[%d]", rc);
        rc = OB_ERROR;
        goto error;
      }
    }

    snprintf(memtableName, 20, "[%lu_%lu-%lu]", sstID.major_version, sstID.minor_version_start, sstID.minor_version_end);

    switch(state)
    {
      case 0:
        snprintf(memtableStateString, 10, "%s", "UNKNOW");
        break;
      case 1:
        snprintf(memtableStateString, 10, "%s", "ACTIVE");
        break;
      case 2:
        snprintf(memtableStateString, 10, "%s", "FREEZING");
        break;
      case 3:
        snprintf(memtableStateString, 10, "%s", "FROZEN");
        break;
      case 4:
        snprintf(memtableStateString, 10, "%s", "DUMPING");
        break;
      case 5:
        snprintf(memtableStateString, 10, "%s", "DUMPED");
        break;
      case 6:
        snprintf(memtableStateString, 10, "%s", "DROPING");
        break;
      case 7:
        snprintf(memtableStateString, 10, "%s", "DROPED");
        break;
      default:
        snprintf(memtableStateString, 10, "%s", "ERROR");
        break;
    }

    t = frozenTimeStamp / 1000000;
    local = localtime(&t);
    microSecond = static_cast<int32_t>(frozenTimeStamp % 10000000);
    strftime(timeBuffer, 20, "%Y-%m-%d %H:%M:%S", local);
    snprintf(frozenTimeString, 30, "%s.%d", timeBuffer, microSecond);

    if (dumpedTimeStamp <= frozenTimeStamp && dumpedTimeStamp > 0)
    {
      dumpedTimeStamp = frozenTimeStamp + 1000000;
    }
    t = dumpedTimeStamp / 1000000;
    local = localtime(&t);
    microSecond = static_cast<int32_t>(dumpedTimeStamp % 1000000);
    strftime(timeBuffer, 20, "%Y-%m-%d %H:%M:%S", local);
    snprintf(dumpedTimeString, 30, "%s.%d", timeBuffer, microSecond);

    snprintf(CommitFileIDString, 30, "[%ld,%ld)", startCommitFileID, endCommitFileID);
    storeInfo.to_string(storeInfoString, 20);

    if(state >=5 && state <= 7)
    {
      snprintf(SSTableName, 50, "%ld_%ld-%ld_%ld-%ld.sst", sstID.major_version, sstID.minor_version_start,
               sstID.minor_version_end, startCommitFileID, endCommitFileID);
    }

    bsonObjBuilder.append("UpdateServerID", upsAddress);
    bsonObjBuilder.append("PID", upsInfo.paxos_id_);
    bsonObjBuilder.append("Role", ((int(upsInfo.stat_) == 0) ? "S" : "M"));
    bsonObjBuilder.append("MemtableName", memtableName);
    bsonObjBuilder.append("State", memtableStateString);
    bsonObjBuilder.append("Used(GB)", (memoryUsed / GIGABYTE));
    bsonObjBuilder.append("Limit(GB)", (memoryLimit / GIGABYTE));
    bsonObjBuilder.append("CommitFileID", CommitFileIDString);
    bsonObjBuilder.append("FrozenTime", frozenTimeString);
    bsonObjBuilder.append("DumpedTime", dumpedTimeString);
    bsonObjBuilder.append("StoreInfo", storeInfoString);
    bsonObjBuilder.append("SSTableName", SSTableName);
    bsonobj = bsonObjBuilder.obj();

    snapshotResult.push_back(bsonobj);
  }

done :
  return rc;
error :
  goto done;
}

int32_t Event::getUpdateServerCommitLogStatus(const ObServer &ups, const ObUpsInfo &upsInfo)
{
  int32_t rc = OB_SUCCESS;
  ClogStatus commitLogStatus;
  int64_t flushFileID = 0;
  int64_t replayPoint = 0;

  ObResultCode resultCode;
  ObDataBuffer dataBuffer(buff.ptr(), buff.capacity());
  ObClientManager clientManager = client.get_client_mgr();

  BSONObj bsonobj;
  BSONObjBuilder bsonObjBuilder;
  char commitLogStateString[10] = {'\0'};
  char leaseString[30] = {'\0'};
  char leaseRemainString[15] = {'\0'};
  char logTermString[30] = {'\0'};
  char flushString[40] = {'\0'};
  char timeBuffer[20] = {'\0'};
  time_t t;
  struct tm *local;
  int64_t remainingTime = 0;
  int32_t second = 0;
  int32_t microSecond = 0;

  char upsAddress[30] = {'\0'};
  snprintf(upsAddress, 30, "%d.%d.%d.%d:%d@%d",
           ups.get_ipv4()        & 0xFF,
           (ups.get_ipv4() >> 8 ) & 0xFF,
           (ups.get_ipv4() >> 16) & 0xFF,
           (ups.get_ipv4() >> 24) & 0xFF,
           ups.get_port(),
           ups.cluster_id_);
  (void)logTermString;
  (void)remainingTime;

  if (OB_SUCCESS == rc)
  {
    rc = clientManager.send_request(ups, OB_UPS_GET_COMMIT_LOG_STAT,
                                    DEFAULT_VERSION, DEFAULT_TIMEOUT, dataBuffer);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "send request to update server to get update server commit log status failed, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  if(OB_SUCCESS == rc)
  {
    dataBuffer.get_position() = 0;
    rc = resultCode.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                dataBuffer.get_position());
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "ObResultCode deserialize error, rc=%d", rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  if(OB_SUCCESS == rc)
  {
    dataBuffer.get_position() = 0;
    rc = commitLogStatus.deserialize(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                     dataBuffer.get_position());
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "commitLogStatus deserialize error, rc=%d", rc);
      rc = OB_ERROR;
      goto error;
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &flushFileID);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode flush File ID fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "commit file ID[%ld]", flushFileID);
    }
  }

  if(OB_SUCCESS == rc)
  {
    rc = serialization::decode_vi64(dataBuffer.get_data(), dataBuffer.get_capacity(),
                                    dataBuffer.get_position(), &replayPoint);
    if (OB_SUCCESS != rc)
    {
      YYSYS_LOG(WARN, "decode replay point fail, ret[%d]", rc);
      rc = OB_ERROR;
      goto error;
    }
    else
    {
      YYSYS_LOG(DEBUG, "replay point[%ld]", replayPoint);
    }
  }

  switch(commitLogStatus.state_)
  {
    case 1:
      snprintf(commitLogStateString, 10, "%s", "REPLAY");
      break;
    case 2:
      snprintf(commitLogStateString, 10, "%s", "ACTIVE");
      break;
    case 3:
      snprintf(commitLogStateString, 10, "%s", "FATAL");
      break;
    case 4:
      snprintf(commitLogStateString, 10, "%s", "STOP");
      break;
    default:
      snprintf(commitLogStateString, 10, "%s", "TIMEOUT");
      break;
  }

  t = commitLogStatus.lease_ / 1000000;
  local = localtime(&t);
  microSecond = static_cast<int32_t>(commitLogStatus.lease_ % 1000000);
  strftime(timeBuffer, 20, "%Y-%m-%d %H:%M:%S", local);
  snprintf(leaseString, 30, "%s.%d", timeBuffer, microSecond);

  microSecond = static_cast<int32_t>(commitLogStatus.lease_remain_ % 1000000);
  second = static_cast<int32_t>(commitLogStatus.lease_remain_ / 1000000);
  snprintf(leaseRemainString, 15, "%d.%d", second, microSecond);

  snprintf(flushString, 40, "%ld|%ld", commitLogStatus.flush_, flushFileID);

  bsonObjBuilder.append("UpdateServerID", upsAddress);
  bsonObjBuilder.append("PID", upsInfo.paxos_id_);
  bsonObjBuilder.append("Role", ((int(upsInfo.stat_) == 0) ? "S" : "M"));
  bsonObjBuilder.append("State", commitLogStateString);
  bsonObjBuilder.append("Lease", leaseString);
  bsonObjBuilder.append("Remain", leaseRemainString);
  bsonObjBuilder.append("LogTerm", commitLogStatus.log_term_);
  bsonObjBuilder.append("SendReceiveLogSeq", commitLogStatus.send_receive_);
  bsonObjBuilder.append("CommitReplayLogSeq", commitLogStatus.commit_replay_);
  bsonObjBuilder.append("FlushLogSeq|FlushFileID", flushString);
  bsonObjBuilder.append("commitPoint", commitLogStatus.commit_point_);
  bsonObjBuilder.append("ReplayPoint", replayPoint);
  bsonobj = bsonObjBuilder.obj();
  snapshotResult.push_back(bsonobj);

done :
  return rc;
error :
  goto done;
}

int32_t Event::getCurSnapshot()
{
  int32_t rc             = OB_SUCCESS ;
  int32_t snapType       = -1 ;
  string condition     = NULLSTRING;
  ObServer specificServer;
  string specificIP;
  string specificPort;
  int32_t position = 0;

  (void)snapType;
  BSONObj bsonobj ;

  if( NULLSTRING != root.input.filterCondition )
  {
    condition = root.input.filterCondition;
    position = static_cast<int32_t>(condition.find(":"));
    if( string::npos != static_cast<string::size_type>(position))
    {
      specificIP = condition.substr( 0, position ) ;
      specificPort = condition.substr( position + 1 ) ;
      specificServer.set_ipv4_addr(specificIP.c_str(), (int32_t)atoi(specificPort.c_str()));
    }
    else
    {
      condition = NULLSTRING;
      root.input.filterCondition = NULLSTRING;
    }
  }
  if( root.input.activatedPanel[0].sourceSnapShot ==
      CB_SNAP_CS_MERGE_STAT_TOP )
  {
    snapshotResult.clear();
    chunkServerList.clear();
    snapType = CB_SNAP_CS_MERGE_STAT;
    int64_t maxMergeDurationTimeout = 0;
    rc = getChunkServerList(maxMergeDurationTimeout);
    if (OB_SUCCESS != rc)
    {
      goto error;
    }
    if (condition.empty())
    {
      for (map<oceanbase::common::ObServer, int32_t>::iterator it = chunkServerList.begin(); it != chunkServerList.end(); it++)
      {
        rc = getChunkServerMergeStatus(it->first, it->second, maxMergeDurationTimeout);
        if (OB_SUCCESS != rc)
        {
          rc = OB_SUCCESS;
          continue;
        }
      }
    }
    else
    {
      map<oceanbase::common::ObServer, int32_t>::iterator it = chunkServerList.find(specificServer);
      if (it != chunkServerList.end())
      {
        rc = getChunkServerMergeStatus(it->first, it->second, maxMergeDurationTimeout);
        if (OB_SUCCESS != rc)
        {
          goto error;
        }
      }
      else
      {
        YYSYS_LOG(INFO, "Can not find input server: %s, ret=%d", condition.c_str(), rc);
      }
    }
  }
  else if( root.input.activatedPanel[0].sourceSnapShot ==
           CB_SNAP_RS_CS_INDEX_MERGE_STAT_TOP )
  {
    snapshotResult.clear();
    chunkServerList.clear();
    snapType = CB_SNAP_RS_CS_INDEX_MERGE_STAT;
    int64_t maxMergeDurationTimeout = 0;
    rc = getChunkServerList(maxMergeDurationTimeout);
    if (OB_SUCCESS != rc)
    {
      goto error;
    }
    if (condition.empty())
    {
      for (map<oceanbase::common::ObServer, int32_t>::iterator it = chunkServerList.begin(); it != chunkServerList.end(); it++)
      {
        BSONObj bsonobj;
        BSONObjBuilder bsonObjBuilder;
        rc = getWholeIndexBuildInfoFromRootServer(bsonObjBuilder);
        if (OB_SUCCESS != rc)
        {
          goto error;
        }
        rc = getChunkServerIndexBuildStatus(it->first, it->second, bsonObjBuilder);
        if (OB_SUCCESS != rc)
        {
          rc = OB_SUCCESS;
          goto done;
        }
        bsonobj = bsonObjBuilder.obj();
        snapshotResult.push_back(bsonobj);
      }
    }
    else
    {
      BSONObj bsonobj;
      BSONObjBuilder bsonObjBuilder;
      rc = getWholeIndexBuildInfoFromRootServer(bsonObjBuilder);
      if (OB_SUCCESS != rc)
      {
        goto error;
      }
      map<oceanbase::common::ObServer, int32_t>::iterator it = chunkServerList.find(specificServer);
      if (it != chunkServerList.end())
      {
        rc = getChunkServerIndexBuildStatus(it->first, it->second, bsonObjBuilder);
        if (OB_SUCCESS != rc)
        {
          goto error;
        }
      }
      else
      {
        YYSYS_LOG(INFO, "Can not find input server: %s, ret=%d", condition.c_str(), rc);
      }
      bsonobj = bsonObjBuilder.obj();
      snapshotResult.push_back(bsonobj);
    }
  }
  else if( root.input.activatedPanel[0].sourceSnapShot ==
           CB_SNAP_UPS_MEMTABLE_STAT_TOP )
  {
    snapshotResult.clear();
    updateServerInfoList.clear();
    snapType = CB_SNAP_UPS_MEMTABLE_STAT;
    rc = getUpdateServerInfoList();
    if (OB_SUCCESS != rc)
    {
      goto error;
    }
    if (condition.empty())
    {
      for (map<oceanbase::common::ObServer, oceanbase::common::ObUpsInfo>::iterator it = updateServerInfoList.begin(); it != updateServerInfoList.end(); it++)
      {
        rc = getUpdateServerMemTableInfo(it->first, it->second);
        if (OB_SUCCESS != rc)
        {
          rc = OB_SUCCESS;
          continue;
        }
      }
    }
    else
    {
      map<oceanbase::common::ObServer, oceanbase::common::ObUpsInfo>::iterator it = updateServerInfoList.find(specificServer);
      if (it != updateServerInfoList.end())
      {
        rc = getUpdateServerMemTableInfo(it->first, it->second);
        if (OB_SUCCESS != rc)
        {
          goto error;
        }
      }
      else
      {
        YYSYS_LOG(INFO, "Can not find input server: %s, ret=%d", condition.c_str(), rc);
      }
    }
  }
  else if( root.input.activatedPanel[0].sourceSnapShot ==
           CB_SNAP_UPS_COMMIT_LOG_STAT_TOP )
  {
    snapshotResult.clear();
    updateServerInfoList.clear();
    snapType = CB_SNAP_UPS_COMMIT_LOG_STAT;
    rc = getUpdateServerInfoList();
    if (OB_SUCCESS != rc)
    {
      goto error;
    }
    if (condition.empty())
    {
      for (map<oceanbase::common::ObServer, oceanbase::common::ObUpsInfo>::iterator it = updateServerInfoList.begin(); it != updateServerInfoList.end(); it++)
      {
        rc = getUpdateServerCommitLogStatus(it->first, it->second);
        if (OB_SUCCESS != rc)
        {
          rc = OB_SUCCESS;
          continue;
        }
      }
    }
    else
    {
      map<oceanbase::common::ObServer, oceanbase::common::ObUpsInfo>::iterator it = updateServerInfoList.find(specificServer);
      if (it != updateServerInfoList.end())
      {
        rc = getUpdateServerCommitLogStatus(it->first, it->second);
        if (OB_SUCCESS != rc)
        {
          goto error;
        }
      }
      else
      {
        YYSYS_LOG(INFO, "Can not find input server: %s, ret=%d", condition.c_str(), rc);
      }
    }
  }
  else if( root.input.activatedPanel[0].sourceSnapShot ==
           CB_SNAP_CATALOG_TOP )
  {
    snapType = CB_SNAP_CATALOG ;
  }
  else
  {
    if( root.input.activatedPanel[0].bodyPanelType != BODYTYPE_NORMAL )
    {
      goto done ;
    }

    YYSYS_LOG(WARN, "getCurSnapshot failed, xml gave the wrong sourceSnapShot: %s",
              root.input.activatedPanel[0].sourceSnapShot.c_str()) ;
    rc = OB_ERROR ;
    goto error ;
  }

  if( rc )
  {
    YYSYS_LOG(WARN,"getCurSnapshot failed, can't getSnapshot, rc = %d", rc) ;
    goto error ;
  }

  root.input.last_absoluteMap.clear() ;
  root.input.last_absoluteMap = root.input.cur_absoluteMap ;
  root.input.cur_absoluteMap.clear() ;

  root.input.last_deltaMap.clear() ;
  root.input.last_deltaMap = root.input.cur_deltaMap ;
  root.input.cur_deltaMap.clear() ;

  root.input.last_averageMap.clear() ;
  root.input.last_averageMap = root.input.cur_averageMap ;
  root.input.cur_averageMap.clear() ;

  root.input.last_Snapshot.clear() ;
  root.input.last_Snapshot = root.input.cur_Snapshot ;
  root.input.cur_Snapshot.clear() ;

  for(uint32_t j = 0; j < snapshotResult.size(); j++)
  {
    root.input.cur_Snapshot.push_back(snapshotResult[j]);
  }
  snapshotResult.clear();

  if( OB_SIZE_OVERFLOW != rc && OB_SUCCESS != rc )
  {

    YYSYS_LOG(WARN, "refreshDisplayContent failed, rc=%d", rc ) ;
    goto error ;
  }
  if( OB_SIZE_OVERFLOW == rc )
  {
    rc = OB_SUCCESS ;
  }
  if( TRUE == root.input.isFirstGetSnapshot )
  {
    root.input.isFirstGetSnapshot = FALSE ;
    root.input.last_Snapshot.clear() ;
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::fixedOutputLocation(int32_t start_row, int32_t start_col, int32_t &fixed_row, int32_t &fixed_col, int32_t referRowLength, int32_t referColLength, const string &autoSetType)
{
  int32_t rc  = OB_SUCCESS ;
  fixed_row = start_row ;
  fixed_col = start_col ;
  if( autoSetType == UPPER_LEFT )
  {
    fixed_row = start_row ;
    fixed_col = start_col ;
  }
  else if( autoSetType == MIDDLE_LEFT )
  {
    fixed_row = start_row + referRowLength / 2 ;
    fixed_col = start_col ;
  }
  else if( autoSetType == LOWER_LEFT )
  {
    fixed_row = start_row + referRowLength ;
    fixed_col = start_col ;
  }
  else if( autoSetType == UPPER_MIDDLE )
  {
    fixed_row = start_row ;
    fixed_col = start_col + referColLength / 2 ;
  }
  else if( autoSetType == MIDDLE)
  {
    fixed_row = start_row + referRowLength / 2 ;
    fixed_col = start_col + referColLength / 2 ;
  }
  else if( autoSetType == LOWER_MIDDLE)
  {
    fixed_row = start_row + referRowLength ;
    fixed_col = start_col + referColLength / 2 ;
  }
  else if( autoSetType == UPPER_RIGHT)
  {
    fixed_row = start_row ;
    fixed_col = start_col + referColLength ;
  }
  else if( autoSetType == MIDDLE_RIGHT)
  {
    fixed_row = start_row + referRowLength / 2 ;
    fixed_col = start_col + referColLength ;
  }
  else if( autoSetType == LOWER_RIGHT)
  {
    fixed_row = start_row + referRowLength ;
    fixed_col = start_col + referColLength ;
  }
  else
  {

    YYSYS_LOG(WARN, "fixedOutputLocation failed, wrong autoSetType: %s", autoSetType.c_str()) ;
    rc = OB_ERROR ;
    goto error ;
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::getFieldNameAndColour( const FieldStruct &fieldStruct,
                                      const string &displayMode,
                                      string &fieldName, Colours &fieldColour )
{
  int32_t rc = OB_SUCCESS ;
  if( !fieldStruct.canSwitch )
  {
    fieldName = fieldStruct.absoluteName ;
    fieldColour.backGroundColor = fieldStruct.absoluteColour.backGroundColor ;
    fieldColour.foreGroundColor = fieldStruct.absoluteColour.foreGroundColor ;
  }
  else if( DELTA == displayMode )
  {
    fieldName = fieldStruct.deltaName ;
    fieldColour.backGroundColor = fieldStruct.deltaColour.backGroundColor ;
    fieldColour.foreGroundColor = fieldStruct.deltaColour.foreGroundColor ;
  }
  else if( ABSOLUTE == displayMode )
  {
    fieldName = fieldStruct.absoluteName ;
    fieldColour.backGroundColor = fieldStruct.absoluteColour.backGroundColor ;
    fieldColour.foreGroundColor = fieldStruct.absoluteColour.foreGroundColor ;
  }
  else if( AVERAGE == displayMode )
  {
    fieldName = fieldStruct.averageName ;
    fieldColour.backGroundColor = fieldStruct.averageColour.backGroundColor ;
    fieldColour.foreGroundColor = fieldStruct.averageColour.foreGroundColor ;
  }
  else
  {

    YYSYS_LOG(WARN, "getFieldStructNameAndColour failed, wrong displayMode: %s", displayMode.c_str()) ;
    rc = OB_ERROR ;
    goto error ;
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::refreshDH( DynamicHelp &DH, Position &position )
{
  int32_t rc                = OB_SUCCESS ;
  int32_t Y                 = position.referUpperLeft_Y ;
  int32_t X                 = position.referUpperLeft_X ;
  int32_t start_Y           = Y ;
  int32_t start_X           = X ;
  int32_t pos_X             = start_X ;
  KeySuite *keySuite      = NULL ;
  HotKey *hotkey          = NULL ;
  int32_t hotKey_pos        = 0 ;
  int32_t rowNumber         = 0 ;
  int32_t sum               = 0 ;
  int32_t colNumber         = 0 ;
  int32_t pairNumber        = 0 ;
  int32_t count             = 0;
  string printStr         = NULLSTRING ;
  int32_t cellLength        = DH.cellLength ;
  char *pPrintfstr =
      ( char * )CB_OSS_MALLOC( cellLength * sizeof( char ) ) ;
  if( !pPrintfstr )
  {
    YYSYS_LOG(WARN, "MVPRINTW_TOP failed, can't malloc memory for printfstr :%d", cellLength) ;
    rc = OB_MEM_OVERFLOW ;
    goto error ;
  }

  if( DH.wndOptionRow + DH.optionsRow > position.length_Y )
  {
    YYSYS_LOG(WARN, "refreshDisplayContent failed, tableRow is too small!") ;
    rc = OB_ERROR ;
    goto error ;
  }
  rc = getActivatedKeySuite( &keySuite ) ;
  if( rc )
  {
    goto error ;
  }
  rc = fixedOutputLocation( Y, X, start_Y, start_X,
                            position.length_Y - DH.wndOptionRow - DH.optionsRow,
                            0, DH.autoSetType) ;
  if( rc )
  {
    goto error ;
  }

  {
    ossMemset( pPrintfstr, 0, cellLength );
    ossSnprintf(pPrintfstr, cellLength, "window options"
                "(choose to enter window):" ) ;

    getColourPN( DH.prefixColour, pairNumber ) ;
    attron( COLOR_PAIR( pairNumber ) ) ;
    rc = mvprintw_CBTOP( pPrintfstr, static_cast<int32_t>(ossStrlen( pPrintfstr)), ALIGNMENT_LEFT,
                         start_Y, pos_X ) ;
    if( rc )
    {
      goto error ;
    }
    attroff( COLOR_PAIR( pairNumber ) ) ;
    ++rowNumber ;
  }

  hotKey_pos = 0 ;
  while ( rowNumber < DH.wndOptionRow && hotKey_pos < keySuite->hotKeyLength )
  {
    start_X = position.referUpperLeft_X ;
    start_Y += 1 ;
    sum = 0 ;
    for( colNumber = 0; colNumber< DH.wndOptionCol; ++ colNumber )
    {
      if( sum > position.length_X )
        break ;
      sum += cellLength ;
    }
    rc = fixedOutputLocation( start_Y, X, start_Y, start_X, 0,
                              position.length_X - sum, DH.autoSetType) ;
    if( rc )
    {
      goto error ;
    }

    ossMemset( pPrintfstr, 0, cellLength );
    while ( hotKey_pos < keySuite->hotKeyLength )
    {
      if( start_X + cellLength - X  > position.length_X )
      {
        break ;
      }
      hotkey = &keySuite->hotKey[hotKey_pos];
      if ( !hotkey->wndType )
      {
        ++hotKey_pos ;
        continue ;
      }

      pos_X = start_X ;
      if( JUMPTYPE_FIXED == hotkey->jumpType )
      {
        if( BUTTON_TAB == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength, PREFIX_TAB ) ;
        else if( BUTTON_LEFT == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength, PREFIX_LEFT ) ;
        else if( BUTTON_RIGHT == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength, PREFIX_RIGHT ) ;
        else if( BUTTON_ENTER == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength, PREFIX_ENTER ) ;
        else if( BUTTON_ESC == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength, PREFIX_ESC ) ;
        else if( BUTTON_F5 == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength, PREFIX_F5 ) ;
        else if( BUTTON_Q_LOWER == hotkey->button ||
                 BUTTON_H_LOWER == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength,
                       PREFIX_FORMAT, ( char )hotkey->button ) ;
        else
          ossSnprintf( pPrintfstr, cellLength, PREFIX_NULL ) ;
      }
      else
        ossSnprintf( pPrintfstr, cellLength, PREFIX_FORMAT,
                     ( char )hotkey->button ) ;

      ++count ;
      printStr = pPrintfstr ;
      getColourPN( DH.prefixColour, pairNumber ) ;
      attron( COLOR_PAIR( pairNumber ) ) ;
      rc = mvprintw_CBTOP( printStr, static_cast<int32_t>(printStr.length()), ALIGNMENT_LEFT,
                           start_Y, pos_X ) ;
      if( rc )
      {
        goto error ;
      }
      attroff( COLOR_PAIR( pairNumber ) ) ;
      pos_X += static_cast<int32_t>(printStr.length());
      ossMemset( pPrintfstr, 0, cellLength ) ;
      getColourPN( DH.contentColour, pairNumber ) ;
      attron( COLOR_PAIR( pairNumber ) ) ;
      rc = mvprintw_CBTOP( hotkey->desc, static_cast<int32_t>(hotkey->desc.length()),
                           ALIGNMENT_LEFT, start_Y, pos_X );
      if( rc )
      {
        goto error ;
      }
      attroff( COLOR_PAIR( pairNumber ) ) ;
      start_X += DH.cellLength ;
      ++hotKey_pos ;
      if ( 0 == count % DH.wndOptionCol ||
           hotKey_pos + 1 >= keySuite->hotKeyLength )
      {
        ++rowNumber ;
        break ;
      }
    }
  }

  {
    ++start_Y ;
    start_X = position.referUpperLeft_X ;
    pos_X = start_X ;
    ossSnprintf(pPrintfstr, cellLength,
                "options(use under window above): " ) ;

    getColourPN( DH.prefixColour, pairNumber ) ;
    attron( COLOR_PAIR( pairNumber ) ) ;
    rc = mvprintw_CBTOP( pPrintfstr, static_cast<int32_t>(ossStrlen( pPrintfstr)), ALIGNMENT_LEFT,
                         start_Y, pos_X ) ;
    if( rc )
    {
      goto error ;
    }
    attroff( COLOR_PAIR( pairNumber ) ) ;
  }

  hotKey_pos = 0 ;
  rowNumber = 0 ;
  count = 0 ;
  while ( rowNumber < DH.optionsRow )
  {
    start_Y += 1 ;
    sum = 0 ;
    for( colNumber = 0; colNumber< DH.optionsCol; ++ colNumber )
    {
      if( sum > position.length_X )
        break ;
      sum += cellLength ;
    }
    rc = fixedOutputLocation( start_Y, X, start_Y, start_X, 0,
                              position.length_X - sum, DH.autoSetType) ;
    if( rc )
    {
      goto error ;
    }

    ossMemset( pPrintfstr, 0, cellLength );
    while ( hotKey_pos < keySuite->hotKeyLength )
    {
      if( start_X + cellLength - X  > position.length_X )
      {
        break ;
      }
      hotkey = &keySuite->hotKey[hotKey_pos];
      if ( hotkey->wndType )
      {
        ++hotKey_pos ;
        continue ;
      }

      ++count ;
      pos_X = start_X ;
      if( JUMPTYPE_FIXED == hotkey->jumpType )
      {
        if( BUTTON_TAB == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength, PREFIX_TAB ) ;
        else if( BUTTON_LEFT == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength, PREFIX_LEFT ) ;
        else if( BUTTON_RIGHT == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength, PREFIX_RIGHT ) ;
        else if( BUTTON_ENTER == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength, PREFIX_ENTER ) ;
        else if( BUTTON_ESC == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength, PREFIX_ESC ) ;
        else if( BUTTON_F5 == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength, PREFIX_F5 ) ;
        else if( BUTTON_Q_LOWER == hotkey->button ||
                 BUTTON_H_LOWER == hotkey->button )
          ossSnprintf( pPrintfstr, cellLength,
                       PREFIX_FORMAT, ( char )hotkey->button ) ;
        else
          ossSnprintf( pPrintfstr, cellLength, PREFIX_NULL ) ;
      }
      else
        ossSnprintf( pPrintfstr, cellLength, PREFIX_FORMAT,
                     ( char )hotkey->button ) ;

      printStr = pPrintfstr ;
      getColourPN( DH.prefixColour, pairNumber ) ;
      attron( COLOR_PAIR( pairNumber ) ) ;
      rc = mvprintw_CBTOP( printStr, static_cast<int32_t>(printStr.length()), ALIGNMENT_LEFT,
                           start_Y, pos_X ) ;
      if( rc )
      {
        goto error ;
      }
      attroff( COLOR_PAIR( pairNumber ) ) ;
      pos_X += static_cast<int32_t>(printStr.length());
      ossMemset( pPrintfstr, 0, cellLength ) ;
      getColourPN( DH.contentColour, pairNumber ) ;
      attron( COLOR_PAIR( pairNumber ) ) ;
      rc = mvprintw_CBTOP( hotkey->desc, static_cast<int32_t>(hotkey->desc.length()),
                           ALIGNMENT_LEFT, start_Y, pos_X );
      if( rc )
      {
        goto error ;
      }
      attroff( COLOR_PAIR( pairNumber ) ) ;
      start_X += DH.cellLength ;
      ++hotKey_pos ;

      if ( 0 == count % DH.optionsCol ||
           hotKey_pos + 1 >= keySuite->hotKeyLength )
      {
        ++rowNumber ;
        break ;
      }
    }
  }

done :
  if( pPrintfstr )
    CB_OSS_FREE( pPrintfstr );
  return rc ;
error :
  goto done ;
}
////////////////////////////////////////////////////////
int32_t Event::refreshDE( DynamicExpressionOutPut &DE, Position &position )
{
  int32_t rc                        = OB_SUCCESS ;
  int32_t Y                         = position.referUpperLeft_Y ;
  int32_t X                         = position.referUpperLeft_X ;
  int32_t start_Y                   = Y ;
  int32_t start_X                   = X ;
  int32_t Sum                       = 0 ;
  int32_t expressionLength          = 0 ;
  int32_t pairNumber                = 0 ;
  string result                   = NULLSTRING ;
  int32_t expressionNumber          = 0 ;
  int32_t rowNumber                 = 0 ;
  ExpressionContent *EC           = NULL ;
  rc = fixedOutputLocation( Y, X,
                            start_Y, start_X,
                            position.length_Y - DE.rowNumber,
                            0,
                            DE.autoSetType) ;
  if( rc )
  {
    goto error ;
  }
  for( rowNumber = 0;
       rowNumber < DE.rowNumber && rowNumber < position.length_Y ;
       ++rowNumber )
  {
    start_Y += rowNumber ;
    Sum = 0 ;
    expressionLength = 0 ;
    pairNumber = 0 ;
    result = NULLSTRING ;
    for( expressionNumber = 0; expressionNumber < DE.expressionNumber;
         ++expressionNumber )
    {
      EC = &DE.content[expressionNumber] ;
      if( STATIC_EXPRESSION == EC->expressionType )
      {
        result = EC->expressionValue.text ;
      }
      else if( DYNAMIC_EXPRESSION == EC->expressionType )
      {
        rc = getExpression( EC->expressionValue.expression,
                            result ) ;
        if( rc )
        {
          YYSYS_LOG(WARN,"refreshDisplayContent failed, getExpression failed");
          goto error ;
        }
      }
      if( NULLSTRING == result )
        result = STRING_NULL ;
      expressionLength = static_cast<int32_t>(result.length());
      if( Sum + expressionLength > position.length_X )
        continue ;
      if( EC->rowLocation != rowNumber + 1 )
      {
        continue ;
      }
      Sum += expressionLength ;
      ++Sum ; // add space
    }

    rc = fixedOutputLocation( start_Y, X, start_Y, start_X,
                              0, position.length_X -Sum ,
                              DE.autoSetType) ;
    if( rc )
    {
      goto error ;
    }
    for( expressionNumber = 0; expressionNumber < DE.expressionNumber;
         ++expressionNumber )
    {
      EC = &DE.content[expressionNumber] ;
      if( EC->rowLocation != rowNumber + 1 )
      {
        continue ;
      }
      if( STATIC_EXPRESSION == EC->expressionType )
      {
        result = EC->expressionValue.text ;
      }
      else if( DYNAMIC_EXPRESSION == EC->expressionType )
      {
        getExpression( EC->expressionValue.expression,
                       result ) ;
      }
      if( NULLSTRING == result )
        result = STRING_NULL ;
      expressionLength = static_cast<int32_t>(result.length());
      if( start_X + expressionLength - X > position.length_X )
      {
        continue ;
      }
      getColourPN( EC->colour,pairNumber ) ;
      attron( COLOR_PAIR( pairNumber ) ) ;
      rc = mvprintw_CBTOP( result, expressionLength, EC->alignment,
                           start_Y, start_X ) ;
      if( rc )
      {
        goto error ;
      }
      attroff( COLOR_PAIR( pairNumber ) ) ;
      start_X += expressionLength ;
      ++start_X ;
    }
    start_Y -= rowNumber ;
  }

done :
  return rc ;
error :
  goto done ;
}

int32_t Event::refreshDS_Table( DynamicSnapshotOutPut &DS, int32_t ROW, int32_t COL,
                                Position &position, string autoSetType )
{
  int32_t rc = OB_SUCCESS ;
  BSONObj bsonobj ;
  string baseField              = DS.baseField ;
  int32_t rowNumber               = 0;
  FieldStruct* Fixed            = NULL ;
  FieldStruct* Mobile           = NULL ;
  int32_t FLength                 = 0 ;
  int32_t MLength                 = 0 ;
  InputPanel &input             = root.input ;
  int32_t Y                       = position.referUpperLeft_Y ;
  int32_t X                       = position.referUpperLeft_X ;
  int32_t length_Y                = position.length_Y ;
  int32_t length_X                = position.length_X ;
  int32_t cellLength              = DS.tableCellLength ;
  int32_t start_Y                 = Y ;
  int32_t start_X                 = X ;

  int32_t index_COL               = 0 ;

  int32_t pos_Field               = 0 ;
  int32_t end_fixed_mobile        = 0 ;
  int32_t start_fixed_mobile      = 0 ;
  string displayMode            =
      DISPLAYMODECHOOSER[input.displayModeChooser] ;

  string fieldName              = NULLSTRING ;
  Colours fieldColour ;
  int32_t pairNumber              = 0 ;
  string result                 =  NULLSTRING ;
  uint32_t pos_snapshot           = 0 ;
  fieldColour.backGroundColor   = 6 ;
  fieldColour.foreGroundColor   = 0 ;
  FLength = DS.actualFixedFieldLength ;
  MLength = DS.actualMobileFieldLength ;
  rc = fixedOutputLocation( Y, X, start_Y, start_X, length_Y - ROW * 2, 0,
                            autoSetType ) ;
  if( rc )
  {
    goto error ;
  }

  for( index_COL = 0; index_COL <= COL; ++index_COL )
  {
    if(( index_COL + 1 ) * cellLength > length_X )
      break ;
  }
  ROW = ROW * COL / index_COL ;
  COL = index_COL ;

  for( rowNumber = 0; rowNumber < ROW; ++rowNumber )
  {
    start_Y += rowNumber ;
    if( start_Y - Y >= length_Y )
    {
      break ;
    }
    rc = fixedOutputLocation( start_Y, X, start_Y, start_X,
                              0, length_X -COL * cellLength,
                              autoSetType ) ;
    if( rc )
    {
      goto error ;
    }
    index_COL = 0 ;
    start_fixed_mobile = end_fixed_mobile ;
    while( 1 )
    {
      Fixed = &DS.fixedField[end_fixed_mobile] ;
      if( start_X + cellLength - X > length_X )
      {
        break ;
      }
      if( end_fixed_mobile >= FLength)
      {
        break ;
      }
      if( index_COL >= COL )
      {
        break ;
      }
      rc = getFieldNameAndColour( Fixed[0], displayMode,
                                  fieldName, fieldColour) ;
      if( rc )
      {
        goto error ;
      }
      getColourPN( fieldColour, pairNumber ) ;
      attron( COLOR_PAIR( pairNumber ) ) ;
      rc = mvprintw_CBTOP( fieldName, cellLength, Fixed->alignment,
                           start_Y, start_X ) ;
      if( rc )
      {
        goto error ;
      }
      attroff( COLOR_PAIR( pairNumber ) ) ;
      start_X += cellLength ;
      ++start_X ; // add separate space
      ++end_fixed_mobile ;
      ++index_COL ;
    }
    while( 1 )
    {
      Mobile = &DS.mobileField[end_fixed_mobile - FLength] ;
      if( start_X + cellLength - X > length_X )
        break ;
      if( end_fixed_mobile - FLength >= MLength )
        break ;
      if( index_COL >= COL )
        break ;
      rc = getFieldNameAndColour( Mobile[0], displayMode,
                                  fieldName, fieldColour ) ;
      if( rc )
      {
        goto error ;
      }
      getColourPN( fieldColour, pairNumber ) ;
      attron( COLOR_PAIR( pairNumber ) ) ;
      rc = mvprintw_CBTOP( fieldName, cellLength, Mobile->alignment,
                           start_Y, start_X ) ;
      if( rc )
      {
        goto error ;
      }
      attroff( COLOR_PAIR( pairNumber ) ) ;
      start_X += cellLength ;
      ++start_X ; // add separate space
      ++end_fixed_mobile ;
      ++index_COL ;
    }

    start_Y += 1 ;
    if( start_Y - Y >= length_Y )
    {
      goto done ;
    }

    for( pos_snapshot = 0;
         pos_snapshot < ((input.cur_Snapshot.size() > 0) ? 1 : 0); //only print newest value
         ++pos_snapshot )
    {
      pos_Field = start_fixed_mobile ;
      bsonobj = input.cur_Snapshot[pos_snapshot] ;
      rc = fixedOutputLocation( start_Y, X, start_Y, start_X, 0,
                                length_X -COL * cellLength, autoSetType ) ;
      if( rc )
      {
        goto error ;
      }
      while( 1 )
      {
        Fixed = &DS.fixedField[pos_Field] ;
        if( start_X + cellLength - X > length_X )
          break ;
        if( pos_Field >= FLength )
          break ;
        if( pos_Field >= end_fixed_mobile)
          break ;
        getFieldNameAndColour( Fixed[0], displayMode, fieldName,
                               fieldColour) ;
        getColourPN( fieldColour, pairNumber ) ;
        result = NULLSTRING ;
        rc = getResultFromBSONObj( bsonobj, Fixed->sourceField,
                                   displayMode, result, Fixed->canSwitch,
                                   baseField, Fixed->warningValue,
                                   pairNumber) ;
        attron( COLOR_PAIR( pairNumber ) ) ;
        if( rc )
        {
          goto error ;
        }
        rc = mvprintw_CBTOP( result, cellLength, Fixed->alignment,
                             start_Y, start_X ) ;
        if( rc )
        {
          goto error ;
        }
        attroff( COLOR_PAIR( pairNumber ) ) ;
        start_X += cellLength ;
        ++start_X ; // add separate pace
        ++pos_Field ;
      }
      while( 1 )
      {
        Mobile = &DS.mobileField[pos_Field - FLength] ;
        if( start_X + cellLength - X > length_X )
          break ;
        if( pos_Field >= end_fixed_mobile )
          break ;
        getFieldNameAndColour( Mobile[0], displayMode, fieldName,
                               fieldColour ) ;
        getColourPN( fieldColour, pairNumber ) ;
        result = NULLSTRING ;
        rc = getResultFromBSONObj( bsonobj, Mobile->sourceField,
                                   displayMode, result, Mobile->canSwitch,
                                   baseField, Mobile->warningValue,
                                   pairNumber );
        attron( COLOR_PAIR( pairNumber ) ) ;
        if( rc )
        {
          goto error ;
        }
        rc = mvprintw_CBTOP( result, cellLength, Mobile->alignment,
                             start_Y, start_X ) ;
        if( rc )
        {
          goto error ;
        }
        attroff( COLOR_PAIR( pairNumber ) ) ;
        start_X += cellLength;
        ++start_X ; // add separate pace
        ++pos_Field ;
      }
      start_Y += 1 ;
      if( start_Y - Y >= length_Y )
      {
        break ;
      }
    }
    start_Y -= rowNumber ;
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::refreshDS_List( DynamicSnapshotOutPut &DS, Position &position,
                               const string &autoSetType )
{
  int32_t rc                      = OB_SUCCESS ;
  BSONObj bsonobj ;
  FieldStruct* Fixed            = NULL ;
  FieldStruct* Mobile           = NULL ;
  int32_t FLength               = 0 ;
  int32_t MLength               = 0 ;
  InputPanel &input             = root.input ;
  int32_t Y                     = position.referUpperLeft_Y ;
  int32_t X                     = position.referUpperLeft_X ;
  int32_t length_Y              = position.length_Y ;
  int32_t length_X              = position.length_X ;
  int32_t start_Y               = Y ;
  int32_t start_X               = X ;

  string dividingChar           = DIVIDINGCHAR ;
  int32_t dividingColour          = 0 ;
  string dividingLine           = NULLSTRING ;
  string fieldName              = NULLSTRING ;
  Colours fieldColour ;
  fieldColour.backGroundColor   = 6 ;
  fieldColour.foreGroundColor   = 0 ;
  string displayMode            =
      DISPLAYMODECHOOSER[input.displayModeChooser] ;
  string baseField              = DS.baseField ;
  string serialNumberAlignment  = ALIGNMENT_LEFT ;

  int32_t sum                     = 0 ;
  uint32_t pos_snapshot           = 0 ;
  int32_t pairNumber              = 0 ;
  string result                   =  NULLSTRING ;
  int32_t fLength                 = 0 ;
  int32_t mLength                 = 0 ;
  char serialNumber[SERIALNUMBER_LENGTH] = {0} ;

  (void)pos_snapshot;

  getColourPN( input.colourOfTheDividingLine, dividingColour );
  FLength = DS.actualFixedFieldLength ;
  MLength = DS.actualMobileFieldLength ;
  ossMemset( serialNumber, 0, SERIALNUMBER_LENGTH ) ;

  //每一页显示的数据条数，最后一行留空，并且需要去掉标题行以及分割符
  input.recordCountPerPage = length_Y - 3;

  sum = 0 ;
  for( fLength = 0; fLength < FLength; ++fLength )
  {
    Fixed = &DS.fixedField[fLength] ;
    if( sum + Fixed->contentLength > length_X )
      break ;
    sum += Fixed->contentLength ;
  }
  if( input.fieldPosition >= MLength )
  {
    input.fieldPosition = MLength - 1 ;
  }
  for( mLength = input.fieldPosition;
       mLength < MLength; ++mLength )
  {
    Mobile = &DS.mobileField[mLength] ;
    if( sum + Mobile->contentLength  > length_X )
      break ;
    sum += Mobile->contentLength ;
  }
  rc = fixedOutputLocation( start_Y, X, start_Y, start_X,
                            0, length_X - sum, autoSetType );
  if( rc )
  {
    goto error ;
  }

  start_X += SERIALNUMBER_LENGTH ;
  ++start_X ; // add separate pace

  for( fLength = 0; fLength < FLength; ++fLength )
  {
    Fixed = &DS.fixedField[fLength] ;
    if( start_X + Fixed->contentLength - X > length_X )
      break ;
    rc = getFieldNameAndColour( Fixed[0], displayMode, fieldName,
                                fieldColour) ;
    if( rc )
    {
      goto error ;
    }
    getColourPN( fieldColour, pairNumber ) ;
    attron( COLOR_PAIR( pairNumber ) ) ;
    rc = mvprintw_CBTOP( fieldName, Fixed->contentLength, Fixed->alignment,
                         start_Y, start_X ) ;
    if( rc )
    {
      goto error;
    }
    attroff( COLOR_PAIR( pairNumber ) ) ;

    attron( COLOR_PAIR( dividingColour ) ) ;
    dividingLine = getDividingLine( DIVIDINGCHAR, Fixed->contentLength ) ;
    rc = mvprintw_CBTOP( dividingLine, Fixed->contentLength,
                         Fixed->alignment, start_Y+1, start_X ) ;
    if( rc )
    {
      goto error;
    }
    attroff( COLOR_PAIR( dividingColour ) ) ;
    start_X += Fixed->contentLength ;
    ++start_X ; // add separate pace
  }

  if( input.fieldPosition >= MLength )
  {
    input.fieldPosition = MLength - 1 ;
  }

  for( mLength = input.fieldPosition; mLength < MLength; ++mLength )
  {
    Mobile = &DS.mobileField[mLength] ;
    if( start_X + Mobile->contentLength - X > length_X )
      break ;
    rc = getFieldNameAndColour( Mobile[0], displayMode,
                                fieldName, fieldColour);
    if( rc )
    {
      goto error ;
    }
    getColourPN( fieldColour, pairNumber ) ;
    attron( COLOR_PAIR( pairNumber ) ) ;
    rc = mvprintw_CBTOP( fieldName, Mobile->contentLength,Mobile->alignment,
                         start_Y, start_X ) ;
    if( rc )
    {
      goto error ;
    }
    attroff( COLOR_PAIR( pairNumber ) ) ;

    attron( COLOR_PAIR( dividingColour ) ) ;
    dividingLine = getDividingLine( DIVIDINGCHAR, Mobile->contentLength ) ;
    rc = mvprintw_CBTOP( dividingLine, Mobile->contentLength,
                         Mobile->alignment, start_Y+1, start_X ) ;
    if( rc )
    {
      goto error;
    }
    attroff( COLOR_PAIR( dividingColour ) ) ;
    start_X += Mobile->contentLength ;
    ++start_X ; // add separate pace
  }

  start_Y += 2 ; //
  if( start_Y - Y >= length_Y )
  {
    goto done ;
  }
  fixedOutputLocation( start_Y, X, start_Y, start_X, 0, length_X -sum ,
                       autoSetType ) ;
  //如果是第一页，则从第0条记录开始显示；否则，从上一页最后一条记录的后一条记录开始显示
  //但需要跳过本页的标题栏、分隔符、以及上一页最后一行的空行
  for( int32_t cnt = 0, pos_snapshot = ((root.input.lastPageNumber > 0) ? root.input.lastPageNumber : 0);
       pos_snapshot < static_cast<int32_t>(input.cur_Snapshot.size()) && cnt < input.recordCountPerPage;
       ++pos_snapshot, ++cnt )
  {

    ossMemset( serialNumber, 0, SERIALNUMBER_LENGTH ) ;

    ossSnprintf( serialNumber, SERIALNUMBER_LENGTH,"%3d", pos_snapshot + 1 ) ;
    rc = mvprintw_CBTOP( serialNumber, SERIALNUMBER_LENGTH,
                         serialNumberAlignment, start_Y, start_X ) ;
    if( rc )
    {
      goto error;
    }

    start_X += SERIALNUMBER_LENGTH ;
    ++start_X ; // add separate pace

    bsonobj = input.cur_Snapshot[pos_snapshot] ;
    for( fLength = 0; fLength < FLength; ++fLength )
    {
      Fixed = &DS.fixedField[fLength] ;
      if( start_X + Fixed[fLength].contentLength - X > length_X )
        break ;
      getFieldNameAndColour( Fixed[0], displayMode, fieldName, fieldColour ) ;
      getColourPN( fieldColour, pairNumber ) ;
      result = NULLSTRING ;
      rc = getResultFromBSONObj( bsonobj, Fixed->sourceField, displayMode,
                                 result, Fixed->canSwitch, baseField,
                                 Fixed->warningValue, pairNumber );
      attron( COLOR_PAIR( pairNumber ) ) ;
      if( rc )
      {
        goto error ;
      }
      rc = mvprintw_CBTOP( result, Fixed->contentLength, Fixed->alignment,
                           start_Y, start_X ) ;
      if( rc )
      {
        goto error;
      }
      attroff( COLOR_PAIR( pairNumber ) ) ;
      start_X += Fixed->contentLength ;
      ++start_X ; // add separate pace
    }
    if( input.fieldPosition >= MLength )
    {
      input.fieldPosition = MLength - 1 ;
    }
    for( mLength = input.fieldPosition; mLength < MLength; ++mLength )
    {
      Mobile = &DS.mobileField[mLength] ;
      if( start_X + Mobile->contentLength - X > length_X )
        break ;
      getFieldNameAndColour( Mobile[0], displayMode, fieldName, fieldColour) ;
      getColourPN( fieldColour, pairNumber ) ;
      result =  NULLSTRING ;
      rc = getResultFromBSONObj( bsonobj, Mobile->sourceField, displayMode,
                                 result, Mobile->canSwitch, baseField,
                                 Mobile->warningValue, pairNumber ) ;
      attron( COLOR_PAIR( pairNumber ) ) ;
      if( rc )
      {
        goto error ;
      }
      rc = mvprintw_CBTOP( result, Mobile->contentLength, Mobile->alignment,
                           start_Y, start_X ) ;
      if( rc )
      {
        goto error ;
      }
      attroff( COLOR_PAIR( pairNumber ) ) ;
      start_X += Mobile->contentLength ;
      ++start_X ; // add separate pace
    }
    start_Y += 1 ;
    fixedOutputLocation( start_Y, X, start_Y, start_X, 0,length_X -sum ,
                         autoSetType );
    if( start_Y - Y >= length_Y - 1)
    {
      break ;
    }
  }

done :
  return rc ;
error :
  goto done ;
}
int32_t Event::refreshDS( DynamicSnapshotOutPut &DS, Position &position )
{
  int32_t rc                    = OB_SUCCESS ;
  InputPanel &input             = root.input ;
  string AUTOSETTYPE            = NULLSTRING ;
  string STYLE                  = NULLSTRING ;
  int32_t ROW                   = 0 ;
  int32_t COL                   = 0 ;
  string fieldName              = NULLSTRING ;
  if( GLOBAL == input.snapshotModeChooser )
  {
    AUTOSETTYPE = DS.globalAutoSetType ;
    STYLE = DS.globalStyle ;
    if( TABLE == STYLE )
    {
      ROW = DS.globalRow ;
      COL = DS.globalCol ;
    }
  }
  else if( GROUP == input.snapshotModeChooser )
  {
    AUTOSETTYPE = DS.groupAutoSetType ;
    STYLE = DS.groupStyle ;
    if( TABLE == STYLE )
    {
      ROW = DS.groupRow ;
      COL = DS.groupCol ;
    }
  }
  else if( NODE == input.snapshotModeChooser )
  {
    AUTOSETTYPE = DS.nodeAutoSetType ;
    STYLE = DS.nodeStyle ;
    if( TABLE == STYLE )
    {
      ROW = DS.nodeRow ;
      COL = DS.nodeCol ;
    }
  }
  else
  {
    YYSYS_LOG(WARN, "refreshDisplayContent failed, wrong snapshotModeChooser: %s",
              input.snapshotModeChooser.c_str() ) ;
    rc = OB_ERROR ;
    goto error ;
  }
  if( TABLE == STYLE )
  {
    rc = refreshDS_Table( DS, ROW, COL, position, AUTOSETTYPE ) ;
    if( rc )
    {
      goto error ;
    }
  }
  else
  {
    refreshDS_List( DS, position, AUTOSETTYPE ) ;
    if( rc )
    {
      goto error ;
    }
  }
done :
  return rc ;
error :
  goto done ;

}

int32_t Event::refreshDisplayContent( DisplayContent &displayContent,
                                      string displayType,
                                      Position &actualPosition )
{
  int32_t rc = OB_SUCCESS ;
  int32_t pairNumber  = 0 ;
  if( DISPLAYTYPE_STATICTEXT_HELP_HEADER == displayType ||
      DISPLAYTYPE_STATICTEXT_MAIN        == displayType ||
      DISPLAYTYPE_STATICTEXT_LICENSE     == displayType )
  {
    getColourPN(
          displayContent.staticTextOutPut.colour,
          pairNumber );
    attron( COLOR_PAIR( pairNumber ) ) ;
    mvprintw( actualPosition.referUpperLeft_Y,
              actualPosition.referUpperLeft_X,
              displayContent.staticTextOutPut.outputText ) ;
    attroff( COLOR_PAIR( pairNumber ) ) ;
  }
  else if( DISPLAYTYPE_DYNAMIC_HELP == displayType )
  {
    rc = refreshDH( displayContent.dynamicHelp,
                    actualPosition ) ;
    if( rc )
    {
      goto error ;
    }

  }
  else if( DISPLAYTYPE_DYNAMIC_EXPRESSION == displayType )
  {
    rc = refreshDE( displayContent.dyExOutPut,
                    actualPosition ) ;
    if( rc )
    {
      goto error ;
    }
  }
  else if( DISPLAYTYPE_DYNAMIC_SNAPSHOT == displayType )
  {
    rc = refreshDS( displayContent.dySnapshotOutPut,
                    actualPosition ) ;
    if( rc )
    {
      goto error ;
    }
  }
  else
  {
    YYSYS_LOG(WARN, "refreshDisplayContent failed,wrong displayType: %s",
              displayType.c_str()) ;
    rc = OB_ERROR;
    goto error ;
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::refreshNodeWindow( NodeWindow &window )
{
  int32_t rc = OB_SUCCESS ;
  Position actualPosition ;
  int32_t row = 0;
  int32_t col = 0;
  actualPosition.length_X = 0 ;
  actualPosition.length_Y = 0 ;
  actualPosition.referUpperLeft_X = 0 ;
  actualPosition.referUpperLeft_Y = 0 ;
  rc = getActualPosition( actualPosition, window.position,
                          window.zoomMode, window.occupyMode) ;
  if( rc )
  {
    YYSYS_LOG(WARN, "refreshNodeWindow failed, getActualPosition failed") ;
    rc = OB_ERROR;
    goto error ;
  }
  getmaxyx( stdscr, row, col ) ;
  if( row < window.actualWindowMinRow || col < window.actualWindowMinColumn )
  {
    goto done ;
  }
  rc = refreshDisplayContent( window.displayContent, window.displayType,
                              actualPosition ) ;
  if( rc )
  {
    YYSYS_LOG(WARN, "refreshNodeWindow failed, refreshDisplayContent failed");
    rc = OB_ERROR;
    goto error;
  }

done :
  return rc ;
error :
  goto done ;
}

int32_t Event::refreshHT( HeadTailMap *headtail )
{
  int32_t rc = OB_SUCCESS ;
  int32_t numOfSubWindow = 0 ;
  if( !headtail )
    goto done ;
  for( numOfSubWindow = 0;
       numOfSubWindow < headtail->value.numOfSubWindow;
       ++numOfSubWindow )
  {
    rc = refreshNodeWindow( headtail->value.subWindow[numOfSubWindow] );
    if ( rc )
    {
      YYSYS_LOG(WARN, "refreshHeadTail failed, refreshNodeWindow failed, numOfSubWindow = %d",
                numOfSubWindow ) ;
      rc = OB_ERROR;
      goto error ;
    }
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::refreshBD( BodyMap *body )
{
  int32_t rc = OB_SUCCESS ;
  int32_t numOfSubWindow = 0 ;
  for( numOfSubWindow = 0;
       numOfSubWindow < body->value.numOfSubWindow;
       ++numOfSubWindow )
  {
    rc = refreshNodeWindow( body->value.subWindow[numOfSubWindow] ) ;
    if ( rc )
    {
      YYSYS_LOG(WARN, "refreshBody failed, refreshNodeWindow failed, numOfSubWindow = %d",
                numOfSubWindow ) ;
      rc = OB_ERROR;
      goto error ;
    }
  }
done :
  return rc ;
error :
  goto done ;
}
void Event::initAllColourPairs()
{
  int32_t back = 0 ;
  int32_t fore = 0 ;
  int32_t pairNumber = 0 ;
  for( back = 0; back < COLOR_MULTIPLE; ++back )
  {
    for( fore = 0; fore < COLOR_MULTIPLE; ++fore )
    {
      pairNumber = fore + back * COLOR_MULTIPLE ;
      init_pair( static_cast<short>(pairNumber),
                 static_cast<short>(fore),
                 static_cast<short>(back)) ;
    }
  }
}

int32_t Event::addFixedHotKey()
{
  int32_t rc = OB_SUCCESS ;
  int32_t keyLength = 0 ;
  int32_t keyLengthFromConf = 0 ;
  KeySuite *keySuite = NULL ;
  HotKey *hotkey = NULL ;
  for( int32_t i = 0; i < root.keySuiteLength; ++i )
  {
    keySuite = &root.keySuite[i] ;
    keyLength = keySuite->hotKeyLength ;
    keyLengthFromConf = keySuite->hotKeyLengthFromConf ;
    if( keyLength < keyLengthFromConf )
    {
      hotkey = &keySuite->hotKey[keyLength] ;
      hotkey->button = BUTTON_TAB ;
      hotkey->jumpType = JUMPTYPE_FIXED;
      hotkey->jumpName = "Evaluation Model" ;
      hotkey->desc = "switch display Model" ;
      hotkey->wndType = FALSE ;
      ++keyLength ;
    }
    else
    {
      rc = OB_ERROR ;
      goto error ;
    }

    if( keyLength < keyLengthFromConf )
    {
      hotkey = &keySuite->hotKey[keyLength] ;
      hotkey->button = BUTTON_LEFT;
      hotkey->jumpType = JUMPTYPE_FIXED;
      hotkey->jumpName = "Move left" ;
      hotkey->desc = "move left" ;
      hotkey->wndType = FALSE ;
      ++keyLength ;
    }
    else
    {
      rc = OB_ERROR ;
      goto error ;
    }

    if( keyLength < keyLengthFromConf )
    {
      hotkey = &keySuite->hotKey[keyLength] ;
      hotkey->button = BUTTON_RIGHT ;
      hotkey->jumpType = JUMPTYPE_FIXED ;
      hotkey->jumpName = "Move right" ;
      hotkey->desc = "move right" ;
      hotkey->wndType = FALSE ;
      ++keyLength ;
    }
    else
    {
      rc = OB_ERROR ;
      goto error ;
    }

    if( keyLength < keyLengthFromConf )
    {
      hotkey = &keySuite->hotKey[keyLength] ;
      hotkey->button = BUTTON_ENTER ;
      hotkey->jumpType = JUMPTYPE_FIXED ;
      hotkey->jumpName = "last view" ;
      hotkey->desc = "to last view, used under help window" ;
      hotkey->wndType = FALSE ;
      ++keyLength ;
    }
    else
    {
      rc = OB_ERROR ;
      goto error ;
    }

    if( keyLength < keyLengthFromConf )
    {
      hotkey = &keySuite->hotKey[keyLength] ;
      hotkey->button = BUTTON_ESC;
      hotkey->jumpType = JUMPTYPE_FIXED ;
      hotkey->jumpName = "cancel the operation" ;
      hotkey->desc = "cancel current operation" ;
      hotkey->wndType = FALSE ;
      ++keyLength ;
    }
    else
    {
      rc = OB_ERROR ;
      goto error ;
    }

    if( keyLength < keyLengthFromConf )
    {
      hotkey = &keySuite->hotKey[keyLength] ;
      hotkey->button = BUTTON_F5;
      hotkey->jumpType = JUMPTYPE_FIXED ;
      hotkey->jumpName = "refresh" ;
      hotkey->desc = "refresh immediately" ;
      hotkey->wndType = FALSE ;
      ++keyLength ;
    }
    else
    {
      rc = OB_ERROR ;
      goto error ;
    }

    if( keyLength < keyLengthFromConf )
    {
      hotkey = &keySuite->hotKey[keyLength] ;
      hotkey->button = BUTTON_H_LOWER;
      hotkey->jumpType = JUMPTYPE_FIXED ;
      hotkey->jumpName = "Help" ;
      hotkey->desc = "help" ;
      hotkey->wndType = TRUE ;
      ++keyLength ;
    }
    else
    {
      rc = OB_ERROR ;
      goto error ;
    }

    if( keyLength < keyLengthFromConf )
    {
      hotkey = &keySuite->hotKey[keyLength] ;
      hotkey->button = BUTTON_Q_LOWER;
      hotkey->jumpType = JUMPTYPE_FIXED ;
      hotkey->jumpName = "Quit" ;
      hotkey->desc = "quit" ;
      hotkey->wndType = FALSE ;
      ++keyLength ;
    }
    else
    {
      rc = OB_ERROR ;
      goto error ;
    }

    keySuite->hotKeyLength = keyLength ;
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::matchNameInFieldStruct( const FieldStruct *src,
                                       const string DisplayName )
{
  int32_t rc         = OB_SUCCESS ;
  InputPanel &input  = root.input ;
  string displayMode = DISPLAYMODECHOOSER[input.displayModeChooser] ;
  if( ABSOLUTE == displayMode )
  {
    if( DisplayName == src->absoluteName )
    {
      input.sortingField = src->sourceField ;
    }
    else
    {
      rc = OB_ERROR ;
      goto error ;
    }
  }
  else if( DELTA == displayMode )
  {
    if(  src->canSwitch )
    {
      if( DisplayName == src->deltaName )
      {
        input.sortingField = src->sourceField ;
      }
      else
      {
        rc = OB_ERROR ;
        goto error ;
      }
    }
    else
    {
      if( DisplayName == src->absoluteName )
      {
        input.sortingField = src->sourceField ;
      }
      else
      {
        rc = OB_ERROR ;
        goto error ;
      }
    }
  }
  else if( AVERAGE == displayMode )
  {
    if(  src->canSwitch )
    {
      if( DisplayName == src->averageName )
      {
        input.sortingField = src->sourceField ;
      }
      else
      {
        rc = OB_ERROR ;
        goto error ;
      }
    }
    else
    {
      if( DisplayName == src->absoluteName )
      {
        input.sortingField = src->sourceField ;
      }
      else
      {
        rc = OB_ERROR ;
        goto error ;
      }
    }
  }
  else
  {
    rc = OB_ERROR ;
    goto error ;
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::matchSourceFieldByDisplayName( const string DisplayName )
{
  int32_t rc = OB_SUCCESS ;
  int32_t numOfSubWindow = 0 ;
  FieldStruct *Fixed = NULL ;
  FieldStruct *Mobile = NULL ;
  int32_t FixedLength = 0 ;
  int32_t MobileLength = 0 ;
  InputPanel &input = root.input ;
  input.sortingField = NULLSTRING ;
  Panel &panel = input.activatedPanel->value ;
  NodeWindow *window = NULL ;
  DynamicSnapshotOutPut *DS = NULL ;
  string displayMode = DISPLAYMODECHOOSER[input.displayModeChooser] ;
  for( numOfSubWindow = 0; numOfSubWindow < panel.numOfSubWindow;
       ++numOfSubWindow )
  {
    window = &panel.subWindow[numOfSubWindow] ;
    if( DISPLAYTYPE_DYNAMIC_SNAPSHOT == window->displayType )
    {
      DS = &window->displayContent.dySnapshotOutPut ;
      FixedLength =
          DS->actualFixedFieldLength ;
      MobileLength =
          DS->actualMobileFieldLength ;
      while( FixedLength > 0 )
      {
        --FixedLength ;
        Fixed = &DS->fixedField[FixedLength] ;
        rc = matchNameInFieldStruct( Fixed, DisplayName ) ;
        if( !rc )
        {
          goto done ;
        }
      }
      while( MobileLength> 0 )
      {
        --MobileLength ;
        Mobile = &DS->mobileField[MobileLength] ;
        rc = matchNameInFieldStruct( Mobile, DisplayName ) ;
        if( !rc )
        {
          goto done ;
        }
      }
    }
  }
done :
  return rc ;
}

int32_t Event::eventManagement( int64_t key ,int32_t isFirstStart )
{
  int32_t rc = OB_SUCCESS ;
  isNeedRefresh = true;
  fd_set fds ;
  int32_t maxfd = STDIN + 1 ;
  KeySuite* keySuite = NULL ;
  HotKey* hotKey = NULL ;
  int32_t row = 0 ;
  int32_t col = 0 ;
  int32_t filterNum = 0 ;
  int32_t refreshInterval = 0 ;
  string note = NULLSTRING ;
  string displayName = NULLSTRING ; // use it when sorting
  HeadTailMap *header = NULL ;
  HeadTailMap *footer = NULL ;
  InputPanel &input = root.input ;
  BodyMap *activatedPanel = input.activatedPanel ;
  rc = getActivatedKeySuite( &keySuite ) ;
  if( rc )
  {
    goto error ;
  }
  if( 0 == key )
  {
    goto done ;
  }
  else if( 0 > key )
  {
    rc = OB_ERROR ;
    goto error ;
  }
  else
  {
    for( int32_t i = 0; i < keySuite->hotKeyLength; ++i )
    {
      if( keySuite->hotKey[i].button == key )
      {
        hotKey = &keySuite->hotKey[i] ;
        break;
      }
    }
    if( !hotKey )
    {
      if( !isFirstStart )
      {
        rc = eventManagement( BUTTON_H_LOWER, FALSE );
      }
    }
    else
    {
      if( JUMPTYPE_PANEL == hotKey->jumpType )
      {
        input.lastPageNumber = 0;
        rc = assignActivatedPanelByLabelName( &input.activatedPanel,
                                              hotKey->jumpName ) ;
        if( rc )
        {
          YYSYS_LOG(WARN, "assignPanelByLabelName failed") ;
          rc = OB_ERROR;
          goto error ;
        }
        input.displayModeChooser = 0 ;
        input.snapshotModeChooser = GLOBAL ;
        input.forcedToRefresh_Global = REFRESH ;
        input.sortingField = NULLSTRING ;
        input.sortingWay = NULLSTRING ;
        input.filterNumber = 0 ;
        input.filterCondition= NULLSTRING;
        input.isFirstGetSnapshot = TRUE ;
      }
      else if( JUMPTYPE_FIXED == hotKey->jumpType )
      {
        if( hotKey->button >= 256 )
        {
          if( BUTTON_LEFT == hotKey->button )//left
          {
            --root.input.fieldPosition ;
            if( input.fieldPosition < 0 )
            {
              input.fieldPosition = 0 ;
            }
            input.forcedToRefresh_Local = REFRESH ;
          }
          else if( BUTTON_RIGHT == hotKey->button )//right
          {
            ++input.fieldPosition ;
            input.forcedToRefresh_Local = REFRESH ;
          }
          else if( BUTTON_F5 == hotKey->button )
          {
            input.forcedToRefresh_Global= REFRESH ;
          }
        }
        else if( BUTTON_Q_LOWER == hotKey->button )
        {
          rc = CB_CBTOP_DONE ;
        }
        else if( BUTTON_ENTER == hotKey->button )
        {
          input.forcedToRefresh_Local= REFRESH ;
        }
        else if( BUTTON_ESC == hotKey->button )
        {
          input.forcedToRefresh_Local= REFRESH ;
        }
        else if( BUTTON_TAB == hotKey->button )
        {
          if( BODYTYPE_NORMAL !=
              activatedPanel->bodyPanelType )
          {
            goto done;
          }
          ++input.displayModeChooser ;
          input.displayModeChooser %= DISPLAYMODENUMBER ;
          input.forcedToRefresh_Local = REFRESH ;
        }
        else if( BUTTON_H_LOWER == hotKey->button )
        {
          rc = assignActivatedPanel( &activatedPanel,
                                     BODYTYPE_HELP_DYNAMIC) ;
          rc = getActivatedHeadTailMap( activatedPanel,
                                        &header, &footer) ;
          if( rc )
          {
            goto error ;
          }
          rc = refreshAll( header, activatedPanel, footer, TRUE ) ;
          if( rc )
          {
            goto error ;
          }
          maxfd = STDIN + 1 ;
          FD_ZERO ( &fds ) ;
          FD_SET ( STDIN, &fds ) ;
          rc = select ( maxfd, &fds, NULL, NULL, NULL ) ;
          if( 0 > rc )
          {
            rc = eventManagement( BUTTON_H_LOWER, FALSE ) ;
          }
          else if( 0 < rc )
          {
            ossMemset( cbtopBuffer, 0, BUFFERSIZE ) ;
            read(STDIN, cbtopBuffer, BUFFERSIZE ) ;
            rc = getCbTopKey( cbtopBuffer, key) ;
            if( rc )
            {
              goto error ;
            }
            rc = eventManagement( key, FALSE ) ;
          }
          else
          {
            YYSYS_LOG(WARN, "buttonManagement failed, select ( maxfd, &fds, NULL, NULL, NULL) failed") ;
            rc = OB_ERROR;
            goto error ;
          }
        }
      }
      else if( JUMPTYPE_GLOBAL == hotKey->jumpType )
      {
        if( BODYTYPE_NORMAL != activatedPanel->bodyPanelType )
        {
          rc = eventManagement( BUTTON_H_LOWER, FALSE ) ;
          goto done ;
        }
        input.snapshotModeChooser = GLOBAL ;
        input.forcedToRefresh_Global = REFRESH ;
      }
      else if( JUMPTYPE_STOP_REFRESH == hotKey->jumpType )
      {
        if( BODYTYPE_NORMAL != activatedPanel->bodyPanelType )
        {
          rc = eventManagement( BUTTON_H_LOWER, FALSE ) ;
          goto done ;
        }
        isNeedRefresh = false;
        root.input.refreshState = 0;
      }
      else if( JUMPTYPE_RESTART_REFRESH == hotKey->jumpType )
      {
        if( BODYTYPE_NORMAL != activatedPanel->bodyPanelType )
        {
          rc = eventManagement( BUTTON_H_LOWER, FALSE ) ;
          goto done ;
        }
        isNeedRefresh = true;
        root.input.refreshState = 1;
        input.forcedToRefresh_Global = REFRESH;
      }
      else if( JUMPTYPE_PREVIOUS_PAGE == hotKey->jumpType )
      {
        if( BODYTYPE_NORMAL != activatedPanel->bodyPanelType )
        {
          rc = eventManagement( BUTTON_H_LOWER, FALSE ) ;
          goto done ;
        }
        input.forcedToRefresh_Global =  REFRESH;
        input.lastPageNumber -= input.recordCountPerPage;
        if(input.lastPageNumber < 0)
          input.lastPageNumber = 0;
        noecho();
        curs_set(0);
      }
      else if( JUMPTYPE_NEXT_PAGE == hotKey->jumpType )
      {
        if( BODYTYPE_NORMAL != activatedPanel->bodyPanelType )
        {
          rc = eventManagement( BUTTON_H_LOWER, FALSE ) ;
          goto done ;
        }
        input.forcedToRefresh_Global = REFRESH;
        input.lastPageNumber += input.recordCountPerPage;
        if(input.lastPageNumber > static_cast<int32_t>(input.cur_Snapshot.size()))
          input.lastPageNumber -= input.recordCountPerPage;
        noecho();
        curs_set(0);
      }
      else if( JUMPTYPE_FILTER_CONDITION == hotKey->jumpType )
      {
        if( BODYTYPE_NORMAL != activatedPanel->bodyPanelType )
        {
          rc = eventManagement( BUTTON_H_LOWER, FALSE ) ;
          goto done ;
        }
        note = "please input the filter condition(eg: IP:PORT) : ";
        getmaxyx( stdscr, row, col ) ;
        curs_set( 2 ) ;
        ossMemset( cbtopBuffer, 0, BUFFERSIZE ) ;
        move( row - 1, 0 ) ;
        clrtobot() ;
        echo() ;
        mvprintw( row - 1 , ( col - static_cast<int32_t>(note.length()) ) / 2, note.c_str() ) ;
        if( BUTTON_ESC == getnstr( cbtopBuffer, BUFFERSIZE ) )
        {
          input.forcedToRefresh_Local = REFRESH ;
        }
        else
        {
          input.filterCondition = cbtopBuffer ;
          trim( input.filterCondition ) ;
          input.forcedToRefresh_Global = REFRESH ;
        }
        noecho() ;
        curs_set( 0 ) ;
      }
      else if( JUMPTYPE_NO_FILTER_CONDITION == hotKey->jumpType )
      {
        if( BODYTYPE_NORMAL != activatedPanel->bodyPanelType )
        {
          rc = eventManagement( BUTTON_H_LOWER, FALSE ) ;
          goto done ;
        }
        input.filterCondition = NULLSTRING;
        input.forcedToRefresh_Global = REFRESH ;
      }
      else if( JUMPTYPE_FILTER_NUMBER == hotKey->jumpType )
      {
        if( BODYTYPE_NORMAL != activatedPanel->bodyPanelType )
        {
          rc = eventManagement( BUTTON_H_LOWER, FALSE ) ;
          goto done ;
        }
        note = "please input the filter number(the number is additive) : ";
        getmaxyx( stdscr, row, col ) ;
        curs_set( 2 ) ;
        ossMemset( cbtopBuffer, 0, BUFFERSIZE ) ;
        move( row - 1, 0 ) ;
        clrtobot() ;
        echo() ;
        mvprintw( row - 1 , ( col - static_cast<int32_t>(note.length()) ) / 2, note.c_str() ) ;
        if( BUTTON_ESC == getnstr( cbtopBuffer, BUFFERSIZE ) )
        {
          input.forcedToRefresh_Local = REFRESH ;
        }
        else
        {
          displayName = cbtopBuffer ;
          trim( displayName ) ;
          rc = strToNum( displayName.c_str(), filterNum ) ;
          if( rc )
          {
            filterNum = 0 ;
            rc = OB_SUCCESS ;
          }
          input.filterNumber += filterNum ;
          if( 0 > input.filterNumber )
          {
            input.filterNumber = 0 ;
          }
          input.forcedToRefresh_Global = REFRESH ;
        }
        noecho() ;
        curs_set( 0 ) ;
      }
      else if( JUMPTYPE_NO_FILTER_NUMBER == hotKey->jumpType )
      {
        if( BODYTYPE_NORMAL != activatedPanel->bodyPanelType )
        {
          rc = eventManagement( BUTTON_H_LOWER, FALSE ) ;
          goto done ;
        }
        if( 0 == root.input.filterNumber )
          goto done ;
        input.filterNumber = 0;
        input.forcedToRefresh_Global = REFRESH ;
      }
      else if( JUMPTYPE_REFRESHINTERVAL == hotKey->jumpType )
      {
        if( BODYTYPE_NORMAL != activatedPanel->bodyPanelType )
        {
          rc = eventManagement( BUTTON_H_LOWER, FALSE ) ;
          goto done ;
        }
        note = "please input the refresh interval(eg: 5) : ";
        getmaxyx( stdscr, row, col ) ;
        curs_set( 2 ) ;
        ossMemset( cbtopBuffer, 0, BUFFERSIZE ) ;
        move( row - 1, 0 ) ;
        clrtobot() ;
        echo() ;
        mvprintw( row - 1 , ( col - static_cast<int32_t>(note.length()) ) / 2, note.c_str() ) ;
        if( BUTTON_ESC == getnstr( cbtopBuffer, BUFFERSIZE ) )
        {
          input.forcedToRefresh_Local = REFRESH ;
        }
        else
        {
          displayName = cbtopBuffer ;
          trim( displayName ) ;
          rc = strToNum( displayName.c_str(), refreshInterval ) ;
          if( rc || 1 > refreshInterval || 3600 < refreshInterval )
          {
            rc = OB_SUCCESS ;
          }
          else
          {
            input.refreshInterval = refreshInterval ;
            input.forcedToRefresh_Global = REFRESH ;
          }
        }
        noecho() ;
        curs_set( 0 ) ;
      }
    }
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::refreshAll( HeadTailMap *header, BodyMap *body,
                           HeadTailMap *footer, int32_t refreshAferClean )
{
  int32_t rc = OB_SUCCESS ;
  clear() ;
  if( refreshAferClean )
  {
    refresh() ;
  }
  rc = refreshHT( header ) ;
  if( rc )
  {
    goto error ;
  }
  rc = refreshBD( body ) ;
  if( rc )
  {
    goto error ;
  }
  rc = refreshHT( footer ) ;
  if( rc )
  {
    goto error ;
  }
  refresh() ;
done:
  return rc ;
error:
  goto done ;
}

struct timeval waitTime ;
struct timeval startTime ;
struct timeval endTime ;
int32_t Event::runCBTOP( int32_t useSSL )
{
  int32_t rc = OB_SUCCESS ;
  HeadTailMap* header = NULL ;
  HeadTailMap* footer = NULL ;
  int64_t key = 0 ; // the operation need to do
  fd_set fds ;

  int32_t maxfd  = STDIN + 1 ;

  (void)useSSL;

  isNeedRefresh = true;
  root.input.forcedToRefresh_Global = NOTREFRESH ;
  root.input.forcedToRefresh_Local= NOTREFRESH ;

  rc = storeRootWindow( root ) ;
  if( rc )
  {
    ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
    ossSnprintf( errStr, errStrLength,
                 "%s readConfiguration failed"OSS_NEWLINE,
                 errStrBuf ) ;
    goto error ;
  }
  rc = addFixedHotKey() ;
  if( rc )
  {

    ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
    ossSnprintf( errStr, errStrLength, "%s addFixedHotKey failed"OSS_NEWLINE,
                 errStrBuf ) ;
    goto error ;
  }
  rc = assignActivatedPanel( &root.input.activatedPanel, BODYTYPE_MAIN) ;
  if( rc )
  {

    ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
    ossSnprintf( errStr, errStrLength,
                 "%s assignActivatedPanel failed"OSS_NEWLINE, errStrBuf ) ;
    goto error ;
  }

  root.input.displayModeChooser = 0;
  if (hasInputRefreshInterval)
  {
    root.input.refreshInterval = inputRefreshInterval;
  }

  initAllColourPairs() ;
  while( 1 )
  {
    gettimeofday( &startTime, 0 ) ;
    rc = getActivatedHeadTailMap( root.input.activatedPanel, &header, &footer) ;
    if( rc )
    {

      ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,
                   "%s getActivatedHeadTailMap failed"OSS_NEWLINE,
                   errStrBuf ) ;
      goto error ;
    }
    if( BODYTYPE_NORMAL == root.input.activatedPanel->bodyPanelType )
    {
      rc = getCurSnapshot() ;
      if( rc )
      {
        goto error ;
      }
    }
    if(isNeedRefresh)
    {
      root.input.refreshState = 1;
      rc = refreshAll( header, root.input.activatedPanel, footer, TRUE ) ;
      if( rc )
      {
        goto error ;
      }
    }
    waitTime.tv_sec = root.input.refreshInterval ;
    waitTime.tv_usec = 0 ;

    gettimeofday( &endTime, 0 ) ;

    waitTime.tv_sec -= ( endTime.tv_sec - startTime.tv_sec ) ;
    waitTime.tv_usec -= ( endTime.tv_usec - startTime.tv_usec ) ;
    if( 0 > waitTime.tv_sec )
    {
      waitTime.tv_sec = 0 ;
      waitTime.tv_usec = 0 ;
    }
    else if( 0 > waitTime.tv_usec )
    {
      if( 1 > waitTime.tv_sec )
      {
        waitTime.tv_usec += waitTime.tv_sec * 1000000 ;
        if( 0 > waitTime.tv_usec )
        {
          waitTime.tv_usec = 0 ;
        }
        waitTime.tv_sec = 0 ;
      }
      else
      {
        waitTime.tv_sec -= 1 ;
        waitTime.tv_usec += 1000000 ;
      }
    }

    while( 1 )
    {
      FD_ZERO ( &fds ) ;
      FD_SET ( STDIN, &fds ) ;
      rc = select ( maxfd, &fds, NULL, NULL, &waitTime ) ;
      if( rc < 0 )
      {
        rc = refreshAll( header, root.input.activatedPanel, footer, FALSE ) ;
        if( rc )
        {
          goto error ;
        }
        break ;
      }
      else if( rc > 0 )
      {
        if ( FD_ISSET ( STDIN, &fds ) )
        {
          ossMemset( cbtopBuffer, 0, BUFFERSIZE ) ;
          read( STDIN, cbtopBuffer, BUFFERSIZE ) ;
          rc = getCbTopKey( cbtopBuffer, key) ;
          if( rc )
          {
            goto error ;
          }
          rc = eventManagement( key, 1 ) ;
          if( rc )
          {
            goto error ;
          }
          if( root.input.refreshState == 0 )
          {
            rc = refreshHT(header);
            refresh();
          }
          if(rc)
          {
            goto error;
          }
        }
      }
      else //time out
      {
        break ;
      }
      if( REFRESH == root.input.forcedToRefresh_Global )
      {
        root.input.forcedToRefresh_Global = NOTREFRESH ;
        break ;
      }
      if( REFRESH == root.input.forcedToRefresh_Local )
      {
        rc = refreshAll( header, root.input.activatedPanel, footer, FALSE ) ;
        if( rc )
        {
          goto error ;
        }
        root.input.forcedToRefresh_Local = NOTREFRESH ;
      }
    }
  }
done :
  return rc ;
error :
  goto done ;
}

int32_t Event::printCBTOP()
{
  int32_t rc = OB_SUCCESS;
  switch(printDataType)
  {
    case 1:
      rc = printMemtableData();
      if(rc)
      {
        goto error;
      }
      break;
    case 2:
      rc = printCommitlogData();
      if(rc)
      {
        goto error;
      }
      break;
    case 3:
      rc = printMergeStatus();
      if(rc)
      {
        goto error;
      }
      break;
    case 4:
      rc = printIndexBuildStatus();
      if(rc)
      {
        goto error;
      }
      break;
    default:
      break;
  }

done :
  return rc;
error :
  goto done;
}

int32_t Event::printMemtableData()
{
  int32_t rc = OB_SUCCESS;
  snapshotResult.clear();
  updateServerInfoList.clear();
  rc = getUpdateServerInfoList();
  if (OB_SUCCESS != rc)
  {
    goto error;
  }

  for(map<oceanbase::common::ObServer, oceanbase::common::ObUpsInfo>::iterator it = updateServerInfoList.begin();
      it != updateServerInfoList.end(); it++)
  {
    rc = getUpdateServerMemTableInfo(it->first, it->second);
    if(OB_SUCCESS != rc)
    {
      rc = OB_SUCCESS;
      continue;
    }
  }
  buildBsonArrayAndPrint();

  if(rc)
  {
    YYSYS_LOG(WARN, "print memtable data failed, rc = %d", rc);
    goto error;
  }

done :
  return rc;
error:
  goto done;
}

int32_t Event::printCommitlogData()
{
  int32_t rc = OB_SUCCESS;
  snapshotResult.clear();
  updateServerInfoList.clear();
  rc = getUpdateServerInfoList();
  if (OB_SUCCESS != rc)
  {
    goto error;
  }

  for(map<oceanbase::common::ObServer, oceanbase::common::ObUpsInfo>::iterator it = updateServerInfoList.begin();
      it != updateServerInfoList.end(); it++)
  {
    rc = getUpdateServerCommitLogStatus(it->first, it->second);
    if(OB_SUCCESS != rc)
    {
      rc = OB_SUCCESS;
      continue;
    }
  }
  buildBsonArrayAndPrint();

  if(rc)
  {
    YYSYS_LOG(WARN, "print commit log data failed, rc = %d", rc);
    goto error;
  }

done :
  return rc;
error:
  goto done;
}

int32_t Event::printMergeStatus()
{
  int32_t rc = OB_SUCCESS;
  snapshotResult.clear();
  chunkServerList.clear();

  int64_t maxMergeDurationTimeout = 0;
  rc = getChunkServerList(maxMergeDurationTimeout);
  if (OB_SUCCESS != rc)
  {
    goto error;
  }

  for(map<oceanbase::common::ObServer, int32_t>::iterator it = chunkServerList.begin();
      it != chunkServerList.end(); it++)
  {
    rc = getChunkServerMergeStatus(it->first, it->second, maxMergeDurationTimeout);
    if(OB_SUCCESS != rc)
    {
      rc = OB_SUCCESS;
      continue;
    }
  }
  buildBsonArrayAndPrint();

  if(rc)
  {
    YYSYS_LOG(WARN, "print merge status failed, rc = %d", rc);
    goto error;
  }

done :
  return rc;
error:
  goto done;
}

int32_t Event::printIndexBuildStatus()
{
  int32_t rc = OB_SUCCESS;
  snapshotResult.clear();
  chunkServerList.clear();

  int64_t maxMergeDurationTimeout = 0;
  rc = getChunkServerList(maxMergeDurationTimeout);
  if (OB_SUCCESS != rc)
  {
    goto error;
  }

  for(map<oceanbase::common::ObServer, int32_t>::iterator it = chunkServerList.begin();
      it != chunkServerList.end(); it++)
  {
    BSONObj bsonobj;
    BSONObjBuilder bsonObjBuilder;
    rc = getWholeIndexBuildInfoFromRootServer(bsonObjBuilder);
    if(OB_SUCCESS != rc)
    {
      goto error;
    }
    rc = getChunkServerIndexBuildStatus(it->first, it->second, bsonObjBuilder);
    if(OB_SUCCESS != rc)
    {
      rc = OB_SUCCESS;
      goto done;
    }
    bsonobj = bsonObjBuilder.obj();
    snapshotResult.push_back(bsonobj);
  }
  buildBsonArrayAndPrint();

  if(rc)
  {
    YYSYS_LOG(WARN, "print index build status failed, rc = %d", rc);
    goto error;
  }

done :
  return rc;
error:
  goto done;
}

void Event::buildBsonArrayAndPrint()
{
  BSONObj bsonObj;
  BSONObjBuilder bsonObjBuilder;
  BSONArray bsonArray;
  BSONArrayBuilder bsonArrayBuilder;

  for(uint32_t i = 0; i < snapshotResult.size(); i++)
  {
    bsonArrayBuilder.append(snapshotResult[i]);
  }
  bsonArray = bsonArrayBuilder.arr();
  bsonObjBuilder.append("body", bsonArray);
  bsonObj = bsonObjBuilder.obj();

  printf("%s\n", bsonObj.toString().c_str());
}

void init ( po::options_description &desc )
{
  ADD_PARAM_OPTIONS_BEGIN ( desc )
      COMMANDS_OPTIONS
    #ifdef CB_SSL
      ( OPTION_SSL, "use SSL connection" )
    #endif
      ADD_PARAM_OPTIONS_END
}

void displayArg ( po::options_description &desc )
{
  std::cout << desc << std::endl ;
}

int32_t resolveArgument ( po::options_description &desc,
                          int32_t argc, char **argv )
{
  int32_t rc = OB_SUCCESS ;

  po::variables_map vm ;
  try
  {
    po::store ( po::parse_command_line ( argc, argv, desc ), vm ) ;
    po::notify ( vm ) ;
  }
  catch ( po::unknown_option &e )
  {
    std::cerr <<  "Unknown option!" << std::endl ;
 //           << e.get_option_name () << std::endl ;
    rc = OB_INVALID_ARGUMENT ;
    goto error ;
  }
  catch ( po::invalid_option_value &e )
  {
    std::cerr <<  "Invalid argument!" << std::endl ;
   //         << e.get_option_name () << std::endl ;
    rc = OB_INVALID_ARGUMENT ;
    goto error ;
  }
  catch( po::error &e )
  {
    std::cerr << e.what () << std::endl ;
    rc = OB_INVALID_ARGUMENT ;
    goto error ;
  }

  if ( vm.count ( OPTION_HELP ) )
  {
    displayArg ( desc ) ;
    rc = CBTOP_HELP_ONLY ;
    goto done ;
  }

  if( vm.count( OPTION_HOSTNAME ) )
  {
    hostname= vm[OPTION_HOSTNAME].as<string>();
  }
  if( vm.count( OPTION_SERVICENAME) )
  {
    serviceName = vm[OPTION_SERVICENAME].as<string>();
  }
  if( vm.count( OPTION_INTERVAL) )
  {
    rc = strToNum(vm[OPTION_INTERVAL].as<string>().c_str(), inputRefreshInterval);
    if(rc || 1 > inputRefreshInterval || 3600 < inputRefreshInterval)
    {
      rc = OB_SUCCESS;
      inputRefreshInterval = 3;
    }
    else
    {
      hasInputRefreshInterval = true;
    }
  }
  if(vm.count(OPTION_PRINT_DATA_TYPE))
  {
    isPrintToTerminal = true;
    string printTypeString = vm[OPTION_PRINT_DATA_TYPE].as<string>();
    if(printTypeString == OPTION_MEMTABLE)
    {
      printDataType = 1;
    }
    else if(printTypeString == OPTION_COMMITLOG)
    {
      printDataType = 2;
    }
    else if(printTypeString == OPTION_MERGE)
    {
      printDataType = 3;
    }
    else if(printTypeString == OPTION_INDEX)
    {
      printDataType = 4;
    }
    else
    {
      printDataType = -1;
    }
    if( 1 > printDataType || 4 < printDataType)
    {
      rc = OB_ERROR;
      std::cerr<< "Error! Invalid arguments:" << printTypeString << endl;
    }
  }

#ifdef CB_SSL
  if( vm.count ( OPTION_SSL ) )
  {
    useSSL = TRUE ;
  }
#endif

done :
  return rc ;
error :
  goto done ;
}

int32_t main( int32_t argc, char **argv)
{
  int32_t rc = 0 ;

  //set log parameters
  YYSYS_LOGGER.setLogLevel("INFO");
  YYSYS_LOGGER.setFileName("log/ybtop.log", true);
  YYSYS_LOGGER.setMaxFileSize(256 * 1024L * 1024L);
  YYSYS_LOG(INFO, "------------------------------------------------------");
  YYSYS_LOG(INFO, "        YBase Interactive Snapshot Monitor Log        ");
  YYSYS_LOG(INFO, "------------------------------------------------------");
  ob_init_memory_pool();

  Event ybtop ;
  po::options_description desc ( "Command options" ) ;
  init ( desc ) ;
  rc = resolveArgument ( desc, argc, argv ) ;
  if ( rc )
  {
    if ( CBTOP_HELP_ONLY != rc )
    {
      std::cerr<< "Error: Invalid arguments"OSS_NEWLINE ;
      displayArg ( desc ) ;
    }
    goto done ;
  }

  ybtop.rootServerMaster.set_ipv4_addr(hostname.c_str(), (int32_t)atoi(serviceName.c_str()));
  rc = ybtop.client.initialize(ybtop.rootServerMaster);
  if(OB_SUCCESS != rc)
  {
    ossSnprintf(errStrBuf, errStrLength, "%s", errStr);
    ossSnprintf(errStr, errStrLength,
                "%s initialize client object failed, ret:%d"OSS_NEWLINE,
                errStrBuf, rc);
    YYSYS_LOG(ERROR, "initialize client object failed, ret:%d", rc);
    rc = OB_ERROR;
    goto error;
  }

  if(isPrintToTerminal)
  {
    rc = ybtop.printCBTOP();
    if(rc && CB_CBTOP_DONE != rc)
    {
      YYSYS_LOG(ERROR, "can't print YBTOP! ret:%d", rc);
      goto error;
    }
    else
    {
      goto done;
    }
  }
  fclose(stderr);

  initscr() ;

  start_color() ;
  cbreak() ;
  keypad( stdscr, FALSE ) ;
  noecho() ;
  curs_set( 0 ) ;
  rc = ybtop.runCBTOP( useSSL ) ;
  if( rc && CB_CBTOP_DONE != rc )
  {
    YYSYS_LOG(ERROR, "can't run YBTOP! ret:%d", rc);
    goto error ;
  }
done :
  curs_set( 0 ) ;
  endwin() ;
  std::cerr << errStr << std::endl;
  return rc ;
error :
  goto done ;

  return 0;
}

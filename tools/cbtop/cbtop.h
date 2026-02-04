#ifndef CB_TOP_H
#define CB_TOP_H

#include "ncurses/include/curses.h"
#include "bson/bsonobj.h"
#include "ob_define.h"
#include "ob_client_manager.h"
#include "ob_buffer.h"
#include "data_buffer.h"
#include "ob_result.h"
#include "ob_server.h"
#include "ob_base_client.h"
#include "ob_ups_info.h"
#include "ossUtil.h"
#include "ossMem.h"
#include "oss.hpp"
#include "oss.h"
#include <sys/select.h>
#include <sys/time.h>
#include <termios.h>
#include <string.h>
#include <cstring>
#include <cctype>
#include <string>
#include <vector>
#include <map>
#include <stdint.h>
#include <boost/program_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/typeof/typeof.hpp>

using namespace bson;
using namespace std;
using namespace oceanbase::common;
using namespace boost::property_tree;
namespace po = boost::program_options;

#define CBTOP_SAFE_DELETE(p) \
  do {                       \
    if (p) {                 \
      CB_OSS_DEL []p ;       \
      p = NULL ;             \
    }                        \
  } while (0)

#define ADD_PARAM_OPTIONS_BEGIN(desc)\
  desc.add_options()
#define ADD_PARAM_OPTIONS_END ;

#define STDIN 0

#define OPTION_HELP "help"
#define OPTION_INSERT "insert"
#define OPTION_UPDATE "update"
#define OPTION_QUERY "query"
#define OPTION_DELETE "delete"
#define OPTION_CS "cs"
#define OPTION_CS_NUM "cs_num"
#define OPTION_COLLECTION "collection"
#define OPTION_COLLECTION_NUM "collection_num"
#define OPTION_INDEX_SCALE "index_scale"
#define OPTION_THREAD "thread"
#define OPTION_HOST "host"
#define OPTION_PORT "port"

#define CBTOP_VERSION "cbtop 2.0"
#define CBTOP_DEFAULT_CONFPATH "etc/conf/ybtop.xml"
#define CBTOP_DEFAULT_HOSTNAME "localhost"
#define CBTOP_DEFAULT_SERVICENAME "2500"
#define CBTOP_REFRESH_QUIT_HELP "Refresh: F5, Quit:q, Help:h"
#define CBTOP_HELP_ONLY 999999
#define NULLSTRING ""
#define STRING_NULL "NULL"
#define DIVIDINGCHAR "-"
#define OUTPUT_FORMATTING "%.3f"

#define ZOOM_MODE_ALL "ZOOM_MODE_ALL"
#define ZOOM_MODE_NONE "ZOOM_MODE_NONE"
#define ZOOM_MODE_POS "ZOOM_MODE_POS"
#define ZOOM_MODE_ROW_POS "ZOOM_MODE_ROW_POS"
#define ZOOM_MODE_COL_POS "ZOOM_MODE_COL_POS"
#define ZOOM_MODE_POS_X "ZOOM_MODE_POS_X"
#define ZOOM_MODE_ROW_POS_X "ZOOM_MODE_ROW_POS_X"
#define ZOOM_MODE_COL_POS_X "ZOOM_MODE_COL_POS_X"
#define ZOOM_MODE_POS_Y "ZOOM_MODE_POS_Y"
#define ZOOM_MODE_ROW_POS_Y "ZOOM_MODE_ROW_POS_Y"
#define ZOOM_MODE_COL_POS_Y "ZOOM_MODE_COL_POS_Y"
#define ZOOM_MODE_ROW_COL "ZOOM_MODE_ROW_COL"
#define ZOOM_MODE_COL "ZOOM_MODE_COL"
#define ZOOM_MODE_ROW "ZOOM_MODE_ROW"

#define OCCUPY_MODE_NONE "OCCUPY_MODE_NONE"
#define OCCUPY_MODE_WINDOW_BELOW "OCCUPY_MODE_WINDOW_BELOW"

#define JUMPTYPE_PANEL "JUMPTYPE_PANEL"
#define JUMPTYPE_FUNC "JUMPTYPE_FUNC"
#define JUMPTYPE_FIXED "JUMPTYPE_FIXED"
#define JUMPTYPE_GLOBAL "JUMPTYPE_GLOBAL"
#define JUMPTYPE_GROUP "JUMPTYPE_GROUP"
#define JUMPTYPE_NODE "JUMPTYPE_NODE"
#define JUMPTYPE_FILTER_CONDITION "JUMPTYPE_FILTER_CONDITION"
#define JUMPTYPE_NO_FILTER_CONDITION "JUMPTYPE_NO_FILTER_CONDITION"
#define JUMPTYPE_FILTER_NUMBER "JUMPTYPE_FILTER_NUMBER"
#define JUMPTYPE_NO_FILTER_NUMBER "JUMPTYPE_NO_FILTER_NUMBER"
#define JUMPTYPE_REFRESHINTERVAL "JUMPTYPE_REFRESHINTERVAL"
#define JUMPTYPE_PREVIOUS_PAGE "JUMPTYPE_PREVIOUS_PAGE"
#define JUMPTYPE_NEXT_PAGE "JUMPTYPE_NEXT_PAGE"
#define JUMPTYPE_STOP_REFRESH "JUMPTYPE_STOP_REFRESH"
#define JUMPTYPE_RESTART_REFRESH "JUMPTYPE_RESTART_REFRESH"

#define SORTINGWAY_ASC  "1"
#define SORTINGWAY_DESC "-1"

#define BODYTYPE_MAIN "BODYTYPE_MAIN"
#define BODYTYPE_NORMAL "BODYTYPE_NORMAL"
#define BODYTYPE_HELP_DYNAMIC "BODYTYPE_HELP_DYNAMIC"

#define TABLE "TABLE"
#define LIST "LIST"

#define DISPLAYTYPE_NULL "DISPLAYTYPE_NULL"
#define DISPLAYTYPE_STATICTEXT_HELP_HEADER "DISPLAYTYPE_STATICTEXT_HELP_HEADER"
#define DISPLAYTYPE_STATICTEXT_LICENSE "DISPLAYTYPE_STATICTEXT_LICENSE"
#define DISPLAYTYPE_STATICTEXT_MAIN "DISPLAYTYPE_STATICTEXT_MAIN"
#define DISPLAYTYPE_DYNAMIC_HELP "DISPLAYTYPE_DYNAMIC_HELP"
#define DISPLAYTYPE_DYNAMIC_EXPRESSION "DISPLAYTYPE_DYNAMIC_EXPRESSION"
#define DISPLAYTYPE_DYNAMIC_SNAPSHOT "DISPLAYTYPE_DYNAMIC_SNAPSHOT"

#define SERIALNUMBER_LENGTH 4
#define STATIC_EXPRESSION "STATIC_EXPRESSION"
#define DYNAMIC_EXPRESSION "DYNAMIC_EXPRESSION"

#define EXPRESSION_BODY_LABELNAME "EXPRESSION_BODY_LABELNAME"
#define EXPRESSION_VERSION "EXPRESSION_VERSION"
#define EXPRESSION_REFRESH_QUIT_HELP "EXPRESSION_REFRESH_QUIT_HELP"
#define EXPRESSION_REFRESH_TIME "EXPRESSION_REFRESH_TIME"
#define EXPRESSION_REFRESH_STATE "EXPRESSION_REFRESH_STATE"
#define EXPRESSION_LOCAL_TIME "EXPRESSION_LOCAL_TIME"
#define EXPRESSION_HOSTNAME "EXPRESSION_HOSTNAME"
#define EXPRESSION_SERVICENAME "EXPRESSION_SERVICENAME"
#define EXPRESSION_USRNAME "EXPRESSION_USRNAME"
#define EXPRESSION_DISPLAYMODE "EXPRESSION_DISPLAYMODE"
#define EXPRESSION_SNAPSHOTMODE "EXPRESSION_SNAPSHOTMODE"
#define EXPRESSION_FILTER_NUMBER "EXPRESSION_FILTER_NUMBER"
#define EXPRESSION_SORTINGWAY "EXPRESSION_SORTINGWAY"
#define EXPRESSION_SORTINGFIELD "EXPRESSION_SORTINGFIELD"
#define EXPRESSION_SNAPSHOTMODE_INPUTNAME "EXPRESSION_SNAPSHOTMODE_INPUTNAME"

#define UPPER_LEFT "UPPER_LEFT"
#define MIDDLE_LEFT "MIDDLE_LEFT"
#define LOWER_LEFT "LOWER_LEFT"
#define UPPER_MIDDLE "UPPER_MIDDLE"
#define MIDDLE "MIDDLE"
#define LOWER_MIDDLE "LOWER_MIDDLE"
#define UPPER_RIGHT "UPPER_RIGHT"
#define MIDDLE_RIGHT "MIDDLE_RIGHT"
#define LOWER_RIGHT "LOWER_RIGHT"

#define ALIGNMENT_LEFT "LEFT"
#define ALIGNMENT_CENTER "CENTER"
#define ALIGNMENT_RIGHT "RIGHT"

#define CB_SNAP_CS_MERGE_STAT_TOP "CB_SNAP_CS_MERGE_STAT_TOP"
#define CB_SNAP_RS_CS_INDEX_MERGE_STAT_TOP "CB_SNAP_RS_CS_INDEX_MERGE_STAT_TOP"
#define CB_SNAP_UPS_MEMTABLE_STAT_TOP "CB_SNAP_UPS_MEMTABLE_STAT_TOP"
#define CB_SNAP_UPS_COMMIT_LOG_STAT_TOP "CB_SNAP_UPS_COMMIT_LOG_STAT_TOP"
#define CB_SNAP_NULL "CB_SNAP_NULL"
#define CB_SNAP_CONTEXTS_TOP "CB_SNAP_CONTEXTS_TOP"
#define CB_SNAP_CONTEXTS_CURRENT_TOP "CB_SNAP_CONTEXTS_CURRENT_TOP"
#define CB_SNAP_SESSIONS_CURRENT_TOP "CB_SNAP_SESSIONS_CURRENT_TOP"
#define CB_SNAP_DATABASE_TOP "CB_SNAP_DATABASE_TOP"
#define CB_SNAP_SYSTEM_TOP "CB_SNAP_SYSTEM_TOP"
#define CB_SNAP_CATALOG_TOP "CB_SNAP_CATALOG_TOP"

#define CB_SNAP_CONTEXTS                0
#define CB_SNAP_CONTEXTS_CURRENT        1
#define CB_SNAP_CS_MERGE_STAT           2
#define CB_SNAP_UPS_COMMIT_LOG_STAT     3
#define CB_SNAP_UPS_MEMTABLE_STAT       4
#define CB_SNAP_RS_CS_INDEX_MERGE_STAT  5
#define CB_SNAP_DATABASE                6
#define CB_SNAP_SYSTEM                  7
#define CB_SNAP_CATALOG                 8
#define CB_SNAP_TRANSACTIONS            9
#define CB_SNAP_TRANSACTIONS_CURRENT    10
#define CB_SNAP_ACCESSPLANS             11
#define CB_SNAP_HEALTH                  12

#define DELTA     "DELTA"
#define ABSOLUTE  "ABSOLUTE"
#define AVERAGE   "AVERAGE"

const int32_t DISPLAYMODENUMBER = 3;
const string DISPLAYMODECHOOSER[DISPLAYMODENUMBER] = { ABSOLUTE,
                                                       DELTA,
                                                       AVERAGE
                                                     };

#define TRUE  1
#define FALSE 0
#define ANYVALUE 0

#define GLOBAL "GLOBAL"
#define GROUP  "GROUP"
#define NODE   "NODE"

#define CB_ERROR    -1
#define HEADER_NULL -1
#define FOOTER_NULL -1

#define CB_CBTOP_DONE 1

#define REFRESH    0
#define NOTREFRESH 1

#define COLOR_MULTIPLE 8
#define BUTTON_LEFT    4479771
#define BUTTON_RIGHT   4414235
#define BUTTON_TAB     9
#define BUTTON_ENTER   13
#define BUTTON_ESC     27
#define BUTTON_H_LOWER 'h'
#define BUTTON_Q_LOWER 'q'
#define BUTTON_F5      542058306331

#define PREFIX_TAB      " Tab : "
#define PREFIX_LEFT     "  <- : "
#define PREFIX_RIGHT    "  -> : "
#define PREFIX_ENTER    "Enter: "
#define PREFIX_ESC      " ESC : "
#define PREFIX_F5       " F5  : "
#define PREFIX_NULL     "NULL : "
#define PREFIX_FORMAT   "  %c  : "

#define REFERUPPERLEFT_X "referUpperLeft_X"
#define REFERUPPERLEFT_Y "referUpperLeft_Y"
#define LENGTH_X "length_X"
#define LENGTH_Y "length_Y"
#define AUTOSETTYPE_XML "autoSetType"
#define COLOUR_FOREGROUNDCOLOR "colour.foreGroundColor"
#define COLOUR_BACKGROUNDCOLOR "colour.backGroundColor"
#define PREFIXCOLOUR_FOREGROUNDCOLOR "prefixColour.foreGroundColor"
#define PREFIXCOLOUR_BACKGROUNDCOLOR "prefixColour.backGroundColor"
#define CONTENTCOLOUR_FOREGROUNDCOLOR "contentColour.foreGroundColor"
#define CONTENTCOLOUR_BACKGROUNDCOLOR "contentColour.backGroundColor"
#define WNDROW     "wndOptionRow"
#define WNDCOLUMN  "wndOptionColumn"
#define OPTIONROW  "optionRow"
#define OPTIONCOL  "optionColumn"
#define CELLLENGTH "cellLength"
#define EXPRESSIONNUMBER "expressionNumber"
#define ROWNUMBER  "rowNumber"
#define EXPRESSIONCONTENT "ExpressionContent"
#define ALIGNMENT  "alignment"
#define ROWLOCATION "rowLocation"
#define EXPRESSIONTYPE "expressionType"
#define EXPRESSIONLENGTH "expressionLength"
#define EXPRESSIONVALUE_TEXT "expressionValue.text"
#define EXPRESSIONVALUE_EXPRESSION "expressionValue.expression"
#define GLOBAL_AUTOSETTYPE "globalAutoSetType"
#define GROUP_AUTOSETTYPE "groupAutoSetType"
#define NODE_AUTOSETTYPE "nodeAutoSetType"
#define GLOBAL_STYLE "globalStyle"
#define GROUP_STYLE  "groupStyle"
#define NODE_STYLE   "nodeStyle"
#define GLOBAL_ROW "globalRow"
#define GROUP_ROW  "groupRow"
#define NODE_ROW   "nodeRow"
#define GLOBAL_COL "globalCol"
#define GROUP_COL  "groupCol"
#define NODE_COL   "nodeCol"
#define TABLE_CELLLENGTH "tableCellLength"
#define BASEFIELD "baseField"
#define FIELDLENGTH "fieldLength"
#define FIXED "fixed"
#define MOBILE "mobile"
#define FIELDSTRUCT "FieldStruct"
#define ABSOLUTENAME "absoluteName"
#define SOURCEFIELD "sourceField"
#define CONTENTLENGTH "contentLength"
#define ABSOLUTECOLOUR_FOREGROUNDCOLOR "absoluteColour.foreGroundColor"
#define ABSOLUTECOLOUR_BACKGROUNDCOLOR "absoluteColour.backGroundColor"
#define WARNINGVALUE_ABSOLUTE_MAX_LIMITVALUE "warningValue.absoluteMaxLimitValue"
#define WARNINGVALUE_ABSOLUTE_MIN_LIMITVALUE "warningValue.absoluteMinLimitValue"
#define WARNINGVALUE_DELTA_MAX_LIMITVALUE "warningValue.deltaMaxLimitValue"
#define WARNINGVALUE_DELTA_MIN_LIMITVALUE "warningValue.deltaMinLimitValue"
#define WARNINGVALUE_AVERAGE_MAX_LIMITVALUE "warningValue.averageMaxLimitValue"
#define WARNINGVALUE_AVERAGE_MIN_LIMITVALUE "warningValue.averageMinLimitValue"
#define CANSWITCH "canSwitch"
#define DELTANAME "deltaName"
#define AVERAGENAME "averageName"
#define DELTACOLOUR_FOREGROUNDCOLOR "deltaColour.foreGroundColor"
#define DELTACOLOUR_BACKGROUNDCOLOR "deltaColour.backGroundColor"
#define AVERAGECOLOUR_FOREGROUNDCOLOR "averageColour.foreGroundColor"
#define AVERAGECOLOUR_BACKGROUNDCOLOR "averageColour.backGroundColor"
#define NUMOFSUBWINDOW "numOfSubWindow"
#define NODEWINDOW "NodeWindow"
#define ACTUALWINDOWMINROW "actualWindowMinRow"
#define ACTUALWINDOWMINCOLUMN "actualWindowMinColumn"
#define ZOOMMODE "zoomMode"
#define DISPLAYTYPE    "displayType"
#define DISPLAYCONTENT "displayContent"
#define OCCUPYMODE "occupyMode"
#define POSITION "position"
#define EVENT "Event"
#define ROOTWINDOW "RootWindow"
#define REFERWINDOWROW "referWindowRow"
#define REFERWINDOWCOLUMN "referWindowColumn"
#define ACTUALWINDOWROW "actualWindowRow"
#define ACTUALWINDOWCOLUMN "actualWindowColumn"
#define REFRESHINTERVAL "refreshInterval"
#define COLOUROFTHECHANGE_FOREGROUNDCOLOR "colourOfTheChange.foreGroundColor"
#define COLOUROFTHECHANGE_BACKGROUNDCOLOR "colourOfTheChange.backGroundColor"
#define COLOUROFTHEDIVIDINGLINE_FOREGROUNDCOLOR "colourOfTheChange.foreGroundColor"
#define COLOUROFTHEDIVIDINGLINE_BACKGROUNDCOLOR "colourOfTheChange.backGroundColor"
#define COLOUROFTHEMAX_FOREGROUNDCOLOR "colourOfTheChange.foreGroundColor"
#define COLOUROFTHEMAX_BACKGROUNDCOLOR "colourOfTheChange.backGroundColor"
#define COLOUROFTHEMIN_FOREGROUNDCOLOR "colourOfTheChange.foreGroundColor"
#define COLOUROFTHEMIN_BACKGROUNDCOLOR "colourOfTheChange.backGroundColor"
#define KEYSUITES "KeySuites"
#define KEYSUITELENGTH "keySuiteLength"
#define KEYSUITE "KeySuite"
#define MARK "mark"
#define HOTKEYLENGTH "hotKeyLength"
#define HOTKEY "HotKey"
#define BUTTON "button"
#define JUMPTYPE "jumpType"
#define JUMPNAME "jumpName"
#define KEYDESC "desc"
#define WNDTYPE "wndtype"
#define HEADERS "Headers"
#define HEADERLENGTH "headerLength"
#define HEADTAILMAP "HeadTailMap"
#define KEY "key"
#define VALUE "value"
#define BODIES "Bodies"
#define BODYLENGTH "bodyLength"
#define BODYMAP "BodyMap"
#define HEADERKEY "headerKey"
#define FOOTERKEY "footerKey"
#define LABELNAME "labelName"
#define BODYPANELTYPE "bodyPanelType"
#define HOTKEYSUITETYPE "hotKeySuiteType"
#define HELPPANELTYPE "helpPanelType"
#define SOURCESNAPSHOT "sourceSnapShot"
#define FOOTERS "Footers"
#define FOOTERLENGTH "footerLength"

#define OPTION_CONFPATH      "confpath"
#define OPTION_HOSTNAME      "rootserver"
#define OPTION_SERVICENAME   "portname"
#define OPTION_VERSION       "version"
#define OPTION_SSL           "ssl"
#define OPTION_INTERVAL        "interval"
#define OPTION_PRINT_DATA_TYPE "printtype"
#define OPTION_MEMTABLE        "memtable"
#define OPTION_COMMITLOG       "commitlog"
#define OPTION_MERGE           "merge"
#define OPTION_INDEX           "index"

#define OSS_NEWLINE        "\n"

char HELP_DETAIL[] = "[Help for SDBTOP]";
char SDB_TOP_LICENSE[] =
    "Licensed Materials - Property of SequoiaDB"OSS_NEWLINE
    "Copyright SequoiaDB Corp. 2013-2015 All Rights Reserved.";
char SDB_TOP_DESC[] =
    " #### ####  ####  #####  ###  ####   For help type h or ..."OSS_NEWLINE
    "#     #   # #   #   #   #   # #   #  sdbtop -h: usage"OSS_NEWLINE
    " ###  #   # ####    #   #   # ####"OSS_NEWLINE
    "    # #   # #   #   #   #   # #"OSS_NEWLINE
    "####  ####  ####    #    ###  #"OSS_NEWLINE
    OSS_NEWLINE
    "SDB Interactive Snapshot Monitor V2.0"OSS_NEWLINE
    "Use these keys to ENTER:";

#define GIGABYTE (1024 * 1024 * 1024)

#define BUFFERSIZE         256
char cbtopBuffer[BUFFERSIZE] = {0} ;
const int32_t errStrLength = 1024 ;
char errStr[errStrLength] = {0} ;
char errStrBuf[errStrLength] = {0} ;
char progPath[ OSS_MAX_PATHSIZE + 1 ] = { 0 } ;
string confPath = CBTOP_DEFAULT_CONFPATH ;
string hostname = CBTOP_DEFAULT_HOSTNAME ;
string serviceName = CBTOP_DEFAULT_SERVICENAME ;
int32_t useSSL = FALSE ;

int32_t inputRefreshInterval = 3;
int32_t printDataType = 1;
bool isNeedRefresh = true;
bool hasInputRefreshInterval = false;
bool isPrintToTerminal = false;

#define ADD_PARAM_OPTIONS_BEGIN( desc )\
  desc.add_options()

#define ADD_PARAM_OPTIONS_END ;

#define COMMANDS_STRING( a, b ) (string(a) +string( b)).c_str()
#define COMMANDS_OPTIONS \
  ( COMMANDS_STRING(OPTION_HELP, ",h"), "help" )\
  ( COMMANDS_STRING(OPTION_HOSTNAME, ",r"), boost::program_options::value<string>(), "root server, default: localhost" ) \
  ( COMMANDS_STRING(OPTION_SERVICENAME, ",p"), boost::program_options::value<string>(), "port name, default: 2500" ) \
  ( COMMANDS_STRING(OPTION_INTERVAL, ",f"), boost::program_options::value<string>(), "refresh interval, default: 3 (seconds)" ) \
  ( COMMANDS_STRING(OPTION_PRINT_DATA_TYPE, ",t"),boost::program_options::value<string>(), "print data type, default: memtable" )

struct Colours
{
    int32_t foreGroundColor ;
    int32_t backGroundColor ;
} ;

struct StaticTextOutPut
{
    char *outputText ;
    string   autoSetType ;
    Colours colour ;
} ;

struct ExpValueStruct
{
    string text ;
    string expression ;
} ;

class ExpressionContent : public CBObject
{
  public:
    string expressionType ;

    int32_t expressionLength ;

    ExpValueStruct expressionValue ;

    string alignment ;

    Colours colour ;

    int32_t rowLocation ;
} ;

struct DynamicExpressionOutPut
{
    ExpressionContent *content ;

    string autoSetType ;

    int32_t expressionNumber ;

    int32_t rowNumber ;
} ;

struct FiledWarningValue
{
    int64_t absoluteMaxLimitValue ;
    int64_t absoluteMinLimitValue ;

    int64_t deltaMaxLimitValue ;
    int64_t deltaMinLimitValue ;

    int64_t averageMaxLimitValue ;
    int64_t averageMinLimitValue ;
} ;


class FieldStruct : public CBObject
{
  public:

    string deltaName ;
    string absoluteName ;
    string averageName ;

    string sourceField ;
    int32_t contentLength ;
    string alignment ;
    Colours deltaColour ;
    Colours absoluteColour ;
    Colours averageColour ;

    int32_t canSwitch ;

    FiledWarningValue warningValue ;
} ;

struct DynamicSnapshotOutPut
{
    FieldStruct* fixedField ;
    FieldStruct* mobileField ;
    int32_t actualFixedFieldLength ;
    int32_t actualMobileFieldLength ;
    int32_t fieldLength ;
    string globalAutoSetType ;
    string groupAutoSetType ;
    string nodeAutoSetType ;
    string baseField ;
    int32_t tableCellLength ;
    string globalStyle ;
    string groupStyle ;
    string nodeStyle ;
    int32_t globalRow ;
    int32_t globalCol ;
    int32_t groupRow ;
    int32_t groupCol ;
    int32_t nodeRow ;
    int32_t nodeCol ;
} ;

struct DynamicHelp
{
    int32_t wndOptionRow ;
    int32_t wndOptionCol ;
    int32_t optionsRow ;
    int32_t optionsCol ;
    int32_t cellLength ;
    Colours prefixColour ;
    Colours contentColour ;
    string autoSetType ;
} ;

struct DisplayContent
{
    StaticTextOutPut staticTextOutPut ;
    DynamicExpressionOutPut dyExOutPut ;
    DynamicSnapshotOutPut dySnapshotOutPut ;
    DynamicHelp dynamicHelp ;
} ;

struct Position
{
    int32_t referUpperLeft_X ;
    int32_t referUpperLeft_Y ;

    int32_t length_X ;
    int32_t length_Y ;
} ;

class NodeWindow : public CBObject
{
  public:
    int32_t actualWindowMinRow ;
    int32_t actualWindowMinColumn ;

    string zoomMode ;

    string displayType ;

    DisplayContent displayContent ;

    Position position ;

    string occupyMode;
};

struct Panel
{
    NodeWindow* subWindow ;
    int32_t numOfSubWindow ;
};

class HotKey : public CBObject
{
  public:

    int64_t button ;
    string jumpType ;
    string jumpName ;
    string desc ;
    int32_t wndType ;
} ;

class KeySuite : public CBObject
{
  public:

    int64_t mark ;

    int32_t hotKeyLength ;

    int32_t hotKeyLengthFromConf ;

    HotKey *hotKey ;
} ;

class HeadTailMap : public CBObject
{
  public:
    int32_t key ;
    Panel value ;
} ;

class BodyMap : public CBObject
{
  public:
    int32_t headerKey ;
    Panel value ;
    int32_t footerKey ;
    string labelName ;
    int64_t hotKeySuiteType ;
    string sourceSnapShot ;
    string bodyPanelType ;
    string helpPanelType ;
} ;

struct InputPanel
{
    int32_t displayModeChooser ;
    string snapshotModeChooser;
    string groupName ;
    string nodeName ;
    int32_t fieldPosition ;
    int32_t isFirstGetSnapshot ;
    vector<BSONObj> last_Snapshot ;
    vector<BSONObj> cur_Snapshot ;
    map<string, string> last_absoluteMap ;
    map<string, string> last_deltaMap ;
    map<string, string> last_averageMap ;
    map<string, string> cur_absoluteMap ;
    map<string, string> cur_deltaMap ;
    map<string, string> cur_averageMap ;
    string confPath ;
    int32_t refreshInterval ;
    int32_t refreshState;
    int32_t forcedToRefresh_Local ;
    int32_t forcedToRefresh_Global ;
    BodyMap* activatedPanel ;
    Colours colourOfTheMax ;
    Colours colourOfTheMin ;
    Colours colourOfTheChange ;
    Colours colourOfTheDividingLine ;
    string sortingWay ;
    string sortingField ;
    string filterCondition ;
    int32_t filterNumber ;
    int32_t lastPageNumber ;
    int32_t recordCountPerPage ;
} ;

struct RootWindow
{
    int32_t referWindowRow ;
    int32_t referWindowColumn ;
    int32_t actualWindowMinRow ;
    int32_t actualWindowMinColumn ;
    HeadTailMap *header ;
    int32_t headerLength ;
    BodyMap *body ;
    int32_t bodyLength ;
    HeadTailMap *footer ;
    int32_t footerLength ;
    InputPanel input ;
    KeySuite *keySuite ;
    int32_t keySuiteLength ;
} ;

class Event : public CBObject
{
  public: // features
    RootWindow root ;
    //sdb* coord ;
    oceanbase::common::ObServer rootServerMaster;
    oceanbase::common::ObBaseClient client;
  public://consturct function
    Event() ;
    ~Event() ;
  public: // operation

    int32_t assignActivatedPanelByLabelName( BodyMap **activatedPanel,
                                             string labelName ) ;

    int32_t assignActivatedPanel( BodyMap **activatedPanel,
                                  string bodyPanelType ) ;

    int32_t getActivatedHeadTailMap(  BodyMap *activatedPanel,
                                      HeadTailMap **header,
                                      HeadTailMap **footer ) ;

    int32_t getActualPosition( Position &actualPosition, Position &referPosition,
                               const string zoomMode, const string occupyMode ) ;

    int32_t getActivatedKeySuite( KeySuite **keySuite ) ;

    int32_t mvprintw_CBTOP( const string &expression, int32_t expressionLength,
                             const string &alignment,
                             int32_t start_row, int32_t start_col ) ;

    int32_t mvprintw_CBTOP( const char *expression, int32_t expressionLength,
                             const string &alignment,
                             int32_t start_row, int32_t start_col ) ;

    void getColourPN( Colours colour, int32_t &colourPairNumber );

    int32_t getResultFromBSONObj( const BSONObj &bsonobj,
                                  const string &sourceField,
                                  const string &displayMode,
                                  string &result, int32_t canSwitch,
                                  const string& baseField,
                                  const FiledWarningValue& waringValue,
                                  int32_t &colourPairNumber ) ;

    int32_t getExpression( string& expression, string& result ) ;

    int32_t getCurSnapshot() ;

    int32_t fixedOutputLocation( int32_t start_row, int32_t start_col,
                                 int32_t &fixed_row, int32_t &fixed_col,
                                 int32_t referRowLength, int32_t referColLength,
                                 const string &autoSetType ) ;

    int32_t getFieldNameAndColour( const FieldStruct &fieldStruct,
                                   const string &displayMode, string &fieldName,
                                   Colours &fieldColour ) ;

    int32_t refreshDH( DynamicHelp &DH, Position &position ) ;

    int32_t refreshDE( DynamicExpressionOutPut &DE, Position &position ) ;

    int32_t refreshDS_Table( DynamicSnapshotOutPut &DS, int32_t ROW, int32_t COL,
                             Position &position, string autoSetType ) ;
    int32_t refreshDS_List( DynamicSnapshotOutPut &DS,
                            Position &position,
                            const string &autoSetType ) ;
    int32_t refreshDS( DynamicSnapshotOutPut &DS,
                       Position &position ) ;

    int32_t refreshDisplayContent( DisplayContent &displayContent,
                                   string displayType,
                                   Position &actualPosition ) ;

    int32_t refreshNodeWindow( NodeWindow &window ) ;

    int32_t refreshHT( HeadTailMap *headtail ) ;

    int32_t refreshBD( BodyMap *body ) ;

    void initAllColourPairs() ;

    int32_t addFixedHotKey() ;

    int32_t matchNameInFieldStruct( const FieldStruct *src,
                                    const string DisplayName ) ;
    int32_t matchSourceFieldByDisplayName( const string DisplayName ) ;

    int32_t eventManagement( int64_t key ,int32_t isFirstStart ) ;
    int32_t refreshAll( HeadTailMap *header, BodyMap *body,
                        HeadTailMap *footer, int32_t refreshAferClean ) ;

    int32_t runCBTOP( int32_t useSSL = 0 ) ;

    int32_t printCBTOP();
    int32_t printMemtableData();
    int32_t printCommitlogData();
    int32_t printMergeStatus();
    int32_t printIndexBuildStatus();
    void buildBsonArrayAndPrint();

  private:
    int32_t getChunkServerList(int64_t &maxMergeDurationTimeout);
    int32_t getChunkServerMergeStatus(const ObServer &cs, const int32_t &index, const int64_t &maxMergeDurationTimeout);
    int32_t getWholeIndexBuildInfoFromRootServer(BSONObjBuilder &BSONObjBuilder);
    int32_t getChunkServerIndexBuildStatus(const ObServer &cs, const int32_t &index, BSONObjBuilder &BSONObjBuilder);
    int32_t getUpdateServerInfoList();
    int32_t getUpdateServerMemTableInfo(const ObServer &ups, const ObUpsInfo &upsInfo);
    int32_t getUpdateServerCommitLogStatus(const ObServer &ups, const ObUpsInfo &upsInfo);
    int32_t getRootServerLocalTime(int64_t &rsLocalTime);

  private:
    static const int32_t DEFAULT_TIMEOUT = 1000000; // 1 second
    static const int32_t DEFAULT_VERSION = 1;
    buffer buff;
    std::map<oceanbase::common::ObServer, oceanbase::common::ObUpsInfo> updateServerInfoList;
    std::map<oceanbase::common::ObServer, int32_t> chunkServerList;
    vector<BSONObj> snapshotResult;
};

static inline std::string &ltrim ( std::string &s )
{
   s.erase ( s.begin(), std::find_if ( s.begin(), s.end(),
             std::not1 ( std::ptr_fun<int, int>(std::isspace)))) ;
   return s ;
}

static inline std::string &rtrim ( std::string &s )
{
   s.erase ( std::find_if ( s.rbegin(), s.rend(),
             std::not1 ( std::ptr_fun<int, int>(std::isspace))).base(),
             s.end() ) ;
   return s ;
}

static inline std::string &trim ( std::string &s )
{
   return ltrim ( rtrim ( s ) ) ;
}

static inline std::string &doubleQuotesTrim( std::string &s )
{
   const int32_t strLength = static_cast<int32_t>(s.length());
   if( 2 >= strLength )
   {
      return s ;
   }
   if( '\"' == s[0] && '\"' == s[strLength-1] )
   {
      s = s.substr( 1, strLength-2 ) ;
   }
   return s ;
}

static inline int32_t isExist( map<string, string> &src, const string &key )
{
   return ( src.find( key ) != src.end() ) ;
}

static inline std::string getDividingLine( const string &dividingChar,
                                           int32_t dividingLength )
{
   string line = NULLSTRING ;
   while( dividingLength-- )
   {
      line += dividingChar ;
   }
   return line ;
}

int32_t strToNum( const char *str, int32_t &number )
{
   int32_t rc           = OB_SUCCESS ;
   int32_t pos          = 0 ;
   int32_t isPositive = TRUE ;
   number    = 0 ;
   while( str[pos] )
   {
      if( 0 == pos )
      {
         if( '-' == str[pos] )
         {
            isPositive = FALSE ;
            ++pos ;
         }
         else if ( '+' == str[pos] )
         {
            isPositive = TRUE ;
            ++pos ;
         }
      }
      if( '0' > str[pos] ||'9' < str[pos] )
      {
         number = 0;
         rc = OB_ERROR ;
         goto error;
      }
      number *= 10 ;
      number = number + str[pos] -'0' ;
      ++pos;
   }
   if( !isPositive )
   {
      number *= -1 ;
   }
done:
   return rc ;
error :
   goto done ;
}

int32_t getCbTopKey( const char *keyBuffer, int64_t &key )
{
   int32_t rc         = OB_SUCCESS ;
   uint32_t bufLength = 0 ;
   bufLength = ( uint32_t )ossStrlen( keyBuffer ) ;
   if( 0 < bufLength )
   {
      if( 3 <= bufLength && 91 == keyBuffer[bufLength-2]
                         && 27 == keyBuffer[bufLength-3] )
      {
         if( 68 == keyBuffer[bufLength-1] )
         {
            key = BUTTON_LEFT ;
         }
         else if(  67 == keyBuffer[bufLength-1] )
         {
            key = BUTTON_RIGHT ;
         }
         else
         {
            key = 0 ;
         }
      }
      else if( 5 <= bufLength && 53 == keyBuffer[bufLength-2]
                              && 49 == keyBuffer[bufLength-3]
                              && 91 == keyBuffer[bufLength-4]
                              && 27 == keyBuffer[bufLength-5] )
      {
         if( 126 == keyBuffer[bufLength-1] )
         {
            key = BUTTON_F5 ;
         }
         else
         {
            key = 0 ;
         }
      }
      else
      {
         key = keyBuffer[bufLength-1] ;
      }
   }
   else
   {
      rc = OB_ERROR ;
      key = 0 ;
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

static inline int32_t formattingOutput( char *pBuffer, const int32_t fixedLength,
                                      const char *pSrc )
{
   int32_t rc = OB_SUCCESS ;
   int32_t i  = 0 ;
   try
   {
      for( i = 0; i < fixedLength && '\0' != pSrc[i]; ++i )
      {
         pBuffer[i] = pSrc[i] ;
      }
      pBuffer[i] = '\0' ;
   }
   catch( std::exception &e )
   {
      ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,
                   "%s SNPRINTF_TOP failed,"
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

static inline int32_t MVPRINTW( const int32_t start_row, int32_t start_col,
                              const int32_t fixedLength, const char *printf_str,
                              const string &alignment )
{
   int32_t rc           = OB_SUCCESS ;
   int32_t col_offset   = 0 ;
   int32_t printfLength = static_cast<int32_t>(ossStrlen( printf_str )) ;
   if( ALIGNMENT_LEFT == alignment )
   {
      mvprintw( start_row, start_col, printf_str ) ;
   }
   else if( ALIGNMENT_RIGHT == alignment )
   {
      col_offset = fixedLength -  printfLength ;
      start_col += col_offset ;
      mvprintw( start_row, start_col, printf_str ) ;
   }
   else if( ALIGNMENT_CENTER == alignment )
   {
      col_offset = ( fixedLength - printfLength ) / 2 ;
      start_col += col_offset ;
      mvprintw( start_row, start_col, printf_str ) ;
   }
   else
   {
      ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,
                   "%s MVPRINTW wrong alignment:%s"OSS_NEWLINE,
                   errStrBuf, alignment.c_str() ) ;
      rc = OB_ERROR ;
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

int32_t storePosition( ptree pt_position, Position& position )
{
   int32_t rc = OB_SUCCESS ;
   try
   {
      position.referUpperLeft_X =
            pt_position.get<int32_t>( REFERUPPERLEFT_X ) ;
      position.referUpperLeft_Y =
            pt_position.get<int32_t>( REFERUPPERLEFT_Y ) ;
      position.length_X         =
            pt_position.get<int32_t>( LENGTH_X ) ;
      position.length_Y         =
            pt_position.get<int32_t>( LENGTH_Y ) ;
   }
   catch( std::exception &e )
   {
      ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,
                   "%s readPosition failed,"
                   "e.what():%s"OSS_NEWLINE, errStrBuf, e.what() ) ;
      rc = OB_ERROR ;
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

int32_t storeDE( ptree pt_display,
               DynamicExpressionOutPut &dEContent )
{
   int32_t rc                     = OB_SUCCESS ;
   int32_t exNumber               = 0 ;
   ExpressionContent *pEContent = NULL ;
   try
   {
      dEContent.autoSetType =
            pt_display.get<string>( AUTOSETTYPE_XML ) ;
      dEContent.expressionNumber =
            pt_display.get<int32_t>( EXPRESSIONNUMBER ) ;
      dEContent.rowNumber =
            pt_display.get<int32_t>( ROWNUMBER ) ;
      dEContent.content =
            CB_OSS_NEW ExpressionContent[dEContent.expressionNumber] ;
      exNumber = 0 ;
      for( BOOST_AUTO( child_display, pt_display.begin() );
           child_display != pt_display.end();
           ++child_display )
      {
         if( child_display->first == EXPRESSIONCONTENT )
         {
            pEContent = &dEContent.content[exNumber] ;
            if( exNumber >= dEContent.expressionNumber )
            {
               rc = OB_ERROR ;
               goto error;
            }
            pEContent->alignment =
                  child_display->
                        second.get<string>( ALIGNMENT ) ;
            pEContent->colour.foreGroundColor =
                  child_display->
                        second.get<int32_t>( COLOUR_FOREGROUNDCOLOR ) ;
            pEContent->colour.backGroundColor =
                  child_display->
                        second.get<int32_t>( COLOUR_BACKGROUNDCOLOR ) ;
            pEContent->rowLocation =
                  child_display->
                        second.get<int32_t>( ROWLOCATION ) ;
            pEContent->expressionType =
                  child_display->
                        second.get<string>( EXPRESSIONTYPE ) ;
            pEContent->expressionLength =
                  child_display->second.get<int32_t>( EXPRESSIONLENGTH ) ;
            if( 1 > pEContent->expressionLength )
            {
               ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
               ossSnprintf( errStr, errStrLength,
                            "%s readDisplayContent failed"
                            "expressionLength too short:%d"OSS_NEWLINE,
                            errStrBuf,
                            pEContent->expressionLength ) ;
               goto error ;
            }
            if( DYNAMIC_EXPRESSION ==
                pEContent->expressionType )
            {
               pEContent->expressionValue.expression =
               child_display->
                     second.get<string>( EXPRESSIONVALUE_EXPRESSION ) ;
            }
            else if( pEContent->expressionType ==
                     STATIC_EXPRESSION )
            {
               pEContent->expressionValue.text =
                     child_display->
                           second.get<string>( EXPRESSIONVALUE_TEXT ) ;
            }
            else
            {
               ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
               ossSnprintf( errStr, errStrLength,
                            "%s readDisplayContent failed"
                            "expressionType == %s"OSS_NEWLINE,
                            errStrBuf,
                            pEContent->expressionType.c_str() ) ;
               rc = OB_ERROR ;
               goto error ;
            }
            ++exNumber ;
         }
      }
      dEContent.expressionNumber = exNumber ;
   }
   catch( std::exception &e )
   {
      ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,"%s readDisplayContent failed"
                   "(displayType == DISPLAYTYPE_DYNAMIC_EXPRESSION),"
                   "e.what():%s"OSS_NEWLINE,
                   errStrBuf, e.what() ) ;
      rc = OB_ERROR ;
      goto error ;
   }
done:
   return rc ;
error:
   goto done ;
}

int32_t storeDS( ptree pt_display,
               DynamicSnapshotOutPut & dSContent)
{
   int32_t rc              = OB_SUCCESS ;
   int32_t actualFixedNum  = 0;
   int32_t actualMobileNum = 0;
   FieldStruct *fixed    = NULL ;
   FieldStruct *mobile   = NULL ;
   try
   {
      dSContent.globalAutoSetType =
            pt_display.get<string>( GLOBAL_AUTOSETTYPE ) ;
      dSContent.groupAutoSetType =
            pt_display.get<string>( GROUP_AUTOSETTYPE ) ;
      dSContent.nodeAutoSetType =
            pt_display.get<string>( NODE_AUTOSETTYPE ) ;

      dSContent.globalStyle = pt_display.get<string>( GLOBAL_STYLE ) ;
      dSContent.groupStyle = pt_display.get<string>( GROUP_STYLE ) ;
      dSContent.nodeStyle = pt_display.get<string>( NODE_STYLE ) ;
      if( TABLE == dSContent.globalStyle )
      {
         dSContent.globalRow = pt_display.get<int32_t>( GLOBAL_ROW ) ;
         dSContent.globalCol = pt_display.get<int32_t>( GLOBAL_COL ) ;
         dSContent.tableCellLength = pt_display.get<int32_t>( TABLE_CELLLENGTH ) ;
      }

      if( TABLE == dSContent.groupStyle )
      {
         dSContent.groupRow = pt_display.get<int32_t>( GROUP_ROW ) ;
         dSContent.groupCol = pt_display.get<int32_t>( GROUP_COL ) ;
         dSContent.tableCellLength = pt_display.get<int32_t>( TABLE_CELLLENGTH ) ;
      }

      if( TABLE == dSContent.nodeStyle )
      {
         dSContent.nodeRow = pt_display.get<int32_t>( NODE_ROW ) ;
         dSContent.nodeCol = pt_display.get<int32_t>( NODE_COL ) ;
         dSContent.tableCellLength = pt_display.get<int32_t>( TABLE_CELLLENGTH ) ;
      }

      dSContent.baseField = pt_display.get<string>( BASEFIELD ) ;
      dSContent.fieldLength = pt_display.get<int32_t>( FIELDLENGTH ) ;
      dSContent.fixedField =
            CB_OSS_NEW FieldStruct[dSContent.fieldLength] ;
      dSContent.mobileField =
            CB_OSS_NEW FieldStruct[dSContent.fieldLength] ;

   }
   catch( std::exception &e )
   {
      ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,"%s readDisplayContent failed"
                   "(displayType == DISPLAYTYPE_DYNAMIC_SNAPSHOT),"
                   "e.what():%s"OSS_NEWLINE, errStrBuf, e.what() ) ;
      rc = OB_ERROR ;
      goto error;
   }

   actualFixedNum = 0;
   actualMobileNum = 0;
   for( BOOST_AUTO( child_display, pt_display.begin() );
        child_display != pt_display.end();
        ++child_display )
   {
      if( child_display->first == FIXED )
      {
         for( BOOST_AUTO( child_pFixed, child_display->second.begin() );
              child_pFixed != child_display->second.end();
              ++child_pFixed )
         {
            if( child_pFixed->first == FIELDSTRUCT )
            {
               fixed = &dSContent.fixedField[actualFixedNum] ;
               if( actualFixedNum >= dSContent.fieldLength )
               {
                  break ;
               }
               fixed->absoluteName =
                     child_pFixed->second.get<string>( ABSOLUTENAME ) ;
               fixed->sourceField =
                     child_pFixed->second.get<string>( SOURCEFIELD ) ;
               fixed->contentLength =
                     child_pFixed->second.get<int32_t>( CONTENTLENGTH ) ;
               fixed->alignment =
                     child_pFixed->second.get<string>( ALIGNMENT ) ;

               fixed->absoluteColour.foreGroundColor =
                     child_pFixed->
                           second.get<int32_t>(
                                 ABSOLUTECOLOUR_FOREGROUNDCOLOR ) ;
               fixed->absoluteColour.backGroundColor =
                     child_pFixed->
                           second.get<int32_t>(
                                 ABSOLUTECOLOUR_BACKGROUNDCOLOR ) ;

               try
               {
                  fixed->warningValue.absoluteMaxLimitValue =
                     child_pFixed->
                           second.get<int64_t>(
                                 WARNINGVALUE_ABSOLUTE_MAX_LIMITVALUE ) ;
                  fixed->warningValue.absoluteMinLimitValue =
                     child_pFixed->
                           second.get<int64_t>(
                                 WARNINGVALUE_ABSOLUTE_MIN_LIMITVALUE ) ;
               }
               catch( std::exception &e )
               {
                  fixed->warningValue.absoluteMaxLimitValue = 0 ;
                  fixed->warningValue.absoluteMinLimitValue = 0 ;
               }

               fixed->canSwitch= child_pFixed->
                                       second.get<int32_t>( CANSWITCH ) ;
               if( 1 == fixed->canSwitch )
               {
                  fixed->deltaName =
                        child_pFixed->
                              second.get<string>(
                                    DELTANAME ) ;
                  fixed->averageName =
                        child_pFixed->
                              second.get<string>(
                                    AVERAGENAME ) ;
                  fixed->deltaColour.foreGroundColor =
                        child_pFixed->
                              second.get<int32_t>(
                                    DELTACOLOUR_FOREGROUNDCOLOR ) ;
                  fixed->deltaColour.backGroundColor =
                        child_pFixed->
                              second.get<int32_t>(
                                    DELTACOLOUR_BACKGROUNDCOLOR ) ;

                  fixed->averageColour.foreGroundColor =
                        child_pFixed->
                              second.get<int32_t>(
                                    AVERAGECOLOUR_FOREGROUNDCOLOR ) ;
                  fixed->averageColour.backGroundColor =
                        child_pFixed->second.get<int32_t>(
                              AVERAGECOLOUR_BACKGROUNDCOLOR ) ;
                  try
                  {
                     fixed->warningValue.deltaMaxLimitValue =
                           child_pFixed->
                                 second.get<int64_t>(
                                       WARNINGVALUE_DELTA_MAX_LIMITVALUE ) ;
                     fixed->warningValue.deltaMinLimitValue =
                           child_pFixed->
                                 second.get<int64_t>(
                                       WARNINGVALUE_DELTA_MIN_LIMITVALUE ) ;
                     fixed->warningValue.averageMaxLimitValue =
                           child_pFixed->
                                 second.get<int64_t>(
                                       WARNINGVALUE_AVERAGE_MAX_LIMITVALUE ) ;
                     fixed->warningValue.averageMinLimitValue =
                           child_pFixed->
                                 second.get<int64_t>(
                                       WARNINGVALUE_AVERAGE_MIN_LIMITVALUE ) ;
                  }
                  catch( std::exception &e )
                  {
                     fixed->warningValue.deltaMaxLimitValue = 0 ;
                     fixed->warningValue.deltaMinLimitValue = 0 ;
                     fixed->warningValue.averageMaxLimitValue = 0 ;
                     fixed->warningValue.averageMinLimitValue= 0 ;

                  }

               }
            }
            ++actualFixedNum ;
         }
      }
      else if( MOBILE == child_display->first )
      {
         for( BOOST_AUTO( child_pMobile, child_display->second.begin() );
              child_pMobile != child_display->second.end();
              ++child_pMobile )
         {
            if( child_pMobile->first == FIELDSTRUCT )
            {
               mobile = &dSContent.mobileField[actualMobileNum] ;
               if( actualMobileNum >=
                     dSContent.fieldLength )
               {
                  break ;
               }
               mobile->absoluteName =
                     child_pMobile->
                           second.get<string>(
                                 ABSOLUTENAME ) ;
               mobile->sourceField =
                     child_pMobile->
                           second.get<string>(
                                 SOURCEFIELD ) ;
               mobile->contentLength =
                     child_pMobile->
                           second.get<int32_t>(
                                 CONTENTLENGTH ) ;
               mobile->alignment =
                     child_pMobile->
                           second.get<string>(
                                 ALIGNMENT ) ;

               mobile->absoluteColour.foreGroundColor =
                     child_pMobile->
                           second.get<int32_t>(
                                 ABSOLUTECOLOUR_FOREGROUNDCOLOR ) ;
               mobile->absoluteColour.backGroundColor =
                     child_pMobile->
                           second.get<int32_t>(
                                 ABSOLUTECOLOUR_BACKGROUNDCOLOR ) ;
               try
               {
                  mobile->warningValue.absoluteMaxLimitValue =
                        child_pMobile->
                              second.get<int64_t>(
                                    WARNINGVALUE_ABSOLUTE_MAX_LIMITVALUE ) ;
                  mobile->warningValue.absoluteMinLimitValue =
                        child_pMobile->
                              second.get<int64_t>(
                                    WARNINGVALUE_ABSOLUTE_MIN_LIMITVALUE ) ;
               }
               catch( std::exception &e )
               {
                  mobile->warningValue.absoluteMaxLimitValue = 0 ;
                  mobile->warningValue.absoluteMinLimitValue = 0 ;
               }

               mobile->canSwitch=
                     child_pMobile->
                           second.get<int32_t>(
                                 CANSWITCH ) ;
               if( 1 == mobile->canSwitch )
               {
                  mobile->deltaName =
                        child_pMobile->
                              second.get<string>(
                                    DELTANAME ) ;
                  mobile->averageName =
                        child_pMobile->
                              second.get<string>(
                                    AVERAGENAME ) ;
                  mobile->deltaColour.foreGroundColor =
                        child_pMobile->
                              second.get<int32_t>(
                                    DELTACOLOUR_FOREGROUNDCOLOR ) ;
                  mobile->deltaColour.backGroundColor =
                        child_pMobile->
                              second.get<int32_t>(
                                    DELTACOLOUR_BACKGROUNDCOLOR ) ;
                  mobile->averageColour.foreGroundColor =
                        child_pMobile->
                              second.get<int32_t>(
                                    AVERAGECOLOUR_FOREGROUNDCOLOR ) ;
                  mobile->averageColour.backGroundColor =
                        child_pMobile->
                              second.get<int32_t>(
                                    AVERAGECOLOUR_BACKGROUNDCOLOR ) ;

                  try
                  {
                     mobile->warningValue.deltaMaxLimitValue =
                           child_pMobile->
                                 second.get<int64_t>(
                                       WARNINGVALUE_DELTA_MAX_LIMITVALUE ) ;
                     mobile->warningValue.deltaMinLimitValue =
                           child_pMobile->
                                 second.get<int64_t>(
                                       WARNINGVALUE_DELTA_MIN_LIMITVALUE ) ;
                     mobile->warningValue.averageMaxLimitValue =
                           child_pMobile->
                                 second.get<int64_t>(
                                       WARNINGVALUE_AVERAGE_MAX_LIMITVALUE ) ;
                     mobile->warningValue.averageMinLimitValue =
                           child_pMobile->
                                 second.get<int64_t>(
                                       WARNINGVALUE_AVERAGE_MIN_LIMITVALUE ) ;
                  }
                  catch( std::exception &e )
                  {
                     mobile->warningValue.deltaMaxLimitValue= 0 ;
                     mobile->warningValue.deltaMinLimitValue= 0 ;
                     mobile->warningValue.averageMaxLimitValue= 0 ;
                     mobile->warningValue.averageMinLimitValue= 0 ;
                  }
               }
            }
               ++actualMobileNum ;
         }
      }
   }
   dSContent.actualFixedFieldLength= actualFixedNum ;
   dSContent.actualMobileFieldLength= actualMobileNum ;
done:
   return rc ;
error:
   goto done ;
}

int32_t storeDisplayContent( ptree pt_displayContent,
                           DisplayContent &display, string displayType )
{
   int32_t rc                           = OB_SUCCESS ;
   StaticTextOutPut &sTContent        = display.staticTextOutPut ;
   DynamicExpressionOutPut &dEContent = display.dyExOutPut ;
   DynamicSnapshotOutPut &dSContent   = display.dySnapshotOutPut ;
   DynamicHelp &dHContent             = display.dynamicHelp ;
   if( DISPLAYTYPE_STATICTEXT_HELP_HEADER == displayType ||
       DISPLAYTYPE_STATICTEXT_LICENSE     == displayType ||
       DISPLAYTYPE_STATICTEXT_MAIN        == displayType )
   {
      try
      {
         sTContent.autoSetType =
               pt_displayContent.get<string>( AUTOSETTYPE_XML ) ;
         sTContent.colour.foreGroundColor =
               pt_displayContent.get<int32_t>( COLOUR_FOREGROUNDCOLOR ) ;
         sTContent.colour.backGroundColor =
               pt_displayContent.get<int32_t>( COLOUR_BACKGROUNDCOLOR ) ;
         if( DISPLAYTYPE_STATICTEXT_HELP_HEADER == displayType )
            sTContent.outputText = HELP_DETAIL ;
         else if( DISPLAYTYPE_STATICTEXT_LICENSE == displayType )
            sTContent.outputText = SDB_TOP_LICENSE ;
         else if( DISPLAYTYPE_STATICTEXT_MAIN == displayType )
            sTContent.outputText = SDB_TOP_DESC ;
      }
      catch( std::exception &e )
      {
         ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
         ossSnprintf( errStr, errStrLength,"%s readDisplayContent failed"
                      "(displayType == DISPLAYTYPE_STATICTEXT_HELP_HEADER), "
                      "e.what():%s"OSS_NEWLINE,
                      errStrBuf, e.what() ) ;
         rc = OB_ERROR ;
         goto error ;
      }
   }
   else if( DISPLAYTYPE_DYNAMIC_HELP == displayType )
   {
      try
      {
         dHContent.autoSetType =
               pt_displayContent.get<string>( AUTOSETTYPE_XML ) ;
         dHContent.prefixColour.foreGroundColor =
               pt_displayContent.get<int32_t>( PREFIXCOLOUR_FOREGROUNDCOLOR ) ;
         dHContent.prefixColour.backGroundColor =
               pt_displayContent.get<int32_t>( PREFIXCOLOUR_BACKGROUNDCOLOR ) ;
         dHContent.contentColour.foreGroundColor =
               pt_displayContent.get<int32_t>( CONTENTCOLOUR_FOREGROUNDCOLOR ) ;
         dHContent.contentColour.backGroundColor =
               pt_displayContent.get<int32_t>( CONTENTCOLOUR_BACKGROUNDCOLOR ) ;
         dHContent.wndOptionRow = pt_displayContent.get<int32_t>( WNDROW ) ;
         dHContent.wndOptionCol= pt_displayContent.get<int32_t>( WNDCOLUMN ) ;
         dHContent.optionsRow = pt_displayContent.get<int32_t>( OPTIONROW ) ;
         dHContent.optionsCol= pt_displayContent.get<int32_t>( OPTIONCOL ) ;
         dHContent.cellLength= pt_displayContent.get<int32_t>( CELLLENGTH ) ;
      }
      catch( std::exception &e )
      {
         ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
         ossSnprintf( errStr, errStrLength,"%s readDisplayContent failed"
                      "(displayType == DISPLAYTYPE_DYNAMIC_HELP),"
                      "e.what():%s"OSS_NEWLINE,
                      errStrBuf, e.what() ) ;
         rc = OB_ERROR ;
         goto error ;
      }
   }
   else if( DISPLAYTYPE_DYNAMIC_EXPRESSION == displayType )
   {
      rc = storeDE( pt_displayContent, dEContent ) ;
      if( rc )
      {
         goto error ;
      }
   }
   else if( DISPLAYTYPE_DYNAMIC_SNAPSHOT == displayType )
   {
      rc = storeDS( pt_displayContent, dSContent ) ;
      if( rc )
      {
         goto error ;
      }
   }
   else
   {
      ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,"%s readDisplayContent failed,"
                   "displayType is wrong\n", errStrBuf ) ;
      rc = OB_ERROR ;
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

int32_t storePanelValue( ptree pt_value, Panel& value )
{
   int32_t rc             = OB_SUCCESS ;
   int32_t numOfSubWindow = 0 ;
   try
   {
      value.numOfSubWindow = pt_value.get<int32_t>( NUMOFSUBWINDOW ) ;
      value.subWindow = CB_OSS_NEW NodeWindow[value.numOfSubWindow] ;
      numOfSubWindow = 0 ;
      for( BOOST_AUTO( child_value, pt_value.begin() );
           child_value != pt_value.end(); ++child_value )
      {
         if( child_value->first == NODEWINDOW )
         {
            if( numOfSubWindow >= value.numOfSubWindow )
            {
               ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
               ossSnprintf( errStr, errStrLength,
                            "%s readPanelValue failed,"
                            " numOfSubWindow >= value.numOfSubWindow\n",
                            errStrBuf ) ;
               rc = OB_ERROR ;
               goto error ;
            }
            value.subWindow[numOfSubWindow].actualWindowMinRow =
                  child_value->second.get<int32_t>( ACTUALWINDOWMINROW ) ;
            value.subWindow[numOfSubWindow].actualWindowMinColumn =
                  child_value->second.get<int32_t>( ACTUALWINDOWMINCOLUMN ) ;
            value.subWindow[numOfSubWindow].zoomMode =
                  child_value->second.get<string>( ZOOMMODE ) ;
            value.subWindow[numOfSubWindow].displayType =
                  child_value->second.get<string>( DISPLAYTYPE ) ;
            try
            {
               value.subWindow[numOfSubWindow].occupyMode =
                     child_value->second.get<string>( OCCUPYMODE ) ;
            }
            catch( std::exception &e )
            {
               value.subWindow[numOfSubWindow].occupyMode = OCCUPY_MODE_NONE;
            }
            for( BOOST_AUTO( child_nodewindow, child_value->second.begin() );
                 child_nodewindow != child_value->second.end();
                 ++child_nodewindow )
            {
               if( child_nodewindow->first == DISPLAYCONTENT )
               {
                  rc =
                        storeDisplayContent(
                              child_nodewindow->second,
                              value.subWindow[numOfSubWindow].displayContent,
                              value.subWindow[numOfSubWindow].displayType ) ;
                  if( rc)
                  {
                     ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
                     ossSnprintf( errStr, errStrLength,
                                  "%s readPanelValue failed, "
                                  "can't readDisplayContent\n", errStrBuf ) ;
                     goto error ;
                  }
               }
               else if( child_nodewindow->first == POSITION )
               {
                  rc = storePosition(
                              child_nodewindow->second,
                              value.subWindow[numOfSubWindow].position ) ;
                  if( rc )
                  {
                     ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
                     ossSnprintf( errStr, errStrLength,
                                  "%s readPanelValue failed, "
                                  "can't readPosition\n", errStrBuf ) ;
                     goto error ;
                  }
               }
            }
            ++numOfSubWindow ;
         }
      }
      value.numOfSubWindow = numOfSubWindow ;
   }
   catch( std::exception &e )
   {
      ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,"%s readPanelValue failed,"
                   "e.what():%s\n", errStrBuf, e.what() ) ;
      rc = OB_ERROR ;
      goto error ;
   }
done :
   return rc ;
error :
   goto done ;
}

int32_t storeHeaders( ptree pt_HDs, RootWindow &root )
{
   int32_t rc           = OB_SUCCESS ;
   int32_t headerLength = 0 ;
   HeadTailMap *heder = NULL ;
   try
   {
      root.headerLength = pt_HDs.get<int32_t>( HEADERLENGTH ) ;
      root.header = CB_OSS_NEW HeadTailMap[root.headerLength] ;
   }
   catch( std::exception &e )
   {
      ossSnprintf( errStrBuf, errStrLength,
                   "%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,
                   "%s readConfiguration failed,"
                   "e.what():%s"OSS_NEWLINE,
                   errStrBuf, e.what() ) ;
      rc = OB_ERROR ;
      goto error ;
   }
   headerLength = 0 ;
   for( BOOST_AUTO( pt_header, pt_HDs.begin() );
        pt_header != pt_HDs.end();
        ++pt_header )
   {
      if( HEADTAILMAP == pt_header->first )
      {
         heder = &root.header[headerLength] ;
         if( headerLength >= root.headerLength )
         {
            ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
            ossSnprintf( errStr, errStrLength,
                         "%s readConfiguration failed,"
                         "scope: headerLength>="
                         "root.headerLength"OSS_NEWLINE,
                         errStrBuf ) ;
            rc = OB_ERROR ;
            goto error ;
         }
         try
         {
            heder->key = pt_header->second.get<int32_t>( KEY ) ;
         }
         catch( std::exception &e )
         {
            ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
            ossSnprintf( errStr, errStrLength,
                         "%s readConfiguration failed,"
                         "e.what():%s"OSS_NEWLINE,
                      errStrBuf, e.what() ) ;
            rc = OB_ERROR ;
            goto error ;
         }
         for( BOOST_AUTO( pt_HValue, pt_header->second.begin() );
              pt_HValue != pt_header->second.end();
              ++pt_HValue )
         {
            if( VALUE == pt_HValue->first )
            {
               rc = storePanelValue( pt_HValue->second, heder->value ) ;
               if( rc )
               {
                  goto error ;
               }
            }
         }
         ++headerLength ;
      }
   }
   root.headerLength = headerLength ;
done :
   return rc ;
error :
   goto done ;
}

int32_t storeBodies( ptree pt_BDs, RootWindow &root )
{
   int32_t rc         = OB_SUCCESS ;
   int32_t bodyLength = 0 ;
   BodyMap *body    = NULL ;
   try
   {
      root.bodyLength = pt_BDs.get<int32_t>( BODYLENGTH ) ;

      root.body = CB_OSS_NEW BodyMap[root.bodyLength] ;
   }
   catch( std::exception &e )
   {
      ossSnprintf( errStrBuf, errStrLength,
                   "%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,
                   "%s readConfiguration failed,"
                   "e.what():%s"OSS_NEWLINE,
                   errStrBuf, e.what() ) ;
      rc = OB_ERROR ;
      goto error ;
   }
   bodyLength = 0 ;
   for( BOOST_AUTO( pt_body, pt_BDs.begin() );
        pt_body != pt_BDs.end();
        ++pt_body )
   {
      if( pt_body->first == BODYMAP )
      {
         body = &root.body[bodyLength] ;
         if( bodyLength >= root.bodyLength )
         {
            ossSnprintf( errStrBuf, errStrLength,
                         "%s", errStr ) ;
            ossSnprintf( errStr, errStrLength,
                         "%s readConfiguration failed,"
                         "scope: bodyLength>="
                         "root.bodyLength"OSS_NEWLINE,
                         errStrBuf ) ;
            rc = OB_ERROR ;
            goto error ;
         }
         try
         {
            body->headerKey =
                  pt_body->
                        second.get<int32_t>(
                              HEADERKEY ) ;
            body->footerKey =
                  pt_body->
                        second.get<int32_t>(
                              FOOTERKEY ) ;
            body->labelName =
                  pt_body->
                        second.get<string>(
                              LABELNAME ) ;
            body->bodyPanelType =
                  pt_body->
                        second.get<string>(
                              BODYPANELTYPE ) ;
            if( BODYTYPE_MAIN   == body->bodyPanelType ||
                BODYTYPE_NORMAL == body->bodyPanelType )
            {
               body->hotKeySuiteType =
                     pt_body->
                           second.get<int64_t>(
                                 HOTKEYSUITETYPE ) ;
               body->helpPanelType =
                     pt_body->
                           second.get<string>(
                                 HELPPANELTYPE ) ;
               if( BODYTYPE_NORMAL == body->bodyPanelType )
               {
                  body->sourceSnapShot =
                        pt_body->
                              second.get<string>(
                                    SOURCESNAPSHOT ) ;
               }
               else
               {
                  body->sourceSnapShot = CB_SNAP_NULL ;
               }
            }
         }
         catch( std::exception &e )
         {
            ossSnprintf( errStrBuf, errStrLength,
                         "%s", errStr ) ;
            ossSnprintf( errStr, errStrLength,
                         "%s readConfiguration failed,"
                         "e.what():%s"OSS_NEWLINE,
                         errStrBuf, e.what() ) ;
            rc = OB_ERROR ;
            goto error ;
         }
         for( BOOST_AUTO( pt_BValue, pt_body->second.begin() );
              pt_BValue != pt_body->second.end();
              ++pt_BValue )
         {
            if( VALUE == pt_BValue->first )
            {
               rc = storePanelValue( pt_BValue->second,
                                      body->value ) ;
               if( rc )
               {
                  goto error ;
               }
            }
         }
         ++bodyLength ;
      }
   }
   root.bodyLength = bodyLength ;
done :
   return rc ;
error :
   goto done ;
}

int32_t storeFooters( ptree pt_FTs, RootWindow &root )
{
   int32_t rc            = OB_SUCCESS ;
   int32_t footerLength  = 0 ;
   HeadTailMap *footer = NULL ;
   try
   {
      root.footerLength = pt_FTs.get<int32_t>( FOOTERLENGTH ) ;

      root.footer = CB_OSS_NEW HeadTailMap[root.footerLength] ;
   }
   catch( std::exception &e )
   {
      ossSnprintf( errStrBuf, errStrLength,
                   "%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,
                   "%s readConfiguration failed,"
                   "e.what():%s"OSS_NEWLINE,
                   errStrBuf, e.what() ) ;
      rc = OB_ERROR ;
      goto error ;
   }
   footerLength = 0 ;
   for( BOOST_AUTO( pt_footer, pt_FTs.begin() );
        pt_footer != pt_FTs.end();
        ++pt_footer )
   {
      if( HEADTAILMAP == pt_footer->first )
      {
         footer = &root.footer[footerLength] ;
         if( footerLength >= root.footerLength )
         {
            ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
            ossSnprintf( errStr, errStrLength,
                         "%s readConfiguration failed,"
                         "scope: footerLength >="
                         "root.footerLength"OSS_NEWLINE,
                         errStrBuf ) ;
            rc = OB_ERROR ;
            goto error ;
         }
         footer->key = pt_footer->second.get<int32_t>( KEY ) ;
         for( BOOST_AUTO( pt_FValue, pt_footer->second.begin() );
              pt_FValue != pt_footer->second.end();
              ++pt_FValue )
         {
            if( pt_FValue->first == VALUE )
            {
               rc = storePanelValue( pt_FValue->second,
                                     footer->value ) ;
               if( rc )
               {
                  goto error;
               }
            }
         }
         ++footerLength;
      }
   }
   root.footerLength = footerLength;
done :
   return rc ;
error :
   goto done ;
}

int32_t storeKeySuites( ptree pt_KSs, RootWindow &root )
{
   int32_t rc             = OB_SUCCESS ;
   int32_t keySuiteLength = 0 ;
   int32_t hotKeyLength   = 0 ;
   KeySuite *keySuite   = NULL ;
   HotKey *hotkey       = NULL ;
   try
   {
      root.keySuiteLength = pt_KSs.get<int32_t>( KEYSUITELENGTH ) ;

      root.keySuite =
            CB_OSS_NEW KeySuite[root.keySuiteLength] ;
   }
   catch( std::exception &e )
   {
      ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,
                   "%s readConfiguration failed,"
                   " scope: child_root->first ==..... "
                   ",e.what():%s"OSS_NEWLINE,

                   errStrBuf, e.what() ) ;
      rc = OB_ERROR ;
      goto error ;
   }
   keySuiteLength = 0 ;
   for( BOOST_AUTO( pt_keySuite, pt_KSs.begin() );
        pt_keySuite != pt_KSs.end();
        ++pt_keySuite )
   {
      if( KEYSUITE == pt_keySuite->first )
      {
         keySuite = &root.keySuite[keySuiteLength] ;
         if( keySuiteLength >= root.keySuiteLength )
         {
            ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
            ossSnprintf( errStr, errStrLength,
                         "%s readConfiguration failed,"
                         " keySuiteLength >="
                         "root.keySuiteLength"OSS_NEWLINE,
                         errStrBuf ) ;
            rc = OB_ERROR ;
            goto error ;
         }
         try
         {
            keySuite->mark = pt_keySuite->second.get<int64_t>( MARK ) ;
            keySuite->hotKeyLength = pt_keySuite->second.get<int32_t>(
                                           HOTKEYLENGTH ) ;
            keySuite->hotKeyLengthFromConf = keySuite->hotKeyLength ;
            keySuite->hotKey = CB_OSS_NEW HotKey[keySuite->hotKeyLength ] ;
         }
         catch( std::exception &e)
         {
            ossSnprintf( errStrBuf, errStrLength,
                         "%s", errStr ) ;
            ossSnprintf( errStr, errStrLength,
                         "%s readConfiguration failed,"
                         "e.what():%s\n",
                         errStrBuf, e.what() ) ;
            rc = OB_ERROR ;
            goto error ;
         }
         hotKeyLength = 0 ;
         for( BOOST_AUTO( pt_hotkey, pt_keySuite->second.begin() );
              pt_hotkey != pt_keySuite->second.end();
              ++pt_hotkey )
         {
            if( pt_hotkey->first == HOTKEY )
            {
               hotkey = &keySuite->hotKey[hotKeyLength] ;
               if( hotKeyLength >= keySuite->hotKeyLength )
               {
                  ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
                  ossSnprintf( errStr, errStrLength,
                               "%s readConfiguration failed, "
                               "hotKeyLength >= "
                               "keySuite->hotKeyLength"
                               OSS_NEWLINE,
                               errStrBuf ) ;
                  rc = OB_ERROR ;
                  goto error ;
               }
               try
               {
                  hotkey->button =
                        pt_hotkey->
                              second.get<char>(
                                    BUTTON ) ;
                  hotkey->jumpType =
                        pt_hotkey->
                              second.get<string>(
                                    JUMPTYPE ) ;
                  hotkey->jumpName =
                        pt_hotkey->
                              second.get<string>(
                                    JUMPNAME ) ;
                  hotkey->desc = pt_hotkey->
                                 second.get<string>(KEYDESC) ;
                  string wndtype = pt_hotkey->
                                 second.get<string>(WNDTYPE) ;
                  if ( 0 == ossStrncmp( wndtype.c_str(),
                                        "true", wndtype.length() ) )
                  {
                     hotkey->wndType = TRUE ;
                  }
                  else
                  {
                     hotkey->wndType = FALSE ;
                  }
               }
               catch( std::exception &e )
               {
                  ossSnprintf( errStrBuf, errStrLength,
                               "%s", errStr ) ;
                  ossSnprintf( errStr, errStrLength,
                               "%s readConfiguration failed,%s",
                               errStrBuf, e.what() ) ;
                  rc = OB_ERROR ;
                  goto error ;
               }
               ++hotKeyLength ;
            }
         }
         keySuite->hotKeyLength = hotKeyLength ;
         ++keySuiteLength ;
      }
   }
   root.keySuiteLength = keySuiteLength ;
done :
   return rc ;
error :
   goto done ;
}

int32_t storeRootWindow( RootWindow &root )
{
   int32_t rc = OB_SUCCESS ;
   ptree pt_sdbtopXML ;
   ptree pt_Event ;
   root.input.confPath = confPath ;
   InputPanel &input = root.input ;
   try
   {
      read_xml( root.input.confPath, pt_sdbtopXML ) ;
   }
   catch( std::exception &e )
   {
      ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,
                   "%s readConfiguration failed,"
                   "check configuration file %s is exist,"
                   "e.what():%s\n",
                   errStrBuf,
                   root.input.confPath.c_str(),
                   e.what() ) ;
      rc = OB_ERROR ;
      goto error ;
   }
   try
   {
      pt_Event = pt_sdbtopXML.get_child( EVENT ) ;
   }
   catch( std::exception &e )
   {
      ossSnprintf( errStrBuf, errStrLength,
                   "%s", errStr ) ;
      ossSnprintf( errStr, errStrLength,
                   "%s readConfiguration failed,"
                   "e.what():%s"OSS_NEWLINE,
                   errStrBuf, e.what() ) ;
      rc = OB_INVALID_ARGUMENT ;
      goto error ;
   }
   for( BOOST_AUTO( child_event, pt_Event.begin() );
        child_event != pt_Event.end(); ++child_event )
   {
      if( child_event->first == ROOTWINDOW )
      {
         try
         {
            root.referWindowRow =
                  child_event->
                        second.get<int32_t>(
                              REFERWINDOWROW ) ;
            root.referWindowColumn =
                  child_event->
                        second.get<int32_t>(
                              REFERWINDOWCOLUMN ) ;
            root.actualWindowMinRow =
                  child_event->
                        second.get<int32_t>(
                              ACTUALWINDOWMINROW );
            root.actualWindowMinColumn =
                  child_event->
                        second.get<int32_t>(
                              ACTUALWINDOWMINCOLUMN ) ;
            input.refreshInterval =
                  child_event->
                        second.get<int32_t>(
                              REFRESHINTERVAL ) ;
            input.colourOfTheDividingLine.foreGroundColor =
                  child_event->
                        second.get<int32_t>(
                              COLOUROFTHEDIVIDINGLINE_FOREGROUNDCOLOR ) ;
            input.colourOfTheDividingLine.backGroundColor =
                  child_event->
                        second.get<int32_t>(
                              COLOUROFTHEDIVIDINGLINE_BACKGROUNDCOLOR ) ;
            input.colourOfTheChange.foreGroundColor =
                  child_event->
                        second.get<int32_t>(
                              COLOUROFTHECHANGE_FOREGROUNDCOLOR ) ;
            input.colourOfTheChange.backGroundColor =
                  child_event->
                        second.get<int32_t>(
                              COLOUROFTHECHANGE_BACKGROUNDCOLOR ) ;
            input.colourOfTheMax.foreGroundColor =
                  child_event->
                        second.get<int32_t>(
                              COLOUROFTHEMAX_FOREGROUNDCOLOR ) ;
            input.colourOfTheMax.backGroundColor =
                  child_event->
                        second.get<int32_t>(
                              COLOUROFTHEMAX_BACKGROUNDCOLOR ) ;
            input.colourOfTheMin.foreGroundColor =
                  child_event->
                        second.get<int32_t>(
                              COLOUROFTHEMIN_FOREGROUNDCOLOR ) ;
            input.colourOfTheMin.backGroundColor =
                  child_event->
                        second.get<int32_t>(
                              COLOUROFTHEMIN_BACKGROUNDCOLOR ) ;
         }
         catch( std::exception &e )
         {
            ossSnprintf( errStrBuf, errStrLength,"%s", errStr ) ;
            ossSnprintf( errStr, errStrLength,
                         "%s readConfiguration failed,"
                         "e.what():%s"OSS_NEWLINE,
                         errStrBuf, e.what() ) ;
            rc = OB_ERROR ;
            goto error ;
         }
         for( BOOST_AUTO( child_root, child_event->second.begin() );
              child_root != child_event->second.end();
              ++child_root )
         {
            if( KEYSUITES == child_root->first )
            {
               rc = storeKeySuites( child_root->second, root ) ;
               if( rc )
               {
                  goto error ;
               }
            }
            else if( HEADERS == child_root->first )
            {
               rc = storeHeaders( child_root->second, root ) ;
               if( rc )
               {
                  goto error ;
               }
            }
            else if( child_root->first == BODIES )
            {
               rc = storeBodies( child_root->second, root ) ;
               if( rc )
               {
                  goto error ;
               }
            }
            else if( child_root->first == FOOTERS )
            {
               rc = storeFooters( child_root->second, root ) ;
               if( rc )
               {
                  goto error ;
               }
            }
         }
      }
   }
done :
   return rc ;
error :
   goto done ;
}

#endif

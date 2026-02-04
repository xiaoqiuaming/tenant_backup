#!/usr/bin/env python
# encoding=utf-8
import sys
import time
import os
import ConfigParser
import getopt
import socket
import time
import re

CB_VERSION = '1.3'


def showUsage():
    sys.stderr.write("""Usage:
        python nodeops.py -[c:r] [--help] [--path] [--as_list] [--force] [--config]
        
-c [Command]:
    init --path [path] --as_list [yaoadminsvrs]
        to generate the config file
        'path'    : the absolute path of the installed yaobase.
        'as_list' : all yaoadminsvrs in the cluster. This is the first step before the other commands.
    start --config [configfile]
        when use the 'start' command, must attach '--config' to read the property from your custom configFile.
    stop
        to stop all servers(default signal 15). Attach '--force' to send signal 9
    start_ss[ds|ts|as]
        to start the given single server
    stop_as[ss|ts|ds]
        to stop the given single server
        
-r [Master yaoadminsvr]:
    appoint master yaoadminsvr info, format "ip:port"
    
--help: show the usage

--force when you use the 'stop' command,
        attach '--force' will use '-9' signal to kill the server process instead of '-15'

--v1.2: if yaobase version is 1.2, attach it.  default is 1.3

eg(step by step):
    python nodeops.py -c init --path /home/admin/oceanbase/ --as_list 10.0.1.14:2500,10.0.1.15:2500,10.0.1.16:2500
    python nodeops.py -c start --config custom.cfg
    python nodeops.py -c start_as
    python nodeops.py -c start_as -r 10.0.1.14:2500
    python nodeops.py -c stop_as
    python nodeops.py -c stop --force
""")


def main():
    YAOADMINSVR = "YaoAdminSvr"
    YAOTXNSVR = "YaoTxnSvr"
    YAOSQLSVR = "YaoSqlSvr"
    YAODATASVR = "YaoDataSvr"
    command = None
    cbaseConfig = None
    yaoadminsvrs = None
    force = False
    isInit = False
    customConfig = None
    cbasePath = None
    masterAs = None

    #default config file
    asConfigName = "./as.cfg"
    tsConfigName = "./ts.cfg"
    ssConfigName = "./ss.cfg"
    dsConfigName = "./ds.cfg"

    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:r:", ["help", "force", "v1.2", "path=", "as_list=", "config="])
    except getopt.GetoptError:
        print('the command you typed can not be recognized, please try again')
        sys.exit(1)
    if len(sys.argv) == 1:
        showUsage()
        sys.exit(0)
    for opt, value in opts:
        if opt == '-c':
            command = value
            if (value == "init"):
                isInit = True
        elif opt == '-r':
            masterAs = value
            if not masterAs and checkIpaddr(masterAs.split(":")[0]) and int(
                    masterAs.split(":")[1]).isdigit():
                print("input master yaoadminsvr info is invalid, as=%s" % masterAs)
                sys.exit(1)
        elif opt == '--help':
            showUsage()
            sys.exit(0)
        elif opt == '--path':
            cbasePath = value
        elif opt == '--as_list':
            yaoadminsvrs = value
        elif opt == '--force':
            force = True
        elif opt == '--config':
            customConfig = value
        elif opt == '--v1.2':
            global CB_VERSION
            CB_VERSION = '1.2'
        else:
            sys.exit(0)

    if isInit:
        if cbasePath == None or cbasePath.strip() == '':
            print(
                "no cbasePath, please use the command '-c init --path [path] --as_list [yaoadminsvrs]' to init"
            )
            sys.exit(1)
        if yaoadminsvrs == None or yaoadminsvrs.strip() == '':
            print(
                "no as_list, please use the command '-c init --path [path] --as_list [yaoadminsvrs]' to init"
            )
            sys.exit(1)

        #get default parameters from etc/*.config.bin files
        cbaseSsConfig = CbaseConfig(ssConfigName, 'ss')
        cbaseDsConfig = CbaseConfig(dsConfigName, 'ds')
        cbaseAsConfig = CbaseConfig(asConfigName, 'as')
        cbaseTsConfig = CbaseConfig(tsConfigName, 'ts')

        cbaseSsConfig.initDefaultDataFromEtc(cbasePath, yaoadminsvrs)
        cbaseDsConfig.initDefaultDataFromEtc(cbasePath, yaoadminsvrs)
        cbaseAsConfig.initDefaultDataFromEtc(cbasePath, yaoadminsvrs)
        cbaseTsConfig.initDefaultDataFromEtc(cbasePath, yaoadminsvrs)

        sys.exit(0)

    if command.strip() == 'start' and not customConfig:
        print("no configfile, please use the command '-c start --config [configfile]'")
        sys.exit(1)
    if (customConfig):
        if (os.path.exists(customConfig) == False):
            print("customConfig doesn't exist")
            sys.exit(1)
        else:
            asConfigName = customConfig
            tsConfigName = customConfig
            ssConfigName = customConfig
            dsConfigName = customConfig

    # get ip address
    host = get_public_ip()

    # get NIC name
    NIC = os.popen("/sbin/ip addr | grep '%s' | head -1 |awk '{printf $NF}'" % host).read()

    # start command
    masterAsIP, masterAsPORT = None, None
    if command.startswith("start"):
        startAll = False
        if command.strip() == 'start':
            startAll = True
        if command == 'start_as' or startAll:
            yaoadminSvr = None
            # get master AS
            cbaseConfig = CbaseConfig(asConfigName, 'as')
            yaoadminsvrs, cbasePath = getCommonInfo(cbaseConfig)
            if masterAs:
                masterAsIP, masterAsPORT = masterAs.split(":")[0], masterAs.split(":")[1]
            else:
                masterAsIP, masterAsPORT = getMasterServerInfo(cbasePath, yaoadminsvrs)

            # Init AS
            if (cbaseConfig.isSectionExist("YaoAdminSvr") and (command.startswith('start') == True)):
                yaoadminSvr = YaoAdminSvr(
                    cbasePath, YAOADMINSVR, host,
                    cbaseConfig.get("port", YAOADMINSVR),
                    masterAsIP, masterAsPORT, NIC,
                    cbaseConfig.get("clusterId", YAOADMINSVR),
                    cbaseConfig.get("paxosNum", YAOADMINSVR),
                    cbaseConfig.get("clusterNum", YAOADMINSVR),
                    cbaseConfig.get("isPaxosEnable", YAOADMINSVR),
                    cbaseConfig.get("AsNum", YAOADMINSVR),
                    cbaseConfig.get("TsNum", YAOADMINSVR))

            #check process is exist or not
            if yaoadminSvr != None:
                if yaoadminSvr.start():
                    time.sleep(10)
                    if (isProcessExits(cbasePath, "yaoadminsvr")):
                        print('start yaoadminsvr successfully')
                        sys.exit(0)
                else:
                    print('start yaoadminsvr failed')
                    sys.exit(1)

        if command == 'start_ts' or startAll:
            yaotxnSvr = None
            # get master AS
            cbaseConfig = CbaseConfig(tsConfigName, 'ts')
            yaoadminsvrs, cbasePath = getCommonInfo(cbaseConfig)
            if masterAs:
                masterAsIP, masterAsPORT = masterAs.split(":")[0], masterAs.split(":")[1]
            else:
                masterAsIP, masterAsPORT = getMasterServerInfo(cbasePath, yaoadminsvrs)

            # Init yaotxnsvr
            if (cbaseConfig.isSectionExist("YaoTxnSvr") and (command.startswith('start') == True)):
                yaotxnSvr = YaoTxnSvr(
                    cbasePath, YAOTXNSVR, masterAsIP,
                    cbaseConfig.get("port", YAOTXNSVR),
                    cbaseConfig.get("serverPort", YAOTXNSVR),
                    cbaseConfig.get("mergePort", YAOTXNSVR), NIC,
                    cbaseConfig.get("clusterId", YAOTXNSVR),
                    cbaseConfig.get("paxosId", YAOTXNSVR))

            #check process is exist or not
            if yaotxnSvr != None:
                if yaotxnSvr.start():
                    time.sleep(10)
                    if (isProcessExits(cbasePath, "yaotxnsvr")):
                        print('start yaotxnsvr successfully')
                        sys.exit(0)
                else:
                    print('start yaotxnsvr failed')
                    sys.exit(1)

        if command == 'start_ss' or startAll:
            yaosqlSvr = None
            # get master AS
            cbaseConfig = CbaseConfig(ssConfigName, 'ss')
            yaoadminsvrs, cbasePath = getCommonInfo(cbaseConfig)
            if masterAs:
                masterAsIP, masterAsPORT = masterAs.split(":")[0], masterAs.split(":")[1]
            else:
                masterAsIP, masterAsPORT = getMasterServerInfo(cbasePath, yaoadminsvrs)

            # Init yaosqlsvr
            if (cbaseConfig.isSectionExist("YaoSqlSvr") and (command.startswith('start') == True)):
                yaosqlSvr = YaoSqlSvr(
                    cbasePath, YAOSQLSVR, masterAsIP,
                    cbaseConfig.get("port", YAOSQLSVR),
                    cbaseConfig.get("serverPort", YAOSQLSVR),
                    cbaseConfig.get("mysqlPort", YAOSQLSVR), NIC,
                    cbaseConfig.get("clusterId", YAOSQLSVR),
                    cbaseConfig.get("isLms", YAOSQLSVR))

            #check process is exist or not
            if yaosqlSvr != None:
                if yaosqlSvr.start():
                    time.sleep(10)
                    if (isProcessExits(cbasePath, "yaosqlsvr")):
                        print('start yaosqlsvr successfully')
                        sys.exit(0)
                else:
                    print('start yaosqlsvr failed')
                    sys.exit(1)

        if command == 'start_ds' or startAll:
            yaodataSvr = None
            # get master AS
            cbaseConfig = CbaseConfig(dsConfigName, 'ds')
            yaoadminsvrs, cbasePath = getCommonInfo(cbaseConfig)
            if masterAs:
                masterAsIP, masterAsPORT = masterAs.split(":")[0], masterAs.split(":")[1]
            else:
                masterAsIP, masterAsPORT = getMasterServerInfo(cbasePath, yaoadminsvrs)

            # Init yaodataSvr
            if (cbaseConfig.isSectionExist("YaoDataSvr")
                and (command.startswith('start') == True)):
                    yaodataSvr = YaoDataSvr(
                    cbasePath, YAODATASVR, masterAsIP,
                    cbaseConfig.get("port", YAODATASVR),
                    cbaseConfig.get("serverPort", YAODATASVR),
                    cbaseConfig.get("appName", YAODATASVR), NIC,
                    cbaseConfig.get("clusterId", YAODATASVR))

            #check process is exist or not
            if yaodataSvr != None:
                if yaodataSvr.start():
                    time.sleep(10)
                    if (isProcessExits(cbasePath, "yaodatasvr")):
                        print('start yaodatasvr successfully')
                        sys.exit(0)
                else:
                    print('start yaodatasvr failed')
                    sys.exit(1)

    elif command.startswith("stop"):
        stopAll = False
        if command.strip() == 'stop':
            stopAll = True
        if command == "stop_as" or stopAll:
            cbaseConfig = CbaseConfig(asConfigName, 'as')
            yaoadminsvrs, cbasePath = getCommonInfo(cbaseConfig);
            killServer(cbasePath, "yaoadminsvr", 9 if force else 15)
        if command == "stop_ts" or stopAll:
            cbaseConfig = CbaseConfig(tsConfigName, 'ts')
            yaoadminsvrs, cbasePath = getCommonInfo(cbaseConfig);
            killServer(cbasePath, "yaotxnsvr", 9 if force else 15)
        if command == "stop_ss" or stopAll:
            cbaseConfig = CbaseConfig(ssConfigName, 'ss')
            yaoadminsvrs, cbasePath = getCommonInfo(cbaseConfig);
            killServer(cbasePath, "yaosqlsvr", 9 if force else 15)
        if command == "stop_ds" or stopAll:
            cbaseConfig = CbaseConfig(dsConfigName, 'ds')
            yaoadminsvrs, cbasePath = getCommonInfo(cbaseConfig);
            killServer(cbasePath, "yaodatasvr", 9 if force else 15)
        print('stop done')
        sys.exit(0)
    else:
        pass

def checkIpaddr(hostip):
    pat = re.compile(r'([0-9]{1,3})\.')
    r = re.findall(pat, hostip + ".")
    if len(r) and len([x for x in r if int(x)>=0 and int(x)<=255]) == 4:
        return True
    return False

def getCommonInfo(cbaseConfig):
    yaoadminsvrs = cbaseConfig.get("yaoadminsvrs", "common")
    if yaoadminsvrs == None or yaoadminsvrs.strip() == '':
        print("no as_list, please use the command '-c init --path [path] --as_list [yaoadminsvrs]' to init");
        sys.exit(1)
    cbasePath = cbaseConfig.get("cbasePath", "common");
    if (cbasePath == None or cbasePath.strip() == ''):
        print("no cbasePath, please use the command '-c init --path [path] --as_list [yaoadminsvrs]' to init");
        sys.exit(1)
    return yaoadminsvrs, cbasePath

def isProcessExits(cbasePath, server):
    pid = os.popen("cat %s/run/%s.pid" % (cbasePath, server)).read();
    ret = os.popen("ps ux | awk '{print $2}' | grep %s" % pid).read()
    return pid == ret
        
def getPid(cbasePath, server):
    return os.popen("cat %s/run/%s.pid" % (cbasePath, server)).read();

def showProcessExitInfo(pid):
    print ("program has been exist : pid=%s" % pid);

def killServer(cbasePath, server, sig=15):
    if (os.path.exists("%s/run/%s.pid" % (cbasePath, server)) == False):
        warn("No %s running now!" % server);
        return
    ret = os.system("cat %s/run/%s.pid | xargs kill -%d;" % (cbasePath, server, sig))
    if (ret):
        warn("No %s running now!" % server);
    else:
        buffer = "force " if(sig == 9) else ""
        print("%skill %s" % (buffer, server));

def warn(str):
    print ("\033[35m[  WARN ]\033[0m %s" % str);

def getYaoAdminSvrList(cbasePath, masterAsIP, masterAsPORT):
    return os.popen("%s/bin/as_admin -r %s -p %s stat -o all_server | grep yaoadminsvrs" % (cbasePath, masterAsIP, masterAsPORT)).read()

def get_public_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip

def getMasterServerInfo(cbasePath, yaoadminsvrs):
    if yaoadminsvrs == None or yaoadminsvrs.strip() == '':
        print ("find inner error")
        sys.exit(1)

    as_list = yaoadminsvrs.split(',')
    for yaoadminSvr in (as_list):
        str = os.popen("%s/bin/as_admin -r %s -p %s get_rs_leader" % (cbasePath, yaoadminSvr.split(':')[0], yaoadminSvr.split(':')[1])).read();
        #if str.find('as election done, leader yaoadminsvr') != -1:
        if str.find('rs election done, leader rootserver') != -1:
            index1 = str.rfind('[');
            index2 = str.rfind(']');
            master = str[index1+1:index2-2];
            try:
                ip = master.split(':')[0];
                port = master.split(':')[1];
                if (ip == None or port == None or ip.strip() == '' or port.strip() == ''):
                    print("the as_list maybe wrong, please use the command '-c init --path [path] --as_list [yaoadminsvrs]' to init")
                    sys.exit(1)
                return ip,port
            except IndexError:
                print("getMasterServerInfo:the as_list in config file maybe wrong, please write again");
                sys.exit(1)
            break;
        else:
            continue
    print('get main yaoadminsvr failed, quiting...')
    sys.exit(1)


class CbaseConfig(object):
    def __init__(self, configFile, type):
        self.configFile = configFile
        self.type = type
        if os.path.exists(self.configFile) and type != None:
            self.cfg = ConfigParser.ConfigParser()
            try:
                self.cfg.read(self.configFile)
            except ConfigParser.ParsingError:
                print("please check the parameter in your config file. It may be wrong.")
                sys.exit(1)
    
    def tell(self):
        if (os.path.exists(self.configFile) == False):
            print("no config file. Please use the command '-c init' to generate the config file.")
            sys.exit(1)
    
    def reboot(self):
        self.__init_local_config();
        self.cfg = ConfigParser.ConfigParser()
        self.cfg.read(self.configFile)
        print("config file %s generated." % self.configFile)

    def isSectionExist(self, sectionName):
        self.tell()
        sections = self.cfg.sections();
        for section in sections:
            if(sectionName == section):
                return True
        return False

    def get_option(self, section):
        self.tell()
        return self.cfg.options(section)

    def get(self, key, section):
        self.tell()
        try:
            ret = self.cfg.get(section=section, option=key)
            return ret
        except ConfigParser.ParsingError:
            return ''
    
    def remove(self, section):
        self.tell()
        try:
            self.cfg.remove_section(section)
            self.cfg.write(open(self.configFile, 'w'))
            return True
        except Exception:
            pass
    
    def delConfigFile(self, configFile):
        #os.remove(configFile)
        pass

    def set(self, key, val, section):
        self.tell()
        try:
            self.cfg.set(section=section, option=key, value=val)
            self.cfg.write(open(self.configFile, 'w'))
            return True
        except Exception:
            pass
    
    def initDefaultDataFromEtc(self, cbasePath, yaoadminsvrs):
        self.reboot()
        self.set("cbasePath", cbasePath, "common")
        self.set("yaoadminsvrs", yaoadminsvrs, "common")
        self.tell();
        if self.type == "as":
            print("in " + self.type)
            rootConfig = cbasePath + "/etc/yaoadminsvr.config.bin"
            if(os.path.exists(rootConfig)):
                if(self.isSectionExist("YaoAdminSvr") == False):
                    self.cfg.add_section("YaoAdminSvr")
                else:
                    self.remove("YaoAdminSvr")
                    self.cfg.add_section("YaoAdminSvr")
                try:
                    port = (os.popen("grep -aw port %s" % rootConfig).read().split("=")[1]).strip()
                    clusterId = (os.popen("grep -aw cluster_id %s" % rootConfig).read().split("=")[1]).strip()
                    paxosNum = ''
                    clusterNum = ''
                    isPaxosEnable = ''
                    if CB_VERSION == '1.3':
                        paxosNum = (os.popen("grep -aw use_paxos_num %s" % rootConfig).read().split("=")[1]).strip()
                        clusterNum = (os.popen("grep -aw use_cluster_num %s" % rootConfig).read().split("=")[1]).strip()
                        isPaxosEnable = (os.popen("grep -aw is_use_paxos %s" % rootConfig).read().split("=")[1]).strip()
                    AsNum = (os.popen("grep -aw rs_paxos_number %s" % rootConfig).read().split("=")[1]).strip()
                    TsNum = (os.popen("grep -aw ups_quorum_scale %s" % rootConfig).read().split("=")[1]).strip()
                    self.set("port", port, "YaoAdminSvr")
                    self.set("clusterId", clusterId, "YaoAdminSvr")
                    self.set("paxosNum", paxosNum, "YaoAdminSvr")
                    self.set("clusterNum", clusterNum, "YaoAdminSvr")
                    self.set("isPaxosEnable", isPaxosEnable, "YaoAdminSvr")
                    self.set("AsNum", AsNum, "YaoAdminSvr")
                    self.set("TsNum", TsNum, "YaoAdminSvr")
                except IndexError,e:
                    print e
                    print('init as config failed, exit!')
                    sys.exit(1)
            elif os.path.exists(self.configFile):
                self.delConfigFile(self.configFile)
        
        if self.type == "ts":
            print("in " + self.type)
            tsConfig = cbasePath + "/etc/yaotxnsvr.config.bin"
            if(os.path.exists(tsConfig)):
                if(self.isSectionExist("YaoTxnSvr") == False):
                    self.cfg.add_section("YaoTxnSvr")
                else:
                    self.remove("YaoTxnSvr")
                    self.cfg.add_section("YaoTxnSvr")
                try:
                    port = (os.popen("grep -aw root_server_port %s" % tsConfig).read().split("=")[1]).strip()
                    serverPort = (os.popen("grep -aw port %s" % tsConfig).read().split("=")[1]).strip()
                    mergePort = (os.popen("grep -aw inner_port %s" % tsConfig).read().split("=")[1]).strip()
                    clusterId = (os.popen("grep -aw cluster_id %s" % tsConfig).read().split("=")[1]).strip()
                    paxosid = ''
                    if CB_VERSION == '1.3':
                        paxosid = (os.popen("grep -aw paxos_id %s" % tsConfig).read().split("=")[1]).strip()
                    self.set("port", port, "YaoTxnSvr")
                    self.set("paxosid", paxosid, "YaoTxnSvr")
                    self.set("mergePort", mergePort, "YaoTxnSvr")
                    self.set("clusterId", clusterId, "YaoTxnSvr")
                    self.set("serverPort", serverPort, "YaoTxnSvr")
                except IndexError,e:
                    print e
                    print('init ts config failed, exit!')
                    sys.exit(1)
            elif os.path.exists(self.configFile):
                self.delConfigFile(self.configFile)

        if self.type == "ss":
            print("in " + self.type)
            ssConfig = cbasePath + "/etc/yaosqlsvr.config.bin"
            if(os.path.exists(ssConfig)):
                if(self.isSectionExist("YaoSqlSvr") == False):
                    self.cfg.add_section("YaoSqlSvr")
                else:
                    self.remove("YaoSqlSvr")
                    self.cfg.add_section("YaoSqlSvr")
                try:
                    port = (os.popen("grep -aw root_server_port %s" % ssConfig).read().split("=")[1]).strip()
                    serverPort = (os.popen("grep -aw port %s" % ssConfig).read().split("=")[1]).strip()
                    mysqlPort = (os.popen("grep -aw obmysql_port %s" % ssConfig).read().split("=")[1]).strip()
                    clusterId = (os.popen("grep -aw cluster_id %s" % ssConfig).read().split("=")[1]).strip()
                    isLms = (os.popen("grep -aw lms %s" % ssConfig).read().split("=")[1]).strip()
                    self.set("port", port, "YaoSqlSvr")
                    self.set("serverPort", serverPort, "YaoSqlSvr")
                    self.set("mysqlPort", mysqlPort, "YaoSqlSvr")
                    self.set("clusterId", clusterId, "YaoSqlSvr")
                    self.set("isLms", isLms, "YaoSqlSvr")
                except IndexError,e:
                    print e
                    print('init ss config failed, exit!')
                    sys.exit(1)
            elif os.path.exists(self.configFile):
                self.delConfigFile(self.configFile)

        if self.type == "ds":
            dsConfig = cbasePath + "/etc/yaodatasvr.config.bin"
            print("in " + self.type)
            if(os.path.exists(dsConfig)):
                if(self.isSectionExist("YaoDataSvr") == False):
                    self.cfg.add_section("YaoDataSvr")
                else:
                    self.remove("YaoDataSvr")
                    self.cfg.add_section("YaoDataSvr")
                try:
                    port = (os.popen("grep -aw root_server_port %s" % dsConfig).read().split("=")[1]).strip()
                    serverPort = (os.popen("grep -aw port %s" % dsConfig).read().split("=")[1]).strip()
                    appName = (os.popen("grep -aw appname %s" % dsConfig).read().split("=")[1]).strip()
                    clusterId = (os.popen("grep -aw cluster_id %s" % dsConfig).read().split("=")[1]).strip()
                    self.set("port", port, "YaoDataSvr")
                    self.set("serverPort", serverPort, "YaoDataSvr")
                    self.set("appName", appName, "YaoDataSvr")
                    self.set("clusterId", clusterId, "YaoDataSvr")
                except IndexError,e:
                    print e
                    print('init ds config failed, exit!')
                    sys.exit(1)
            elif os.path.exists(self.configFile):
                self.delConfigFile(self.configFile)

    def __init_local_config(self):
        asInfo = '''# 请修改配置文件
[common]
#cbase启动目录(绝对路径)
cbasePath = 
#集群中所有yaoadminsvr，用逗号隔开，例如10.0.0.1:2500,10.0.0.2:2500,10.0.0.3:2500
yaoadminsvas = 
#------------------------------------------------------------------------------
[YaoAdminSvr]
#例: yaoadminsvr -r 10.0.0.1:2500 -R 10.0.0.1:2500 -i bond0 -C 0 -G 1 -K 1 -F true -U 1 -u 1

#YaoAdminSvr端口
port = 
#集群Id, 对应例子中的-C
clusterId = 
#paxos组数, 对应例子中的-G
paxosNum = 
#集群个数, 对应例子中的-K
clusterNum = 
#是否开启paxos, 对应例子中的-F
isPaxosEnable = 
#集群AS的数量, 对应例子中的-U
AsNum = 
#集群Ts的数量, 对应例子中的-u
TsNum = 
'''
        tsInfo = '''# 请修改配置文件
[common]
#cbase启动目录(绝对路径)
cbasePath = 
#集群中所有yaoadminsvr，用逗号隔开，例如10.0.0.1:2500,10.0.0.2:2500,10.0.0.3:2500
yaoadminsvrs = 
#------------------------------------------------------------------------------
[YaoTxnSvr]
#例: yaotxnsvr -r 10.0.0.1:2500 -p 2700 -m 2701 -i bond0 -C 0 -g 0

#YaoAdminSvr端口
port = 
#YaoTxnSvr服务端口, 对应例子中的-p
serverPort = 
#YaoTxnSvr每日合并端口, 对应例子中的-m
mergePort = 
#集群Id, 对应例子中的-C
clusterId = 
#paxos组Id, 对应例子中的-g
paxosId = 
'''

        ssInfo = '''# 请修改配置文件
[common]
#cbase启动目录(绝对路径)
cbasePath = 
#集群中所有yaoadminsvr，用逗号隔开，例如10.0.0.1:2500,10.0.0.2:2500,10.0.0.3:2500
yaoadminsvrs = 
#------------------------------------------------------------------------------
[YaoSqlSvr]
#例: yaosqlsvr -r 10.0.0.1:2500 -p 2800 -z 2880 -i bond0 -C 0

#YaoAdminSvr端口
port = 
#YaoSqlSvr服务端口, 对应例子中的-p
serverPort = 
#YaoSqlSvr提供的mysql连接端口, 对应例子中的-z
mysqlPort = 
#集群Id, 对应例子中的-C
clusterId = 
#是否为Lms
isLms = 
'''

        dsInfo = '''# 请修改配置文件
[common]
#cbase启动目录(绝对路径)
cbasePath = 
#集群中所有yaoadminsvr，用逗号隔开，例如10.0.0.1:2500,10.0.0.2:2500,10.0.0.3:2500
yaoadminsvrs = 
#------------------------------------------------------------------------------
[YaoDataSvr]
#例: yaodatasvr -r 10.0.0.1:2500 -p 2600 -n obtest -i bond0 -C 0

#YaoAdminSvr端口
port = 
#YaoDataSvr, 对应例子中的-p
serverPort = 
#oceanbase实例名, 对应例子中的-n
appName = 
#集群Id, 对应例子中的-C
clusterId = 
'''
        if os.path.exists(self.configFile):
            pass
        fd = open(self.configFile, 'w')
        if self.type == "as":
            fd.write(asInfo)
        if self.type == "ts":
            fd.write(tsInfo)
        if self.type == "ss":
            fd.write(ssInfo)
        if self.type == "ds":
            fd.write(dsInfo)
            fd.flush()
            fd.close()

class ObServer(object):
    def __init__(self, name, NIC, cbasePath, clusterId, ip, port):
        self.name = name
        self.NIC = NIC
        self.cbasePath = cbasePath
        self.clusterId = clusterId
        self.ip = ip;
        self.port = port;

    def start(self):
        print(self.name + " is starting...");

    def killSelf(self):
        print(self.name + " is killed.");

class YaoAdminSvr(ObServer):
    def __init__(self, cbasePath, name, ip, port, masterAsIP, masterAsPORT, NIC, clusterId, paxosNum, clusterNum, isPaxosEnable, AsNum, TsNum):
        super(YaoAdminSvr, self).__init__(name, NIC, cbasePath, clusterId, ip, port)
        self.masterAsIP = masterAsIP
        self.masterAsPORT = masterAsPORT
        self.paxosNum = paxosNum
        self.clusterNum = clusterNum
        self.isPaxosEnable = isPaxosEnable
        self.AsNum = AsNum
        self.TsNum = TsNum

    def getStartCMD(self):
        if CB_VERSION == '1.2':
            if self.ip == self.masterAsIP:
                return "bin/yaoadminsvr -r %s:%s -R %s:%s -i %s -C %s -U %s -u %s" % (self.ip, self.port, self.masterAsIP, self.masterAsPORT, self.NIC, self.clusterId, self.AsNum, self.TsNum);
            else:
                return "bin/yaoadminsvr -r %s:%s -R %s:%s -i %s -C %s" % (self.ip, self.port, self.masterAsIP, self.masterAsPORT, self.NIC, self.clusterId);
        else:
            return "bin/yaoadminsvr -r %s:%s -R %s:%s -i %s -C %s -G %s -K %s -F %s -U %s -u %s" % (self.ip, self.port, self.masterAsIP, self.masterAsPORT, self.NIC, self.clusterId, self.paxosNum, self.clusterNum, self.isPaxosEnable.lower(), self.AsNum, self.TsNum);

    def killSelf(self, sig=15):
        ret = os.system("cat %s/run/yaoadminsvr.pid | xargs kill -%d;" % (self.cbasePath, sig))
        return ret
    
    def start(self):
        if(isProcessExits(self.cbasePath, "yaoadminsvr")):
            print("==========================")
            showProcessExitInfo(getPid(self.cbasePath, "yaoadminsvr"));
            return 0
        pwd = os.getcwd()
        os.chdir(self.cbasePath)
        os.system(self.getStartCMD())
        os.chdir(pwd)
        return 1;

class YaoTxnSvr(ObServer):
    def __init__(self, cbasePath, name, ip, port, serverPort, mergePort, NIC, clusterId, paxosId):
        super(YaoTxnSvr, self).__init__(name, NIC, cbasePath, clusterId, ip, port)
        self.serverPort = serverPort
        self.mergePort = mergePort
        self.paxosId = paxosId

    def getStartCMD(self):
        if CB_VERSION == '1.2':
            return "bin/yaotxnsvr -r %s:%s -p %s -m %s -i %s -C %s" % (self.ip, self.port, self.serverPort, self.mergePort, self.NIC, self.clusterId);
        else:
            return "bin/yaotxnsvr -r %s:%s -p %s -m %s -i %s -C %s -g %s" % (self.ip, self.port, self.serverPort, self.mergePort, self.NIC, self.clusterId, self.paxosId);

    def killSelf(self, sig=15):
        ret = os.system("cat %s/run/yaotxnsvr.pid | xargs kill -%d;" % (self.cbasePath, sig))
        return ret
    
    def start(self):
        if(isProcessExits(self.cbasePath, "yaotxnsvr")):
            showProcessExitInfo(getPid(self.cbasePath, "yaotxnsvr"));
            return 0
        pwd = os.getcwd()
        os.chdir(self.cbasePath)
        os.system(self.getStartCMD())
        os.chdir(pwd)
        return 1;

class YaoSqlSvr(ObServer):
    def __init__(self, cbasePath, name, ip, port, serverPort, mysqlPort, NIC, clusterId, isLms):
        super(YaoSqlSvr, self).__init__(name, NIC, cbasePath, clusterId, ip, port)
        self.serverPort = serverPort
        self.mysqlPort = mysqlPort
        self.isLms = isLms

    def getStartCMD(self):
        lmsStr = "-t lms" if (self.isLms == "True" or self.isLms == "true") else ""
        return "bin/yaosqlsvr -r %s:%s -p %s -z %s -i %s -C %s %s" % (self.ip, self.port, self.serverPort, self.mysqlPort, self.NIC, self.clusterId, lmsStr);

    def killSelf(self, sig=15):
        ret = os.system("cat %s/run/yaosqlsvr.pid | xargs kill -%d;" % (self.cbasePath, sig))
        return ret
    
    def start(self):
        if(isProcessExits(self.cbasePath, "yaosqlsvr")):
            showProcessExitInfo(getPid(self.cbasePath, "yaosqlsvr"));
            return 0
        pwd = os.getcwd()
        os.chdir(self.cbasePath)
        os.system(self.getStartCMD())
        os.chdir(pwd)
        return 1;

class YaoDataSvr(ObServer):
    def __init__(self, cbasePath, name, ip, port, serverPort, appName, NIC, clusterId):
        super(YaoDataSvr, self).__init__(name, NIC, cbasePath, clusterId, ip, port)
        self.serverPort = serverPort
        self.appName = appName

    def getStartCMD(self):
        return "bin/yaodatasvr -r %s:%s -p %s -n %s -i %s -C %s" % (self.ip, self.port, self.serverPort, self.appName, self.NIC, self.clusterId);

    def killSelf(self, sig=15):
        ret = os.system("cat %s/run/yaodatasvr.pid | xargs kill -%d;" % (self.cbasePath, sig))
        return ret
    
    def start(self):
        if(isProcessExits(self.cbasePath, "yaodatasvr")):
            showProcessExitInfo(getPid(self.cbasePath, "yaodatasvr"));
            return 0
        pwd = os.getcwd()
        os.chdir(self.cbasePath)
        os.system(self.getStartCMD())
        os.chdir(pwd)
        return 1;

if __name__ == '__main__':
    t = time.time()
    os.chdir(os.path.dirname(os.path.abspath(sys.argv[0])))
    try:
        main()
    except:
        raise

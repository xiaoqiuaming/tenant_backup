![logo](https://raw.github.com/alibaba/oceanbase/oceanbase_0.3/doc/%E5%9B%BE%E7%89%87%E5%A4%B9/logo.jpg)
<font size=5><b>[English Version](https://github.com/alibaba/oceanbase/wiki/Oceanbase)</b></font>

OceanBase是[阿里巴巴集团](http://page.china.alibaba.com/shtml/about/ali_group1.shtml)自主研发的可扩展的关系型数据库，OceanBase实现了跨行跨表的事务，支持数千亿条记录、数百TB数据上的SQL操作，截止到2012年8月为止，OceanBase数据库支持了阿里巴巴集团下多个重要业务的数据存储，支持业务包括收藏夹、直通车报表、天猫评价等，截止2013年4月份，OceanBase线上业务的数据量已经超过一千亿条。

从模块划分的角度看，OceanBase可以划分为四个模块：主控服务器RootServer、更新服务器UpdateServer、基准数据服务器ChunkServer以及合并服务器MergeServer。OceanBase系统内部按照时间线将数据划分为基准数据和增量数据，基准数据是只读的，所有的修改更新到增量数据中，系统内部通过合并操作定期将增量数据融合到基准数据中。

<h1>最新动态</h1>
<font color=“#F00”><b>2013/06/19，合并最新的bugfix和特性到0.41分支上：</b> </font>

【重要】RootServer: Chunk Server在合并失败后，如果发现其他两个副本已经成功合并，会本地直接删除失败的副本，以前的版本RootServer并没有处理主动删除replica的情况;

SQL: ob_tablet_get.cpp的reset方法没有实现完全，这个bug会导致select * from t1 as z where k1 = 1不断执行时失败；

SQL：group_min_max.test执行cs出core，原因是ob_row_store重用时有错，造成sort操作符访问到额外数据，致使比较类型出错，std::sort会core掉; 

[查看发布说明](https://github.com/alibaba/oceanbase/wiki/OceanBase-0.4.1-1209%E5%8F%91%E5%B8%83%E8%AF%B4%E6%98%8E)

<h1>发行日志</h1>
- <font color=“#F00”><b>2013/04/28，整理提交了V0.4.1代码和相关技术文档。</b> </font>
- 2013/03/04，整理提交了V0.3.1代码和相关技术文档。

<h1>版本特性</h1>

- 使用libeasy网络框架代替了原来的tbnet，实现更高的网络处理性能
- 全面支持mysql协议（支持所有兼容libmysql库的driver，包含终端客户端mysql程序等）
- 支持SQL的客户端库
  - 提供了兼容JDBC的obdatasource库，实现OB集群内负载均衡策略
  - 提供了二进制兼容libmysql的libobsql库，实现OB集群内负载均衡策略
- 全面支持SQL
  - DML语句：select, insert, replace, delete, update
       - 支持select的大部分常用语法，包括任意复杂单表查询、集合交并差操作、受限的join、受限的子查询等
       - 支持的数据类型包括int, varchar, timestamp, float, double等
  - DDL语句：create table, drop table
  - 支持SQL事务
       - Start transaction, commit, rollback
       - SELECT FOR UPDATE
       - 实现了READ-COMMITED隔离级别的MVCC并发控制
  - 用户权限控制  
       - Create user, drop user, rename user, alter user, set password等
       - Grant, revoke等
  - 服务器端Prepared statement
       - 支持SQL语句prepare, execute, drop prepare等
       - 支持兼容mysql二进制协议的prepared statement
  - 其他语句
       - 通过set语句支持用户自定义变量
       - Show tables, show variables, show grants, show warnings, show columns, describe等
       - Explain
- 易用性改进
  - 引入了内部表机制，很多SQL功能基于内部表实现
       - Schema使用内部表管理，废除了schema配置文件
       - 引入了内部trigger通知机制
       - OB自身的配置使用内部表管理
  - 自监控
       - OB自身的监控信息可以通过查询内部表获得

 

<h1>资源列表</h1>
* [OceanBase架构](https://github.com/alibaba/oceanbase/wiki/OceanBase%E6%9E%B6%E6%9E%84%E4%BB%8B%E7%BB%8D)
* [OceanBase安装部署](https://github.com/alibaba/oceanbase/wiki/OceanBase-0.4-%E5%AE%89%E8%A3%85%E6%8C%87%E5%8D%97)
* [OceanBase SQL 用户参考手册](https://github.com/alibaba/oceanbase/wiki/OceanBase-SQL-%E7%94%A8%E6%88%B7%E5%8F%82%E8%80%83%E6%89%8B%E5%86%8C)
* [OceanBase SQL管理员手册](https://github.com/alibaba/oceanbase/wiki/OceanBase-SQL%E7%AE%A1%E7%90%86%E5%91%98%E6%89%8B%E5%86%8C)
* [OceanBase客户端使用指南](https://github.com/alibaba/oceanbase/wiki/OceanBase%E5%AE%A2%E6%88%B7%E7%AB%AF-%E7%94%A8%E6%88%B7%E6%8C%87%E5%8D%97)
* [ChunkServer设计文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/chunkserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [mergeServer设计文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/mergeserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [rootServer设计文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/rootserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [updateServer设计文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/updateserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [运维文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/%E4%BD%BF%E7%94%A8%E8%BF%90%E7%BB%B4)
* [Project Plan](https://github.com/alibaba/oceanbase/wiki/Project-Plan) 


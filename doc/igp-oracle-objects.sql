--任务表
create table IGP_CONF_TASK
(
  TASK_ID           NUMBER not null,
  TASK_DESCRIBE     VARCHAR2(255),
  DEV_ID            NUMBER,
  DEV_PORT          NUMBER,
  PROXY_DEV_ID      NUMBER,
  PROXY_DEV_PORT    NUMBER,
  COLLECT_TYPE      NUMBER,
  COLLECT_PERIOD    NUMBER,
  COLLECTTIMEOUT    NUMBER,
  COLLECT_TIME      NUMBER,
  COLLECT_PATH      CLOB,
  SHELL_TIMEOUT     NUMBER,
  PARSE_TMPID       NUMBER,
  DISTRBUTE_TMPID   NUMBER,
  SUC_DATA_TIME     DATE,
  SUC_DATA_POS      NUMBER,
  ISUSED            NUMBER default 1,
  ISUPDATE          NUMBER default 0,
  MAXCLTTIME        NUMBER default 10,
  SHELL_CMD_PREPARE VARCHAR2(2000),
  SHELL_CMD_FINISH  VARCHAR2(2000),
  COLLECT_TIMEPOS   NUMBER default 0,
  DBDRIVER          VARCHAR2(200),
  DBURL             VARCHAR2(200),
  THREADSLEEPTIME   NUMBER,
  BLOCKEDTIME       NUMBER default 0,
  COLLECTOR_NAME    VARCHAR2(200),
  PARAMRECORD       NUMBER default 0,
  GROUP_ID          NUMBER,
  END_DATA_TIME     DATE,
  PARSERID          NUMBER,
  DISTRIBUTORID     NUMBER,
  REDO_TIME_OFFSET  NUMBER default 60,
  PROB_STARTTIME NUMBER DEFAULT -1 NOT NULL
)

--模板表
create table IGP_CONF_TEMPLET
(
  TMPID        NUMBER not null,
  TMPTYPE      NUMBER,
  TMPNAME      VARCHAR2(100),
  EDITION      VARCHAR2(20),
  TEMPFILENAME VARCHAR2(100)
)

--补采任务表
create table IGP_CONF_RTASK
(
  ID             NUMBER,
  TASKID         NUMBER,
  FILEPATH       CLOB,
  COLLECTTIME    DATE,
  STAMPTIME      DATE,
  COLLECTOR_NAME VARCHAR2(200),
  READOPTTYPE    NUMBER,
  COLLECTDEGRESS NUMBER,
  COLLECTSTATUS  NUMBER,
  CAUSE          CLOB
)

--设备表
create table IGP_CONF_DEVICE
(
  DEV_ID    INTEGER not null,
  DEV_NAME  VARCHAR2(20),
  CITY_ID   NUMBER,
  OMCID     INTEGER,
  VENDOR    CHAR(6),
  HOST_IP   VARCHAR2(30),
  HOST_USER VARCHAR2(20),
  HOST_PWD  VARCHAR2(20),
  HOST_SIGN VARCHAR2(40),
	ENCODE    VARCHAR2(20),
	ZHTIMEZONE NUMBER
)

--用户表
create table IGP_CONF_USER
(
  ID       NUMBER(4) not null,
  USERNAME VARCHAR2(25) not null,
  PASSWORD VARCHAR2(100) not null,
  GROUPID  NUMBER(4)
)

--用户分组表
create table IGP_CONF_USERGROUP
(
  ID          NUMBER(4) not null,
  GROUPNAME   VARCHAR2(25) not null,
  IDS         VARCHAR2(256) not null,
  NOTE        VARCHAR2(500)
)

--厂家表
create table IGP_CONF_VENDOR
(
  ID         NUMBER(4) not null,
  VENDORNAME_CH VARCHAR2(50) not null,
  VENDORNAME_EN VARCHAR2(50)
)

-- 汇总接口表
create table LOG_CLT_INSERT
(
  OMCID           NUMBER,
  CLT_TBNAME      VARCHAR2(30),
  STAMPTIME       DATE,
  VSYSDATE        DATE,
  INSERT_COUNTNUM NUMBER,
  IS_CAL          NUMBER default 0,
  TASKID          NUMBER
)

-- 告警表
create table IGP_DATA_ALARM
(
  ID            NUMBER not null,
  ALARMLEVEL    NUMBER default 1 not null,
  TITLE         VARCHAR2(255) not null,
  SRC           VARCHAR2(255) not null,
  STATUS        NUMBER default 0 not null,
  DESCRIPTION   VARCHAR2(1000),
  OCCUREDTIME   DATE not null,
  PROCESSEDTIME DATE,
  TS            DATE,
  ERRORCODE     NUMBER,
  TASKID        NUMBER,
  SENTTIMES     NUMBER default 0 not null
)

--屏蔽表
CREATE TABLE IGP_CONF_IGNORES 
   (	
   	TASKID NUMBER NOT NULL, 
		PATH VARCHAR2(1000) NOT NULL, 
		ISUSED NUMBER DEFAULT 0 NOT NULL, 
		MODIF_TIME DATE
   );
CREATE UNIQUE INDEX PK_IGP_CONF_IGNORES ON IGP_CONF_IGNORES (TASKID, PATH);

-- IGP日志表
create table IGP_DATA_LOG
(
  LOG_TIME         DATE,
  TASK_ID          NUMBER,
  TASK_DESCRIPTION VARCHAR2(255),
  TASK_TYPE        VARCHAR2(50),
  TASK_STATUS      VARCHAR2(50),
  TASK_DETAIL      VARCHAR2(4000),
  TASK_EXCEPTION   VARCHAR2(4000),
  DATA_TIME        DATE,
  COST_TIME        NUMBER,
  TASK_RESULT      VARCHAR2(50)
)
-- Add comments to the columns 
comment on column IGP_DATA_LOG.LOG_TIME
  is '记录当前日志的时间';
comment on column IGP_DATA_LOG.TASK_ID
  is '任务号';
comment on column IGP_DATA_LOG.TASK_DESCRIPTION
  is '任务描述';
comment on column IGP_DATA_LOG.TASK_TYPE
  is '任务类型，“正常任务”、“补采任务”';
comment on column IGP_DATA_LOG.TASK_STATUS
  is '任务状态，”开始“、”解析“、”入库“、”结束“';
comment on column IGP_DATA_LOG.TASK_DETAIL
  is '详情';
comment on column IGP_DATA_LOG.TASK_EXCEPTION
  is '异常信息';
comment on column IGP_DATA_LOG.DATA_TIME
  is '采集的时间点';
comment on column IGP_DATA_LOG.COST_TIME
  is '目前消耗的时间（秒）';
comment on column IGP_DATA_LOG.TASK_RESULT
  is '采集结果，“成功”、“部分成功”、“失败”';

--序列
create sequence SEQ_IGP_CONF_TASK    start with 1 increment by 1 nocycle;
create sequence SEQ_IGP_CONF_TEMPLET start with 1 increment by 1 nocycle;
create sequence SEQ_IGP_CONF_RTASK   start with 1 increment by 1 nocycle;
create sequence SEQ_IGP_CONF_DEVICE  start with 1 increment by 1 nocycle;
create sequence SEQ_IGP_CONF_USER    start with 1 increment by 1 nocycle;
create sequence SEQ_IGP_CONF_USERGROUP   start with 1 increment by 1 nocycle;
create sequence SEQ_IGP_CONF_VENDOR  start with 1 increment by 1 nocycle;
create sequence SEQ_IGP_DATA_ALARM  start with 1 increment by 1 nocycle;

--初始化用户账号数据
truncate table IGP_CONF_USERGROUP;
insert into IGP_CONF_USERGROUP(id,GROUPNAME,IDS,NOTE) values(1,'管理员','0','管理员对系统有不受限制的完全访问权');
truncate table IGP_CONF_USER;
insert into IGP_CONF_USER(id,USERNAME,PASSWORD,GROUPID) values(1,'igp','427b420550992db0281716393c4b3b84',1);

--初始化厂商数据
truncate table igp_conf_vendor;
insert into igp_conf_vendor(id,vendorname_ch,vendorname_en) values(1,'华为','HuaWei');
insert into igp_conf_vendor(id,vendorname_ch,vendorname_en) values(2,'中兴','ZTE');
insert into igp_conf_vendor(id,vendorname_ch,vendorname_en) values(3,'爱立信','Ericsson');
insert into igp_conf_vendor(id,vendorname_ch,vendorname_en) values(4,'贝尔','Bell');
insert into igp_conf_vendor(id,vendorname_ch,vendorname_en) values(5,'阿朗','Alcatel-Lucent');
insert into igp_conf_vendor(id,vendorname_ch,vendorname_en) values(6,'西门子','Siemens');
insert into igp_conf_vendor(id,vendorname_ch,vendorname_en) values(7,'摩托罗拉','Motorola');
insert into igp_conf_vendor(id,vendorname_ch,vendorname_en) values(8,'思科','Cisco');
insert into igp_conf_vendor(id,vendorname_ch,vendorname_en) values(9,'诺基亚','Nokia');

commit
package task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import templet.TempletBase;
import templet.TempletRecord;
import util.LogMgr;
import util.Util;
import access.AbstractAccessor;
import datalog.DataLogInfo;
import delayprobe.DelayProbeMgr;
import framework.ConstDef;
import framework.Factory;

/**
 * 采集对象类
 * 
 * @author IGP TDT
 * @since 1.0
 * @version 1.0.0 1.0.1 liangww 2012-07-17 公开distTmpRecord，parseTmpRecord获取方法
 */
public class CollectObjInfo implements java.io.Serializable {

	private static final long serialVersionUID = 7786131247745284757L;

	// 采集对象的基本信息
	protected long keyID = 0; // KeyID 等于 taskID，当补采任务的是时候 KeyID 等于 补采表中的第一列

	private int groupId = 0; // 组编号 added by Xumg

	private String describe; // 采集任务的描述

	protected long taskId = 0; // 采集任务编号

	private DevInfo devInfo; // 设备编号

	private int devPort = 0; // 采集设备的端口，某些采集方式需要设置端口

	private DevInfo proxyDevInfo; // 中转设备编号

	private int proxyDevPort = 0; // 中转采集设备的端口，某些采集方式需要设置端口

	private int collectType = 0; // 采集类型

	private int collectTimeOut = 0; // 采集超时(多长时间无数据，认为采集超时间)

	private int collectPeriod = 0; // 采集周期

	private int collectTime = 0; // 采集启动时间

	private int collectTimePos = 0; // 采集时间与当前时间的差值

	protected String collectPath; // 被采数据路径

	private String shellCmdPrepare = ""; // 采集之前的指令

	private String shellCmdFinish = ""; // 采集之后的指令

	private int shellTimeOut; // SHELL指令执行超时时长

	private int parseTmpID = 0; // 解析模板ID

	private int parseTmpType = 0; // 解析模板类型

	private TempletBase parseTemplet; // 模板解析器 PARSE_TEMPLET CLOB FALSE FALSE

	private int disTmpID = 0; // 数据分发ID TYPE INT FALSE FALSE FALSE

	private TempletBase distributeTemplet; // 模板分发器 CLOB FALSE

	private int redoTimeOffset = 0;// 补采粒度偏移量

	private int parserID;// 数据解析器编号

	private int distributorID;// 数据分发器编号

	// 其他信息
	private Timestamp lastCollectTime;// 最后成功数据采集时间

	private int lastCollectPos;// 最后成功采集位置

	protected boolean usedFlag = false;// 判断是否正在被使用

	private int maxReCollectTime = 0;// 最大补采次数

	private int activeTableIndex = -1;// 当前活跃的表

	// 当利用数据库来采集的时候使用
	private String dbDriver = "";// 设备驱动

	private String dbUrl = ""; // 连接方式

	protected Thread threadHandle = null;

	// 为了避免sqlldr阻止线程，在sqlldr之前设置时间。
	// 如果10分钟之内。sqllder 还是未完成的话，强制将线程关闭
	private Timestamp sqlldrTime;

	private int threadSleepTime = 0;// 线程启动后停止多少秒后开始运行

	// By Xumg:sqllder 在下述时间之内未完成则强制结束线程，单位：分钟
	// 如果m_blockedTime值为0，则一直等待
	private int blockedTime;

	private String hostName;

	private Timestamp endDataTime = null; // 数据结束时间 chensj 2010.01.28

	protected Logger log = LogMgr.getInstance().getSystemLogger();

	private String tempTempFileName = null;// 临时模板文件名 liuwx 2010.03.30

	private TempletRecord parseTmpRecord; // 解析模板数据库中对应在记录结构

	private TempletRecord distTmpRecord; // 分发模板数据库中对应在记录结构

	protected String sysName; // 此对象在系统内部的名称，由我们程序在内部进行定义,和外界配置无关

	// chensijiang 2010-08-20
	// 探针开始时间，X分钟开始探，同时在igp_conf_task表增加一字段：PROB_STARTTIME
	// 类型NUMBER，不可为NULL，默认值-1
	protected int probeTime = -1;

	public Map<String, String> filenameMap = new HashMap<String, String>();

	public String spasOmcId = "";

	/**
	 * 此任务的日志信息对象
	 */
	protected final DataLogInfo logInfo = new DataLogInfo();

	public CollectObjInfo(long taskID) {
		taskId = taskID;
		keyID = taskID;
		sysName = String.valueOf(taskId);
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String strName) {
		hostName = strName;
	}

	// 开始采集时间
	public Timestamp startTime = new Timestamp(System.currentTimeMillis());

	// 记录条数
	public int m_nAllRecordCount = 0;

	public Timestamp getSqlldrTime() {
		return sqlldrTime;
	}

	public void setSqlldrTime(Timestamp ts) {
		this.sqlldrTime = ts;
	}

	public Thread getCollectThread() {
		return threadHandle;
	}

	public void setCollectThread(Thread hThreadHandle) {
		threadHandle = hThreadHandle;
	}

	public int getSleepTime() {
		return threadSleepTime;
	}

	public void setThreadSleepTime(int time) {
		threadSleepTime = time;
	}

	public int get_LastCollectPos() {
		return lastCollectPos;
	}

	/*
	 * 组编号
	 */
	public int getGroupId() {
		return groupId;
	}

	public void setGroupId(int id) {
		groupId = id;
	}

	/*
	 * 任务描述
	 */
	public void setDescribe(String TaskDescribe) {
		describe = TaskDescribe;
	}

	/*
	 * 任务描述
	 */
	public String getDescribe() {
		return describe == null ? "" : describe;
	}

	/*
	 * 任务序号
	 */
	public void setTaskID(long nTaskId) {
		taskId = nTaskId;
	}

	/*
	 * 任务序号
	 */
	public long getTaskID() {
		return taskId;
	}

	public void setActiveTableIndex(int index) {
		activeTableIndex = index;
	}

	public int getActiveTableIndex() {
		return activeTableIndex;
	}

	/*
	 * 设备ID
	 */
	public void setDevInfo(DevInfo devInfo) {
		this.devInfo = devInfo;
	}

	/*
	 * 设备ID
	 */
	public DevInfo getDevInfo() {
		return devInfo;
	}

	/*
	 * 设备端口
	 */
	public void setDevPort(int port) {
		devPort = port;
	}

	/*
	 * 设备端口
	 */
	public int getDevPort() {
		return devPort;
	}

	/*
	 * 设置代理设备信息
	 */
	public void setProxyDevInfo(DevInfo devInfo) {
		proxyDevInfo = devInfo;
	}

	/*
	 * 获取代理设备信息
	 */
	public DevInfo getProxyDevInfo() {
		return proxyDevInfo;
	}

	/*
	 * 代理设备端口
	 */
	public void setProxyDevPort(int port) {
		proxyDevPort = port;
	}

	/*
	 * 获取代理设备端口
	 */
	public int getProxyDevPort() {
		return proxyDevPort;
	}

	/*
	 * 设置采集类型
	 */
	public void setCollectType(int type) {
		collectType = type;
	}

	/*
	 * 获取采集类型
	 */
	public int getCollectType() {
		return collectType;
	}

	/*
	 * 设置采集超时时长
	 */
	public void setCollectTimeOut(int timeout) {
		collectTimeOut = timeout;
	}

	/*
	 * 获取采集超时时长
	 */
	public int getCollectTimeOut() {
		return collectTimeOut;
	}

	/**
	 * 设置采集周期
	 */
	public void setPeriod(int period) {
		collectPeriod = period;
	}

	/**
	 * 获取采集周期
	 */
	public int getPeriod() {
		return collectPeriod;
	}

	/**
	 * 设置采集时间
	 */
	public void setCollectTime(int time) {
		collectTime = time;
	}

	/*
	 * 获取采集时间
	 */
	public int getCollectTime() {
		return collectTime;
	}

	/**
	 * 设置采集时间与采集文件差值
	 */
	public void setCollectTimePos(int offset) {
		this.collectTimePos = offset;
	}

	public int getCollectTimePos() {
		return this.collectTimePos;
	}

	/**
	 * 设置采集路径
	 */
	public void setCollectPath(String path) {
		this.collectPath = path;
	}

	/**
	 * 获取采集路径
	 */
	public String getCollectPath() {
		return this.collectPath;
	}

	/*
	 * 设置采集前需要执行的Shell命令或者是SQL语句等
	 */
	public void setShellCmdPrepare(String cmd) {
		this.shellCmdPrepare = cmd;
	}

	/*
	 * 获取采集前需要执行的Shell命令或者是SQL语句等
	 */
	public String getShellCmdPrepare() {
		return this.shellCmdPrepare;
	}

	/*
	 * 设置采集后需要执行的Shell命令或者是SQL语句等
	 */
	public void setShellCmdFinish(String cmd) {
		this.shellCmdFinish = cmd;
	}

	/*
	 * 获取采集后需要执行的Shell命令或者是SQL语句等
	 */
	public String getShellCmdFinish() {
		return this.shellCmdFinish;
	}

	/*
	 * 获取Shell命令或者是SQL语句 执行时的超时时长
	 */
	public int getShellTimeout() {
		return this.shellTimeOut;
	}

	/**
	 * 设置Shell命令或者是SQL语句 执行时的超时时长
	 */
	public void setShellTimeout(int timeout) {
		this.shellTimeOut = timeout;
	}

	/*
	 * 设置解析模板ID
	 */
	public void setParseTmpID(int ParseTmpID) {
		this.parseTmpID = ParseTmpID;
	}

	/*
	 * 获取解析模板ID
	 */
	public int getParseTmpID() {
		return this.parseTmpID;
	}

	/*
	 * 设置解析模板类型
	 */
	public void setParseTmpType(int parseTmpType) {
		this.parseTmpType = parseTmpType;
	}

	/*
	 * 获取解析模板类型
	 */
	public int getParseTmpType() {
		return this.parseTmpType;
	}

	/*
	 * 设置解析模板对象
	 */
	public void setParseTemplet(TempletBase tmpBase) {
		this.parseTemplet = tmpBase;
	}

	/*
	 * 获取解析模板对象
	 */
	public TempletBase getParseTemplet() {
		return this.parseTemplet;
	}

	/*
	 * 设置分发模板对象
	 */
	public void setDistributeTemplet(TempletBase distributeTemplet) {
		this.distributeTemplet = distributeTemplet;
	}

	/*
	 * 获取分发模板对象
	 */
	public TempletBase getDistributeTemplet() {
		return this.distributeTemplet;
	}

	/**
	 * 设置最后成功采集时间
	 */
	public void setLastCollectTime(Timestamp ts) {
		this.lastCollectTime = ts;
	}

	/**
	 * 获取最后成功采集时间
	 */
	public Timestamp getLastCollectTime() {
		return this.lastCollectTime;
	}

	/**
	 * 设置最后成功采集的位置
	 */
	public void setLastCollectPos(int pos) {
		this.lastCollectPos = pos;
	}

	/**
	 * 添加采集位置
	 */
	public void addLastCollectPos(int nAdd) {
		this.lastCollectPos += nAdd;
	}

	/**
	 * 设置当前配置的使用状态
	 */
	public void setUsed(boolean isUsed) {
		this.usedFlag = isUsed;
	}

	/**
	 * 获取当前配置的状态
	 */
	public boolean isUsed() {
		return this.usedFlag;
	}

	/*
	 * 设置重新采集次数
	 */
	public void setMaxReCollectTime(int nMaxReCollectTime) {
		this.maxReCollectTime = nMaxReCollectTime;
	}

	/*
	 * 获取重新采集次数
	 */
	public int getMaxReCollectTime() {
		return this.maxReCollectTime;
	}

	/*
	 * 设置数据库采集时候的数据库连接的驱动
	 */
	public String getDBDriver() {
		return this.dbDriver;
	}

	/*
	 * 获取数据库采集时候的数据库连接的驱动
	 */
	public void setDBDriver(String driver) {
		this.dbDriver = driver;
	}

	/*
	 * 获取数据库采集时候的数据库连接的Url
	 */
	public String getDBUrl() {
		return this.dbUrl;
	}

	/*
	 * 设置数据库采集时候的数据库连接的Url
	 */
	public void setDBUrl(String url) {
		this.dbUrl = url;
	}

	/**
	 * 超时时间
	 */
	public int getBlockedTime() {
		return blockedTime;
	}

	public void setBlockedTime(int t) {
		blockedTime = t;
	}

	public long getKeyID() {
		return keyID;
	}

	public void setKeyID(long KeyID) {
		keyID = KeyID;
	}

	public void buildData(ResultSet rs) throws Exception {
		buildObj(rs);
	}

	public void buildObj(ResultSet rs, Date scantime) throws Exception {
		// log.debug("从igp_conf_task表读到的task_id：" + rs.getLong("TASK_ID") +
		// "，时间："
		// + Util.getDateString(rs.getTimestamp("SUC_DATA_TIME")));
		if (TaskMgr.getInstance().isActive(rs.getLong("TASK_ID"), false)) {
			log.debug(sysName + " is active");
			return;
		}

		buildObj(rs);
		
		if (checkDataTime()) {
			addTaskItem(scantime);
		}
	}

	/*
	 * 判断数据时间范围， 只有当数据结束时间为null，或最后采集数据时间小于等于数据结束时间时， 才把任务加入采集队列。
	 */
	private boolean checkDataTime() {
		if (endDataTime == null) {
			return true;
		}
		if (lastCollectTime.getTime() <= endDataTime.getTime()) {
			return true;
		}
		return false;
	}

	public void buildObj(ResultSet rs) throws Exception {
		this.setGroupId(rs.getInt("GROUP_ID")); // added by xumg
		this.setDescribe(rs.getString("Task_Describe"));
		this.setTaskID(rs.getLong("TASK_ID"));
		DevInfo devInfo = new DevInfo();
		devInfo.setID(rs.getInt("DEV_ID"));
		devInfo.setName(rs.getString("DEV_NAME"));
		devInfo.setIP(rs.getString("HOST_IP"));
		devInfo.setHostUser(rs.getString("HOST_USER"));
		devInfo.setHostPwd(rs.getString("HOST_PWD"));
		devInfo.setHostSign(rs.getString("HOST_SIGN"));
		devInfo.setEncode(rs.getString("ENCODE"));
		devInfo.setOmcID(rs.getInt("OMCID"));
		devInfo.setCityID(rs.getInt("CITY_ID"));// added by xumg
		devInfo.setVendor(rs.getString("vendor"));
		this.setDBDriver(rs.getString("DBDRIVER"));
		this.setDBUrl(rs.getString("DBURL"));
		this.setDevInfo(devInfo);
		this.setDevPort(rs.getInt("DEV_PORT"));
		DevInfo proxdevInfo = new DevInfo();
		proxdevInfo.setID(rs.getInt("PROXY_DEV_ID"));
		proxdevInfo.setName(rs.getString("PROXY_DEV_NAME"));
		proxdevInfo.setIP(rs.getString("PROXY_HOST_IP"));
		proxdevInfo.setHostUser(rs.getString("PROXY_HOST_USER"));
		proxdevInfo.setHostPwd(rs.getString("PROXY_HOST_PWD"));
		proxdevInfo.setHostSign(rs.getString("PROXY_HOST_SIGN"));
		this.setProxyDevInfo(proxdevInfo);

		this.setProxyDevPort(rs.getInt("PROXY_DEV_PORT"));
		this.setCollectType(rs.getInt("COLLECT_TYPE"));
		this.setCollectTimeOut(rs.getInt("CollectTimeOut"));
		this.setPeriod(rs.getInt("COLLECT_PERIOD"));
		this.setCollectTime(0);
		this.setCollectTimePos(rs.getInt("COLLECT_TIMEPOS"));
		this.setShellCmdPrepare(rs.getString("SHELL_CMD_PREPARE"));
		this.setShellCmdFinish(rs.getString("SHELL_CMD_FINISH"));

		this.setParserID(rs.getInt("PARSERID"));
		this.setDistributorID(rs.getInt("DISTRIBUTORID"));

		this.setRedoTimeOffset(rs.getInt("REDO_TIME_OFFSET"));
		this.setProbeTime(rs.getInt("prob_starttime"));

		// 结束的数据时间
		endDataTime = rs.getTimestamp("end_data_time");

		if (Util.isOracle()) // oracle 数据库
		{
			String strPath = ConstDef.ClobParse(rs.getClob("COLLECT_PATH"));
			this.setCollectPath(strPath);
		} else if (Util.isSybase()) // sybase 数据库
		{
			this.setCollectPath(rs.getString("COLLECT_PATH"));
		} else if (Util.isMysql())	// mysql
		{
			this.setCollectPath(rs.getString("COLLECT_PATH"));
		}

		this.setShellTimeout(rs.getInt("SHELL_TIMEOUT"));
		this.setParseTmpID(rs.getInt("PARSE_TMPID"));
		this.setParseTmpType(rs.getInt("TMPTYPE_P"));

		this.parseTmpRecord = new TempletRecord();
		this.parseTmpRecord.setId(rs.getInt("PARSE_TMPID"));
		this.parseTmpRecord.setType(rs.getInt("TMPTYPE_P"));
		this.parseTmpRecord.setName(rs.getString("TMPNAME_P"));
		this.parseTmpRecord.setEdition(rs.getString("EDITION_P"));
		this.parseTmpRecord.setFileName(rs.getString("TEMPFILENAME_P"));

		this.parseTemplet = Factory.createTemplet(parseTmpRecord);
		// 分发模板ID
		this.setDisTmpID(rs.getInt("DISTRBUTE_TMPID"));

		this.distTmpRecord = new TempletRecord();
		this.distTmpRecord.setId(rs.getInt("DISTRBUTE_TMPID"));
		this.distTmpRecord.setType(rs.getInt("TMPTYPE_D"));
		this.distTmpRecord.setName(rs.getString("TMPNAME_D"));
		this.distTmpRecord.setEdition(rs.getString("EDITION_D"));
		this.distTmpRecord.setFileName(rs.getString("TEMPFILENAME_D"));
		// 创建发布模板信息
		// this.distributeTemplet = new DistributeTemplet();
		// this.distributeTemplet.buildTmp(getDisTmpID());
		this.distributeTemplet = Factory.createTemplet(distTmpRecord);

		this.setLastCollectTime(rs.getTimestamp("SUC_DATA_TIME"));

		this.setLastCollectPos(rs.getInt("SUC_DATA_POS"));
		// 设置重采次数
		this.setMaxReCollectTime(rs.getInt("MAXCLTTIME"));
		// 线程启动后睡眠时长(以分钟计)
		this.setThreadSleepTime(rs.getInt("THREADSLEEPTIME"));
		// 超时时间
		// XXX chensj 2012.3.5
		// 这里写死，填0，不让它生效。强行终止任务线程的方式有问题，可能会导致后续时间点的任务时间错乱。今天广东联通二期与新疆电信报了这个问题。
		this.setBlockedTime(0);
	}

	protected void addTaskItem(Date scantime) {
		Calendar cal = Calendar.getInstance();
		int minutes = cal.get(Calendar.MINUTE);
		int hours = cal.get(Calendar.HOUR_OF_DAY);
		int m = cal.get(Calendar.DAY_OF_MONTH);

		boolean bAdd = false;
		int time = -1;

		switch (this.getPeriod()) {
			case ConstDef.COLLECT_PERIOD_FOREVER :
				// 永久线程立即执行
				bAdd = true;
				break;
			case ConstDef.COLLECT_PERIOD_HOUR :
			case ConstDef.COLLECT_PERIOD_4HOUR :
			case ConstDef.COLLECT_PERIOD_HALFDAY :
				time = minutes;
				break;
			case ConstDef.COLLECT_PERIOD_WEEK :
			case ConstDef.COLLECT_PERIOD_DAY :
				// 按天来执行的
				time = hours;
				break;
			case ConstDef.COLLECT_PERIOD_MINUTE_HALFHOUR :
				// 按半个小时执行
				time = minutes % 30;
				break;
			case ConstDef.COLLECT_PERIOD_MINUTE_QUARTER :
				time = minutes % 15;
				break;
			case ConstDef.COLLECT_PERIOD_5MINUTE :
				// 按5分钟执行
				time = minutes % 5;
				break;
			case ConstDef.COLLECT_PERIOD_10MINUTE :
				// 按10分钟执行
				time = minutes % 10;
				break;
			case ConstDef.COLLECT_PERIOD_2MINUTE :
				// 按2分钟执行
				time = minutes % 2;
				break;
			case ConstDef.COLLECT_PERIOD_MONTH :
				// 按月执行
				time = m;
				break;
			case ConstDef.COLLECT_PERIOD_ONE_MINUTE :
				// 一分钟周期
				time = minutes % 1;
				break;
			default :
				log.debug(sysName + " : without period type(" + this.getPeriod() + ").");
				return;
		}

		if (time != -1)
			bAdd = isReady(time, scantime.getTime());

		if (bAdd) {
			startTask();
		}
	}

	public void startTask() {
		log.debug("start task:" + this.getTaskID() + "，时间：" + Util.getDateString(this.getLastCollectTime()));

		//如果会话数已满，则退出，暂不执行此任务
		if(TaskMgr.getInstance().sessionPoolHandler(this) == 3)
			return;
		
		if (TaskMgr.getInstance().addTask(this)) {
			AbstractAccessor accessor = Factory.createAccessor(this);
			this.setCollectThread(accessor);
			accessor.setName("Task ["+this.getTaskID()+"] time ["+Util.getDateString(this.getLastCollectTime())+"]");
			accessor.start();
			log.debug("task started:" + accessor.getTaskInfo().getTaskID() + "，时间：" + Util.getDateString(this.getLastCollectTime()));
		}
	}

	private boolean isReady(int unit, long scanTime) {
		boolean bReturn = false;

		int collectTime = this.getCollectTime();
		collectTime = collectTime > 59 ? 0 : collectTime;
		long startTime = this.getLastCollectTime().getTime() + this.getCollectTimePos() * 60 * 1000;

		if ((scanTime - startTime) >= getPeriodTime()) {
			DelayProbeMgr.getTaskEntrys().remove(getTaskID());
			bReturn = true;
		} else {
			if (unit >= collectTime && startTime < scanTime) {
				DelayProbeMgr.getTaskEntrys().remove(getTaskID());
				bReturn = true;
			}
		}

		if (!bReturn) {
			TaskMgr.getInstance().tempTasks.add(this);
		}
		return bReturn;
	}

	public long getPeriodTime() {
		long time = 0;
		switch (getPeriod()) {
			case ConstDef.COLLECT_PERIOD_FOREVER :
				time = 3600 * 1000;
				break;
			case ConstDef.COLLECT_PERIOD_HOUR :
				time = 3600 * 1000;
				break;
			case ConstDef.COLLECT_PERIOD_4HOUR :
				time = 4 * 3600 * 1000;
				break;
			case ConstDef.COLLECT_PERIOD_DAY :
				time = 24 * 3600 * 1000;
				break;
			case ConstDef.COLLECT_PERIOD_MINUTE_HALFHOUR :
				// 半个小时一次
				time = 30 * 60 * 1000;
				break;
			case ConstDef.COLLECT_PERIOD_MINUTE_QUARTER :
				// 15分钟一次
				time = 15 * 60 * 1000;
				break;
			case ConstDef.COLLECT_PERIOD_5MINUTE :
				// 5分钟一次
				time = 5 * 60 * 1000;
				break;
			case ConstDef.COLLECT_PERIOD_10MINUTE :
				// 10分钟一次
				time = 10 * 60 * 1000;
				break;
			case ConstDef.COLLECT_PERIOD_2MINUTE :
				// 2分钟一次
				time = 2 * 60 * 1000;
				break;
			case ConstDef.COLLECT_PERIOD_ONE_MINUTE :
				// 1分钟一次
				time = 1 * 60 * 1000;
				break;
			case ConstDef.COLLECT_PERIOD_HALFDAY :
				time = 12 * 3600 * 1000;
				break;
			case ConstDef.COLLECT_PERIOD_WEEK : // 一周一次
				time = 7 * 24 * 3600 * 1000;
				break;

			case ConstDef.COLLECT_PERIOD_MONTH : // 一月一次 //add 2011-10-17
				// ,例如当前时间为2011-09-09
				// ,下个月时间为2011-10-09
				Timestamp stamp = this.getLastCollectTime();
				Calendar c = Calendar.getInstance();
				c.setTime(stamp);
				c.add(Calendar.MONTH, +1);
				time = c.getTime().getTime() - this.getLastCollectTime().getTime();
				break;
		}

		return time;
	}

	/** 在任务采集完后执行的操作 更新到下一个时间点 */
	public boolean doAfterCollect() {
		long lastCollectTime = getLastCollectTime().getTime();
		Timestamp timeStamp = new Timestamp(lastCollectTime + getPeriodTime());
		return saveLastCollectTime(timeStamp);
	}

	public boolean saveLastCollectTime(Timestamp time) {
		setLastCollectTime(time);
		setLastCollectPos(0);
		TaskMgr.getInstance().setLastImportTimePos(getTaskID(), time, 0);
		String logStr = sysName + ": update stamptime :" + getDescribe() + "  " + time;
		log.debug(logStr);
		log(DataLogInfo.STATUS_END, logStr);
		return true;
	}

	public Timestamp getEndDataTime() {
		return endDataTime;
	}

	public void setEndDataTime(Timestamp endDataTime) {
		this.endDataTime = endDataTime;
	}

	public String getTempTempFileName() {
		return tempTempFileName;
	}

	public void setTempTempFileName(String tempTempFileName) {
		this.tempTempFileName = tempTempFileName;
	}

	@Override
	public String toString() {
		return sysName;
	}

	public int getParserID() {
		return parserID;
	}

	public void setParserID(int parserID) {
		this.parserID = parserID;
	}

	public int getDistributorID() {
		return distributorID;
	}

	public void setDistributorID(int distributorID) {
		this.distributorID = distributorID;
	}

	public int getRedoTimeOffset() {
		return redoTimeOffset;
	}

	public void setRedoTimeOffset(int redoTimeOffset) {
		this.redoTimeOffset = redoTimeOffset;
	}

	/**
	 * 获取分发模板ID
	 */
	public int getDisTmpID() {
		return disTmpID;
	}

	/**
	 * 设置分发模板ID
	 */
	public void setDisTmpID(int disTmpID) {
		this.disTmpID = disTmpID;
	}

	public String getSysName() {
		return sysName;
	}

	public int getProbeTime() {
		return probeTime;
	}

	public void setProbeTime(int probeTime) {
		this.probeTime = probeTime;
	}

	/**
	 * 获取此任务的日志信息对象，对其修改，调用其addLog()方法，则添加一条数据库日志。<br />
	 * 例如：<br>
	 * collectObjInfo.getLogInfo().setTaskStatus("解析"); <br />
	 * collectObjInfo.getLogInfo().setTaskDetail("正在解析xx文件"); <br />
	 * collectObjInfo.getLogInfo().addLog(); <br />
	 * 
	 * @return 此任务的日志信息对象
	 * @deprecated 改用此类的log()方法
	 */
	@Deprecated
	public DataLogInfo getLogInfo() {
		return logInfo;
	}

	/**
	 * 添加一条数据库日志
	 * 
	 * @param taskStatus
	 *            任务状态，”开始“、”解析“、”入库“、”结束“
	 * @param taskDetail
	 *            详情
	 * @param taskException
	 *            异常信息
	 * @param taskResult
	 *            采集结果，“成功”、“部分成功”、“失败”
	 */
	public void log(String taskStatus, String taskDetail, Throwable taskException, String taskResult) {
		logInfo.setTaskId(getTaskID());
		logInfo.setTaskDescription(getDescribe());
		logInfo.setTaskType((this instanceof RegatherObjInfo) ? DataLogInfo.TYPE_RTASK : DataLogInfo.TYPE_NORMAL);
		logInfo.setTaskStatus(taskStatus);
		logInfo.setTaskDetail(taskDetail);
		logInfo.setTaskException(taskException);
		logInfo.setDataTime(getLastCollectTime());
		logInfo.setCostTime(startTime == null ? 0 : new Date().getTime() - startTime.getTime());
		logInfo.setTaskResult(taskResult);
		logInfo.addLog();
	}

	/**
	 * 添加一条数据库日志
	 * 
	 * @param taskStatus
	 *            任务状态，”开始“、”解析“、”入库“、”结束“
	 * @param taskDetail
	 *            详情
	 */
	public void log(String taskStatus, String taskDetail) {
		log(taskStatus, taskDetail, null);
	}

	/**
	 * 添加一条数据库日志
	 * 
	 * @param taskStatus
	 *            任务状态，”开始“、”解析“、”入库“、”结束“
	 * @param taskDetail
	 *            详情
	 * @param taskException
	 *            异常信息
	 */
	public void log(String taskStatus, String taskDetail, Throwable taskException) {
		log(taskStatus, taskDetail, taskException, null);
	}

	/**
	 * 获取名字
	 * 
	 * @return
	 */
	public String getName() {
		return getTaskID() + "-" + getTaskID();
	}

	/**
	 * 获取全名
	 * 
	 * @return
	 */
	public String getFullName() {
		return getName() + " " + getDescribe();
	}

	public TempletRecord getParseTmpRecord() {
		return parseTmpRecord;
	}

	public void setParseTmpRecord(TempletRecord parseTmpRecord) {
		this.parseTmpRecord = parseTmpRecord;
	}

	public TempletRecord getDistTmpRecord() {
		return distTmpRecord;
	}

	public void setDistTmpRecord(TempletRecord distTmpRecord) {
		this.distTmpRecord = distTmpRecord;
	}

}

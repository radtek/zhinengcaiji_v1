package db.pojo;

import java.sql.Timestamp;
import java.util.Date;

/**
 * IGP_CONF_TASK表的POJO对象。
 * 
 * @author ChenSijiang 2010-5-25
 * @since 1.0
 */
public class Task {

	private long taskId;

	private String taskDescribe = "";

	private int devId;

	private int devPort;

	private int proxyDevId;

	private int proxyDevPort;

	private CollectType collectType;

	private CollectPeriod collectPeriod;

	private int collectTimeout;

	private int collectTime;

	private String collectPath = "";

	private int shellTimeout;

	private int parseTmpId;

	private int distributeTmpId;

	private Timestamp sucDataTime = new Timestamp(new Date().getTime());

	private int sucDataPos;

	private int isUsed;

	private int isUpdate;

	private int maxCltTime;

	private String shellCmdPrepare = "";

	private String shellCmdFinish = "";

	private int collectTimepos;

	private String dbDriver = "";

	private String dbUrl = "";

	private int threadSleepTime;

	private int blockTime;

	private String collectorName = "";

	private int paramRecord;

	private int groupId;

	private Timestamp endDataTime = new Timestamp(new Date().getTime());

	private int parserId;

	private int distributorId;

	private int redoTimeOffset;

	public Task() {
		super();
	}

	public Task(long taskId) {
		super();
		this.taskId = taskId;
	}

	public Task(int taskId, String taskDescribe, int devId, int devPort, int proxyDevId, int proxyDevPort, CollectType collectType,
			CollectPeriod collectPeriod, int collectTimeout, int collectTime, String collectPath, int shellTimeout, int parseTmpId,
			int distributeTmpId, Timestamp sucDataTime, int sucDataPos, int isUsed, int isUpdate, int maxCltTime, String shellCmdPrepare,
			String shellCmdFinish, int collectTimepos, String dbDriver, String dbUrl, int threadSleepTime, int blockTime, String collectorName,
			int paramRecord, int groupId, Timestamp endDataTime, int parserId, int distributorId, int redoTimeOffset) {
		super();
		this.taskId = taskId;
		this.taskDescribe = taskDescribe;
		this.devId = devId;
		this.devPort = devPort;
		this.proxyDevId = proxyDevId;
		this.proxyDevPort = proxyDevPort;
		this.collectType = collectType;
		this.collectPeriod = collectPeriod;
		this.collectTimeout = collectTimeout;
		this.collectTime = collectTime;
		this.collectPath = collectPath;
		this.shellTimeout = shellTimeout;
		this.parseTmpId = parseTmpId;
		this.distributeTmpId = distributeTmpId;
		this.sucDataTime = sucDataTime;
		this.sucDataPos = sucDataPos;
		this.isUsed = isUsed;
		this.isUpdate = isUpdate;
		this.maxCltTime = maxCltTime;
		this.shellCmdPrepare = shellCmdPrepare;
		this.shellCmdFinish = shellCmdFinish;
		this.collectTimepos = collectTimepos;
		this.dbDriver = dbDriver;
		this.dbUrl = dbUrl;
		this.threadSleepTime = threadSleepTime;
		this.blockTime = blockTime;
		this.collectorName = collectorName;
		this.paramRecord = paramRecord;
		this.groupId = groupId;
		this.endDataTime = endDataTime;
		this.parserId = parserId;
		this.distributorId = distributorId;
		this.redoTimeOffset = redoTimeOffset;
	}

	public long getTaskId() {
		return taskId;
	}

	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	public String getTaskDescribe() {
		return taskDescribe;
	}

	public void setTaskDescribe(String taskDescribe) {
		this.taskDescribe = taskDescribe;
	}

	public int getDevId() {
		return devId;
	}

	public void setDevId(int devId) {
		this.devId = devId;
	}

	public int getDevPort() {
		return devPort;
	}

	public void setDevPort(int devPort) {
		this.devPort = devPort;
	}

	public int getProxyDevId() {
		return proxyDevId;
	}

	public void setProxyDevId(int proxyDevId) {
		this.proxyDevId = proxyDevId;
	}

	public int getProxyDevPort() {
		return proxyDevPort;
	}

	public void setProxyDevPort(int proxyDevPort) {
		this.proxyDevPort = proxyDevPort;
	}

	public CollectType getCollectType() {
		return collectType;
	}

	public void setCollectType(CollectType collectType) {
		this.collectType = collectType;
	}

	public CollectPeriod getCollectPeriod() {
		return collectPeriod;
	}

	public void setCollectPeriod(CollectPeriod collectPeriod) {
		this.collectPeriod = collectPeriod;
	}

	public int getCollectTimeout() {
		return collectTimeout;
	}

	public void setCollectTimeout(int collectTimeout) {
		this.collectTimeout = collectTimeout;
	}

	public int getCollectTime() {
		return collectTime;
	}

	public void setCollectTime(int collectTime) {
		this.collectTime = collectTime;
	}

	public String getCollectPath() {
		return collectPath;
	}

	public void setCollectPath(String collectPath) {
		this.collectPath = collectPath;
	}

	public int getShellTimeout() {
		return shellTimeout;
	}

	public void setShellTimeout(int shellTimeout) {
		this.shellTimeout = shellTimeout;
	}

	public int getParseTmpId() {
		return parseTmpId;
	}

	public void setParseTmpId(int parseTmpId) {
		this.parseTmpId = parseTmpId;
	}

	public int getDistributeTmpId() {
		return distributeTmpId;
	}

	public void setDistributeTmpId(int distributeTmpId) {
		this.distributeTmpId = distributeTmpId;
	}

	public Timestamp getSucDataTime() {
		return sucDataTime;
	}

	public void setSucDataTime(Timestamp sucDataTime) {
		this.sucDataTime = sucDataTime;
	}

	public int getSucDataPos() {
		return sucDataPos;
	}

	public void setSucDataPos(int sucDataPos) {
		this.sucDataPos = sucDataPos;
	}

	public int getIsUsed() {
		return isUsed;
	}

	public void setIsUsed(int isUsed) {
		this.isUsed = isUsed;
	}

	public int getIsUpdate() {
		return isUpdate;
	}

	public void setIsUpdate(int isUpdate) {
		this.isUpdate = isUpdate;
	}

	public int getMaxCltTime() {
		return maxCltTime;
	}

	public void setMaxCltTime(int maxCltTime) {
		this.maxCltTime = maxCltTime;
	}

	public String getShellCmdPrepare() {
		return shellCmdPrepare;
	}

	public void setShellCmdPrepare(String shellCmdPrepare) {
		this.shellCmdPrepare = shellCmdPrepare;
	}

	public String getShellCmdFinish() {
		return shellCmdFinish;
	}

	public void setShellCmdFinish(String shellCmdFinish) {
		this.shellCmdFinish = shellCmdFinish;
	}

	public int getCollectTimepos() {
		return collectTimepos;
	}

	public void setCollectTimepos(int collectTimepos) {
		this.collectTimepos = collectTimepos;
	}

	public String getDbDriver() {
		return dbDriver;
	}

	public void setDbDriver(String dbDriver) {
		this.dbDriver = dbDriver;
	}

	public String getDbUrl() {
		return dbUrl;
	}

	public void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}

	public int getThreadSleepTime() {
		return threadSleepTime;
	}

	public void setThreadSleepTime(int threadSleepTime) {
		this.threadSleepTime = threadSleepTime;
	}

	public int getBlockTime() {
		return blockTime;
	}

	public void setBlockTime(int blockTime) {
		this.blockTime = blockTime;
	}

	public String getCollectorName() {
		return collectorName;
	}

	public void setCollectorName(String collectorName) {
		this.collectorName = collectorName;
	}

	public int getParamRecord() {
		return paramRecord;
	}

	public void setParamRecord(int paramRecord) {
		this.paramRecord = paramRecord;
	}

	public int getGroupId() {
		return groupId;
	}

	public void setGroupId(int groupId) {
		this.groupId = groupId;
	}

	public Timestamp getEndDataTime() {
		return endDataTime;
	}

	public void setEndDataTime(Timestamp endDataTime) {
		this.endDataTime = endDataTime;
	}

	public int getParserId() {
		return parserId;
	}

	public void setParserId(int parserId) {
		this.parserId = parserId;
	}

	public int getDistributorId() {
		return distributorId;
	}

	public void setDistributorId(int distributorId) {
		this.distributorId = distributorId;
	}

	public int getRedoTimeOffset() {
		return redoTimeOffset;
	}

	public void setRedoTimeOffset(int redoTimeOffset) {
		this.redoTimeOffset = redoTimeOffset;
	}

}

package db.pojo;

import java.util.Date;

import util.Util;

/**
 * 补采表POJO
 * <P>
 * 对应IGP_CONF_RTASK表一条记录
 * </p>
 * 
 * @author YangJian
 * @since 1.0
 */
public class RTask {

	private long id;

	private long taskID;

	private String filePath;

	private String collectTime;

	private String stampTime;

	private String collectorName;

	private int readoptType;

	private int collectDegress;

	private int collectStatus;

	// 预计开始时间
	private String preStartTime;

	// 补采原因
	private String cause;

	public String getCause() {
		return cause;
	}

	public void setCause(String cause) {
		this.cause = cause;
	}

	public RTask() {
		super();
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getTaskID() {
		return taskID;
	}

	public void setTaskID(long taskID) {
		this.taskID = taskID;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getCollectTime() {
		return collectTime;
	}

	public void setCollectTime(String collectTime) {
		this.collectTime = collectTime;
	}

	public String getStampTime() {
		return stampTime;
	}

	public void setStampTime(String stampTime) {
		if (stampTime == null) {
			this.stampTime = Util.getDateString(new Date());
			return;
		}
		this.stampTime = stampTime;
	}

	public String getCollectorName() {
		return collectorName;
	}

	public void setCollectorName(String collectorName) {
		this.collectorName = collectorName;
	}

	public int getReadoptType() {
		return readoptType;
	}

	public void setReadoptType(int readoptType) {
		this.readoptType = readoptType;
	}

	public int getCollectDegress() {
		return collectDegress;
	}

	public void setCollectDegress(int collectDegress) {
		this.collectDegress = collectDegress;
	}

	public int getCollectStatus() {
		return collectStatus;
	}

	public void setCollectStatus(int collectStatus) {
		this.collectStatus = collectStatus;
	}

	public String getPreStartTime() {
		return preStartTime;
	}

	public void setPreStartTime(String preStartTime) {
		this.preStartTime = preStartTime;
	}

}

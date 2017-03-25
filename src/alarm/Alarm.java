package alarm;

import java.util.Date;

/**
 * 告警实体类
 * 
 * @author ltp Apr 20, 2010
 * @since 3.0
 */
public class Alarm {

	private long id;

	private byte alarmLevel; // 告警级别:

	// 1:不确定告警，2:警告，3:轻微告警，4:主要告警，5:严重告警/重大告警,默认为1
	private String title;

	private String source;

	private byte status; // 告警状态(指记录状态): 0为新告警(需要发送)，1为已发送成功告警,-1为发送失败告警,

	// -2为放弃发送（经过多次发送无效后）
	private int sentTimes; // 已发送次数,默认为0,每次发送失败后累加1

	private String description;

	private int errorCode;

	private long taskID; // 任务编号

	private Date occuredTime;

	private Date processedTime;

	public Alarm() {
		super();
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = removeNoise(description, 1000);
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public byte getAlarmLevel() {
		return alarmLevel;
	}

	public void setAlarmLevel(byte alarmLevel) {
		this.alarmLevel = alarmLevel;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = removeNoise(title, 255);
	}

	public Date getProcessedTime() {
		return processedTime;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = removeNoise(source, 255);
	}

	public byte getStatus() {
		return status;
	}

	public void setStatus(byte status) {
		this.status = status;
	}

	public Date getOccuredTime() {
		return occuredTime;
	}

	public void setOccuredTime(Date occuredTime) {
		this.occuredTime = occuredTime;
	}

	public void setProcessedTime(Date processedTime) {
		this.processedTime = processedTime;
	}

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public long getTaskID() {
		return taskID;
	}

	public void setTaskID(long taskID) {
		this.taskID = taskID;
	}

	public int getSentTimes() {
		return sentTimes;
	}

	public void setSentTimes(int sentTimes) {
		this.sentTimes = sentTimes;
	}

	/*
	 * 这主要是为数据采集记录下相关信息,length为截取后的长度
	 */
	private String removeNoise(String value, int length) {
		String reVal = null;
		if (value != null) {
			int len = value.length();
			if (len > length) {
				value = value.substring(0, 10) + "..." + value.substring(len - (length - 13), len);
			}
			reVal = value.replaceAll("[']+", "''");
		}
		return reVal;
	}

	public static void main(String[] args) {
		Alarm a = new Alarm();
		String str = a.removeNoise("select * from aa where time = to_date('2010-05-21 19:00:00','YYYY-MM-DD HH24:MI:SS')", 20);
		System.out.println(str);
		System.out.println(str.length());

	}

}

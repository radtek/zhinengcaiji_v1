package db.pojo;

import java.util.Date;

/**
 * 和汇总程序衔接的POJO
 * <P>
 * 对应LOG_CLT_INSERT表一条记录
 * </p>
 * 
 * @author YangJian
 * @since 1.0
 */
public class LogCltInsert {

	private int taskID; // 任务编号

	private int omcID; // omc id

	private String tbName; // 插入的clt表名

	private Date stampTime; // 采集的数据时间点

	private Date vSysDate; // 记录入库时间

	private int count; // 插入clt表中的记录条数

	private byte calFlag = 0; // 是否已经汇总，默认0为没汇总，1为已经汇总

	public LogCltInsert() {
		super();
	}

	public int getTaskID() {
		return taskID;
	}

	public void setTaskID(int taskID) {
		this.taskID = taskID;
	}

	public int getOmcID() {
		return omcID;
	}

	public void setOmcID(int omcID) {
		this.omcID = omcID;
	}

	public String getTbName() {
		return tbName;
	}

	public void setTbName(String tbName) {
		this.tbName = tbName;
	}

	public Date getStampTime() {
		return stampTime;
	}

	public void setStampTime(Date stampTime) {
		this.stampTime = stampTime;
	}

	public Date getVSysDate() {
		return vSysDate;
	}

	public void setVSysDate(Date sysDate) {
		vSysDate = sysDate;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public byte getCalFlag() {
		return calFlag;
	}

	public void setCalFlag(byte calFlag) {
		this.calFlag = calFlag;
	}

}

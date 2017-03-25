package store.sqlldrManage.bean;

import java.util.Date;

public class SqlldrPro {

	/**
	 * 进程实例
	 */
	public Process pro;
	
	/**
	 * 进程开始时间
	 */
	public Date startTime;
	
	/**
	 * 命令
	 */
	public String cmd;
	
	/**
	 * 任务id
	 */
	public long taskID;
	
	/**
	 * 当前进程状态 1:alive 0:dead
	 */
	public int status;

	public Process getPro() {
		return pro;
	}

	public void setPro(Process pro) {
		this.pro = pro;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public String getCmd() {
		return cmd;
	}

	public void setCmd(String cmd) {
		this.cmd = cmd;
	}

	public long getTaskID() {
		return taskID;
	}

	public void setTaskID(long taskID) {
		this.taskID = taskID;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

}

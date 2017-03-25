package db.pojo;

public class CollectLog {

	private String logTime;               // 记录当前日志的时间

	private int taskId;                   // 任务号

	private String taskDescription;       // 任务描述

	private String taskType;              // 任务类型

	private String taskStatus;            // 任务状态

	private String dataTime;              // 采集的时间点

	private int costTime;                 // 目前消耗的时间（秒）

	private String taskResult;            // 采集结果

	private String taskDetail;            // 详情

	private String taskException;         // 异常信息

	private String dataEndTime;           // 提供条件查询的结束时间

	public String getLogTime() {
		return logTime;
	}

	public void setLogTime(String logTime) {
		this.logTime = logTime;
	}

	public int getTaskId() {
		return taskId;
	}

	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}

	public String getTaskDescription() {
		return taskDescription;
	}

	public void setTaskDescription(String taskDescription) {
		this.taskDescription = taskDescription;
	}

	public String getTaskType() {
		return taskType;
	}

	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}

	public String getTaskStatus() {
		return taskStatus;
	}

	public void setTaskStatus(String taskStatus) {
		this.taskStatus = taskStatus;
	}

	public String getDataTime() {
		return dataTime;
	}

	public void setDataTime(String dataTime) {
		this.dataTime = dataTime;
	}

	public int getCostTime() {
		return costTime;
	}

	public void setCostTime(int costTime) {
		this.costTime = costTime;
	}

	public String getTaskResult() {
		return taskResult;
	}

	public void setTaskResult(String taskResult) {
		this.taskResult = taskResult;
	}

	public String getTaskDetail() {
		return taskDetail;
	}

	public void setTaskDetail(String taskDetail) {
		this.taskDetail = taskDetail;
	}

	public String getTaskException() {
		return taskException;
	}

	public void setTaskException(String taskException) {
		this.taskException = taskException;
	}

	public String getDataEndTime() {
		return dataEndTime;
	}

	public void setDataEndTime(String dataEndTime) {
		this.dataEndTime = dataEndTime;
	}

}

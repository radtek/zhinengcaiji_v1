package datalog;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.Date;

/**
 * 描述igp_data_log表中的信息，用于记录数据库日志。
 * 
 * @author ChenSijiang 2010-07-23
 * @since 1.1
 */
public class DataLogInfo implements Cloneable {

	private Timestamp logTime;

	private long taskId;

	private String taskDescription;

	private String taskType;

	private String taskStatus;

	private String taskDetail;

	private String taskException;

	private Timestamp dataTime;

	private long costTime;

	private String taskResult;

	public static final String TYPE_NORMAL = "正常任务";

	public static final String TYPE_RTASK = "补采任务";

	public static final String STATUS_START = "开始";

	public static final String STATUS_PARSE = "解析";

	public static final String STATUS_DIST = "入库";

	public static final String STATUS_END = "结束";

	public static final String RESULT_OK = "成功";

	public static final String RESULT_POK = "部分成功";

	public static final String RESULT_FAIL = "失败";

	/**
	 * 构造方法
	 */
	public DataLogInfo() {
		super();
	}

	/**
	 * 构造方法
	 * 
	 * @param taskId
	 *            任务号
	 * @param taskDescription
	 *            任务描述
	 * @param taskType
	 *            任务类型，“正常任务”、“补采任务”
	 * @param taskStatus
	 *            任务状态，”开始“、”解析“、”入库“、”结束“
	 * @param taskDetail
	 *            详情
	 * @param taskException
	 *            异常信息
	 * @param dataTime
	 *            采集的时间点
	 * @param costTime
	 *            目前消耗的时间。注意：此处记录毫秒数，但提交到igp_data_log表时，是秒数，以便查看
	 * @param taskResult
	 *            采集结果，“成功”、“部分成功”、“失败”
	 */
	public DataLogInfo(int taskId, String taskDescription, String taskType, String taskStatus, String taskDetail, String taskException,
			Timestamp dataTime, long costTime, String taskResult) {
		this(null, taskId, taskDescription, taskType, taskStatus, taskDetail, taskException, dataTime, costTime, taskResult);
	}

	/**
	 * 构造方法
	 * 
	 * @param logTime
	 *            记录当前日志的时间
	 * @param taskId
	 *            任务号
	 * @param taskDescription
	 *            任务描述
	 * @param taskType
	 *            任务类型，“正常任务”、“补采任务”
	 * @param taskStatus
	 *            任务状态，”开始“、”解析“、”入库“、”结束“
	 * @param taskDetail
	 *            详情
	 * @param taskException
	 *            异常信息
	 * @param dataTime
	 *            采集的时间点
	 * @param costTime
	 *            目前消耗的时间。注意：此处记录毫秒数，但提交到igp_data_log表时，是秒数，以便查看
	 * @param taskResult
	 *            采集结果，“成功”、“部分成功”、“失败”
	 */
	public DataLogInfo(Timestamp logTime, int taskId, String taskDescription, String taskType, String taskStatus, String taskDetail,
			String taskException, Timestamp dataTime, long costTime, String taskResult) {
		super();
		this.logTime = logTime;
		this.taskId = taskId;
		this.taskDescription = taskDescription;
		this.taskType = taskType;
		this.taskStatus = taskStatus;
		this.taskDetail = taskDetail;
		this.taskException = taskException;
		this.dataTime = dataTime;
		this.costTime = costTime;
		this.taskResult = taskResult;
	}

	/**
	 * 记录当前日志的时间
	 * 
	 * @return
	 */
	public Timestamp getLogTime() {
		return logTime;
	}

	/**
	 * 记录当前日志的时间
	 * 
	 * @param logTime
	 */
	public void setLogTime(Timestamp logTime) {
		this.logTime = logTime;
	}

	/**
	 * 记录当前日志的时间
	 * 
	 * @param logTime
	 */
	public void setLogTime(Date logTime) {
		this.logTime = (logTime != null ? new Timestamp(logTime.getTime()) : null);
	}

	/**
	 * 任务号
	 * 
	 * @return
	 */
	public long getTaskId() {
		return taskId;
	}

	/**
	 * 任务号
	 * 
	 * @param taskId
	 */
	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	/**
	 * 任务描述
	 * 
	 * @return
	 */
	public String getTaskDescription() {
		return taskDescription == null ? "" : taskDescription;
	}

	/**
	 * 任务描述
	 * 
	 * @param taskDescription
	 */
	public void setTaskDescription(String taskDescription) {
		this.taskDescription = taskDescription;
	}

	/**
	 * 任务类型，“正常任务”、“补采任务”
	 * 
	 * @return
	 */
	public String getTaskType() {
		return taskType == null ? "" : taskType;
	}

	/**
	 * 任务类型，“正常任务”、“补采任务”
	 * 
	 * @param taskType
	 */
	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}

	/**
	 * 任务状态，”开始“、”解析“、”入库“、”结束“
	 * 
	 * @return
	 */
	public String getTaskStatus() {
		return taskStatus == null ? "" : taskStatus;
	}

	/**
	 * 任务状态，”开始“、”解析“、”入库“、”结束“
	 * 
	 * @param taskStatus
	 */
	public void setTaskStatus(String taskStatus) {
		this.taskStatus = taskStatus;
	}

	/**
	 * 详情
	 * 
	 * @return
	 */
	public String getTaskDetail() {
		return taskDetail == null ? "" : taskDetail;
	}

	/**
	 * 详情
	 * 
	 * @param taskDetail
	 */
	public void setTaskDetail(String taskDetail) {
		this.taskDetail = taskDetail;
	}

	/**
	 * 异常信息
	 * 
	 * @return
	 */
	public String getTaskException() {
		return taskException == null ? "" : taskException;
	}

	/**
	 * 异常信息
	 * 
	 * @param taskException
	 */
	public void setTaskException(String taskException) {
		this.taskException = taskException;
	}

	/**
	 * 异常信息
	 * 
	 * @param taskException
	 */
	public void setTaskException(Throwable taskException) {
		if (taskException == null) {
			this.taskException = "";
		} else {
			StringWriter writer = new StringWriter();
			taskException.printStackTrace(new PrintWriter(writer));
			writer.flush();
			try {
				writer.close();
			} catch (IOException e) {
			}
			this.taskException = writer.toString();
		}
	}

	/**
	 * 采集的时间点
	 * 
	 * @return
	 */
	public Timestamp getDataTime() {
		return dataTime;
	}

	/**
	 * 采集的时间点
	 * 
	 * @param dataTime
	 */
	public void setDataTime(Timestamp dataTime) {
		this.dataTime = dataTime;
	}

	/**
	 * 采集的时间点
	 * 
	 * @param dataTime
	 */
	public void setDataTime(Date dataTime) {
		this.dataTime = (dataTime != null ? new Timestamp(dataTime.getTime()) : null);
	}

	/**
	 * 目前消耗的时间。注意：此处记录毫秒数，但提交到igp_data_log表时，是秒数，以便查看
	 * 
	 * @return
	 */
	public long getCostTime() {
		return costTime;
	}

	/**
	 * 目前消耗的时间。注意：此处记录毫秒数，但提交到igp_data_log表时，是秒数，以便查看
	 * 
	 * @param costTime
	 */
	public void setCostTime(long costTime) {
		this.costTime = costTime;
	}

	/**
	 * 采集结果，“成功”、“部分成功”、“失败”
	 * 
	 * @return
	 */
	public String getTaskResult() {
		return taskResult == null ? "" : taskResult;
	}

	/**
	 * 采集结果，“成功”、“部分成功”、“失败”
	 * 
	 * @param taskResult
	 */
	public void setTaskResult(String taskResult) {
		this.taskResult = taskResult;
	}

	/**
	 * 将此日志的副本添加到数据库
	 */
	public void addLog() {
		try {
			DataLogMgr.getInstance().addLog((DataLogInfo) clone());
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
	}

}

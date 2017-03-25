package db.pojo;

import java.util.ArrayList;
import java.util.List;

/**
 * 任务生命周期信息
 * 
 * @author ChenSijiang 2010-10-22
 * @since 1.1
 */
public class TaskLifeCycleInfo extends Task {

	private String costTime; // 运行时间

	private String dataTime; // 当前运行的数据时间，就是suc_data_time

	private List<RtaskLifeCycleInfo> reclts = new ArrayList<RtaskLifeCycleInfo>();// 本采集任务的所有补采任务

	private int recltsCount; // 补采任务的数量

	public TaskLifeCycleInfo() {
		super();
	}

	public String getCostTime() {
		return costTime;
	}

	public void setCostTime(String costTime) {
		this.costTime = costTime;
	}

	public String getDataTime() {
		return dataTime;
	}

	public void setDataTime(String dataTime) {
		this.dataTime = dataTime;
	}

	public List<RtaskLifeCycleInfo> getReclts() {
		return reclts;
	}

	public int getRecltsCount() {
		recltsCount = reclts.size();
		return recltsCount;
	}
}

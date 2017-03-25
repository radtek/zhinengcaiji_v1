package db.pojo;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;

import task.RegatherObjInfo;
import task.RegatherStatistics;
import util.Util;
import db.dao.TaskDAO;

public class RtaskLifeCycleInfo extends RTask {

	private String costTime = ""; // 运行时间

	private String recltType; // 补采类型，手动或自动

	private String recltStatus;// 补采状态

	private String shortPath; // 用于在界面显示短的采集路径

	private String startTime; // 预计开始采集时间

	public RtaskLifeCycleInfo(RTask rtask) {
		super();
		setId(rtask.getId());
		setTaskID(rtask.getTaskID());
		setFilePath(rtask.getFilePath().replace("\n", "").replace("\r", ""));
		setCollectTime(rtask.getCollectTime());
		setStampTime(rtask.getStampTime());
		setReadoptType(rtask.getReadoptType());
		setCollectStatus(rtask.getCollectStatus());
		setCause(rtask.getCause());
	}

	public RtaskLifeCycleInfo() {
		super();
	}

	public String getCostTime() {
		return costTime;
	}

	public void setCostTime(String costTime) {
		this.costTime = costTime;
	}

	public String getRecltType() {
		if (getReadoptType() == 0) {
			recltType = "自动添加";
		} else {
			recltType = "手动添加";
		}
		return recltType;
	}

	public String getRecltStatus() {
		switch (getCollectStatus()) {
			case 0 :
				recltStatus = "待运行";
				break;
			case -1 :
				recltStatus = "已达最大补采次数";
				break;
			case 3 :
				recltStatus = "已完成";
				break;
			default :
				recltStatus = "其它(" + getCollectStatus() + ")";
				break;
		}
		return recltStatus;
	}

	public String getShortPath() {
		if (Util.isNotNull(getFilePath())) {
			if (getFilePath().length() > 33) {
				shortPath = getFilePath().substring(0, 30) + "...";
			} else {
				shortPath = getFilePath();
			}
		} else {
			shortPath = "";
		}
		return shortPath;
	}

	public String getRcltTimes() {
		if (getCostTime().equals("未运行")) {
			return "未运行";
		}
		if (getCollectStatus() == 3) {
			return getRecltStatus();
		}
		RegatherObjInfo rinfo = new RegatherObjInfo(getId() + 10000000, getTaskID());
		rinfo.setCollectPath(getFilePath());
		try {
			rinfo.setLastCollectTime(new Timestamp(Util.getDate1(getCollectTime()).getTime()));
		} catch (ParseException e) {
		}
		int recltTimes = RegatherStatistics.getInstance().getRecltTimes(rinfo);

		return "第" + (recltTimes - 1) + "次";
	}

	/**
	 * 采集次数
	 * 
	 * @return
	 */
	public String getStartTime() {
		RegatherObjInfo rinfo = new RegatherObjInfo(getId() + 10000000, getTaskID());
		rinfo.setCollectPath(getFilePath());
		try {
			rinfo.setLastCollectTime(new Timestamp(Util.getDate1(getCollectTime()).getTime()));
		} catch (ParseException e) {
		}
		int recltTimes = RegatherStatistics.getInstance().getRecltTimes(rinfo);

		try {
			long time = Util.getDate1(getStampTime()).getTime() + new TaskDAO().getById(getTaskID()).getRedoTimeOffset() * recltTimes * 60 * 1000;
			startTime = Util.getDateString(new Date(time));
		} catch (ParseException e) {

		}
		return startTime;
	}

}

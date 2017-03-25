package parser.hw.am;

import java.util.Date;

/**
 * 原始采集告警类 RawDataEntry
 * 
 * @version 1.0.0 1.0.1 liangww 2012-06-27 增加alarmText属性<br>
 */
public class RawDataEntry {

	String alarmSeq;// 告警流水号。

	String netSeq;// 网络流水号。

	String neFlag;// 网元标识

	String neName;// 网元名称

	String neType;// 网元类型。

	String alarmID;// 告警ID

	String alarmName;// 告警名称。

	String alarmKind;// 告警种类。

	String alarmLevel;// 告警级别。

	String alarmStatus;// 告警状态。

	String alarmType;// 告警类型。

	Date happenTime;// 发生时间。 oriAlarm.alarmTime

	Date restoreTime;// 恢复时间。

	String locationInfo;// 定位信息。

	String objType;// 对象类型。

	String objFdn;// 对象标识

	String objName;// 对象名称

	// liangww add 2012-06-27
	String alarmText = null; // 告警内容

	//
	String bscId = null;		// rncid==bscid

	String btsId = null;

	String cellId = null;

	public String getAlarmSeq() {
		return alarmSeq;
	}

	public void setAlarmSeq(String alarmSeq) {
		this.alarmSeq = alarmSeq;
	}

	public String getNetSeq() {
		return netSeq;
	}

	public void setNetSeq(String netSeq) {
		this.netSeq = netSeq;
	}

	public String getNeFlag() {
		return neFlag;
	}

	public void setNeFlag(String neFlag) {
		this.neFlag = neFlag;
	}

	public String getNeName() {
		return neName;
	}

	public void setNeName(String neName) {
		this.neName = neName;
	}

	public String getNeType() {
		return neType;
	}

	public void setNeType(String neType) {
		this.neType = neType;
	}

	public String getAlarmID() {
		return alarmID;
	}

	public void setAlarmID(String alarmID) {
		this.alarmID = alarmID;
	}

	public String getAlarmName() {
		return alarmName;
	}

	public void setAlarmName(String alarmName) {
		this.alarmName = alarmName;
	}

	public String getAlarmKind() {
		return alarmKind;
	}

	public void setAlarmKind(String alarmKind) {
		this.alarmKind = alarmKind;
	}

	public String getAlarmLevel() {
		return alarmLevel;
	}

	public void setAlarmLevel(String alarmLevel) {
		this.alarmLevel = alarmLevel;
	}

	public String getAlarmStatus() {
		return alarmStatus;
	}

	public void setAlarmStatus(String alarmStatus) {
		this.alarmStatus = alarmStatus;
	}

	public String getAlarmType() {
		return alarmType;
	}

	public void setAlarmType(String alarmType) {
		this.alarmType = alarmType;
	}

	public Date getHappenTime() {
		return happenTime;
	}

	public void setHappenTime(Date happenTime) {
		this.happenTime = happenTime;
	}

	public Date getRestoreTime() {
		return restoreTime;
	}

	public void setRestoreTime(Date restoreTime) {
		this.restoreTime = restoreTime;
	}

	public String getLocationInfo() {
		return locationInfo;
	}

	public void setLocationInfo(String locationInfo) {
		this.locationInfo = locationInfo;
	}

	public String getObjType() {
		return objType;
	}

	public void setObjType(String objType) {
		this.objType = objType;
	}

	public String getObjFdn() {
		return objFdn;
	}

	public void setObjFdn(String objFdn) {
		this.objFdn = objFdn;
	}

	public String getObjName() {
		return objName;
	}

	public void setObjName(String objName) {
		this.objName = objName;
	}

	// @Override
	// public String toString() {
	// return "RawDataEntry [alarmID=" + alarmID + ", alarmKind=" + alarmKind
	// + ", alarmLevel=" + alarmLevel + ", alarmName=" + alarmName
	// + ", alarmSeq=" + alarmSeq + ", alarmStatus=" + alarmStatus
	// + ", alarmType=" + alarmType + ", happenTime=" + happenTime
	// + ", locationInfo=" + locationInfo + ", neFlag=" + neFlag
	// + ", neName=" + neName + ", neType=" + neType + ", netSeq="
	// + netSeq + ", objType=" + objType + ", restoreTime="
	// + restoreTime + "]";
	// }

	/**
	 * 是否清除告警
	 * 
	 * @return
	 */
	public boolean isCleanAlarm() {
		return alarmStatus != null && this.alarmStatus.indexOf("已恢复") != -1;
	}

	public String getAlarmText() {
		return alarmText;
	}

	public void setAlarmText(String alarmText) {
		this.alarmText = alarmText;
	}

	public String getBscId() {
		return bscId;
	}

	public void setBscId(String bscId) {
		this.bscId = bscId;
	}

	public String getBtsId() {
		return btsId;
	}

	public void setBtsId(String btsId) {
		this.btsId = btsId;
	}

	public String getCellId() {
		return cellId;
	}

	public void setCellId(String cellId) {
		this.cellId = cellId;
	}

}

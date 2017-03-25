package parser.dzlgzxt;

import java.sql.Timestamp;

/**
 * 软件管理工单内容字段。
 * 
 * @author ChenSijiang 2012-5-25
 */
class SoftMgrFormContent {

	/* 工单编号。 */
	String formId;

	/* 派发时间 */
	Timestamp sendDate;

	/* 升级开始时间 */
	Timestamp updateStartTime;

	/* 升级结束时间 */
	Timestamp updateEndTime;

	/* 网络类型 */
	String netType;

	/* 升级类型 */
	String updateType;

	/* 升级的对象节点 */
	String updateElement;

	/* 设备类型 */
	String deviceType;

	/* 厂商名称 */
	String deviceVendor;

	/* 软/硬件申请前版本 */
	String preVersion;

	/* 新软件补丁号 */
	String patchVersion;

	/* 升级后版本 */
	String afterVersion;

	@Override
	public String toString() {
		return "SoftMgrFormContent [formId=" + formId + ", sendDate=" + sendDate + ", updateStartTime=" + updateStartTime + ", updateEndTime="
				+ updateEndTime + ", netType=" + netType + ", updateType=" + updateType + ", updateElement=" + updateElement + ", deviceType="
				+ deviceType + ", deviceVendor=" + deviceVendor + ", preVersion=" + preVersion + ", patchVersion=" + patchVersion + ", afterVersion="
				+ afterVersion + "]";
	}

}

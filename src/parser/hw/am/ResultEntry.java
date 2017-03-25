package parser.hw.am;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import util.Util;
import cn.uway.alarmbox.db.Alarm;
import cn.uway.alarmbox.protocol.monitor.OriAlarm;
import cn.uway.alarmbox.protocol.monitor.OriAlarmV3;

/**
 * 最终要输出的条目。
 * 
 * @author ChenSijiang
 * @version 1.0 1.0.1 liangww 2012-05-24 toOriAlarm增加设置alarm_sub_type为alarmIntId<br>
 *          1.0.2 liangww 2012-07-26 增加中兴wcdma getZteWcdmaResultEntry<br>
 */
public class ResultEntry {

	public static final int DEFAULT_ALARM_TYPE = 2;// 硬件告警。

	public static final int NETYPE_W = 2;

	public static final int NETYPE_G = 1;

	public static final int AII_COMMUNICATION = 1101;// 通信告警

	public static final int AII_HANDLED = 1102;// 处理告警

	public static final int AII_ENVIRONMENT = 1103;// 环境告警

	public static final int AII_SERVICE_QUALITY = 1104;// 服务质量告警

	public static final int AII_DEVICE = 1105;// 设备告警

	// 华为
	private static final Map<String, Integer> HW_ALARM_TYPE_MAPPING = new HashMap<String, Integer>();

	private static final Map<String, Integer> HW_ALARM_STATE_MAPPING = new HashMap<String, Integer>();

	private static final Map<String, Integer> HW_ALARM_GRADE_MAPPING = new HashMap<String, Integer>();

	private static final Map<String, Integer> HW_ALARM_INT_ID_MAPPING = new HashMap<String, Integer>();

	// 中兴
	private static final Map<String, Integer> ZTE_WCDMA_ALARM_GRADE_MAPPING = new HashMap<String, Integer>();

	private static final Map<String, Integer> ZTE_WCDMA_ALARM_INT_ID_MAPPING = new HashMap<String, Integer>();

	static {
		// 华为
		HW_ALARM_INT_ID_MAPPING.put("环境系统", AII_ENVIRONMENT);
		HW_ALARM_INT_ID_MAPPING.put("处理出错", AII_HANDLED);
		HW_ALARM_INT_ID_MAPPING.put("电源系统", AII_DEVICE);
		HW_ALARM_INT_ID_MAPPING.put("硬件系统", AII_DEVICE);
		HW_ALARM_INT_ID_MAPPING.put("软件系统", AII_DEVICE);
		HW_ALARM_INT_ID_MAPPING.put("信令系统", AII_COMMUNICATION);
		HW_ALARM_INT_ID_MAPPING.put("中继系统", AII_COMMUNICATION);
		HW_ALARM_INT_ID_MAPPING.put("运行系统", AII_COMMUNICATION);
		HW_ALARM_INT_ID_MAPPING.put("通讯系统", AII_COMMUNICATION);
		HW_ALARM_INT_ID_MAPPING.put("业务质量", AII_SERVICE_QUALITY);
		HW_ALARM_INT_ID_MAPPING.put("网管内部", AII_SERVICE_QUALITY);
		HW_ALARM_INT_ID_MAPPING.put("完整性违背", AII_SERVICE_QUALITY);
		HW_ALARM_INT_ID_MAPPING.put("操作违背", AII_SERVICE_QUALITY);
		HW_ALARM_INT_ID_MAPPING.put("物理违背", AII_SERVICE_QUALITY);
		HW_ALARM_INT_ID_MAPPING.put("安全服务或机制违背", AII_SERVICE_QUALITY);
		HW_ALARM_INT_ID_MAPPING.put("时间域违背", AII_SERVICE_QUALITY);

		HW_ALARM_TYPE_MAPPING.put("电源系统", 2);
		HW_ALARM_TYPE_MAPPING.put("环境系统", 2);
		HW_ALARM_TYPE_MAPPING.put("信令系统", 1);
		HW_ALARM_TYPE_MAPPING.put("中继系统", 1);
		HW_ALARM_TYPE_MAPPING.put("硬件系统", 3);
		HW_ALARM_TYPE_MAPPING.put("软件系统", 4);
		HW_ALARM_TYPE_MAPPING.put("运行系统", 4);
		HW_ALARM_TYPE_MAPPING.put("通讯系统", 1);
		HW_ALARM_TYPE_MAPPING.put("业务质量", 5);
		HW_ALARM_TYPE_MAPPING.put("处理出错", 4);
		HW_ALARM_TYPE_MAPPING.put("网管内部", 4);
		HW_ALARM_TYPE_MAPPING.put("完整性违背", 4);
		HW_ALARM_TYPE_MAPPING.put("操作违背", 4);
		HW_ALARM_TYPE_MAPPING.put("物理违背", 4);
		HW_ALARM_TYPE_MAPPING.put("安全服务或机制违背", 4);
		HW_ALARM_TYPE_MAPPING.put("时间域违背", 4);

		HW_ALARM_STATE_MAPPING.put("未确认&已恢复", 0);
		HW_ALARM_STATE_MAPPING.put("已确认&已恢复", 0);
		HW_ALARM_STATE_MAPPING.put("未确认&未恢复", 1);
		HW_ALARM_STATE_MAPPING.put("已确认&未恢复", 1);
		HW_ALARM_STATE_MAPPING.put("未确认事件", 2);
		HW_ALARM_STATE_MAPPING.put("已确认事件", 2);

		HW_ALARM_GRADE_MAPPING.put("紧急", Alarm.ALARM_GRADE_A_SERIOUS);
		HW_ALARM_GRADE_MAPPING.put("重要", Alarm.ALARM_GRADE_A_MEDIUM);
		HW_ALARM_GRADE_MAPPING.put("次要", Alarm.ALARM_GRADE_A_SLIGHT);
		HW_ALARM_GRADE_MAPPING.put("提示", Alarm.ALARM_GRADE_B_PROMPT);

		// 中兴
		// value=1 → 16（异常）
		// value=2 → 2（严重告警）
		// value=3 → 1（重大告警）
		// value=4 → 4（普通告警）
		// value=5 → 8（通知）
		ZTE_WCDMA_ALARM_GRADE_MAPPING.put("1", Alarm.ALARM_GRADE_B_ERROR);
		ZTE_WCDMA_ALARM_GRADE_MAPPING.put("2", Alarm.ALARM_GRADE_A_SERIOUS);
		ZTE_WCDMA_ALARM_GRADE_MAPPING.put("3", Alarm.ALARM_GRADE_A_MEDIUM);
		ZTE_WCDMA_ALARM_GRADE_MAPPING.put("4", Alarm.ALARM_GRADE_A_SLIGHT);
		ZTE_WCDMA_ALARM_GRADE_MAPPING.put("5", Alarm.ALARM_GRADE_B_PROMPT);

		ZTE_WCDMA_ALARM_INT_ID_MAPPING.put("communicationsAlarm", AII_COMMUNICATION);
		ZTE_WCDMA_ALARM_INT_ID_MAPPING.put("processingErrorAlarm", AII_HANDLED);
		ZTE_WCDMA_ALARM_INT_ID_MAPPING.put("environmentalAlarm", AII_ENVIRONMENT);
		ZTE_WCDMA_ALARM_INT_ID_MAPPING.put("qualityOfServiceAlarm", AII_SERVICE_QUALITY);
		ZTE_WCDMA_ALARM_INT_ID_MAPPING.put("equipmentAlarm", AII_DEVICE);
	}

	public String alarmId;

	public Date alarmTime;

	public String cityId;

	public String vendor; // ZY0808

	public int omcId;

	public int neType;// 1:gsm 2:wcdma

	public String neSysId;

	public String neName;

	public String titleText;

	public String alarmText;// 将所有采集的字段用 /n 连接

	public String locateInfo;

	public String probableCause;// 可能原因 <不填>

	// 告警状态 0表示活动的告警 1表示清除的告警，2表示事件（通知）
	// 根据STATE取值做映射换算：
	// 未确认&已恢复 0
	// 已确认&已恢复 0
	// 未确认&未恢复 1
	// 已确认&未恢复 1
	// 未确认事件 2
	// 已确认事件 2
	public int alarmState;

	// 告警处理状态 0 已发短信通知、1 已派发工单 2已发送邮件 3 已入经验库
	public int alarmDisState;

	public Date eventTime;

	public Date configTime;

	public Date cancelTime;

	public String configMan;

	public String cancelMan;

	// 归一的告警类型 按位区分，性能告警为1， 硬件告警为2 硬件统一性能和硬件告警的细分定义，根据EVENTTYPE做映射
	public int alarmType;

	// 归一的告警级别 按位区分，普通告警为4，重大告警为1，严重告警为2
	// 根据SEVERITY取值划分：紧急，重要，次要，提示 对应1,2,4,8
	public int alarmGrade;

	// 告警索引号 每一类告警的唯一编号，用户关联经验库
	// 厂家+版本+固定位长的ALARMID(长度不足左补齐)【16位】
	public String alarmIntId;

	public String neTypeOrg;// 厂家网元类型 NETYPE

	public String alarmIdOrg;// 厂家网管平台上的告警号流水号 NESN

	public String alarmTypeOrg;// 厂家告警类型 厂家OMC内定义的告警类型 EVENTTYPE

	public String alarmGradeOrg;// 厂家告警级别 厂家OMC内定义的告警级别 SEVERITY

	public String alarmIdPlatform;// 其它网管平台告警流水号 如果从网管平台采集告警，则用此字段关联 <不填>

	// 网元类型 　目前定义由Omc为1,bsc（RNC）为2,bts为3,cell为4
	// 根据NETYPE和 OBJTYPE取值换算：
	// NETYPE=DBS3900 WCDMA and OBJTYPE is null 3
	// NETYPE=BSC6810(R11+) and OBJTYPE is null 2
	// NETYPE=BTS3900 WCDMA and OBJTYPE is null 3
	// NETYPE=OMC and OBJTYPE is null 1
	// NETYPE=BSC6900 GU and OBJTYPE is null 2
	// NETYPE=BSC6900 UMTS and OBJTYPE is null 2
	// OBJTYPE=CELL or OBJTYPE=RRU 4
	public int neLevel;

	public OriAlarm toOriAlarm() {
		OriAlarmV3 a = new OriAlarmV3();
		a.setFlowNo(alarmId);
		a.setTitleText(titleText);
		a.setAlarmText(alarmText);
		a.setAlarmType(alarmType);
		// liangww modify 2012-04-26修改为当为-1时，设置为默认的轻微级别
		a.setAlarmGrade(alarmGrade != -1 ? alarmGrade : Alarm.ALARM_GRADE_A_SLIGHT);
		a.setVendor(vendor);
		a.setOmcId(String.valueOf(omcId));
		a.setCityId(Integer.parseInt(cityId));
		a.setNetId(neSysId);
		a.setNeLevel(neLevel);
		a.setAlarmTime(Util.getDateString(alarmTime));
		a.setAlarmIntId(Integer.parseInt(alarmIntId));
		a.setAlarmTypeOrg(Integer.parseInt(alarmTypeOrg));
		a.setNeName(neName);
		a.setNeType(neType);

		// liangww add 2012-04-16 设置为a类告警
		a.setAlarmType2(OriAlarm.ALARM_TYPE2_A);
		// 设置event_time
		a.setEventTime(Util.getDateString(this.eventTime));
		// liangww add 2012-05-24 设置alarm_sub_type为alarmIntId
		a.setAlarmSubType(Integer.parseInt(alarmIntId));

		return a;
	}

	@Override
	public String toString() {
		return "ResultEntry [alarmDisState=" + alarmDisState + ", alarmGrade=" + alarmGrade + ", alarmGradeOrg=" + alarmGradeOrg + ", alarmId="
				+ alarmId + ", alarmIdOrg=" + alarmIdOrg + ", alarmIdPlatform=" + alarmIdPlatform + ", alarmIntId=" + alarmIntId + ", alarmState="
				+ alarmState + ", alarmText=" + alarmText + ", alarmTime=" + alarmTime + ", alarmType=" + alarmType + ", alarmTypeOrg="
				+ alarmTypeOrg + ", cancelMan=" + cancelMan + ", cancelTime=" + cancelTime + ", cityId=" + cityId + ", configMan=" + configMan
				+ ", configTime=" + configTime + ", eventTime=" + eventTime + ", locateInfo=" + locateInfo + ", neLevel=" + neLevel + ", neName="
				+ neName + ", neSysId=" + neSysId + ", neType=" + neType + ", neTypeOrg=" + neTypeOrg + ", omcId=" + omcId + ", probableCause="
				+ probableCause + ", titleText=" + titleText + ", vendor=" + vendor + "]";
	}

	public static int map3GHwNeType(String netType, String objType) {
		if (Util.isNull(netType))
			return -1;
		if (netType.equalsIgnoreCase("DBS3900 WCDMA") && Util.isNull(objType))
			return 3;
		if (netType.equalsIgnoreCase("BSC6810(R11+)") && Util.isNull(objType))
			return 2;
		if (netType.equalsIgnoreCase("BTS3900 WCDMA") && Util.isNull(objType))
			return 3;
		if (netType.equalsIgnoreCase("OMC") && Util.isNull(objType))
			return 1;
		if (netType.equalsIgnoreCase("BSC6900 GU") && Util.isNull(objType))
			return 2;
		if (netType.equalsIgnoreCase("BSC6900 UMTS") && Util.isNull(objType))
			return 2;
		if (!Util.isNull(objType))
			return 4;
		return -1;
	}

	public static int mapHwEventType(String eventType) {
		if (HW_ALARM_TYPE_MAPPING.containsKey(eventType))
			return HW_ALARM_TYPE_MAPPING.get(eventType);
		return -1;
	}

	public static int mapHwAlarmState(String alarmState) {
		if (HW_ALARM_STATE_MAPPING.containsKey(alarmState))
			return HW_ALARM_STATE_MAPPING.get(alarmState);
		return -1;
	}

	public static int mapHwAlarmGrade(String ser) {
		if (HW_ALARM_GRADE_MAPPING.containsKey(ser))
			return HW_ALARM_GRADE_MAPPING.get(ser);
		return -1;
	}

	/**
	 * 获取华为的alarm_int_id
	 * 
	 * @param raw
	 * @return
	 */
	public static int makeHWAlarmIntId0(String raw) {
		if (HW_ALARM_INT_ID_MAPPING.containsKey(raw))
			return HW_ALARM_INT_ID_MAPPING.get(raw);
		return -1;
	}

	public static String makeHWAlarmId(String version, int omcId, String nesn) {
		return Summarizing.HW_VENDOR + version + omcId + nesn;
	}

	/**
	 * 获取3g华为 resultEntry
	 * 
	 * @param qe
	 * @param omcId
	 * @param entry
	 * @param neLevel
	 * @return
	 */
	public static ResultEntry get3GHwResultEntry(QueriedEntry qe, int omcId, RawDataEntry entry, int neLevel) {
		ResultEntry re = new ResultEntry();
		// re.alarmId = ResultEntry.makeAlarmId(qe.version,
		// dataSource.getOmcId(), entry.netSeq);
		re.alarmId = ResultEntry.makeHWAlarmId(qe.version, omcId, entry.netSeq);

		re.cityId = String.valueOf(qe.cityId);
		re.vendor = Summarizing.HW_VENDOR;

		re.omcId = omcId;
		re.alarmState = ResultEntry.mapHwAlarmState(entry.alarmStatus);
		re.neType = ResultEntry.NETYPE_W;
		re.alarmGrade = ResultEntry.mapHwAlarmGrade(entry.alarmLevel);
		re.alarmIntId = ResultEntry.makeHWAlarmIntId0(entry.alarmType) + "";
		re.alarmTypeOrg = String.valueOf(ResultEntry.mapHwEventType(entry.alarmType));
		re.alarmType = ResultEntry.DEFAULT_ALARM_TYPE;
		re.neLevel = neLevel;

		re.build(qe, entry);
		return re;
	}

	/**
	 * 获取2g华为 resultEntry
	 * 
	 * @param qe
	 * @param omcId
	 * @param entry
	 * @param neLevel
	 * @return
	 */
	public static ResultEntry get2GHwResultEntry(QueriedEntry qe, int omcId, RawDataEntry entry, int neLevel) {

		return get3GHwResultEntry(qe, omcId, entry, neLevel);
	}

	/**
	 * 获取中兴wcdma的
	 * 
	 * @param ser
	 * @return
	 */
	public static int mapZteWcdmaAlarmGrade(String ser) {
		if (ZTE_WCDMA_ALARM_GRADE_MAPPING.containsKey(ser))
			return ZTE_WCDMA_ALARM_GRADE_MAPPING.get(ser);
		return -1;
	}

	/**
	 * 获取中兴wcdma的alarm_int_id
	 * 
	 * @param raw
	 * @return
	 */
	public static int makeZteWcdmaAlarmIntId(String raw) {
		if (ZTE_WCDMA_ALARM_INT_ID_MAPPING.containsKey(raw))
			return ZTE_WCDMA_ALARM_INT_ID_MAPPING.get(raw);
		return -1;
	}

	public static String makeZteAlarmId(int omcId, int neType, String alarmKind, String neSysId) {
		return Summarizing.ZTE_VENDOR + omcId + neType + alarmKind + neSysId;
	}

	/**
	 * 获取中兴 zte resultEntry
	 * 
	 * @param qe
	 * @param omcId
	 * @param entry
	 * @param neLevel
	 * @return
	 */
	public static ResultEntry getZteWcdmaResultEntry(QueriedEntry qe, int omcId, RawDataEntry entry, int neLevel) {
		ResultEntry re = new ResultEntry();

		re.cityId = String.valueOf(qe.cityId);
		re.vendor = Summarizing.ZTE_VENDOR;

		re.omcId = omcId;
		re.neType = ResultEntry.NETYPE_W;
		re.alarmGrade = mapZteWcdmaAlarmGrade(entry.alarmLevel);
		re.alarmIntId = makeZteWcdmaAlarmIntId(entry.alarmType) + "";
		re.alarmTypeOrg = makeZteWcdmaAlarmIntId(entry.alarmType) + "";
		re.alarmType = ResultEntry.DEFAULT_ALARM_TYPE;
		re.neLevel = neLevel;

		re.alarmId = makeZteAlarmId(omcId, re.neType, entry.alarmKind, qe.neSysid);

		re.build(qe, entry);
		return re;
	}

	void build(QueriedEntry qe, RawDataEntry entry) {
		alarmTime = entry.happenTime;
		neSysId = qe.neSysid;
		neName = qe.neName;
		titleText = entry.alarmName;
		alarmText = entry.alarmText;

		locateInfo = entry.locationInfo;
		probableCause = null;
		alarmDisState = -1;
		eventTime = entry.happenTime;
		configTime = null;
		cancelTime = entry.restoreTime;
		configMan = null;
		cancelMan = null;
		neTypeOrg = entry.neType;
		alarmIdOrg = entry.netSeq;
		alarmGradeOrg = entry.alarmLevel;
	}

}

package parser.zte.am;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.omg.CosNotification.Property;
import org.omg.CosNotification.StructuredEvent;

import cn.uway.alarmbox.db.pool.DBUtil;

import parser.corba.nms.NMSCorbaParser;
import parser.corba.nms.util.CorbaCollectUtil;
import parser.hw.am.ABSender;
import parser.hw.am.RawDataEntry;
import util.DbPool;
import util.Util;
import NotificationIRPNotifications.NonFilterableEventBody;

/**
 * 山东 中兴的 w网 corba接口 告警 解析器 WV1CorbaAmParser
 * 
 * @author liangww 2012-6-27
 * @version 1.0 1.0.1 liangww 2012-08-06 屏蔽了汇总的算法，只是把数据插入到测试表<br>
 */
public class WV1CorbaSdAmParser extends NMSCorbaParser {

	private final static String sql = "insert into clt_zte_w_am_test("
			+ "event_name,domain_name,type_name,head_name,alarm_i,alarm_p,alarm_q,alarm_t,alarm_v,alarm_j,alarm_nn,alarm_w,alarm_o,alarm_d,alarm_e,alarm_jj,alarm_b,alarm_kk,alarm_c,alarm_g,alarm_h,alarm_a,alarm_f,alarm_k,alarm_l,alarm_m,alarm_n,alarm_s)"
			+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	protected List<Alarm> record = new ArrayList<Alarm>();

	protected ABSender sender = null;

	/* 重试次数。 */
	private static final int RETRY_TIMES = 5;

	public int getOmcId() {
		return this.collectObjInfo.getDevInfo().getOmcID();
	}

	@Override
	protected void handleOneAlarm(StructuredEvent alarm) throws Exception {
		// TODO Auto-generated method stub

		// 添加到list，插入到测试表
		add(alarm);

		// 以下是调试过的，并且是ok的
		// initSender();
		//
		//
		// RawDataEntry rawDataEntry = getRawDataEntry(alarm, this.collectObjInfo.getDevInfo().getEncode());
		//
		// if(Util.isNull(rawDataEntry.getBscId()))
		// {
		// return ;
		// }
		//
		// QueriedEntry queriedEntry = null;
		// int neLevel = 0;
		//
		// if(Util.isNotNull(rawDataEntry.getCellId()))
		// {
		// queriedEntry = Summarizing.getZTEWcdma(getOmcId(), rawDataEntry.getBscId(), rawDataEntry.getBtsId(), rawDataEntry.getCellId());
		// neLevel = MappingTables.CELL_LEVEL;
		// }
		// if(queriedEntry == null && Util.isNotNull(rawDataEntry.getBtsId()))
		// {
		// queriedEntry = Summarizing.getZTEWcdma(getOmcId(), rawDataEntry.getBscId(), rawDataEntry.getBtsId(), null);
		// neLevel = MappingTables.BTS_LEVEL;
		// }
		// if(queriedEntry == null && Util.isNotNull(rawDataEntry.getBscId()))
		// {
		// queriedEntry = Summarizing.getZTEWcdma(getOmcId(), rawDataEntry.getBscId(), null, null);
		// neLevel = MappingTables.BSC_LEVEL;
		// }
		//
		//
		// if(queriedEntry != null)
		// {
		// queriedEntry.init(neLevel);
		//
		// if(Util.isNull(queriedEntry.neSysid) || Util.isNull(queriedEntry.neName))
		// {
		// log.warn(logKey + "queriedEntry.neSysid or neName is null");
		// return ;
		// }
		//
		//
		// ResultEntry re = new ResultEntry();
		// re = ResultEntry.getZteWcdmaResultEntry(queriedEntry, getOmcId(), rawDataEntry, neLevel);
		//
		// //告警清除
		// if(re.cancelTime != null)
		// {
		// OriAlarm oa = re.toOriAlarm();
		// }
		// else
		// {
		// OriAlarm oa = re.toOriAlarm();
		// //
		// sender.send(oa, RETRY_TIMES);
		// }
		//
		// }

	}

	/**
	 * 初始化发送器
	 */
	public void initSender() {
		if (sender == null) {
			String senderName = "" + this.collectObjInfo.getDevInfo().getOmcID();
			// 初始化sender
			String host = this.collectObjInfo.getProxyDevInfo().getIP();
			int port = this.collectObjInfo.getProxyDevPort();
			sender = new ABSender(host, port, senderName, logKey);
			log.debug(String.format("%s init sender, host:%s, port:%s", logKey, host, port));
		}
	}

	/**
	 * 
	 * @param str
	 * @return
	 */
	static Map<String, String> getMapValue(String str) {
		Map<String, String> map = new HashMap<String, String>();

		if (str != null) {
			String[] array = str.split(",");
			for (int i = 0; i < array.length; i++) {
				String[] subArray = array[i].split("=");
				if (subArray.length > 2) {
					map.put(subArray[0], subArray[1]);
				}
			}
		}
		return map;
	}

	private static RawDataEntry getRawDataEntry(StructuredEvent alarm, String encode) throws Exception {
		RawDataEntry re = new RawDataEntry();

		// structured events
		Property[] eventsPro = alarm.filterable_data;
		// ALARMID //这个不对的,因为它是每一个都不一样的，所以清除和确认没办法找回
		re.setAlarmID(CorbaCollectUtil.findValue(eventsPro, "a"));
		// happen time alarmTime
		String value = CorbaCollectUtil.findValue(eventsPro, "b");
		re.setHappenTime(getDate(value));
		//
		re.setAlarmLevel(CorbaCollectUtil.findValue(eventsPro, "h"));
		// 告警原始类型
		re.setAlarmType(CorbaCollectUtil.findValue(eventsPro, "jj"));

		//
		NonFilterableEventBody body = CorbaCollectUtil.parseNonFilterableEventBody(alarm.remainder_of_body);
		Property[] res = body.name_value_pairs;
		String alarmName = tranform(CorbaCollectUtil.findValue(res, "i"), encode);

		alarmName = alarmName == null ? "" : alarmName.replace("\n", "/n");
		// 告警标题
		re.setAlarmName(alarmName);
		// 近端IMA组故障(198019223)
		re.setAlarmKind(Util.getStrFromLast(alarmName, "", ")", "("));

		// 告警内容
		re.setAlarmText(tranform(CorbaCollectUtil.findValue(res, "j"), encode));
		// 定位信息 单板类型:SDTA;

		String locationInfo = Util.getStr(CorbaCollectUtil.findValue(res, "j"), "单板类型:", ";");
		re.setLocationInfo(locationInfo);
		// 恢复时间（清除告警时间一个意思）
		re.setRestoreTime(getDate(CorbaCollectUtil.findValue(res, "ll")));

		String str = tranform(CorbaCollectUtil.findValue(res, "nn"), encode);
		// rncid
		re.setBscId(Util.getStr(str, "SUBNETWORKID=", "@"));

		//
		str = tranform(CorbaCollectUtil.findValue(res, "j"), encode);
		// btsid
		re.setBtsId(Util.getStr(str, "局向ID(站点):", ";"));
		// cellid
		re.setCellId(Util.getStr(str, "CellID:", ";"));

		return re;
	}

	static Date getDate(String value) {
		String time = null;
		if (value == null || (time = Util.getStr(value, "(", ")")) == null) {
			return null;
		}
		try {
			return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
		}

		return null;
	}

	public void add(StructuredEvent alarm) throws Exception {
		String encode = this.collectObjInfo.getDevInfo().getEncode();

		Alarm destAlarm = new Alarm();

		destAlarm.event_name = alarm.header.fixed_header.event_name;
		destAlarm.domain_name = alarm.header.fixed_header.event_type.domain_name;
		destAlarm.type_name = alarm.header.fixed_header.event_type.type_name;
		destAlarm.head_name = CorbaCollectUtil.findValue(alarm.header.variable_header, "MccMnc");

		NonFilterableEventBody body = CorbaCollectUtil.parseNonFilterableEventBody(alarm.remainder_of_body);
		Property[] res = body.name_value_pairs;
		destAlarm.alarm_i = tranform(CorbaCollectUtil.findValue(res, "i"), encode);
		destAlarm.alarm_p = tranform(CorbaCollectUtil.findValue(res, "p"), encode);
		destAlarm.alarm_q = tranform(CorbaCollectUtil.findValue(res, "q"), encode);
		destAlarm.alarm_t = tranform(CorbaCollectUtil.findValue(res, "t"), encode);
		destAlarm.alarm_v = tranform(CorbaCollectUtil.findValue(res, "v"), encode);
		destAlarm.alarm_j = tranform(CorbaCollectUtil.findValue(res, "j"), encode);
		destAlarm.alarm_nn = tranform(CorbaCollectUtil.findValue(res, "nn"), encode);
		destAlarm.alarm_w = tranform(CorbaCollectUtil.findValue(res, "w"), encode);
		destAlarm.alarm_o = tranform(CorbaCollectUtil.findValue(res, "o"), encode);

		Property[] eventsPro = alarm.filterable_data;
		destAlarm.alarm_d = tranform(CorbaCollectUtil.findValue(eventsPro, "d"), encode);
		destAlarm.alarm_e = tranform(CorbaCollectUtil.findValue(eventsPro, "e"), encode);
		destAlarm.alarm_jj = tranform(CorbaCollectUtil.findValue(eventsPro, "jj"), encode);
		destAlarm.alarm_b = tranform(CorbaCollectUtil.findValue(eventsPro, "b"), encode);
		destAlarm.alarm_kk = tranform(CorbaCollectUtil.findValue(eventsPro, "kk"), encode);
		destAlarm.alarm_c = tranform(CorbaCollectUtil.findValue(eventsPro, "c"), encode);
		destAlarm.alarm_g = tranform(CorbaCollectUtil.findValue(eventsPro, "g"), encode);
		destAlarm.alarm_h = tranform(CorbaCollectUtil.findValue(eventsPro, "h"), encode);
		destAlarm.alarm_a = tranform(CorbaCollectUtil.findValue(eventsPro, "a"), encode);
		destAlarm.alarm_f = tranform(CorbaCollectUtil.findValue(eventsPro, "f"), encode);
		destAlarm.alarm_k = tranform(CorbaCollectUtil.findValue(eventsPro, "k"), encode);
		destAlarm.alarm_l = tranform(CorbaCollectUtil.findValue(eventsPro, "l"), encode);
		destAlarm.alarm_m = tranform(CorbaCollectUtil.findValue(eventsPro, "m"), encode);
		destAlarm.alarm_n = tranform(CorbaCollectUtil.findValue(eventsPro, "n"), encode);
		destAlarm.alarm_s = tranform(CorbaCollectUtil.findValue(eventsPro, "s"), encode);

		record.add(destAlarm);
		if (record.size() > 100) {
			insert();
			record.clear();
		}
	}

	@Override
	public void dispose() {
		// TODO Auto-generated method stub
		//
		if (record.size() > 0) {
			insert();
			record.clear();
		}
	}

	/**
	 * 插入
	 */
	void insert() {
		Connection connection = null;
		PreparedStatement ps = null;

		try {
			connection = DbPool.getConn();
			connection.setAutoCommit(false);
			ps = connection.prepareStatement(sql);

			for (int i = 0; i < record.size(); i++) {
				// + "eq_alarm_Id,net_alarm_Id,obj_sign,obj_name,obj_type,ne_sign,ne_name,ne_type,alarm_Id,alarm_name,"
				Alarm alarm = record.get(i);
				ps.setString(1, alarm.event_name);
				ps.setString(2, alarm.domain_name);
				ps.setString(3, alarm.type_name);
				ps.setString(4, alarm.head_name);
				ps.setString(5, alarm.alarm_i);
				ps.setString(6, alarm.alarm_p);
				ps.setString(7, alarm.alarm_q);
				ps.setString(8, alarm.alarm_t);
				ps.setString(9, alarm.alarm_v);
				ps.setString(10, alarm.alarm_j);
				ps.setString(11, alarm.alarm_nn);
				ps.setString(12, alarm.alarm_w);
				ps.setString(13, alarm.alarm_o);
				ps.setString(14, alarm.alarm_d);
				ps.setString(15, alarm.alarm_e);
				ps.setString(16, alarm.alarm_jj);
				ps.setString(17, alarm.alarm_b);
				ps.setString(18, alarm.alarm_kk);
				ps.setString(19, alarm.alarm_c);
				ps.setString(20, alarm.alarm_g);
				ps.setString(21, alarm.alarm_h);
				ps.setString(22, alarm.alarm_a);
				ps.setString(23, alarm.alarm_f);
				ps.setString(24, alarm.alarm_k);
				ps.setString(25, alarm.alarm_l);
				ps.setString(26, alarm.alarm_m);
				ps.setString(27, alarm.alarm_n);
				ps.setString(28, alarm.alarm_s);

				ps.addBatch();
				// liangww add 2012-05-22 增加1000提交一次
			}
			ps.executeBatch();
			connection.commit();
			log.warn(logKey + " 插入测试数据" + record.size() + "成功");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.warn("", e);
			// 如果失败则回滚
			try {
				connection.rollback();
			} catch (Exception e1) {
			}
		} finally {
			DBUtil.close(null, ps, connection);
		}

	}

	class Alarm {

		String event_name;

		String domain_name;

		String type_name;

		String head_name;

		String alarm_i;

		String alarm_p;

		String alarm_q;

		String alarm_t;

		String alarm_v;

		String alarm_j;

		String alarm_nn;

		String alarm_w;

		String alarm_o;

		String alarm_d;

		String alarm_e;

		String alarm_jj;

		String alarm_b;

		String alarm_kk;

		String alarm_c;

		String alarm_g;

		String alarm_h;

		String alarm_a;

		String alarm_f;

		String alarm_k;

		String alarm_l;

		String alarm_m;

		String alarm_n;

		String alarm_s;
	}

}

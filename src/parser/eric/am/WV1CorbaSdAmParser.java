package parser.eric.am;

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
 * 山东 爱立信的 w网 corba接口 告警 解析器 WV1CorbaAmParser
 * 
 * @author liangww 2012-8-03
 */
public class WV1CorbaSdAmParser extends NMSCorbaParser {

	private final static String sql = "insert into clt_eric_w_am_test "
			+ " (event_name,domain_name,type_name,alarm_a,alarm_kk,alarm_f,alarm_i,alarm_j,alarm_nn,alarm_n,alarm_k,alarm_m,alarm_o,alarm_v,alarm_p,alarm_q,alarm_s,alarm_d,alarm_e,alarm_b,alarm_c,alarm_g,alarm_h,alarm_jj)"
			+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
		//

		add(alarm);

	}

	public void add(StructuredEvent alarm) {
		Alarm destAlarm = new Alarm();

		destAlarm.event_name = alarm.header.fixed_header.event_name;
		destAlarm.domain_name = alarm.header.fixed_header.event_type.domain_name;
		destAlarm.type_name = alarm.header.fixed_header.event_type.type_name;

		NonFilterableEventBody body = CorbaCollectUtil.parseNonFilterableEventBody(alarm.remainder_of_body);
		Property[] res = body.name_value_pairs;
		destAlarm.alarm_a = CorbaCollectUtil.findValue(res, "a");
		destAlarm.alarm_kk = CorbaCollectUtil.findValue(res, "kk");
		destAlarm.alarm_f = CorbaCollectUtil.findValue(res, "f");
		destAlarm.alarm_i = CorbaCollectUtil.findValue(res, "i");
		destAlarm.alarm_j = CorbaCollectUtil.findValue(res, "j");
		destAlarm.alarm_nn = CorbaCollectUtil.findValue(res, "nn");
		destAlarm.alarm_n = CorbaCollectUtil.findValue(res, "n");
		destAlarm.alarm_k = CorbaCollectUtil.findValue(res, "k");
		destAlarm.alarm_m = CorbaCollectUtil.findValue(res, "m");
		destAlarm.alarm_o = CorbaCollectUtil.findValue(res, "o");
		destAlarm.alarm_v = CorbaCollectUtil.findValue(res, "v");
		destAlarm.alarm_p = CorbaCollectUtil.findValue(res, "p");
		destAlarm.alarm_q = CorbaCollectUtil.findValue(res, "q");
		destAlarm.alarm_s = CorbaCollectUtil.findValue(res, "s");

		Property[] eventsPro = alarm.filterable_data;
		destAlarm.alarm_d = CorbaCollectUtil.findValue(eventsPro, "d");
		destAlarm.alarm_e = CorbaCollectUtil.findValue(eventsPro, "e");
		destAlarm.alarm_b = CorbaCollectUtil.findValue(eventsPro, "b");
		destAlarm.alarm_c = CorbaCollectUtil.findValue(eventsPro, "c");
		destAlarm.alarm_g = CorbaCollectUtil.findValue(eventsPro, "g");
		destAlarm.alarm_h = CorbaCollectUtil.findValue(eventsPro, "h");
		destAlarm.alarm_jj = CorbaCollectUtil.findValue(eventsPro, "jj");

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
				ps.setString(4, alarm.alarm_a);
				ps.setString(5, alarm.alarm_kk);
				ps.setString(6, alarm.alarm_f);
				ps.setString(7, alarm.alarm_i);
				ps.setString(8, alarm.alarm_j);
				ps.setString(9, alarm.alarm_nn);
				ps.setString(10, alarm.alarm_n);
				ps.setString(11, alarm.alarm_k);
				ps.setString(12, alarm.alarm_m);
				ps.setString(13, alarm.alarm_o);
				ps.setString(14, alarm.alarm_v);
				ps.setString(15, alarm.alarm_p);
				ps.setString(16, alarm.alarm_q);
				ps.setString(17, alarm.alarm_s);
				ps.setString(18, alarm.alarm_d);
				ps.setString(19, alarm.alarm_e);
				ps.setString(20, alarm.alarm_b);
				ps.setString(21, alarm.alarm_c);
				ps.setString(22, alarm.alarm_g);
				ps.setString(23, alarm.alarm_h);
				ps.setString(24, alarm.alarm_jj);

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

		String alarm_a;

		String alarm_kk;

		String alarm_f;

		String alarm_i;

		String alarm_j;

		String alarm_nn;

		String alarm_n;

		String alarm_k;

		String alarm_m;

		String alarm_o;

		String alarm_v;

		String alarm_p;

		String alarm_q;

		String alarm_s;

		String alarm_d;

		String alarm_e;

		String alarm_b;

		String alarm_c;

		String alarm_g;

		String alarm_h;

		String alarm_jj;
	}

}

package parser.hw.am;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import parser.AbstractStreamParser;
import task.CollectObjInfo;
import util.DbPool;
import util.Util;
import cn.uway.alarmbox.db.pool.DBUtil;
import cn.uway.alarmbox.protocol.monitor.CleanFlowNo;

/**
 * SocketCollect 通过Socket连接方式处理M2000北向告警字符流 M2000StreamParser
 * 
 * @author liangww 2012-4-16<br>
 * @version 1.0<br>
 *          1.0.1 liangww 2012-04-28 增加name成员，并写log时增加相关任务信息<br>
 *          1.0.2 liangww 2012-05-25 修改handle算法,增加getNodeB函数<br>
 *          1.0.3 liangww 2012-06-04 增加告警清除功能<br>
 */
public class M2000StreamParser extends AbstractStreamParser {

	public static final String SQL = "insert into %s( "
			+ "eq_alarm_Id,net_alarm_Id,obj_sign,obj_name,obj_type,ne_sign,ne_name,ne_type,alarm_Id,alarm_name,"
			+ "alarm_class,alarm_level,alarm_state,alarm_type,start_time,cancel_time,position_cause)" + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	/* 重试次数。 */
	private static final int RETRY_TIMES = 5;

	/* 报文开始标记。 */
	private static final String START_FLAG = "<+++>";

	/* 报文结束标记。 */
	private static final String END_FLAG = "<--->";

	/* 报文件中的键值分隔符。 */
	private static final String KEY_VAL_SPLIT = "=";

	private StringBuilder buffer;

	// liangww modify 2012-06-21 修改为protected 属性
	protected ABSender sender = null;

	protected String name = null;		//

	protected List<RawDataEntry> list = new ArrayList<RawDataEntry>();

	public M2000StreamParser() {
		super();
		this.buffer = new StringBuilder();
	}

	@Override
	public boolean parseData() throws Exception {
		return true;
	}

	@Override
	public void parse(InputStream in, OutputStream out) throws IOException {
		String line = null;
		boolean findStart = false;
		BufferedReader reader = null;

		// initSender();

		reader = getReader(in);
		while ((line = reader.readLine()) != null) {
			line = line.trim();
			if (line.equals(END_FLAG)) {
				handle(buffer.toString());
				buffer.setLength(0);
				findStart = false;
			} else if (findStart) {
				buffer.append(line).append("\n");
			} else if (line.equals(START_FLAG)) {
				findStart = true;
			}
		}// while ((line = reader.readLine()) != null)
	}

	protected void handle(String buff) {
		if (Util.isNull(buff) || buff.contains("告警握手"))
			return;

		String data = buff.trim();
		RawDataEntry entry = parseRawData(data);

		this.list.add(entry);
		if (list.size() > 100) {
			insert(String.format(SQL, "clt_hw_w_am_test"), list);
			list.clear();
		}

		// // 如果为空
		// if ( entry == null ) { return; }
		//
		// int neLevel = 0;
		// QueriedEntry qe = null;
		//
		// // 如果是cell级别
		// if ( "Cell".equals(entry.getObjType()) )
		// {
		// // 1）首先，当对象类型 =
		// // Cell，根据“对象标识”关联同一OMC下的clt_pm_w_hw_objectinstance表OBJECTMEMBER0字段获取小区NE_CELL_ID
		// // 2）关联失败，剔除“对象标识”的第4和第5个逗号的数据，重新关联，再次关联失败，该告警丢弃。
		// neLevel = MappingTables.CELL_LEVEL;
		// //liangww modify 2012-06-20 修改成调用Summarizing
		// qe = Summarizing.getHW3GM2000M1(getOmcId(), neLevel, entry.getObjFdn());
		// if ( qe == null )
		// {
		// if ( entry.getObjFdn() == null || entry.getObjFdn().length() < 45 )
		// {
		// log.warn(String.format("%s 此条数据cell级别无法找到关联关系", name));
		// }
		// else
		// {
		// // 去掉第4个.号的内容“.3221295108”
		// // .3221229568.3221278720.3221291652.3221295108.3222862719
		// String objFdn = entry.getObjFdn().substring(0, 34)
		// + entry.getObjFdn().substring(45);
		// //liangww modify 2012-06-20 修改成调用Summarizing
		// qe = Summarizing.getHW3GM2000M1(getOmcId(), neLevel, objFdn);
		// if ( qe == null )
		// {
		// // liangww modify 2012-05-10 修改输出log内容
		// log.warn(String.format("%s 此条数据截取objFdn无法找到关联关系", name));
		// }// if ( qe == null )
		// }//else
		// }// if ( qe == null )
		// // log.warn(String.format("%s 此条数据截取objFdn找到关联关系1", name));
		// }
		//
		// // 当成bts, alarm_id in (22214,22216,22226)
		// if ( qe == null && ("22214".equals(entry.getAlarmID())
		// || "22216".equals(entry.getAlarmID())
		// || "22226".equals(entry.getAlarmID())) )
		// {
		// neLevel = MappingTables.BTS_LEVEL;
		//
		// String nodeB = getNodeB(entry.getLocationInfo());
		// //liangww modify 2012-06-20 修改成调用Summarizing
		// qe = Summarizing.getHW3GM2000M2(getOmcId(), entry.getNeName(), nodeB);
		//
		// String logStr = (qe == null)? "此条数据在网元名中无法找到关联关系" : "此条数据在网元名中找到关联关系2";
		// log.warn(String.format("%s "+logStr, name));
		// }
		//
		// //如果还是找不到
		// if(qe == null)
		// {
		// neLevel = MappingTables.BTS_LEVEL;
		// //liangww modify 2012-06-20 修改成调用Summarizing
		// qe = Summarizing.getHW3GM2000M1(getOmcId(), neLevel, entry.getNeFlag());
		// String logStr = (qe == null)? "此条数据对象标识找不到关联关系" : "此条数据对象标识找到联关系3";
		// log.warn(String.format("%s "+logStr, name));
		// }
		//
		// if(qe == null)
		// {
		// log.warn(String.format("%s 此条数据找不到关联关系", name));
		// return ;
		// }
		//
		//
		// //如果是清除告警，就清除
		// if(entry.isCleanAlarm())
		// {
		// handleCleanAlarm(entry, qe);
		// return ;
		// }
		//
		// ResultEntry re = new ResultEntry();
		// re = ResultEntry.get3GHwResultEntry(qe, getOmcId(), entry, neLevel);
		// OriAlarm oa = re.toOriAlarm();
		//
		// //发送
		// sender.send(oa, RETRY_TIMES);
	}

	/**
	 * 解析数据
	 * 
	 * @param data
	 * @return
	 */
	protected RawDataEntry parseRawData(String data) {
		try {
			String[] sp = data.split("\n");
			RawDataEntry entry = new RawDataEntry();
			// liangww add 2012-06-27 初始化告警内容
			entry.alarmText = (data != null ? data.trim().replace("\n", "/n") : "");

			for (String s : sp) {
				if (Util.isNull(s))
					continue;
				String[] spCol = s.trim().split(KEY_VAL_SPLIT, 2);
				String key = spCol[0].trim();
				String val = (spCol.length == 2 ? spCol[1].trim() : "");
				if (key.equals("设备告警流水号"))
					entry.alarmSeq = val;
				else if (key.equals("网络流水号"))
					entry.netSeq = val;
				else if (key.equals("网元标识"))
					entry.neFlag = val;
				else if (key.equals("网元名称"))
					entry.neName = val;
				else if (key.equals("网元类型"))
					entry.neType = val;
				else if (key.equalsIgnoreCase("告警ID"))
					entry.alarmID = val;
				else if (key.equals("告警名称"))
					entry.alarmName = val;
				else if (key.equals("告警种类"))
					entry.alarmKind = val;
				else if (key.equals("告警级别"))
					entry.alarmLevel = val;
				else if (key.equals("告警状态"))
					entry.alarmStatus = val;
				else if (key.equals("告警类型"))
					entry.alarmType = val;
				else if (key.equals("发生时间"))
					entry.happenTime = Util.getDate1(val);
				else if (key.equals("恢复时间"))
					entry.restoreTime = Util.getDate1(val);
				else if (key.equals("定位信息"))
					entry.locationInfo = val;
				else if (key.equals("对象类型"))
					entry.objType = val;
				else if (key.equals("对象标识"))
					entry.objFdn = val;
				else if (key.equals("对象名称"))
					entry.objName = val;
			}
			return entry;
		} catch (Exception e) {
			log.warn(name + " 解析一条原始记录时发生例外 - " + data, e);
			return null;
		}
	}

	@Override
	public void setCollectObjInfo(CollectObjInfo collectObjInfo) {
		// TODO Auto-generated method stub
		super.setCollectObjInfo(collectObjInfo);
		// liangww add 2012-04-27 初始化name
		this.name = this.collectObjInfo.getFullName() + '_' + this.collectObjInfo.getDevInfo().getOmcID();

		log.warn(String.format("%s m2000StreamParser v1.3", name));
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
			sender = new ABSender(host, port, senderName, name);
			log.debug(String.format("%s init sender, host:%s, port:%s", name, host, port));
		}
	}

	/**
	 * 获取omcid
	 * 
	 * @return
	 */
	protected int getOmcId() {
		return this.collectObjInfo.getDevInfo().getOmcID();
	}

	/**
	 * 获取reader
	 * 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	private BufferedReader getReader(InputStream in) throws IOException {
		String encode = this.collectObjInfo.getDevInfo().getEncode();
		if (encode == null || encode.equals("")) {
			return new BufferedReader(new InputStreamReader(in));
		}

		return new BufferedReader(new InputStreamReader(in, encode));
	}

	public static String getNodeB(String str) {
		int index = str.indexOf("NodeB标识=");
		if (index == -1) {
			return null;
		}

		int index2 = str.indexOf(",", index + 1);
		if (index2 == -1) {
			return null;
		}

		return str.substring(index + "NodeB标识=".length(), index2);
	}

	/**
	 * 处理清除告警
	 * 
	 * @param entry
	 * @param qe
	 */
	protected void handleCleanAlarm(RawDataEntry entry, QueriedEntry qe) {
		if (entry.getRestoreTime() == null) {
			return;
		}

		CleanFlowNo cleanFlowNo = new CleanFlowNo();
		cleanFlowNo.setFlowNo(ResultEntry.makeHWAlarmId(qe.version, getOmcId(), entry.netSeq));
		cleanFlowNo.setCancleMan("system");
		cleanFlowNo.setCancleTime(Util.getDateString(entry.getRestoreTime()));

		log.warn(String.format("%s 清除告警flowNo:%s", name, cleanFlowNo.getFlowNo()));

		sender.send(cleanFlowNo, RETRY_TIMES);
	}

	public void insert(String sql, List<RawDataEntry> rawDataEntries) {
		Connection connection = null;
		PreparedStatement ps = null;

		try {
			connection = DbPool.getConn();
			connection.setAutoCommit(false);
			ps = connection.prepareStatement(sql);

			for (int i = 0; i < rawDataEntries.size(); i++) {
				// + "eq_alarm_Id,net_alarm_Id,obj_sign,obj_name,obj_type,ne_sign,ne_name,ne_type,alarm_Id,alarm_name,"
				RawDataEntry rde = rawDataEntries.get(i);
				ps.setString(1, rde.alarmSeq);
				ps.setString(2, rde.netSeq);
				ps.setString(3, rde.objFdn);
				ps.setString(4, rde.objName);
				ps.setString(5, rde.objType);

				ps.setString(6, rde.neFlag);
				ps.setString(7, rde.neName);
				ps.setString(8, rde.neType);
				ps.setString(9, rde.alarmID);
				ps.setString(10, rde.alarmName);

				// + "alarm_class,alarm_level,alarm_state,alarm_type,start_time,cancel_time,position_cause)"
				ps.setString(11, rde.alarmKind);
				ps.setString(12, rde.alarmLevel);
				ps.setString(13, rde.alarmStatus);
				ps.setString(14, rde.alarmType);
				ps.setString(15, rde.happenTime == null ? null : Util.getDateString_yyyyMMddHHmmssSSS(rde.happenTime));
				ps.setString(16, rde.restoreTime == null ? null : Util.getDateString_yyyyMMddHHmmssSSS(rde.restoreTime));
				ps.setString(17, rde.locationInfo);

				ps.addBatch();
				// liangww add 2012-05-22 增加1000提交一次
			}
			ps.executeBatch();
			connection.commit();
			log.warn(name + " 插入测试数据" + rawDataEntries.size() + "成功");
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
			// 解析results
		}

	}

}

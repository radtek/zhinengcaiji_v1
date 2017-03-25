package collect;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.tools.ant.util.DateUtils;

import com.linuxense.javadbf.Utils;

import parser.Parser;
import task.CollectObjInfo;
import templet.hw.M2000AlarmTempletP;
import util.DbPool;
import util.LogMgr;

/**
 * SocketCollect 通过Socket连接方式处理M2000北向告警字符流
 * 
 * @author liuwx 2010-2-22
 */
public class SocketCollect extends Parser {

	static Socket server;

	private static final String splitFlag = "=";

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	private static String tablename = "CLT_HW_M2000_ALARM_CHAR_STREAM";

	private static final String insertSql = "insert into CLT_HW_M2000_ALARM_CHAR_STREAM(";

	private static final int BUFFER_SIZE = 1024 * 8;

	private byte[] buffer;

	private int readCount = 0; // 实际从socket输入流中读取的字节数

	private List<String> sqlList = null;

	private String defaultDate = "1970-01-01 08:00:00";

	char[] head;

	char[] tail;

	char[] middle;

	char[] nextHead;

	char[] nextTail;

	private CollectObjInfo collectObjInfo = null;

	private Date upDate = null;

	private long tenMinutePeriod = 10 * 60 * 1000;// 监控周期是10分钟

	private int ten = 10;

	private int insertCount = 0;

	public SocketCollect() {

	}

	public SocketCollect(CollectObjInfo collectObjInfo) {
		this.collectObjInfo = collectObjInfo;

		Calendar c = Calendar.getInstance();
		c.setTime(new Date());
		int minute = c.get(Calendar.MINUTE);
		c.set(Calendar.MINUTE, (int) Math.floor(minute / ten) * ten);
		c.set(Calendar.SECOND, 0);

		upDate = c.getTime();

	}

	public void start() {

		buffer = new byte[BUFFER_SIZE];
		sqlList = new ArrayList<String>();
		log.debug(collectObjInfo + " : 启动接入线程接收M2000北向告警字符流.");
		Accesser a = new Accesser("Accesser", collectObjInfo.getDevInfo().getIP(), collectObjInfo.getDevPort());
		a.start();
		log.debug(collectObjInfo + " : 启动解析线程解析M2000北向告警字符流.");
		SocketDataParser sdp = new SocketDataParser("Parser");
		sdp.start();
	}

	/* 接入线程 用于接入数据 */
	class Accesser extends Thread {

		String ip;

		int port;

		InputStream in;

		public Accesser(String name, String ip, int port) {
			super(name);
			try {
				this.ip = ip;
				this.port = port;
				server = new Socket(ip, port);
				in = server.getInputStream();
			} catch (IOException e) {
				log.error(collectObjInfo + " : 接入线程出现异常", e);
			}
		}

		private boolean reAccess() {
			boolean b = false;
			try {
				server = new Socket(ip, port);
				in = server.getInputStream();
				b = true;
			} catch (IOException e) {
				log.error(collectObjInfo + " : 重新接入线程出现异常", e);
			}
			return b;
		}

		@Override
		public void run() {
			while (true) {
				try {
					if (in == null || server.isInputShutdown() || server.isClosed() || !server.isConnected()) {
						reAccess();
						log.debug(collectObjInfo + " ------------------ reAccess ----------------------");
					}

					synchronized (buffer) {
						while ((readCount = in.read(buffer)) != -1) {
							buffer.notifyAll();
							while (readCount > 0)
								buffer.wait();
						}
					}
				} catch (SocketException se) {
					log.error(collectObjInfo + "------------连接拒绝，或者服务器关闭，或者连接失败。----------");
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e1) {

					}
					boolean b = reAccess();
					if (b)
						log.debug(collectObjInfo + "------------------ reAccess succ at exception----------------------");
				} catch (Exception e) {
					log.error("", e);
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e1) {

					}
				}

			}
		}

	}

	/**
	 * 字符串str中是否包含containFlag字符串
	 * 
	 * @param str
	 * @param containFlag
	 * @return
	 */
	public boolean isContainFlag(String str, String containFlag) {
		boolean flag = false;
		str = str.trim();
		if (str == null || "".equals(str)) {
			return false;
		}
		String keyAndValue[] = str.split(splitFlag);
		if (keyAndValue.length > 0) {
			if (keyAndValue[0].trim().equals(containFlag)) {
				flag = true;
			}
		}
		return flag;
	}

	/**
	 * 取得第一个splitFlag标志后的字符串
	 * 
	 * @param lineData
	 * @return
	 */
	public String getValue(String lineData) {
		if (lineData == null || lineData.equals("")) {
			return "";
		}
		return lineData.substring(lineData.indexOf(splitFlag) + 1).trim();
	}

	/** 数据解析线程 */
	class SocketDataParser extends Thread {

		byte[] remainingBytes = null;

		private boolean flag = true;

		public SocketDataParser(String name) {
			super(name);
		}

		synchronized boolean getFlag() {
			return flag;
		}

		synchronized void shutdown() {
			this.flag = false;
		}

		public void clearBuffer() {
			// buffer = new byte[BUFFER_SIZE];
			readCount = 0;
		}

		private void handleData() throws InterruptedException {
			byte[] bytes = null;
			synchronized (buffer) {
				while (readCount <= 0)
					buffer.wait();

				// 从缓存中取出数据
				bytes = new byte[readCount];
				System.arraycopy(buffer, 0, bytes, 0, readCount);

				// 清空缓存
				clearBuffer();

				// 通知继续接入数据
				buffer.notifyAll();
			}

			// 解析数据业务逻辑
			parse(bytes);

			// 分发数据

			if (sqlList != null && sqlList.size() > 0) {
				distribute(sqlList);
			}

		}

		// 解析数据业务逻辑
		private void parse(byte[] data) {
			String sql = null;
			if (data == null)
				return;

			// 处理剩余未处理的字节集
			byte[] allData = data;
			int len = data.length;
			if (remainingBytes != null) {
				int rLen = remainingBytes.length;
				int newLen = len + rLen;
				allData = new byte[newLen];
				System.arraycopy(remainingBytes, 0, allData, 0, rLen);
				System.arraycopy(data, 0, allData, rLen, len);
			}

			boolean startFound = false;
			boolean endFound = false;

			int startPos = 0;
			int endPos = 0;
			int aLen = allData.length;
			for (int i = 0; i < aLen; i++) {
				byte b = allData[i];
				char c = (char) b;

				if (c == '<') {
					if ((i + 4) >= aLen)
						break;
					if (allData[i + 1] == '+' && allData[i + 2] == '+' && allData[i + 3] == '+' && allData[i + 4] == '>') {
						startFound = true;
						startPos = i + 4;
						i = startPos;
					} else if ((allData[i + 1] == '-' && allData[i + 2] == '-' && allData[i + 3] == '-' && allData[i + 4] == '>') && startFound) {
						endFound = true;
						endPos = i + 4;
						i = endPos;
					}
				} else
					continue;

				// 如果是一个合法的报文
				if (startFound && endFound) {
					int mLen = (endPos - startPos + 4) + 1;
					byte[] msgBytes = new byte[mLen];
					System.arraycopy(allData, startPos - 4, msgBytes, 0, mLen);
					startFound = endFound = false;
					// 处理一个合法的报文

					// 解析一个合法的报文
					sql = parseMsg(msgBytes);
					if (sql != null) {
						insertCount++;
						sqlList.add(sql);
						// System.out.println("sql 语句：： " + sql);
					}
				}
			}
			// 收集未处理的字节集
			int remainingLen = aLen - endPos;
			remainingBytes = new byte[remainingLen];
			System.arraycopy(allData, endPos, remainingBytes, 0, remainingLen);
		}

		/** 解析一个报文 */
		private String parseMsg(byte[] msgBytes) {
			String msg = new String(msgBytes);
			String[] msgTemp = msg.split(M2000AlarmTempletP.SPLIT);// "\r\n"
			String sql = null;
			if (msgTemp != null) {
				if (msgTemp.length > 1) {
					sql = handSql(msgTemp);
				} else if (msg.split("\n").length > 1) {
					sql = handSql(msgTemp);
				}
			}
			log.debug("task id = " + collectObjInfo.getTaskID() + " ,parseMsg field length. " + msgTemp.length);

			return sql;
		}

		// 分发数据，进行批量入库
		public void distribute(List<String> sqlList) {
			executeBatch(sqlList);
			sqlList.clear();
		}

		@Override
		public void run() {
			while (getFlag()) {
				try {
					handleData();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public String handSql(String[] msg) {
		Date currDate = new Date();
		int omcId = collectObjInfo.getDevInfo().getOmcID();
		if (currDate.getTime() > (upDate.getTime() + tenMinutePeriod)) {
			util.DBLogger.getInstance().log(omcId, tablename, upDate.getTime(), insertCount, collectObjInfo.getTaskID());
			insertCount = 0;
			upDate = new Date(upDate.getTime() + tenMinutePeriod);
		}
		String sdate = util.Util.getDateString(upDate);
		log.debug(collectObjInfo.getTaskID() + " " + sdate);

		String value = null;
		StringBuffer tableHead = new StringBuffer();
		StringBuffer filed = new StringBuffer();
		String pitchValue = null;
		StringBuffer sbSql = new StringBuffer();

		int count = 0;

		for (String lineData : msg) {
			lineData = lineData.trim();

			// if ( isContainFlag(lineData, M2000AlarmTempletP.ALARM_HANDSHAKE)
			// )
			// {
			// return null;
			// // tableHead.append(",ALARM_HANDSHAKE");
			// // value = getValue(lineData);
			// // filed.append("," + (value.length() == 0 ? 0 : value));
			// }
			// else
			// {

			if (util.Util.isNull(lineData))
				continue;

			if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_NUM)) {
				count++;
				tableHead.append(",ALARMNUM");
				value = getValue(lineData);
				filed.append("," + (value.length() == 0 ? 0 : value));
			} else if (isContainFlag(lineData, M2000AlarmTempletP.NETWORK_NUM)) {
				count++;
				tableHead.append(",NETWORKNUM");
				value = getValue(lineData);
				filed.append("," + (value.length() == 0 ? 0 : value));
			} else if (isContainFlag(lineData, M2000AlarmTempletP.OBJECT_IDCODE)) {
				count++;
				tableHead.append(",OBJECTIDCODE");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			} else if (isContainFlag(lineData, M2000AlarmTempletP.OBJECT_NAME)) {
				count++;
				tableHead.append(",OBJECTNAME");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			} else if (isContainFlag(lineData, M2000AlarmTempletP.OBJECT_TYPE)) {
				count++;
				tableHead.append(",OBJECTTYPE");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			}

			else if (isContainFlag(lineData, M2000AlarmTempletP.NETWORK_IDCODE)) {
				count++;
				tableHead.append(",NETWORKIDCODE");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			} else if (isContainFlag(lineData, M2000AlarmTempletP.NETWORK_NAME)) {
				count++;
				tableHead.append(",NETWORKNAME");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			} else if (isContainFlag(lineData, M2000AlarmTempletP.NETWORK_TYPE)) {
				count++;
				tableHead.append(",NETWORKTYPE");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			} else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_ID)) {
				count++;
				tableHead.append(",ALARMID");
				value = getValue(lineData);
				filed.append("," + (value.length() == 0 ? 0 : value));
			}
			//
			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_NAME)) {
				count++;
				tableHead.append(",ALARMNAME");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			} else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_CLASS_ID)) {
				count++;
				tableHead.append(",ALARMCLASSID");
				value = getValue(lineData);
				filed.append("," + (value.length() == 0 ? 0 : value));
			}
			//

			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_CLASS)) {
				count++;
				tableHead.append(",ALARMCLASS");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			}

			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_LEVEL_ID)) {
				count++;
				tableHead.append(",ALARMLEVELID");
				value = getValue(lineData);
				filed.append("," + (value.length() == 0 ? 0 : value));
			}

			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_LEVEL)) {
				count++;
				tableHead.append(",ALARMLEVEL");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			}

			else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_STATUS)) {
				count++;
				tableHead.append(",ALARMSTATUS");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			} else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_TYPE_ID)) {
				count++;
				tableHead.append(",ALARMTYPEID");
				value = getValue(lineData);
				filed.append("," + (value.length() == 0 ? 0 : value));
			} else if (isContainFlag(lineData, M2000AlarmTempletP.ALARM_TYPE)) {
				count++;
				tableHead.append(",ALARMTYPE");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			} else if (isContainFlag(lineData, M2000AlarmTempletP.BEGIN_TIME)) {
				count++;
				tableHead.append(",BEGINTIME");
				value = getValue(lineData);
				filed.append(",to_date('" + (value.length() == 0 ? defaultDate : value) + "','yyyy-MM-dd HH24:mi:ss')");
			} else if (isContainFlag(lineData, M2000AlarmTempletP.RESUME_DATE)) {
				count++;
				tableHead.append(",RESUMEDATE");
				value = getValue(lineData);
				filed.append(",to_date('" + (value.length() == 0 ? defaultDate : value) + "','yyyy-MM-dd HH24:mi:ss')");
			} else if (isContainFlag(lineData, M2000AlarmTempletP.CONFIRM_DATE)) {
				count++;
				tableHead.append(",CONFIRMDATE");
				value = getValue(lineData);
				filed.append(",to_date('" + (value.length() == 0 ? defaultDate : value) + "','yyyy-MM-dd HH24:mi:ss')");

			} else if (isContainFlag(lineData, M2000AlarmTempletP.PITCH_INFO)) {
				count++;
				tableHead.append(",PITCHINFO");
				pitchValue = lineData.substring(lineData.indexOf(splitFlag) + 1);
				filed.append(",'" + (value.length() == 0 ? "" : pitchValue.trim()) + "'");
			} else if (isContainFlag(lineData, M2000AlarmTempletP.OPERATOR)) {
				count++;
				tableHead.append(",OPERATOR");
				value = getValue(lineData);
				filed.append(",'" + (value.length() == 0 ? "" : value) + "'");
			}

			// }
		}
		if (count == 0)
			return null;

		sbSql.append(insertSql);
		sbSql.append("OMCID,");
		sbSql.append("COLLECTTIME,");
		sbSql.append("STAMPTIME");

		sbSql.append(tableHead);
		sbSql.append(")values(");

		sbSql.append(omcId + ",");// omcId
		sbSql.append("sysdate,");// 'yyyy-MM-dd HH24:mi:ss'
		sbSql.append(" to_date('" + sdate + "','yyyy-MM-dd HH24:mi:ss') ");

		sbSql.append(filed);
		sbSql.append(")");
		String sql = sbSql.toString();
		sbSql.delete(0, sbSql.length());
		return sql;
	}

	/* 执行批量sql */
	private void executeBatch(List<String> inserts) {
		Connection connection = DbPool.getConn();
		Statement statement = null;
		try {
			connection.setAutoCommit(false);
			statement = connection.createStatement();
			for (String sql : inserts) {
				statement.addBatch(sql);
			}
			statement.executeBatch();
			connection.commit();
			// inserts.clear();
		} catch (Exception e) {
			if (connection != null) {
				try {
					connection.rollback();
				} catch (SQLException sqlex) {
				}
			}
			log.error(collectObjInfo + ":插入数据时出现异常", e);
		} finally {
			try {
				if (statement != null) {
					statement.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// SocketCollect client = new SocketCollect("localhost", 8765);
		// client.access("localhost", 8765);
		String s = util.Util.getDateString(new Date());
		System.out.println(s);
	}

	@Override
	public boolean parseData() {
		return true;
	}

}

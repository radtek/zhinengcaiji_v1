package parser.hw.am;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.log4j.Logger;

import parser.Parser;
import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.LogMgr;
import util.Util;
import datalog.DataLogInfo;

/**
 * 华为告警日志文件 HWAlarmLog
 * 
 * @author liuwx May 31, 2011
 */
public abstract class HWAlarmLog extends Parser {

	protected char[] remainingBytes;

	protected static final String splitFlag = "=";

	protected static final String TABLENAME = "CLT_HW_M2000_ALARM_CHAR_STREAM";

	public static String sqlInsert = "insert into clt_hw_m2000_alarm_char_stream   (omcid, collecttime, stamptime, begintime, alarmnum, networknum, objectidcode, objectname, objecttype, networkidcode, networkname, networktype, alarmid, alarmname, alarmclassid, alarmclass, alarmlevelid, alarmlevel, alarmstatus, alarmtypeid, alarmtype, resumedate, confirmdate, pitchinfo, operator)values  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private Logger LOG = LogMgr.getInstance().getSystemLogger();

	protected Connection conn = null;

	protected int insertCount = 0;
	{

	}

	public void handSql(String data[]) {

		PreparedStatement preparedStatement = null;

		String sql = sqlInsert;
		try {
			if (conn == null || conn.isClosed()) {
				conn = DbPool.getConn();
			}
			Date begintime = null;// date
			long alarmnum = -1;// number
			long networknum = -1;// number y
			String objectidcode = null;// varchar2(200) y
			String objectname = null;// varchar2(200) y
			String objecttype = null;// varchar2(200) y
			String networkidcode = null;// varchar2(200) y
			String networkname = null;// varchar2(200) y
			String networktype = null;// varchar2(200) y
			long alarmid = -1;// number y
			String alarmname = null;// varchar2(200) y
			long alarmclassid = -1;// number y
			String alarmclass = null;// varchar2(200) y
			long alarmlevelid = -1;// number y
			String alarmlevel = null;// varchar2(200) y
			String alarmstatus = null;// varchar2(200)
			long alarmtypeid = -1;// number y
			String alarmtype = null;// varchar2(200) y
			Date resumedate = null;// date y
			Date confirmdate = null;// date y
			String pitchinfo = null;// varchar2(2000) y
			String operator = null;// varchar2(200) y

			for (String d : data) {
				if (util.Util.isNull(d))
					continue;
				d = d.trim();
				if (d.contains("设备告警流水号  =")) {
					alarmnum = Long.parseLong(d.substring(d.indexOf("=") + 1).trim());
				} else if (d.contains("网络流水号  =")) {
					networknum = Long.parseLong(d.substring(d.indexOf("=") + 1).trim());
				} else if (d.contains("对象标识  =")) {
					objectidcode = d.substring(d.indexOf("=") + 1).trim();
				} else if (d.contains("对象名称  =")) {
					objectname = d.substring(d.indexOf("=") + 1).trim();
				} else if (d.contains("对象类型  =")) {
					objecttype = d.substring(d.indexOf("=") + 1).trim();
				} else if (d.contains("网元标识  =")) {
					networkidcode = d.substring(d.indexOf("=") + 1).trim();
				} else if (d.contains("网元名称  =")) {
					networkname = d.substring(d.indexOf("=") + 1).trim();
				} else if (d.contains("网元类型  =")) {
					networktype = d.substring(d.indexOf("=") + 1).trim();
				} else if (d.contains("告警ID  =")) {
					alarmid = Long.parseLong(d.substring(d.indexOf("=") + 1).trim());
				} else if (d.contains("告警名称  =")) {
					alarmname = d.substring(d.indexOf("=") + 1).trim();
				} else if (d.contains("告警种类  =")) {
					alarmclass = d.substring(d.indexOf("=") + 1).trim();
				} else if (d.contains("告警种类ID  =")) {
					alarmclassid = Long.parseLong(d.substring(d.indexOf("=") + 1).trim());
				} else if (d.contains("告警级别ID  =")) {
					alarmlevelid = Long.parseLong(d.substring(d.indexOf("=") + 1).trim());
				}

				else if (d.contains("告警级别  =")) {
					alarmlevel = d.substring(d.indexOf("=") + 1).trim();
				} else if (d.contains("告警状态  =")) {
					alarmstatus = d.substring(d.indexOf("=") + 1).trim();
				} else if (d.contains("告警类型ID  =")) {
					alarmtypeid = Long.parseLong(d.substring(d.indexOf("=") + 1).trim());
				} else if (d.contains("告警类型  =")) {
					alarmtype = d.substring(d.indexOf("=") + 1).trim();
				} else if (d.contains("发生时间  =")) {
					begintime = new Date(Util.getDate1(d.substring(d.indexOf("=") + 1).trim()).getTime());
				} else if (d.contains("恢复时间  =")) {
					resumedate = new Date(Util.getDate1(d.substring(d.indexOf("=") + 1).trim()).getTime());
				} else if (d.contains("确认时间  =")) {
					confirmdate = new Date(Util.getDate1(d.substring(d.indexOf("=") + 1).trim()).getTime());
				} else if (d.contains("定位信息  =")) {
					pitchinfo = d.substring(d.indexOf("=") + 1).trim();
				} else if (d.contains("操作员  =")) {
					operator = d.substring(d.indexOf("=") + 1).trim();
				}

			}

			preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setInt(1, collectObjInfo.getDevInfo().getOmcID());
			preparedStatement.setTimestamp(2, new Timestamp(new java.util.Date().getTime()));
			preparedStatement.setTimestamp(3, new Timestamp(collectObjInfo.getLastCollectTime().getTime()));
			preparedStatement.setTimestamp(4, begintime == null ? null : new Timestamp(begintime.getTime()));
			preparedStatement.setLong(5, alarmnum);
			preparedStatement.setLong(6, networknum);
			preparedStatement.setString(7, objectidcode);
			preparedStatement.setString(8, objectname);
			preparedStatement.setString(9, objecttype);
			preparedStatement.setString(10, networkidcode);
			preparedStatement.setString(11, networkname);
			preparedStatement.setString(12, networktype);
			preparedStatement.setLong(13, alarmid);

			preparedStatement.setString(14, alarmname);
			preparedStatement.setLong(15, alarmclassid);
			preparedStatement.setString(16, alarmclass);
			preparedStatement.setLong(17, alarmlevelid);
			preparedStatement.setString(18, alarmlevel);
			preparedStatement.setString(19, alarmstatus);
			preparedStatement.setLong(20, alarmtypeid);
			preparedStatement.setString(21, alarmtype);
			preparedStatement.setTimestamp(22, resumedate == null ? null : new Timestamp(resumedate.getTime()));
			preparedStatement.setTimestamp(23, confirmdate == null ? null : new Timestamp(confirmdate.getTime()));
			preparedStatement.setString(24, pitchinfo);
			preparedStatement.setString(25, operator);
			preparedStatement.executeUpdate();
			insertCount++;
		} catch (Exception e) {
			LOG.error("记录数据库日志时异常,sql:" + sql, e);
			for (String s : data) {
				LOG.error(s);
			}
		} finally {
			CommonDB.close(null, preparedStatement, null);
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
	 * 处理缓冲区中的内容
	 * 
	 * @param data
	 * @param length
	 *            datas数组长度
	 */
	public void process(char[] data, int length) {
		// String sql = null;
		if (data == null)
			return;

		// 处理剩余未处理的字节集
		char[] allData = data;
		int len = data.length;
		if (remainingBytes != null) {
			int rLen = remainingBytes.length;
			int newLen = len + rLen;
			allData = new char[newLen];
			System.arraycopy(remainingBytes, 0, allData, 0, rLen);
			System.arraycopy(data, 0, allData, rLen, len);
		}

		boolean startFound = false;
		boolean endFound = false;

		int startPos = 0;
		int endPos = 0;
		int aLen = allData.length;
		for (int i = 0; i < aLen; i++) {
			char b = allData[i];
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
				char[] msgBytes = new char[mLen];
				System.arraycopy(allData, startPos - 4, msgBytes, 0, mLen);
				startFound = endFound = false;
				// 处理一个合法的报文

				// 解析一个合法的报文
				parseMsg(msgBytes);
			}
		}
		// 收集未处理的字节集
		int remainingLen = aLen - endPos;
		remainingBytes = new char[remainingLen];
		System.arraycopy(allData, endPos, remainingBytes, 0, remainingLen);
	}

	/** 解析一个报文 */
	private void parseMsg(char[] msgBytes) {
		String msg = new String(msgBytes);
		// LOG.debug("告警报文: "+msg);
		String[] msgTemp = msg.split("\r\n");
		handSql(msgTemp);

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

	public void dispose() {
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
			}
			conn = null;
		}
	}

	// add
	// 剩余数据
	String remainingData = "";

	// 计算采集的次数
	private int m_ParseTime = 0;

	public boolean buildAlData(char[] chData, int iLen, BufferedWriter bw) {
		boolean bReturn = true;

		remainingData += new String(chData, 0, iLen);

		String logStr = null;

		// 解析的次数
		if (++m_ParseTime % 100 == 0) {
			logStr = this + ": " + collectObjInfo.getDescribe() + " parse time:" + m_ParseTime;
			log.debug(logStr);
			collectObjInfo.log(DataLogInfo.STATUS_PARSE, logStr);
		}
		boolean bLastCharN = false;// 最后一个字符是\n
		if (remainingData.charAt(remainingData.length() - 1) == '\n')
			bLastCharN = true;

		// 分行
		String[] strzRowData = remainingData.split("\n");

		// 没有数据
		if (strzRowData.length == 0)
			return true;

		// 特殊标记表示达到最后一行
		int nRowCount = strzRowData.length - 1;
		remainingData = strzRowData[nRowCount];
		if (remainingData.equals("**FILEEND**"))
			remainingData = "";

		// 如果最后一个字符是\n 下次采集的时候,将是补上\n这个字符
		if (bLastCharN)
			remainingData += "\n";

		try {
			// 最后一行不解析,与下次数据一起解析
			for (int i = 0; i < nRowCount; ++i) {
				if (Util.isNull(strzRowData[i]))
					continue;
				bw.write(strzRowData[i] + "\n");
				bw.flush();
			}
		} catch (Exception e) {
			bReturn = false;
			logStr = this + ": Cause:";
			log.error(logStr, e);
			collectObjInfo.log(DataLogInfo.STATUS_PARSE, logStr, e);
		}

		return bReturn;
	}

	protected boolean commonParse() throws IOException {

		File f = new File(this.getFileName());
		if (!f.exists()) {
			LOG.debug("id=" + collectObjInfo.getKeyID() + " , " + getFileName() + "文件没有找到.");
			return false;
		}
		char[] bytes = new char[1024];
		String encode = "gbk";
		String en = collectObjInfo.getDevInfo().getEncode();
		if (util.Util.isNotNull(en)) {
			encode = en;
		}

		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(getFileName()), encode));

		int i = 0;
		while ((i = br.read(bytes, 0, bytes.length)) != -1) {
			process(bytes, i);
		}
		br.close();
		this.insertLogClt();
		LOG.debug("id=" + collectObjInfo.getKeyID() + " ," + util.Util.getDateString(collectObjInfo.getLastCollectTime()) + ",入库条数:" + insertCount);
		dispose();
		return true;

	}

	public void insertLogClt() {
		DBLogger.getInstance().logForHour(collectObjInfo.getDevInfo().getOmcID(), TABLENAME, collectObjInfo.getLastCollectTime(), insertCount,
				collectObjInfo.getKeyID(), true);
	}

	public static void main(String[] args) {

	}

}

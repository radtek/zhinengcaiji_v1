package parser.hw.am;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import util.DBLogger;
import util.DbPool;
import util.LogMgr;
import util.Util;

public class CV1LogSH extends Parser {

	/**
	 * 被解析的文件
	 */
	private String source = "";

	/**
	 * 采集任务的信息
	 */
	// private CollectObjInfo gatherObjInfo;
	// 一条记录开始和结束标记 ++ --- END
	private final static String startFlag = "+++";

	private final static String endFlag = "---    END";

	private final static String rule = "\\+\\+\\+(.+\r\n)+---    END";

	private String flag = ";";

	// 剩余未解析的字符串
	private String remainingData = "";

	private final static int BUFFER_SIZE = 65536;

	private final static String TABLE_NAME = "CLT_AM_ALARMHISTORY_HW";

	private final static String TABLE_FIELD = "OMCID,COLLECTTIME,STAMPTIME,NAME,EVENT_TIME,TYPE,IINDEX,ALARM_TYPE,ALARM_LEVEL,SORT,ALARM_ID,SYSTEM_TYPE,ALARM_NAME,ALARM_TEXT";

	private static final int INSERT_INTERVAL = 1000; // 执行commit动作的间隔条数

	private int count; // 计数，总共插入成功的条数

	private List<String> inserts = new ArrayList<String>(); // insert 语句

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	public CV1LogSH(CollectObjInfo TaskInfo) {
		super(TaskInfo);
	}

	public CV1LogSH() {
	}

	public boolean parseData() {
		try {
			parse();
		} catch (Exception e) {
			logger.error("解析数据时发生异常", e);
			return false;
		}
		return true;
	}

	public void parse() throws Exception {
		this.source = getFileName();
		FileReader reader = new FileReader(source);
		char[] buff = new char[BUFFER_SIZE];

		int len = 0;
		while ((len = reader.read(buff)) > 0) {
			parseData(buff, len);
		}
		executeBatch();
		reader.close();
		dbLogger.log(collectObjInfo.getDevInfo().getOmcID(), TABLE_NAME, Util.getDateString(collectObjInfo.getLastCollectTime()), count,
				collectObjInfo.getTaskID());
		count = 0;
	}

	private void parseData(char[] data, int length) throws Exception {

		remainingData += new String(data, 0, length);
		String digData = remainingData;

		int endFlagNum = remainingData.lastIndexOf(endFlag) + endFlag.length();
		// 剩余数据
		remainingData = remainingData.substring(endFlagNum, remainingData.length());
		// 完整可以挖掘的数据
		String ruleData = digData.substring(0, endFlagNum);

		regFind(ruleData, rule);
	}

	public void regFind(String str, String regEx) {
		String resultValue = "";
		if (Util.isNull(regEx)) {
			return;
		}
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);

		int count = 0;
		while (m.find()) {
			count++;
			resultValue = m.group();// 找出匹配的结果

			// 挖掘一条记录数据
			String sql = digData(resultValue);
			// 批量提交
			inserts.add(sql);
			if (inserts.size() % INSERT_INTERVAL == 0) {
				executeBatch();
			}
		}
	}

	// 挖掘数据
	public String digData(String data) {
		String rule = "\\s+";
		String ruleData = data;
		// 截取掉头尾标记
		ruleData = ruleData.substring(startFlag.length(), ruleData.length() - endFlag.length());
		String array[] = ruleData.trim().split("\n", 4);
		String value1[] = null;
		String value2[] = null;
		String value3[] = null;
		String value4[] = null;
		StringBuffer buff = new StringBuffer();
		for (int i = 0; i < array.length; i++) {
			switch (i) {
				case 0 :
					value1 = array[0].split(rule, 2);
					for (String s : value1) {
						buff.append(s).append(flag);
					}
					break;
				case 1 :
					value2 = array[1].split(rule);
					for (String s : value2) {
						buff.append(s).append(flag);
					}
					break;
				case 2 :

					value3 = array[2].split("=", 2);
					// System.out.println("3**********"+value3[1]);

					buff.append(value3[1]).append(flag);
					break;
				case 3 :
					value4 = array[3].split("=", 2);
					buff.append(value4[1]);
					break;

			}

		}

		String resultValue = replceStr(buff.toString());
		String inserValue[] = resultValue.split(flag);
		StringBuffer insertValueBuff = new StringBuffer();
		for (int i = 0; i < inserValue.length; i++) {
			if (i == 1) {
				insertValueBuff.append("to_date('" + inserValue[i] + "','YYYY-MM-DD HH24:MI:SS')").append(",");
			} else {
				insertValueBuff.append("'" + inserValue[i] + "',");
			}
		}

		String resultInsertValue = subEndString(insertValueBuff.toString());

		StringBuffer sql = new StringBuffer();
		sql.append("insert into ");
		sql.append(TABLE_NAME + "(" + TABLE_FIELD + ")");
		sql.append(" values (");

		sql.append(collectObjInfo.getDevInfo().getOmcID()).append(
				", sysdate,to_date('" + Util.getDateString(collectObjInfo.getLastCollectTime()) + "','YYYY-MM-DD HH24:MI:SS'),");
		sql.append(resultInsertValue).append(")");

		return sql.toString();
	}

	/* 执行量insert */
	private void executeBatch() {
		Connection con = DbPool.getConn();
		Statement statement = null;
		if (con == null) {
			logger.error("Task-" + collectObjInfo.getTaskID() + "Connection 连接失败");
			return;
		}
		try {
			con.setAutoCommit(false);
			statement = con.createStatement();
			for (String sql : inserts) {
				statement.addBatch(sql);
			}

			statement.executeBatch();
			count += inserts.size();
			inserts.clear();
			con.commit();
		} catch (Exception e) {
			logger.error("批量提交错误", e);
		} finally {
			try {
				logger.debug("Task-" + collectObjInfo.getTaskID() + ": 插入成功数量：" + count);
				if (statement != null) {
					statement.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
			}
		}
	}

	/** 去掉字符串里的\r\n */
	public String replceStr(String str) {
		StringBuffer sb = new StringBuffer();
		for (char s : str.toCharArray()) {
			if (s == '\r' || s == '\n') {
				continue;
			} else {
				sb.append(s);
			}
		}
		return sb.toString();
	}

	/** 截取掉最后一个字符串 */
	public static String subEndString(String str) {
		String strvalue = "";

		if (str != null && !str.equals("")) {
			strvalue = str.substring(0, str.length() - 1);
		}

		return strvalue;
	}

}

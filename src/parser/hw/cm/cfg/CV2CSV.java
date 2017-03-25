package parser.hw.cm.cfg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import parser.Parser;

import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.LogMgr;
import util.Util;

public class CV2CSV extends Parser {

	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	private Map<String, List<String>> allDatas = null; // 保存所有数据

	private List<String> inserts = new ArrayList<String>(); // insert 语句

	private static final int INSERT_INTERVAL = 1000; // 执行commit动作的间隔条数

	private int count; // 计数，总共插入成功的条数

	String source = "";

	public CV2CSV() {
		allDatas = new HashMap<String, List<String>>();
	}

	@Override
	public boolean parseData() {
		try {
			parse();
		} catch (Exception e) {
			logger.error("解析数据时发生异常", e);
			return false;
		}
		return true;
	}

	// 解析数据

	@SuppressWarnings({"rawtypes", "unchecked"})
	public void parse() {
		source = getFileName();
		int omcId = collectObjInfo.getDevInfo().getOmcID();
		int insertCount = 0;
		List<String> list = new ArrayList<String>();
		String strLine = null;
		File file = new File(source);
		BufferedReader reader = null;
		int line = 1;
		try {
			reader = new BufferedReader(new FileReader(file));
			while ((strLine = reader.readLine()) != null) {
				if (!strLine.equals("")) {

					// System.out.println(line + " == " + strLine);
					list.add(strLine);

					if (list.size() == 2) {

						addData(list.get(0), list.get(1));
						list.clear();
					}
					line++;
				}

			}
			Iterator<Entry<String, List<String>>> it = allDatas.entrySet().iterator();
			int num = 1;
			while (it.hasNext()) {
				Map.Entry entry = (Map.Entry) it.next();
				String key = (String) entry.getKey();
				String strKey = key;
				List<String> value = (List) entry.getValue();
				insertCount = value.size();
				String insertStrKey = dealInsertTableField(strKey) + "OMCID ,COLLECTTIME ,STAMPTIME";
				boolean bool = true;
				String resultTableName = "";
				String fieldName = "";

				for (String strlist : value) {
					String createValue = strlist;
					String insertValue = "";
					if (strlist.contains("'")) {
						insertValue = dealInsertValue_one(strlist);
					} else {
						insertValue = dealInsertValue(strlist);
					}
					// String insertValue = dealInsertValue(strlist);
					if (bool) {
						// 取得创建表的表名
						resultTableName = getClassName(key, createValue);

						// 取得创建表字段
						fieldName = dealCreateTableField(key);
						// System.out.println("+++++++"+resultTableName +"
						// "+fieldName);
						// 创建表
						// createTable(resultTableName,fieldName);
						// bool =false;

						// 把表名相同而字段少的添加上
						String resultFieldName = "";
						// if(resultTableName.equals("CBSCBTSLink")){
						resultFieldName = subEndString(addTableFieldOne(resultTableName, fieldName));
						// 创建表
						if (resultTableName.length() > 20) {
							resultTableName = resultTableName.substring(resultTableName.length() - 20, resultTableName.length());
						}
						createTable("CLT_CM_" + resultTableName + "_HW", resultFieldName);

						// fw.write("表名：
						// "+resultTableName+"\n"+fieldName+"\n\n");
						// fw.flush();
						dbLogger.log(omcId, "CLT_CM_" + resultTableName + "_HW", Util.getDateString(collectObjInfo.getLastCollectTime()),
								insertCount, collectObjInfo.getTaskID());
						// 创建表
						// createTable(resultTableName,fieldName);
						bool = false;
					}

					StringBuffer sql = new StringBuffer();
					sql.append("insert into ");
					sql.append("CLT_CM_" + resultTableName + "_HW" + "(" + insertStrKey + ")");
					sql.append(" values (");
					sql.append(insertValue);
					sql.append(collectObjInfo.getDevInfo().getOmcID()).append(
							", sysdate,to_date('" + Util.getDateString(collectObjInfo.getLastCollectTime()) + "','YYYY-MM-DD HH24:MI:SS'))");
					inserts.add(sql.toString());
					// System.out.println("***"+sql.toString());
					// System.out.println(inserts.size());
					executeInsert(false);
				}
				num++;
			}
			executeInsert(true);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			allDatas.clear();
		}

	}

	// 把字段相同的数据添加到同一个集合里
	private void addData(String fields, String datas) {
		if (fields == null || datas == null || fields.equals("") || datas.equals("")) {
			return;
		}
		if (allDatas == null) {
			List<String> list = new ArrayList<String>();
			list.add(datas);
			allDatas.put(fields, list);
		} else if (allDatas.containsKey(fields)) {
			List<String> list = allDatas.get(fields);
			list.add(datas);
		} else {
			List<String> list = new ArrayList<String>();
			list.add(datas);
			allDatas.put(fields, list);
		}
	}

	// 处理insert插入语句中value的值
	/*
	 * private static String dealInsertValue(String str){ String[] value =str.split(","); StringBuffer buff = new StringBuffer(); for (String s :
	 * value){ buff.append("'"+s+"'"+","); } return buff.toString(); }
	 */

	// 处理insert插入语句中value的值
	public static String dealInsertValue(String text) {
		String str = text;
		// str=str+",";
		List<String> strList = new ArrayList<String>();
		StringBuffer sb = new StringBuffer();
		boolean flag = false;
		for (char s : str.toCharArray()) {
			if ((s == ',' || s == '，') && flag == false) {
				strList.add(sb.toString());
				sb = new StringBuffer();
				continue;
			}
			if (s == '\"') {
				if (flag == true)
					flag = false;
				else
					flag = true;
				continue;
			}
			sb.append(s);
		}
		String strArray[] = strList.toArray(new String[strList.size()]);
		String strValue = "";
		for (String ss : strArray) {
			strValue += "'" + ss + "',";
		}
		return strValue;
	}

	// String str = "140,4,DBS3900
	// CDMA基站,4,39D50'48\"N,116D19'57\"E,5_140,129.18.192.140,140,2,0,CBSCCBTS,.3221229568.3221233664.3221282821.3221397509.3221442566,南粤苑,5,5_CbscEquipment,";
	// 处理insert插入语句中value的值
	public static String dealInsertValue_one(String str) {
		String[] value;
		if (str.contains("CBSCCBTS")) {
			String resutlValue = dealValueTwo(str);
			value = resutlValue.split("~");
		} else {
			value = str.split(",");
		}
		// String resutlValue =dealValueTwo(str);
		// String [] Value = resutlValue.split("~");
		String strValue = "";
		for (String ss : value) {
			if (ss.contains("'")) {
				// 39D50'48"N
				strValue += "'" + ss.replace("'", "\'\'") + "',";
			} else {
				strValue += "'" + ss + "',";
			}

		}
		return strValue;

	}

	public static String dealValueTwo(String value) {
		String str = value;
		List<String> strList = new ArrayList<String>();
		StringBuffer sb = new StringBuffer();
		boolean flag = false;
		char[] ch = str.toCharArray();
		for (int i = 0; i < ch.length; i++) {
			if ((ch[i] == ',' || ch[i] == '，') && flag == false) {
				strList.add(sb.toString() + "~");
				sb = new StringBuffer();
				continue;
			}
			if ((ch[i] == '\"' && ch[i - 1] == ',') || (ch[i] == '\"' && ch[i + 1] == ',')) {
				if (flag == true)
					flag = false;
				else
					flag = true;
				continue;
			}
			sb.append(ch[i]);
		}
		String strArray[] = strList.toArray(new String[strList.size()]);
		String strValue = "";
		for (String ss : strArray) {
			strValue += ss;
		}
		return strValue;
	}

	// 处理crate 创建表语句中字段
	private static String dealCreateTableField(String str) {
		String[] field = str.split(",");
		String strField = "";
		StringBuffer buff = new StringBuffer();
		for (String s : field) {
			if (s.equals("TO")) {
				buff.append(s + "1 varchar(200),");
			} else if (s.equals("CLASSNAME")) {
				buff.append(s + "1 varchar(200),");
			} else if (s.equals("omcID")) {
				buff.append(s + "1 varchar(200),");
			} else {
				buff.append(s + " varchar(200),");
			}

		}
		strField = buff.toString() + "OMCID NUMBER,COLLECTTIME DATE,STAMPTIME DATE";
		return strField;
	}

	// 处理insert表语句中字段
	private static String dealInsertTableField(String str) {
		String[] field = str.split(",");
		String strField = "";
		StringBuffer buff = new StringBuffer();
		for (String s : field) {
			if (s.equals("CLASSNAME")) {
				buff.append(s + "1,");
			} else if (s.equals("TO")) {
				buff.append(s + "1,");
			} else if (s.equals("omcID")) {
				buff.append(s + "1,");
			} else {
				buff.append(s + ",");
			}

		}
		strField = buff.toString();
		return strField;
	}

	/* 判断是否要执行insert */
	private void executeInsert(boolean now) throws Exception {
		if (!now) {
			if (inserts.size() % INSERT_INTERVAL == 0) {
				executeBatch();
			}
		} else {
			executeBatch();
		}
	}

	/* 执行量insert */
	private void executeBatch() throws Exception {
		Connection con = DbPool.getConn();
		Statement statement = null;

		try {
			con.setAutoCommit(false);
			statement = con.createStatement();
			// FileWriter fw = null;
			// fw = new FileWriter(new File("D:\\sql\\sql.txt"));
			for (String sql : inserts) {

				/*
				 * try { // fw.write(sql+"\n"); } catch (FileNotFoundException e1) { e1.printStackTrace(); } catch (IOException e) {
				 * e.printStackTrace(); }
				 */
				// System.out.println("**********"+sql);
				// logger.debug("SQL语句：" + sql);
				statement.addBatch(sql);
			}

			statement.executeBatch();
			logger.debug("TaskID-" + collectObjInfo.getTaskID() + ": 批量插入成功，数量：" + inserts.size());
			count += inserts.size();
			inserts.clear();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// logger.debug("本文件共插入成功，数量：" +count);
				con.commit();
				statement.close();
				con.close();
			} catch (Exception e) {
			}
		}
	}

	// 根据字段className的索引找到value中的值
	public static String getClassName(String name, String value) {
		String[] strName = name.split(",");
		String[] strValue = null;
		if (value.contains("'")) {
			strValue = value.split(",");
		} else {
			String resutlValue = dealValue(value);
			strValue = resutlValue.split("~");
		}

		int num = -1;
		for (int i = 0; i < strName.length; i++) {
			if (strName[i].equals("className")) {
				num = i;
				break;
			}
		}

		return strValue[num];
	}

	public static String dealValue(String value) {
		String str = value;
		List<String> strList = new ArrayList<String>();
		StringBuffer sb = new StringBuffer();
		boolean flag = false;
		for (char s : str.toCharArray()) {
			if ((s == ',' || s == '，') && flag == false) {
				strList.add(sb.toString() + "~");
				sb = new StringBuffer();
				continue;
			}
			if (s == '\"') {
				if (flag == true)
					flag = false;
				else
					flag = true;
				continue;
			}
			sb.append(s);
		}
		String strArray[] = strList.toArray(new String[strList.size()]);
		String strValue = "";
		for (String ss : strArray) {
			strValue += ss;
		}
		return strValue;
	}

	// //把表名相同而字段少的添加上 特殊处理CBSCBTSLink表
	public static String addTableFieldOne(String tableName, String field) {
		String oldField = field;
		String s = field;
		String newline = "";
		if (tableName.equals("CBSCBTSLink")) {
			if (!s.contains("OMIPaddr")) {
				newline += "OMIPaddr varchar(200),";
			}
			if (!s.contains("ConnectType")) {
				newline += "ConnectType varchar(200),";
			}
		}
		if (tableName.equals("CBSCSignallingPoint")) {
			if (!s.contains("SpcLength")) {
				newline += "SpcLength varchar(200),";
			}
		}
		if (tableName.equals("CBSCCBTS")) {
			// Latitude,Longitude,
			if (!s.contains("Latitude")) {
				newline += "Latitude varchar(200),";
			}
			if (!s.contains("Longitude")) {
				newline += "Longitude varchar(200),";
			}
		}
		if (tableName.equals("CBSCBtsPhysicalLink")) {

			if (!s.contains("LOCALSECTORID")) {
				newline += "LOCALSECTORID varchar(200),";
			}
			if (!s.contains("ConnectionMode")) {
				newline += "ConnectionMode varchar(200),";
			}
		}
		if (tableName.equals("CBSCBTSCarrier")) {

			if (!s.contains("LocalSectorID")) {
				newline += "LocalSectorID varchar(200),";
			}
		}
		// CBSCBTSCarrier
		String resutlline = oldField + "," + newline;

		return resutlline;
	}

	// 创建表
	protected static boolean createTable(String tableName, String sqlValue) {
		StringBuilder buffer = new StringBuilder();
		buffer.append("create table ").append(tableName);
		buffer.append(" (" + sqlValue + ")");
		try {
			String sql = buffer.toString();
			CommonDB.executeUpdate(sql);
			return true;
		} catch (Exception e) {
			if (e instanceof SQLException) {
				SQLException sqle = (SQLException) e;
				if (sqle.getErrorCode() == 955) {
					logger.debug("数据库中已存在此表:" + tableName);
					return true;
				}
			}
			logger.error("创建表时失败，表名：" + tableName, e);
			return false;
		} finally {
			buffer.delete(0, buffer.length());
		}
	}

	/** 截取掉最后一个字符串 */
	public static String subEndString(String str) {
		String strvalue = "";

		if (str != null && !str.equals("")) {
			strvalue = str.substring(0, str.length() - 1);
		}

		return strvalue;
	}

	public static void main(String[] args) {
		CV2CSV config = new CV2CSV();
		// String filePath = "e:\\ftp_root\\test\\config11.csv";
		CollectObjInfo info = new CollectObjInfo(1);
		DevInfo dev = new DevInfo();
		info.setDevInfo(dev);
		dev.setOmcID(1234);
		info.setLastCollectTime(new Timestamp(11));
		config.parse();

	}

}

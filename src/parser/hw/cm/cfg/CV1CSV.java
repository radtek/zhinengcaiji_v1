package parser.hw.cm.cfg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DBLogger;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

public class CV1CSV extends Parser {

	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	private Map<String, List<String>> allDatas = null; // 保存所有数据

	String source = "";

	private final Map<String, SqlldrInfo> SQLLDR_INFOS = new HashMap<String, SqlldrInfo>();

	private boolean isSPAS = SystemConfig.getInstance().isSPAS();

	String omcId;

	public CV1CSV() {
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
	public void parse() {
		source = getFileName();
		List<String> list = new ArrayList<String>();
		String strLine = null;
		File file = new File(source);
		BufferedReader reader = null;
		Connection destConn = null;
		PreparedStatement descPs = null;
		ResultSet destRs = null;
		destConn = CommonDB.getConnection();
		omcId = String.valueOf(collectObjInfo.getDevInfo().getOmcID());
		if (isSPAS) {
			log.debug("isSPAS=true");
			String nfileName = FilenameUtils.normalize(this.fileName);
			String zip = collectObjInfo.filenameMap.get(nfileName);
			if (zip == null || !zip.contains("_PARA_")) {
				log.warn(collectObjInfo.getTaskID() + " 文件" + nfileName + "，未找到对应的原始压缩包名。list=" + collectObjInfo.filenameMap);
			} else {
				zip = FilenameUtils.getBaseName(zip);
				String[] sp = zip.split("_");
				omcId = sp[5] + sp[6];
			}
		}
		try {
			reader = new BufferedReader(new FileReader(file));
			while ((strLine = reader.readLine()) != null) {
				if (!strLine.equals("")) {
					list.add(strLine);

					if (list.size() == 2) {
						addData(list.get(0), list.get(1));
						list.clear();
					}
				}
			}
			Iterator<Entry<String, List<String>>> it = allDatas.entrySet().iterator();

			while (it.hasNext()) {
				Map.Entry<String, List<String>> entry = (Map.Entry<String, List<String>>) it.next();
				String key = (String) entry.getKey();
				String strKey = key;
				List<String> value = (List<String>) entry.getValue();
				String insertStrKey = dealInsertTableField(strKey);
				boolean bool = true;
				String resultTableName = "";

				List<Integer> indexList = null;
				List<String> tmpHeads = null;
				List<String> sqlValue = new ArrayList<String>();
				String tabName = null;
				StringBuilder sb1 = new StringBuilder();
				for (String strlist : value) {
					String createValue = strlist;
					String insertValue = "";
					if (strlist.contains("'")) {
						insertValue = dealInsertValue_one(strlist);
					} else {
						insertValue = dealInsertValue(strlist);
					}
					if (bool) {
						resultTableName = getClassName(key, createValue);

						if (resultTableName.length() > 20) {
							resultTableName = resultTableName.substring(resultTableName.length() - 20, resultTableName.length());
						}

						tabName = "CLT_CM_" + resultTableName + "_HW";

						if (isSPAS) {
							if (tabName.equalsIgnoreCase("clt_cm_cbsccbts_hw"))
								tabName = "DS_CLT_CM_CBSCCBTS_HW";
							else if (tabName.equalsIgnoreCase("clt_cm_cbscg3sector_hw"))
								tabName = "DS_CLT_CM_CBSCG3SECTOR_HW";
							else if (tabName.equalsIgnoreCase("clt_cm_cbscg3pilot_hw"))
								tabName = "DS_CLT_CM_CBSCG3PILOT_HW";
							else
								continue;
						}

						String sql = "select * from " + tabName + " where 1=0";

						try {
							descPs = destConn.prepareStatement(sql);
							destRs = descPs.executeQuery();

							ResultSetMetaData destMeta = destRs.getMetaData(); // 采集表结构

							int destColCount = destMeta.getColumnCount();
							String[] srcColName = insertStrKey.split(",");

							indexList = new ArrayList<Integer>(srcColName.length);
							tmpHeads = new ArrayList<String>(srcColName.length);

							for (int i = 0; i < srcColName.length; i++)
								tmpHeads.add(null);

							for (int i = 0; i < srcColName.length; i++) {
								indexList.add(i, null);
							}

							for (int j = 1; j <= destColCount; j++) {
								String destColName = destMeta.getColumnName(j).toUpperCase();

								for (int i = 0; i < srcColName.length; i++) {
									if (destColName.equals(srcColName[i].toUpperCase())) {
										tmpHeads.set(i, destColName);
										indexList.set(i, i);
									}
								}
							}

							// 检查哪些字段对应表中不存在，并记录日志
							List<String> tmp = new ArrayList<String>();
							for (int n = 0; n < indexList.size(); n++) {
								Integer v = indexList.get(n);
								if (v != null)
									continue;
								tmp.add(srcColName[n]);
							}
							if (tmp.size() > 0)
								log.debug(this.getCollectObjInfo().getTaskID() + "表" + tabName + " 不存在字段为  " + tmp + "  请确认是否需要在表中添加字段");

						} catch (Exception e) {
							log.error(this.getCollectObjInfo().getTaskID() + " " + tabName + "获取元数据失败", e);
						} finally {
							if (destRs != null)
								destRs.close();
							if (descPs != null)
								descPs.close();

						}

						bool = false;
					}
					StringBuilder valResult = new StringBuilder();
					if (indexList != null && !indexList.isEmpty()) {
						String[] values = insertValue.split(";");
						for (int n = 0; n < indexList.size(); n++) {
							Integer v = indexList.get(n);
							if (v == null)
								continue;
							valResult.append(values[v] + ";");
						}
					}

					sqlValue.add(valResult.toString());

					valResult.delete(0, valResult.length());

				}
				if (isSPAS) {
					if (!tabName.startsWith("DS_"))
						continue;
				}
				for (String s : tmpHeads) {
					if (Util.isNotNull(s))
						sb1.append(s).append(",");
				}
				sb1.deleteCharAt(sb1.toString().length() - 1);
				createSqlldr(sb1.toString(), sqlValue, tabName);
				sqlValue.clear();
				sb1.delete(0, sb1.length());
				runSqlldr();
				SQLLDR_INFOS.clear();
			}
		} catch (Exception e) {
			logger.error("华为cdma配置，解析时出错 - " + fileName, e);
		} finally {
			allDatas.clear();
			try {
				if (destConn != null)
					destConn.close();
			} catch (SQLException e) {

			}
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
			// strValue += "'" + ss + "';";
			strValue += ss + ";";
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
				// strValue += "'" + ss.replace("'", "\'\'") + "';";
				strValue += ss.replace("'", "\'\'") + ";";
			} else {
				// strValue += "'" + ss + "';";
				strValue += ss + ";";
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

	/*
	 * // 处理crate 创建表语句中字段 private static String dealCreateTableField(String str) { String[] field = str.split(","); String strField = "";
	 * StringBuffer buff = new StringBuffer(); for (String s : field) { if ( s.equals("TO") ) { buff.append(s + "1 varchar(200),"); } else if (
	 * s.equals("CLASSNAME") ) { buff.append(s + "1 varchar(200),"); } else if ( s.equals("omcID") ) { buff.append(s + "1 varchar(200),"); } else {
	 * buff.append(s + " varchar(200),"); } } strField = buff.toString() + "OMCID NUMBER,COLLECTTIME DATE,STAMPTIME DATE"; return strField; }
	 */
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
					// logger.debug("数据库中已存在此表:" + tableName);
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

	private void createSqlldr(String key, List<String> list, String tabName) {
		SqlldrInfo sqlldrInfo = new SqlldrInfo();

		String currDate = Util.getDateString_yyyyMMddHHmmssSSS(new Date());
		String name = SystemConfig.getInstance().getCurrentPath() + "\\" + collectObjInfo.getTaskID() + "_" + currDate + "_" + tabName;
		sqlldrInfo.cltFile = name + ".ctl";
		sqlldrInfo.logFile = name + ".log";
		sqlldrInfo.badFile = name + ".bad";
		sqlldrInfo.txtFile = name + ".txt";
		File f = new File(sqlldrInfo.cltFile);
		try {
			PrintWriter pw = new PrintWriter(f);
			pw.println("load data");
			pw.println("CHARACTERSET ZHS16GBK ");
			pw.println("infile '" + sqlldrInfo.txtFile + "' append into table " + tabName);
			pw.println("FIELDS TERMINATED BY \";\"");
			pw.println("TRAILING NULLCOLS");
			pw.print("(" + key + ",");
			pw.print("OMCID,COLLECTTIME Date 'YYYY-MM-DD HH24:MI:SS',STAMPTIME Date 'YYYY-MM-DD HH24:MI:SS'");
			pw.print(")");
			pw.flush();
			pw.close();
		} catch (Exception e) {
			logger.error(" ", e);
		}
		SQLLDR_INFOS.put(tabName, sqlldrInfo);
		FileWriter fw = null;
		try {

			fw = new FileWriter(new File(sqlldrInfo.txtFile), true);

			fw.write(key.replace(",", ";") + "\n");
			StringBuilder tmp = new StringBuilder();
			tmp.append(omcId).append(";").append(Util.getDateString(new Date())).append(";")
					.append(Util.getDateString(collectObjInfo.getLastCollectTime()));
			for (String val : list) {
				fw.write(val.replace("&", ";") + tmp.toString() + ";" + "\n");
			}

			fw.flush();
			fw.close();
		} catch (Exception e) {
			logger.error(" ", e);
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
				}
			}
		}

	}

	private void runSqlldr() {
		String strOracleBase = SystemConfig.getInstance().getDbService();
		String strOracleUserName = SystemConfig.getInstance().getDbUserName();
		String strOraclePassword = SystemConfig.getInstance().getDbPassword();
		Iterator<String> keys = SQLLDR_INFOS.keySet().iterator();
		while (keys.hasNext()) {
			SqlldrInfo info = SQLLDR_INFOS.get(keys.next());
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999", strOracleUserName, strOraclePassword,
					strOracleBase, info.cltFile, info.badFile, info.logFile);
			ExternalCmd externalCmd = new ExternalCmd();
			externalCmd.setCmd(cmd);
			int ret = -1;
			try {
				ret = externalCmd.execute();
			} catch (Exception e) {
				logger.error("", e);
			}
			SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
			try {
				SqlldrResult result = analyzer.analysis(new FileInputStream(info.logFile));
				if (result == null)
					return;

				log.debug(collectObjInfo.getSysName() + ": SQLLDR日志分析结果:ret=" + ret + ", omcid=" + omcId + " 表名=" + result.getTableName() + " 数据时间="
						+ Util.getDateString(collectObjInfo.getLastCollectTime()) + " 入库成功条数=" + result.getLoadSuccCount() + " sqlldr日志="
						+ info.logFile);

				dbLogger.log(Integer.parseInt(omcId), result.getTableName(), collectObjInfo.getLastCollectTime().getTime(),
						result.getLoadSuccCount(), collectObjInfo.getTaskID());
			} catch (Exception e) {
				log.error(collectObjInfo.getSysName() + ": sqlldr日志分析失败，文件名：" + info.logFile + "，原因: ", e);
			}

			// 是否删除日志
			if (SystemConfig.getInstance().isDeleteLog() && ret == 0) {
				deleteLog(info);
			}
		}
		SQLLDR_INFOS.clear();

	}

	private void deleteLog(SqlldrInfo info) {

		// 删除.CTL
		File ctlfile = new File(info.cltFile);
		if (ctlfile.exists())
			ctlfile.delete();

		// 删除.txt文件
		File txtfile = new File(info.txtFile);
		if (txtfile.exists()) {
			txtfile.delete();

		}

		// 删除日志文件
		File txtlog = new File(info.logFile);
		if (txtlog.exists())
			txtlog.delete();

	}

	class SqlldrInfo {

		String txtFile;

		String logFile;

		String badFile;

		String cltFile;
	}

	public static void main(String[] args) {
		CV1CSV config = new CV1CSV();
		// String filePath = "e:\\ftp_root\\test\\config11.csv";
		config.isSPAS = true;
		CollectObjInfo info = new CollectObjInfo(1);

		DevInfo dev = new DevInfo();
		info.setDevInfo(dev);
		dev.setOmcID(1234);
		config.setCollectObjInfo(info);
		info.setLastCollectTime(new Timestamp(11));
		config.setFileName("F:\\liang\\tmp\\2013_1\\0114-0118\\北京\\CMExport_BSC1_136.37.76.4_2013011715.csv");
		config.parse();

	}

}

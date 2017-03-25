package parser.hw.cm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.DBLogger;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * CV1CSV 北京电信华为参数解析
 * 
 * @author
 * @version 1.0.0 1.0.1 liangww 2012-06-07 删除allData成员，增加忽略表set,用于过滤不要入库的表
 */
public class CV1CSV extends Parser {

	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	List<String> list = new ArrayList<String>();

	private final Map<String, SqlldrInfo> SQLLDR_INFOS = new HashMap<String, SqlldrInfo>();

	private String omcId;

	private boolean isSPAS = SystemConfig.getInstance().isSPAS();

	private static char tmpSplit = '~';

	// liangww add 2012-06-07
	private Set<String> ignoreTableSet = new TreeSet<String>(); // 忽略的表名set

	public CV1CSV() {
		// 初始化 liangww add 2012-06-07
		ignoreTableSet.add("CLT_CM_NBRBSCFUNC_HW");
		ignoreTableSet.add("CLT_CM_TIME14_HW");
		ignoreTableSet.add("CLT_CM_OFF_HW");

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
	@SuppressWarnings({"unused"})
	public synchronized void parse() throws Exception {
		
		BufferedReader reader = null;
		try {
			String source = getFileName();
			// String source ="e:\\ftp_root\\test\\cm.csv";

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

			int insertCount = 0;
			File file = new File(source);
			String strLine = null;
			int num = 1;
			String mapkey = "";
			FileReader fr = new FileReader(file);
			reader = new BufferedReader(fr);
			// CSVParser csv = new CSVParser(',', '\"');
			String lastListClassName = null;
			String lastListMapKey = null;
			int lastListMapKeyLength = 0;
			String tmpClassName = null;
			String tmpMapKey = null;
			int tmpMapKeyLength = 0;
			while ((strLine = reader.readLine()) != null) {
				if (!strLine.equals("") && strLine != null) {
					if (strLine.lastIndexOf(",") == strLine.length() - 1)
						strLine = strLine.substring(0, strLine.length() - 1);
					if (num == 1) {
						mapkey = strLine;
						tmpMapKeyLength = getColumnNum(mapkey);
					} else {
						// 初始化
						if (lastListClassName == null) {
							lastListClassName = getClassName(mapkey, strLine);
						}
						if (lastListMapKey == null) {
							lastListMapKey = mapkey;
							lastListMapKeyLength = getColumnNum(mapkey);
						}
						if (num == 2) {
							tmpClassName = getClassName(mapkey, strLine);
							tmpMapKey = mapkey;
						}

						// 当className变了，才开始把上一个className的记录写入sqlldr文件中；或者className没变，但是字段数不等
						if (!lastListClassName.equals(tmpClassName)
								|| (lastListClassName.equals(tmpClassName) && tmpMapKeyLength != lastListMapKeyLength)) {
							if (list.size() > 0) {
								createSqlldr(lastListMapKey, list);
								// 重新初始化
								lastListClassName = tmpClassName;
								lastListMapKey = tmpMapKey;
								lastListMapKeyLength = getColumnNum(tmpMapKey);
							}
							list.clear();
						}
						list.add(strLine);
					}
					num++;
				} else {
					num = 1;
				}
			}
			// 最后一个className写入sqlldr文件中
			if (list.size() > 0) {
				createSqlldr(lastListMapKey, list);
			}
			list.clear();

			runSqlldr();
		} catch (Exception e) {
			throw e;
		} finally {
			if(reader!=null)
				reader.close();
			SQLLDR_INFOS.clear();
			list.clear();
		}
	}

	private int getColumnNum(String mapkey) {
		int tmpMapKeyLength;
		int columnNum = 0;
		for (char c : mapkey.toCharArray()) {
			if (c == ',' || c == '，') {
				columnNum++;
			}
		}
		tmpMapKeyLength = columnNum;
		return tmpMapKeyLength;
	}

	// 根据字段className的索引找到value中的值
	public static String getClassName(String name, String value) {
		List<String> strName = split(name, ',');
		String resutlValue = dealValue(value);
		// String[] strValue = resutlValue.split(tmpSplit);
		List<String> strValue = split(resutlValue, tmpSplit);
		int num = -1;
		// int inclsetptCount = 0;// INCLSETPT出现次数，出现的第二个，要改为INCLSETPT_1
		for (int i = 0; i < strName.size(); i++) {
			if (strName.get(i).toString().equals("className")) {
				num = i;
				break;
			}
		}
		// logger.debug("取得创建表的表名：：" + " className = "+strValue[num]+"*****");
		return strValue.get(num);
	}

	/**
	 * 实现String.split(regex)方法
	 * 
	 * @param string
	 * @param split
	 * @return
	 */
	private static List<String> split(String string, char regex) {
		List<String> paraList = new ArrayList<String>();
		char[] str = string.toCharArray();
		int begin = 0;
		for (int i = 0; i < str.length; i++) {
			if (str[i] == regex) {
				paraList.add(string.substring(begin, i).trim());
				begin = i + 1;
			} else if (i + 1 == str.length) {
				String s = string.substring(begin, ++i);
				if (!"".equals(s.trim()))
					paraList.add(s.trim());
			}
		}
		return paraList;
	}

	public static String dealValue(String value) {
		// if(value.indexOf("OUTCDMACH") > -1){
		// System.out.println(value);
		// }

		String str = value;
		List<String> strList = new ArrayList<String>();
		StringBuffer sb = new StringBuffer();
		boolean flag = false;// 双引号标记
		char tmpChar = '0';
		for (char s : str.toCharArray()) {
			if ((s == ',' || s == '，') && flag == false) {
				// if(tmpChar == ',' || tmpChar == '，'){
				// sb.append(" ");
				// }
				strList.add(sb.toString() + tmpSplit);
				sb = new StringBuffer();
				tmpChar = s;
				continue;
			}
			if (s == '\"') {
				if (flag == true) {
					flag = false;
				} else if (tmpChar == ',' || tmpChar == '，') {
					flag = true;
				} else {
					sb.append(s);
				}
				tmpChar = s;
				continue;
			}
			sb.append(s);
			tmpChar = s;
		}
		if (sb.toString().length() > 0) {
			strList.add(sb.toString());
		}
		String strArray[] = strList.toArray(new String[strList.size()]);
		String strValue = "";
		for (String ss : strArray) {
			strValue += ss;
		}
		return strValue;
	}

	// 处理insert插入语句中value的值
	public static String dealInsertValue(String text) {
		String str = text;
		str = str + ",";
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
		StringBuffer valueBuff = new StringBuffer();
		for (String ss : strArray) {
			// strValue += "'"+ss+"',";
			if (ss == null || ss.equals("")) {
				ss = "null";
			}
			if (ss == null || ss.equals("null")) {
				valueBuff.append(ss + ",");
			} else {
				valueBuff.append("'" + ss + "',");
			}
		}
		return subEndString(valueBuff.toString());
	}

	/** 截取掉最后一个字符串 */
	public static String subEndString(String str) {
		String strvalue = "";

		if (str != null && !str.equals("")) {
			strvalue = str.substring(0, str.length() - 1);
		}

		return strvalue;
	}

	private void createSqlldr(final String key, List<String> list) {
		String tKey = key.replace("INCLSETPT,INCLSETPT,", "INCLSETPT,INCLSETPT_1,");
		SqlldrInfo sqlldrInfo = null;
		sqlldrInfo = new SqlldrInfo();
		String value = null;

		if (list.size() != 0) {
			value = list.get(0);
		}
		String tn = getClassName(tKey, value);

		if (tn.length() > 20) {
			tn = tn.substring(tn.length() - 20, tn.length());
		}
		String tabName = "CLT_CM_" + tn + "_HW";
		if (tn.equalsIgnoreCase("DFNBRCH")) {
			tabName = "CLT_CM_ODTMOC_DFNBRCH_HW";
		} else if (tn.equalsIgnoreCase("DFNBRCH_DFNBRPARA")) {
			tabName = "CLT_CM_ODTMOC_DFNBRCH_P_HW";
		} else if (tn.equalsIgnoreCase("DFNBRCH_ODODFNBRPARA")) {
			tabName = "CLT_CM_ODTMOC_DFNBRCH_OD_HW";
		} else if (tn.equalsIgnoreCase("SFNBRCH_ODOSFNBRPARA")) {
			tabName = "CLT_CM_ODTMOC_SFNBRCH_OD_HW";
		} else if (tn.equalsIgnoreCase("SFNBRCH_SFNBRPARA")) {
			tabName = "CLT_CM_ODTMOC_SFNBRCH_SP_HW";
		} else if (tn.equalsIgnoreCase("SFNBRCH")) {
			tabName = "CLT_CM_ODTMOC_SFNBRCH_HW";
		}

		if (isSPAS) {
			if (tabName.equalsIgnoreCase("clt_cm_cdma1xch_hw"))
				tabName = "DS_CLT_CM_CDMA1XCH_HW";
			else if (tabName.equalsIgnoreCase("clt_cm_cdmadoch_hw"))
				tabName = "DS_CLT_CM_CDMADOCH_HW";
			else
				return;
		}
		// 如果忽略表，就要忽略掉 liangww add 2012-06-07
		else if (this.ignoreTableSet.contains(tabName.toUpperCase())) {
			logger.info(collectObjInfo.getTaskID() + "- 忽略" + tabName);
			return;
		}

		Date now = new Date();
		String currDate = Util.getDateString_yyyyMMddHHmmss(now);
		long time = System.nanoTime();

		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + collectObjInfo.getTaskID() + File.separator);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		String name = dir.getPath() + File.separator + currDate + "_" + time + "_" + tabName;
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
			pw.print("(" + tKey.replace("CLASSNAME", "CLASSNAME1") + ",");
			pw.print("OMCID,COLLECTTIME Date 'YYYY-MM-DD HH24:MI:SS',STAMPTIME Date 'YYYY-MM-DD HH24:MI:SS'");
			pw.print(")");
			pw.flush();
			pw.close();
		} catch (Exception e) {
			logger.error(" ", e);
		}
		// liangww modify 2012-06-07 把tableName作为key修改为 用文件
		// SQLLDR_INFOS.put(tabName, sqlldrInfo);
		SQLLDR_INFOS.put(name, sqlldrInfo);

		FileWriter fw = null;
		try {
			fw = new FileWriter(new File(sqlldrInfo.txtFile), true);

			fw.write(tKey.replace(",", ";").replace("CLASSNAME", "CLASSNAME1") + "\n");
			StringBuilder tmp = new StringBuilder();
			tmp.append(";" + omcId).append(";").append(Util.getDateString(new Date())).append(";")
					.append(Util.getDateString(collectObjInfo.getLastCollectTime()));
			for (String val : list) {
				String resutlValue = dealValue(val);
				String s = resutlValue.replace(String.valueOf(tmpSplit), ";") + tmp.toString() + ";" + "\n";
				if (!Util.isNull(s))
					fw.write(s);
			}
		} catch (Exception e) {
			logger.error(" ", e);
		} finally {
			if (fw != null) {
				try {
					fw.flush();
					fw.close();
				} catch (IOException e) {
				}
			}// if ( fw != null )
		}// finally

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

			// if(cmd.indexOf("OUTCDMACH") == -1)
			// continue;

			int ret = -1;
			try {
				ret = new ExternalCmd().execute(cmd);
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

			boolean isDel = SystemConfig.getInstance().isDeleteWhenOff();
			// 是否删除日志
			if (isDel && ret == 0) {
				deleteLog(info);
			}
		}

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

		CV1CSV csv = new CV1CSV();
		String filePath = "F:\\yy\\igp_v1\\需求\\河北华为参数\\CDMAExport_LF-CBSC01_136.158.228.7_2013091314.csv";
		// String filePath = "F:\\yy\\igp_v1\\需求\\D0045\\原始文件\\test1.csv";
		csv.setFileName(filePath);
		CollectObjInfo info = new CollectObjInfo(1);
		DevInfo dev = new DevInfo();
		info.setDevInfo(dev);
		dev.setOmcID(917);
		info.setLastCollectTime(new Timestamp(11));
		csv.setCollectObjInfo(info);
		try {
			csv.parse();
			// System.out.println("xxx end");
			// csv.setFileName("C:\\Users\\ChenSijiang\\Desktop\\aaa.csv");
			// csv.parse();
			// System.out.println("aaa end");
		} catch (Exception e) {
			e.printStackTrace();
		}
		// List<String> list = CV1CSV.split(";;;;;;;;", ';');

	}

}

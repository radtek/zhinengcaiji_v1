package parser.lucent.w;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import util.DBLogger;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * sqlldr工具类
 * 
 * @author YangJian
 * @since 1.0
 */
public class SqlldrTool {

	private SqlldrParam param;

	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public SqlldrTool(SqlldrParam param) {
		super();
		this.param = param;
	}

	/**
	 * 执行sqlldr
	 * 
	 * @return
	 */
	public boolean execute() {
		if (param == null)
			return false;

		boolean ret = true;
		SqlldrInfo info = fillSqlldrInfo(param.tbName, param.taskID, param.dataTime);
		makeFile_Ctl(info, param);
		makeFile_Txt(info, param);
		run(info, param);

		return ret;
	}

	private SqlldrInfo fillSqlldrInfo(String tbName, long taskID, Timestamp dataTime) {
		SqlldrInfo info = new SqlldrInfo();

		String strDateTime = Util.getDateString_yyyyMMddHHmmss(dataTime);

		Random rnd = new Random();
		int r = rnd.nextInt(5000);

		String path = SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "eric_w_pm" + File.separator;
		Calendar calendar = Calendar.getInstance();
		int month = calendar.get(Calendar.MONTH) + 1; // 当前月份
		int year = calendar.get(Calendar.YEAR);
		path = path + year + "-" + month + File.separator;

		String name = path + taskID + "_" + tbName + "_" + strDateTime + "_" + r;
		info.ctlFile = name + ".ctl";
		info.logFile = name + ".log";
		info.badFile = name + ".bad";
		info.txtFile = name + ".txt";

		return info;
	}

	private void makeFile_Ctl(SqlldrInfo info, SqlldrParam param) {
		File f = new File(info.ctlFile);
		try {
			String columnList = param.listColumnWithLargeColumn(",");
			PrintWriter pw = new PrintWriter(f);
			pw.println("load data");
			pw.println("CHARACTERSET ZHS16GBK ");
			pw.println("infile '" + info.txtFile + "' append into table " + param.tbName);
			pw.println("FIELDS TERMINATED BY \";\"");
			pw.println("TRAILING NULLCOLS");
			pw.print("(" + columnList + ",");
			pw.print("OMCID,COLLECTTIME Date 'YYYY-MM-DD HH24:MI:SS',STAMPTIME Date 'YYYY-MM-DD HH24:MI:SS'");
			pw.print(")");
			pw.flush();
			pw.close();
		} catch (Exception e) {
			log.error(" ", e);
		}
	}

	private void makeFile_Txt(SqlldrInfo info, SqlldrParam param) {
		FileWriter fw = null;
		try {
			fw = new FileWriter(new File(info.txtFile), false);
			fw.write(param.listColumn(";") + ";OMCID;COLLECTTIME;STAMPTIME" + "\n");

			String nowStr = Util.getDateString(new Date());
			String sysFieldValue = param.omcID + ";" + nowStr + ";" + Util.getDateString(param.dataTime);

			String commonFieldValue = param.fieldValue2String(param.commonFields, ";"); // 获取公共字段值列表

			Collection<ArrayList<String>> records = param.records.values(); // 取出此表所有数据
			for (ArrayList<String> record : records) {
				fw.write(list2Str(record, ";") + ";" + commonFieldValue + ";" + sysFieldValue + ";\n");
			}

			fw.flush();
			fw.close();
		} catch (Exception e) {
			log.error(" ", e);
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private String list2Str(List<String> lst, String splitSign) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < lst.size() - 1; i++) {
			String value = lst.get(i);
			value = value == null ? "" : value;
			sb.append(value).append(splitSign);
		}
		String lastElement = lst.get(lst.size() - 1);
		lastElement = lastElement == null ? "" : lastElement;
		sb.append(lastElement);

		return sb.toString();
	}

	private void run(SqlldrInfo info, SqlldrParam param) {
		String strOracleBase = SystemConfig.getInstance().getDbService();
		String strOracleUserName = SystemConfig.getInstance().getDbUserName();
		String strOraclePassword = SystemConfig.getInstance().getDbPassword();

		String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999", strOracleUserName, strOraclePassword,
				strOracleBase, info.ctlFile, info.badFile, info.logFile);
		ExternalCmd externalCmd = new ExternalCmd();

		try {
			int ret = externalCmd.execute(cmd);
			log.debug(param.taskID + ": sqlldr code = " + ret);
		} catch (Exception e) {
			log.error("", e);
		}

		SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
		try {
			SqlldrResult result = analyzer.analysis(new FileInputStream(info.logFile));
			if (result == null)
				return;

			log.debug(param.taskID + ": SQLLDR日志分析结果: omcid=" + param.omcID + " 表名=" + result.getTableName() + " 数据时间=" + param.dataTime + " 入库成功条数="
					+ result.getLoadSuccCount() + " sqlldr日志=" + info.logFile);

			dbLogger.log(param.omcID, result.getTableName(), param.dataTime, result.getLoadSuccCount(), param.taskID);
		} catch (Exception e) {
			log.error(param.taskID + ": sqlldr日志分析失败，文件名：" + info.logFile + "，原因: ", e);
		}

		// 是否删除日志
		if (SystemConfig.getInstance().isDeleteLog()) {
			deleteLog(info);
		}
	}

	private void deleteLog(SqlldrInfo info) {
		File badFile = new File(info.badFile);
		if (badFile.exists() && badFile.isFile())
			return;

		// 删除.CTL
		File ctlfile = new File(info.ctlFile);
		if (ctlfile.exists())
			ctlfile.delete();

		// 删除.txt文件
		File txtfile = new File(info.txtFile);
		if (txtfile.exists()) {
			txtfile.delete();
		}

		// 删除.log文件
		File txtlog = new File(info.logFile);
		if (txtlog.exists())
			txtlog.delete();

	}

}

class SqlldrParam {

	Map<String, ArrayList<String>> records; // 所有行的数据 <moid,r值列表>

	Map<String, Integer> columnMap; // 所有字段,但不包括公共字段 <字段名,字段索引>

	String tbName; // 表名

	/** 大字段 <字段名,大小> 这里的大小决定在写ctl文件时char为多少的值 */
	Map<String, Integer> largeColumns = new HashMap<String, Integer>();

	// 系统级别字段
	long taskID; // 系统任务号

	Timestamp dataTime; // 系统数据时间

	int omcID;

	// 业务公共字段
	public List<Field> commonFields;

	/**
	 * 列出所有的字段,公共字段加到后面
	 * 
	 * @param splitSign
	 *            字段之间以分隔符隔开
	 * @return
	 */
	public String listColumn(String splitSign) {
		int size = columnMap.size();
		if (commonFields != null)
			size = size + commonFields.size();

		List<String> columnList = new ArrayList<String>(size);
		// 初始化list
		for (int k = 0; k < size; k++)
			columnList.add(null);

		Set<Entry<String, Integer>> entries = columnMap.entrySet();
		for (Entry<String, Integer> entry : entries) {
			String columnName = entry.getKey();
			int index = entry.getValue();
			columnList.set(index, columnName);
		}

		// 把公共字段追加到后面
		if (commonFields != null) {
			int i = 0;
			for (Field field : commonFields) {
				int index = columnMap.size() + i;
				columnList.set(index, field.name);
				i++;
			}
		}

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < columnList.size() - 1; i++) {
			sb.append(columnList.get(i)).append(splitSign);
		}
		sb.append(columnList.get(columnList.size() - 1));

		return sb.toString();
	}

	/**
	 * 列出所有的字段,公共字段加到后面 -- 包括大字段的处理
	 * 
	 * @param splitSign
	 *            字段之间以分隔符隔开
	 * @return
	 */
	public String listColumnWithLargeColumn(String splitSign) {
		int size = columnMap.size();
		if (commonFields != null)
			size = size + commonFields.size();

		List<String> columnList = new ArrayList<String>(size);
		// 初始化list
		for (int k = 0; k < size; k++)
			columnList.add(null);

		Set<Entry<String, Integer>> entries = columnMap.entrySet();
		for (Entry<String, Integer> entry : entries) {
			String columnName = entry.getKey();
			int index = entry.getValue();
			columnList.set(index, columnName);
		}

		// 把公共字段追加到后面
		if (commonFields != null) {
			int i = 0;
			for (Field field : commonFields) {
				int index = columnMap.size() + i;
				columnList.set(index, field.name);
				i++;
			}
		}

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < columnList.size() - 1; i++) {
			String colName = columnList.get(i);
			if (largeColumns.containsKey(colName))
				colName = colName + " CHAR(" + largeColumns.get(colName) + ")"; // 处理大字段
			sb.append(colName).append(splitSign);
		}

		String lastColName = columnList.get(columnList.size() - 1);
		if (largeColumns.containsKey(lastColName))
			lastColName = lastColName + " CHAR(" + largeColumns.get(lastColName) + ")"; // 处理大字段
		sb.append(lastColName);

		return sb.toString();
	}

	/**
	 * 把公共字段中的值转换成字符串列表
	 * 
	 * @param fields
	 * @param splitSign
	 * @return
	 */
	public String fieldValue2String(List<Field> fields, String splitSign) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < fields.size() - 1; i++) {
			String value = fields.get(i).value;
			sb.append(value).append(splitSign);
		}
		sb.append(fields.get(fields.size() - 1).value);

		return sb.toString();
	}

	/**
	 * 把公共字段中的值转换成字符串列表(带单引号)
	 * 
	 * @param fields
	 * @param splitSign
	 * @return
	 */
	public String fieldValue2String_1(List<Field> fields, String splitSign) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < fields.size() - 1; i++) {
			String value = "'" + fields.get(i).value + "'";
			sb.append(value).append(splitSign);
		}
		sb.append("'" + fields.get(fields.size() - 1).value + "'");

		return sb.toString();
	}
}

class SqlldrInfo {

	String txtFile;

	String logFile;

	String badFile;

	String ctlFile;
}

class Field {

	String name;

	String value;

	public Field(String name, String value) {
		this.name = name;
		this.value = value;
	}
}

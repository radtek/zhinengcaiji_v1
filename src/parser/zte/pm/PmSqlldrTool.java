package parser.zte.pm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import task.CollectObjInfo;
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
public class PmSqlldrTool {

	private SqlldrParam param;

	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	CollectObjInfo task;

	public PmSqlldrTool(SqlldrParam param, CollectObjInfo task) {
		super();
		this.param = param;
		this.task = task;
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

		String name = SystemConfig.getInstance().getCurrentPath() + File.separator + taskID + "_" + tbName + "_" + strDateTime + "_"
				+ param.tableIndex;
		info.ctlFile = name + ".ctl";
		info.logFile = name + ".log";
		info.badFile = name + ".bad";
		info.txtFile = name + ".txt";

		return info;
	}

	private void makeFile_Ctl(SqlldrInfo info, SqlldrParam param) {
		File f = new File(info.ctlFile);
		try {
			String columnList = param.head;
			PrintWriter pw = new PrintWriter(f);
			pw.println("load data");
			pw.println("CHARACTERSET ZHS16GBK ");
			pw.println("infile '" + info.txtFile + "' append into table " + param.tbName);
			pw.println("FIELDS TERMINATED BY \";\"");
			pw.println("TRAILING NULLCOLS");
			pw.print("(" + columnList + ",");
			pw.print("OMCID,COLLECTTIME Date 'YYYY-MM-DD HH24:MI:SS',STAMPTIME Date 'YYYY-MM-DD HH24:MI:SS',BEGINTIME Date 'YYYY-MM-DD HH24:MI:SS'");
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

			fw.write(param.headTxt + ";OMCID;COLLECTTIME;STAMPTIME,BEGINTIME" + "\n");

			String nowStr = Util.getDateString(new Date());
			String sysFieldValue = param.omcID + ";" + nowStr + ";" + Util.getDateString(param.dataTime) + ";" + param.beginTime;

			Collection<ArrayList<String>> records = param.records.values();
			// // 取出此表所有数据
			for (ArrayList<String> record : records) {
				if (record == null || record.size() == 0)
					continue;
				fw.write(list2Str(record, ";") + ";" + sysFieldValue + ";\n");
				fw.flush();
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
		if (lst == null || lst.size() == 0) {
			return null;
		}
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
		Timestamp sqlldrStartTime = new Timestamp(System.currentTimeMillis());
		int ret = -1;
		try {
			ret = externalCmd.execute(cmd);
		} catch (Exception e) {
			log.error("", e);
		}

		SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
		try {
			SqlldrResult result = analyzer.analysis(new FileInputStream(info.logFile));
			if (result == null)
				return;

			log.debug(param.taskID + ": SQLLDR日志分析结果: omcid=" + param.omcID + " 入库成功条数=" + result.getLoadSuccCount() + " 表名=" + result.getTableName()
					+ " 数据时间=" + param.dataTime + " sqlldr日志=" + info.logFile);

			dbLogger.logForHour(param.omcID, result.getTableName(), param.dataTime, result.getLoadSuccCount(), param.taskID, task.getGroupId(),
					result.getLoadFailCount() + result.getLoadSuccCount(), sqlldrStartTime, ret);
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

	String tbName; // 表名

	// 系统级别字段
	long taskID; // 系统任务号

	Timestamp dataTime; // 系统数据时间

	int omcID;

	String head;

	String headTxt;

	String beginTime;

	int tableIndex;

	// 业务公共字段
	public List<Field> commonFields;

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

	/**
	 * 列出所有的字段,公共字段加到后面
	 * 
	 * @param splitSign
	 *            字段之间以分隔符隔开
	 * @return
	 */
	public String listColumn(String splitSign) {
		int size = commonFields.size();

		List<String> columnList = new ArrayList<String>(size);
		// 初始化list
		for (int k = 0; k < size; k++)
			columnList.add(null);

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < columnList.size() - 1; i++) {
			sb.append(columnList.get(i)).append(splitSign);
		}
		sb.append(columnList.get(columnList.size() - 1));

		return sb.toString();
	}

	public void clear() {
		Collection<ArrayList<String>> col = records.values();
		for (ArrayList<String> list : col) {
			list.clear();
		}

		commonFields.clear();
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

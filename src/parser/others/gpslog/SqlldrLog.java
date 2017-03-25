package parser.others.gpslog;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import util.DBLogger;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * @author liuwx
 */
public class SqlldrLog {

	private SqlldrParam param;

	private SqlldrInfo info;

	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public static final int bufferSize = 100;

	public SqlldrLog(SqlldrParam param) {
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
		info = fillSqlldrInfo(param.tbName, param.taskID, param.dataTime);
		makeFile_Ctl(info, param);
		makeFile_Txt(info, param);
		// run(info, param);

		return ret;
	}

	public void run() {
		run(info, param);
	}

	private SqlldrInfo fillSqlldrInfo(String tbName, long taskID, Timestamp dataTime) {

		String strDateTime = Util.getDateString_yyyyMMddHHmmss(dataTime);

		String name = SystemConfig.getInstance().getCurrentPath() + File.separator + taskID + File.separator + taskID + "_" + tbName + "_"
				+ strDateTime + "_" + new Random().nextInt(1000) + "_" + param.tableIndex;
		File ctl = new File(name + ".ctl");
		File log = new File(name + ".log");
		File bad = new File(name + ".bad");
		File txt = new File(name + ".txt");

		SqlldrInfo info = new SqlldrInfo(tbName, txt, log, bad, ctl);

		return info;
	}

	private void makeFile_Ctl(SqlldrInfo info, SqlldrParam param) {
		File f = info.ctlFile;
		try {
			String columnList = param.head;
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

	public boolean isOut(SqlldrParam param)// SqlldrInfo info,
	{
		if (param.records.size() > bufferSize) {
			writeBuffer(info, param);

			return true;
		}

		return false;
	}

	public void out(SqlldrParam param)// SqlldrInfo info,
	{
		writeBuffer(info, param);

	}

	public void writeBuffer(SqlldrInfo info, SqlldrParam param) {
		String nowStr = Util.getDateString(new Date());
		String sysFieldValue = param.omcID + ";" + nowStr + ";" + Util.getDateString(param.dataTime);

		Collection<ArrayList<String>> records = param.records.values();
		// // 取出此表所有数据
		// try
		// {
		for (ArrayList<String> record : records) {
			if (record == null || record.size() == 0)
				continue;

			info.writerForTxt.write(list2Str(record, ";") + ";" + sysFieldValue + ";\n");

			info.writerForTxt.flush();
			// fw.write(list2Str(record, ";") + ";" + sysFieldValue +
			// ";\n");
			// fw.flush();
		}
		// }
		// catch (IOException e)
		// {
		// log.error("写入文件出现错误");
		// }
		param.records.clear();

	}

	private void makeFile_Txt(SqlldrInfo info, SqlldrParam param) {
		// FileWriter fw = null;
		try {
			// fw = new FileWriter(info.txtFile, false);

			info.writerForTxt.write(param.headTxt + ";OMCID;COLLECTTIME;STAMPTIME" + "\n");
			info.writerForTxt.flush();

			// fw.write(param.headTxt + ";OMCID;COLLECTTIME;STAMPTIME" + "\n");

			// String nowStr = Util.getDateString(new Date());
			// String sysFieldValue = param.omcID + ";" + nowStr + ";"
			// + Util.getDateString(param.dataTime);

			// Collection<ArrayList<String>> records = param.records.values();
			// // // 取出此表所有数据
			// for (ArrayList<String> record : records)
			// {
			// if ( record == null || record.size() == 0 )
			// continue;
			// // fw.write(list2Str(record, ";") + ";" + sysFieldValue +
			// // ";\n");
			// // fw.flush();
			// }
			// fw.flush();
			// fw.close();
		} catch (Exception e) {
			log.error(" ", e);
		} finally {
			// if ( fw != null )
			// {
			// try
			// {
			// fw.close();
			// }
			// catch (IOException e)
			// {
			// }
			// }
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
		String key = String.format("[taskId-%s][%s]", param.taskID, param.dataTime);
		log.debug(key + "当前执行的SQLLDR命令为：" + cmd.replace(strOracleUserName, "*").replace(strOraclePassword, "*"));
		int ret = -1;
		try {
			ret = externalCmd.execute(cmd);
		} catch (Exception e) {
			log.error("", e);
		}

		SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
		try {
			SqlldrResult result = analyzer.analysis(new FileInputStream(info.logFile));
			if (result == null) {
				log.info("SqlldrResult  is null , the logFile is " + info.logFile);
				return;
			}

			log.debug(param.taskID + ": SQLLDR日志分析结果: ret=" + ret + " omcid=" + param.omcID + " 入库成功条数=" + result.getLoadSuccCount() + " 表名="
					+ result.getTableName() + " 数据时间=" + param.dataTime + " sqlldr日志=" + info.logFile);

			dbLogger.logForHour(param.omcID, result.getTableName(), param.dataTime, result.getLoadSuccCount(), param.taskID, true);
		} catch (Exception e) {
			log.error(param.taskID + ": sqlldr日志分析失败，文件名：" + info.logFile + "，原因: ", e);
		}

		// try
		// {
		if (info.writerForCtl != null) {
			info.writerForCtl.flush();
			info.writerForCtl.close();
		}
		if (info.writerForTxt != null) {
			info.writerForTxt.flush();
			info.writerForTxt.close();
		}
		// }
		// catch (IOException e)
		// {
		// log.error("关闭文件出现错误", e);
		// }
		// 是否删除日志
		if (SystemConfig.getInstance().isDeleteLog()) {
			deleteLog(info, ret);
		}
	}

	private void deleteLog(SqlldrInfo info, int ret) {
		File ctlfile = info.ctlFile;
		File txtfile = info.txtFile;
		File txtlog = info.logFile;
		File badFile = info.badFile;
		if (ret == 0) {
			ctlfile.delete();
			txtfile.delete();
			txtlog.delete();
			badFile.delete();
		} else if (ret == 2) {
			ctlfile.delete();
			txtfile.delete();
			badFile.delete();
		}

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

	// String txtFile;
	// String logFile;
	// String badFile;
	// String ctlFile;

	String tableName;

	File txtFile;

	File logFile;

	File badFile;

	File ctlFile;

	PrintWriter writerForTxt;

	PrintWriter writerForCtl;

	public SqlldrInfo(String tableName, File txt, File log, File bad, File ctl) {
		super();
		this.tableName = tableName;
		this.txtFile = txt;
		this.logFile = log;
		this.badFile = bad;
		this.ctlFile = ctl;
		try {
			writerForTxt = new PrintWriter(txt);
			writerForCtl = new PrintWriter(ctl);
		} catch (Exception unused) {
		}
	}

	public void close() {
		IOUtils.closeQuietly(writerForCtl);
		IOUtils.closeQuietly(writerForTxt);

	}

	@Override
	public int hashCode() {
		return tableName.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		SqlldrInfo info = (SqlldrInfo) obj;
		return info.tableName.equalsIgnoreCase(this.tableName);
	}
}

class Field {

	String name;

	String value;

	public Field(String name, String value) {
		this.name = name;
		this.value = value;
	}
}

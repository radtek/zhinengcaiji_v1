package sqlldr;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/*
 * sqlldr module for data warehousing 
 * 
 * @author yuy  
 * @2013-06-21 17:36
 */

public class SqlldrImpl implements SqlldrInterface {

	private Map<String, List<String>> tableCols;

	private long taskId;

	private int omcId;

	private Timestamp time_date;

	private String time_str;

	private String logKey;

	private SystemConfig cfg;

	private String flag;

	// {key/*表名*/,value/*SQLLDR信息*/}
	private Map<String, SqlldrInfo> infos = null;

	// sqlldr日志分析器
	private SqlLdrLogAnalyzer analyzer = null;

	// 表结构的缓存容器
	private static final Map<Long, Map<String, List<String>>> tablesContainer = new HashMap<Long, Map<String, List<String>>>();

	// 系统日志
	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	// db日志
	private static final DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	/**
	 * 特殊表处理 在sqlldr初始化时加上字段类型和长度
	 */
	private Map<String/* tableName */, Map<String/* columnName */, String/* type */>> specialTableMap = null;

	// 分隔符
	public static final String splitStr = "`^";

	public SqlldrImpl(long taskId, int omcId, Timestamp time, String desc) {
		super();
		this.taskId = taskId;
		this.omcId = omcId;
		this.time_date = time;
		this.time_str = Util.getDateString(time);
		this.logKey = String.format("[%s][%s]", taskId, time_str);
		this.cfg = SystemConfig.getInstance();
		this.flag = desc;
	}

	public SqlldrImpl(long taskId, int omcId, Timestamp time, String desc, Map<String, Map<String, String>> specialTableMap) {
		this(taskId, omcId, time, desc);
		this.specialTableMap = specialTableMap;
	}

	public void setTableCols(Map<String, List<String>> tableCols) {
		this.tableCols = tableCols;
	}

	@Override
	public Map<String, List<String>> loadDataStructrue(List<String> tables) {
		// TODO Auto-generated method stub
		Map<String, List<String>> map = tablesContainer.get(taskId);
		if (map != null) {
			tableCols = map;
			return tableCols;
		}
		tableCols = new HashMap<String, List<String>>();
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		ResultSetMetaData meta = null;
		try {
			for (String tb : tables) {
				if (con == null)
					con = DbPool.getConn();
				String sql = "select * from " + tb + " where 1=2";
				st = con.prepareStatement(sql);
				try {
					rs = st.executeQuery();
					meta = rs.getMetaData();
					int count = meta.getColumnCount();
					List<String> cols = new ArrayList<String>();
					for (int i = 0; i < count; i++) {
						String col = meta.getColumnName(i + 1).toUpperCase();
						cols.add(col);
					}
					tableCols.put(tb, cols);
				} catch (Exception e) {
					logger.error(logKey + " - 加载" + tb + "表结构时异常", e);
				}
			}
			tablesContainer.put(taskId, tableCols);
		} catch (Exception e) {
			logger.error(logKey + " - 加载表结构时异常", e);
		} finally {
			CommonDB.closeDBConnection(con, st, rs);
		}
		return tableCols;
	}

	@Override
	public void initSqlldr() {
		// TODO Auto-generated method stub
		File dir = new File(cfg.getCurrentPath() + File.separator + flag + File.separator + taskId + File.separator);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		long rnd = System.currentTimeMillis();
		infos = new HashMap<String, SqlldrInfo>();
		Iterator<String> it = tableCols.keySet().iterator();
		while (it.hasNext()) {
			String tableName = it.next();
			List<String> cols = tableCols.get(tableName);
			String baseName = taskId + "_" + tableName + "_" + Util.getDateString_yyyyMMddHHmmss(this.time_date) + "_" + rnd;
			SqlldrInfo info = new SqlldrInfo(new File(dir, baseName + ".txt"), new File(dir, baseName + ".log"), new File(dir, baseName + ".bad"),
					new File(dir, baseName + ".ctl"));
			infos.put(tableName, info);
			info.writerForCtl.println("LOAD DATA");
			info.writerForCtl.println("CHARACTERSET " + cfg.getSqlldrCharset());
			info.writerForCtl.println("INFILE '" + info.txt.getAbsolutePath() + "' APPEND INTO TABLE " + tableName);
			info.writerForCtl.println("FIELDS TERMINATED BY \"" + splitStr + "\"");
			info.writerForCtl.println("TRAILING NULLCOLS");
			info.writerForCtl.print("(");
			for (int i = 0; i < cols.size(); i++) {
				String colName = cols.get(i);
				info.writerForCtl.print("\"" + colName + "\"");
				info.writerForTxt.print(colName);
				if (colName.equals("COLLECTTIME") || colName.equals("STAMPTIME") || colName.equals("COLLECT_TIME") || colName.equals("START_TIME")
						|| colName.equals("END_TIME") || colName.equals("TESTSTARTTIME")) {
					info.writerForCtl.print(" DATE 'YYYY-MM-DD HH24:MI:SS'");
				}
				if (specialTableMap != null && specialTableMap.get(tableName.toUpperCase()) != null
						&& specialTableMap.get(tableName.toUpperCase()).get(colName) != null) {
					info.writerForCtl.print(" " + specialTableMap.get(tableName.toUpperCase()).get(colName));
				}
				if (i < cols.size() - 1) {
					info.writerForCtl.print(",");
					info.writerForTxt.print(splitStr);
				}
			}
			info.writerForCtl.print(")");
			info.writerForTxt.println();
			info.writerForCtl.flush();
			info.writerForCtl.close();
			info.writerForTxt.flush();
		}
	}

	@Override
	public void writeRows(List<String> row, String tableName) {
		// TODO Auto-generated method stub
		SqlldrInfo info = infos.get(tableName);
		for (int i = 0; i < row.size(); i++) {
			String val = row.get(i);
			val = Util.nvl(val, "");
			info.writerForTxt.print(val);
			if (i < row.size() - 1) {
				info.writerForTxt.print(splitStr);
			}
		}
		info.writerForTxt.println();
		info.writerForTxt.flush();
	}

	@Override
	public boolean runSqlldr() {
		// TODO Auto-generated method stub
		analyzer = new SqlLdrLogAnalyzer();
		Iterator<String> it = infos.keySet().iterator();
		while (it.hasNext()) {
			String tableName = it.next();
			SqlldrInfo info = infos.get(tableName);
			info.close();
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=%s control=%s bad=%s log=%s errors=9999999", cfg.getDbUserName(),
					cfg.getDbPassword(), cfg.getDbService(), 1, info.ctl.getAbsoluteFile(), info.bad.getAbsoluteFile(), info.log.getAbsoluteFile());
			int ret = -1;
			try {
				logger.debug(logKey + "执行sqlldr - " + cmd.replace(cfg.getDbPassword(), "*"));
				ret = new ExternalCmd().execute(cmd);
				SqlldrResult result = analyzer.analysis(info.log.getAbsolutePath());
				dbLogger.log(omcId, result.getTableName(), this.time_str, result.getLoadSuccCount(), taskId);
				logger.debug(logKey
						+ String.format("ret=%s,入库条数=%s,task_id=%s,表名=%s,时间点=%s", ret, result.getLoadSuccCount(), taskId, result.getTableName(),
								time_str));
				if (ret == 0 && cfg.isDeleteLog()) {
					info.txt.delete();
					info.log.delete();
					info.ctl.delete();
					info.bad.delete();
				}
			} catch (Exception e) {
				logger.error(logKey + " - sqlldr时异常", e);
				return false;
			}
		}
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

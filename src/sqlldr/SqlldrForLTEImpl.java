package sqlldr;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
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

public class SqlldrForLTEImpl implements SqlldrInterface {

	private Map<String, List<Column>> tableCols;

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
	private static final Map<Long, Map<String, List<Column>>> tablesContainer = new HashMap<Long, Map<String, List<Column>>>();

	// 系统日志
	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	// db日志
	private static final DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	// 江苏LTE4G网络采集，采集表中用mmeid替换了omcid：解析时把omcid作为mmeid入库
	protected String is4G = SystemConfig.getInstance().getLteIs4G();

	// 分隔符
	public static final String splitStr = "`^";

	public SqlldrForLTEImpl(long taskId, int omcId, Timestamp time, String desc) {
		this.taskId = taskId;
		this.omcId = omcId;
		this.time_date = time;
		this.time_str = Util.getDateString(time);
		this.logKey = String.format("[%s][%s]", taskId, time_str);
		this.cfg = SystemConfig.getInstance();
		this.flag = desc;
	}

	@Override
	public Map<String, List<String>> loadDataStructrue(List<String> tables) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * 加载单个表的表结构
	 * 
	 * @param tableName
	 * @return
	 */
	public List<Column> loadSingleTableStructrue(String tableName) {
		// TODO Auto-generated method stub
		Map<String, List<Column>> map = tablesContainer.get(taskId);
		if (map != null && map.get(tableName) != null) {
			return map.get(tableName);
		}
		if (map == null)
			tableCols = new HashMap<String, List<Column>>();
		else
			tableCols = map;

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		ResultSetMetaData meta = null;
		List<Column> cols = null;
		try {
			if (con == null)
				con = DbPool.getConn();
			String sql = "select * from " + tableName + " where 1=2";
			st = con.prepareStatement(sql);
			try {
				rs = st.executeQuery();
				meta = rs.getMetaData();
				int count = meta.getColumnCount();
				Column column = null;
				cols = new ArrayList<Column>();
				for (int i = 0; i < count; i++) {
					column = new Column();
					String name = meta.getColumnName(i + 1).toUpperCase();
					// 系统字段跳过
					// if ("true".equals(is4G) && name.equalsIgnoreCase("MMEID"))
					// continue;
					// if (name.equalsIgnoreCase("OMCID") || name.equalsIgnoreCase("COLLECTTIME") || name.equalsIgnoreCase("STAMPTIME"))
					// continue;
					column.name = name;
					column.type = meta.getColumnType(i + 1);
					cols.add(column);
				}
				tableCols.put(tableName, cols);
			} catch (Exception e) {
				logger.error(logKey + " - 加载" + tableName + "表结构时异常", e);
			}
			tablesContainer.put(taskId, tableCols);
		} catch (Exception e) {
			logger.error(logKey + " - 加载表结构时异常", e);
		} finally {
			CommonDB.closeDBConnection(con, st, rs);
		}
		return cols;
	}

	public void initSqlldr(String tableName, List<String> colList) {
		// TODO Auto-generated method stub
		File dir = new File(cfg.getCurrentPath() + File.separator + flag + File.separator + taskId + File.separator);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		long rnd = System.currentTimeMillis();
		infos = new HashMap<String, SqlldrInfo>();
		List<Column> cols = tablesContainer.get(taskId).get(tableName);
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
		for (int j = 0; j < colList.size(); j++) {
			String name = colList.get(j);
			for (int i = 0; i < cols.size(); i++) {
				String colName = cols.get(i).name;
				if (!name.equals(colName))
					continue;
				int type = cols.get(i).type;
				info.writerForCtl.print("\"" + colName + "\"");
				// info.writerForTxt.print(colName);
				if (colName.equals("COLLECTTIME") || colName.equals("STAMPTIME") || colName.equals("COLLECT_TIME") || colName.equals("START_TIME")
						|| colName.equals("END_TIME") || colName.equals("TESTSTARTTIME") || type == Types.TIMESTAMP || type == Types.DATE
						|| type == Types.TIME) {
					info.writerForCtl.print(" DATE 'YYYY-MM-DD HH24:MI:SS'");
				}
				if (j < colList.size() - 1) {
					info.writerForCtl.print(",");
					// info.writerForTxt.print(splitStr);
				}
				break;
			}
		}
		info.writerForCtl.print(")");
		// info.writerForTxt.println();
		info.writerForCtl.flush();
		info.writerForCtl.close();
		// info.writerForTxt.flush();
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

	public void writeRows(String vals, String tableName) {
		// TODO Auto-generated method stub
		SqlldrInfo info = infos.get(tableName);
		vals = Util.nvl(vals, "");
		info.writerForTxt.print(vals);
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

	public class Column {

		public String name;

		public int type;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setTableCols(Map<String, List<String>> tableCols) {
		// TODO Auto-generated method stub

	}

	@Override
	public void initSqlldr() {
		// TODO Auto-generated method stub

	}

}

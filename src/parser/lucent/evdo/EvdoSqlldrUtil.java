package parser.lucent.evdo;

import java.io.Closeable;
import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import util.DBLogger;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * 对阿朗EVDO性能数据进行SQLLDR入库。
 * 
 * @author ChenSijiang 2010-12-29 上午09:47:43
 */
public class EvdoSqlldrUtil {

	// key = 表名, value = sqlldr相关信息
	private Map<String, SqlldrInfo> infos = new HashMap<String, SqlldrInfo>();

	private long taskId;

	private int omcId;

	private Timestamp dataTime;

	private String strDataTime;

	private String collectTime;

	private String key;

	private Connection con;

	private DBLogger dblogger = LogMgr.getInstance().getDBLogger();

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final SystemConfig CFG = SystemConfig.getInstance();

	private final Map<String, String> COL_CACHE = new HashMap<String, String>();

	/**
	 * 构造方法。
	 */
	public EvdoSqlldrUtil(long taskId, int omcId, Timestamp dataTime) {
		super();
		this.taskId = taskId;
		this.omcId = omcId;
		this.dataTime = dataTime;
		this.strDataTime = Util.getDateString(dataTime);
		this.collectTime = Util.getDateString(new Date());
		this.key = String.format("[%s,%s]", this.taskId, Util.getDateString(this.dataTime));
	}

	/**
	 * 添加一 行记录。
	 * 
	 * @param records
	 *            记录
	 * @param tableName
	 *            记录所属表名
	 * @param id
	 *            记录ID
	 */
	public void addRow(List<EvdoRecordPair> records, String tableName, String id, List<String> tableCols) {
		SqlldrInfo info = null;
		if (infos.containsKey(tableName)) {
			info = infos.get(tableName);
		} else {
			info = ready(tableName, id, tableCols);
			infos.put(tableName, info);
		}

		StringBuilder buffer = new StringBuilder();
		for (int i = 0; i < tableCols.size(); i++) {
			String col = tableCols.get(i).toUpperCase();
			if (col.equals("OMCID"))
				buffer.append(this.omcId);
			else if (col.equals("STAMPTIME"))
				buffer.append(strDataTime);
			else if (col.equals("COLLECTTIME"))
				buffer.append(collectTime);
			else if (col.equals("ID"))
				buffer.append(id);
			else {
				String val = findValue(records, col);
				val = val == null ? "" : val;
				buffer.append(val);
			}
			if (i < tableCols.size() - 1)
				buffer.append(";");

		}

		info.writerForTxt.println(buffer);
		info.writerForTxt.flush();
		buffer.setLength(0);
		buffer = null;

	}

	private String findValue(List<EvdoRecordPair> records, String sn) {
		String cn = getColname(sn);
		if (cn == null)
			return null;
		for (EvdoRecordPair p : records) {
			if (p.name.equalsIgnoreCase(cn))
				return p.value;
		}
		return null;
	}

	private final Map<String, String> COLNAME_CACHE = new HashMap<String, String>();

	// 根据短名，查原始名
	private String getColname(String sn) {
		if (COLNAME_CACHE.containsKey(sn))
			return COLNAME_CACHE.get(sn);

		String col = null;
		Connection con = getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = con.prepareStatement("select col_name from clt_conf_luc_evdo_col_mapping where short_name=?");
			ps.setString(1, sn);
			rs = ps.executeQuery();
			if (rs.next()) {
				col = rs.getString(1);
				COLNAME_CACHE.put(sn, col);
			}

		} catch (Exception e) {
			logger.error(key + "查原始列名时异常", e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}

			} catch (Exception e) {
			}
		}
		return col;
	}

	/*
	 * 在字段表里查出字段名的id
	 */
	private String getColShortName(String colName) {
		String colId = COL_CACHE.get(colName);
		if (colId != null) {
			return colId;
		}
		Connection con = getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = con.prepareStatement("select short_name from clt_conf_luc_evdo_col_mapping where col_name=?");
			ps.setString(1, colName);
			rs = ps.executeQuery();
			if (rs.next()) {
				colId = rs.getString(1);
			}
			COL_CACHE.put(colName, colId);
		} catch (Exception e) {
			logger.error(key + "获取列ID时异常", e);
			return null;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}

			} catch (Exception e) {
			}
		}

		if (colId == null) {
			colId = addColName(colName);
		}

		return colId;
	}

	/*
	 * 向字段表中添加一个字段名
	 */
	private String addColName(String colName) {
		Connection con = getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;

		String seq = getSeq();
		String shortName = null;
		if (colName.length() <= 30) {
			shortName = colName;
		} else {
			shortName = colName.substring(0, 26) + seq;
		}

		try {
			ps = con.prepareStatement("insert into clt_conf_luc_evdo_col_mapping values" + " ('" + "COL_" + seq + "',?,?)");
			ps.setString(1, colName);
			ps.setString(2, shortName);
			ps.executeUpdate();
		} catch (Exception e) {
			logger.error(key + "添加字段名时异常");
			return null;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}

			} catch (Exception e) {
			}

		}

		String id = getColShortName(colName);

		COL_CACHE.put(colName, id);

		return id;
	}

	private String getSeq() {
		Connection con = getConnection();;
		PreparedStatement ps = null;
		ResultSet rs = null;

		String seq = null;

		try {
			ps = con.prepareStatement("select clt_evdo_mapping_seq.nextval from dual");
			rs = ps.executeQuery();
			if (rs.next()) {
				seq = String.valueOf(rs.getInt(1));
			}
			return seq;
		} catch (Exception e) {
			logger.error(key + "获取clt_evdo_mapping_seq.nextval时异常", e);
			return null;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}

			} catch (Exception e) {
			}
		}
	}

	private SqlldrInfo ready(String tableName, String id, List<String> tableCols) {
		File baseDir = new File(CFG.getCurrentPath() + File.separator + "al_evdo_pm" + File.separator);
		if (!baseDir.exists())
			baseDir.mkdirs();

		String baseFilename = baseDir.getAbsolutePath() + File.separator + taskId + "_" + Util.getDateString_yyyyMMddHH(dataTime) + "_"
				+ tableName.toUpperCase() + "_" + System.currentTimeMillis();

		File txt = new File(baseFilename + ".txt");
		File log = new File(baseFilename + ".log");
		File bad = new File(baseFilename + ".bad");
		File ctl = new File(baseFilename + ".ctl");

		SqlldrInfo info = new SqlldrInfo(txt, log, bad, ctl, tableCols);

		info.writerForCtl.println("load data");
		info.writerForCtl.println("CHARACTERSET " + CFG.getSqlldrCharset());
		info.writerForCtl.println("INFILE '" + txt.getAbsolutePath() + "' APPEND INTO TABLE " + tableName.toUpperCase());
		info.writerForCtl.println("FIELDS TERMINATED BY \";\"");
		info.writerForCtl.println("TRAILING NULLCOLS");
		info.writerForCtl.print("(");

		for (int i = 0; i < tableCols.size(); i++) {
			String col = tableCols.get(i).toUpperCase();
			info.writerForTxt.print(col);
			info.writerForCtl.print(col.equals("COLLECTTIME") || col.equals("STAMPTIME") ? col + " DATE 'YYYY-MM-DD HH24:MI:SS'" : col);
			if (i < tableCols.size() - 1) {
				info.writerForTxt.print(";");
				info.writerForCtl.print(",");
			}
		}
		info.writerForCtl.print(")");
		info.writerForCtl.flush();
		info.writerForTxt.println();
		info.writerForTxt.flush();

		return info;
	}

	protected synchronized Connection getConnection() {
		try {
			if (con == null || con.isClosed()) {
				con = DbPool.getConn();
			}
		} catch (Exception e) {
			logger.error(key + "获取数据库连接时异常", e);
		}
		return con;
	}

	public void commit() {

		if (con != null) {
			try {
				con.close();
			} catch (SQLException unused) {
			}
			con = null;
		}

		Iterator<Entry<String, SqlldrInfo>> it = infos.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, SqlldrInfo> e = it.next();
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", CFG.getDbUserName(),
					CFG.getDbPassword(), CFG.getDbService(), e.getValue().ctl.getAbsoluteFile(), e.getValue().bad.getAbsoluteFile(),
					e.getValue().log.getAbsoluteFile());
			e.getValue().close();
			logger.debug(key + "执行 " + cmd.replace(CFG.getDbPassword(), "*").replace(CFG.getDbUserName(), "*"));
			ExternalCmd execute = new ExternalCmd();
			try {
				int ret = execute.execute(cmd);
				SqlldrResult result = new SqlLdrLogAnalyzer().analysis(e.getValue().log.getAbsolutePath());
				logger.debug(key + "exit=" + ret + " omcid=" + this.omcId + " 入库成功条数=" + result.getLoadSuccCount() + " 表名=" + result.getTableName()
						+ " 数据时间=" + this.strDataTime + " sqlldr日志=" + e.getValue().log.getAbsolutePath());
				dblogger.log(this.omcId, result.getTableName(), this.dataTime, result.getLoadSuccCount(), this.taskId);
				if (ret == 0 && SystemConfig.getInstance().isDeleteLog()) {
					e.getValue().txt.delete();
					e.getValue().ctl.delete();
					e.getValue().log.delete();
					e.getValue().bad.delete();
				}
			} catch (Exception ex) {
				logger.error(key + "sqlldr时异常", ex);
			}
		}

	}

	class SqlldrInfo implements Closeable {

		File txt;

		File log;

		File bad;

		File ctl;

		PrintWriter writerForTxt;

		PrintWriter writerForCtl;

		List<String> tableCols;

		public SqlldrInfo(File txt, File log, File bad, File ctl, List<String> tableCols) {
			super();
			this.txt = txt;
			this.log = log;
			this.bad = bad;
			this.ctl = ctl;
			try {
				writerForTxt = new PrintWriter(txt);
				writerForCtl = new PrintWriter(ctl);
			} catch (Exception unused) {
			}
			SqlldrInfo.this.tableCols = tableCols;
		}

		@Override
		public void close() {
			IOUtils.closeQuietly(writerForCtl);
			IOUtils.closeQuietly(writerForTxt);

		}

	}
}

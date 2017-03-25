package util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

/**
 * 数据库工具类
 * 
 * @author chenrongqiang @ 2013-10-17
 */
public final class DatabaseUtil {

	private static final Logger LOG = LogMgr.getInstance().getSystemLogger();

	/**
	 * 执行一个SELECT语句，获取第一行第一列的int值。
	 * 
	 * @param con
	 *            连接。
	 * @param sql
	 *            SELECT语句。
	 * @param defaultValue
	 *            如果无结果集时，返回什么默认值。
	 * @return 第一行第一列的int值。
	 * @throws SQLException
	 *             查询失败。
	 */
	public static final int getUniqueIntValue(Connection con, String sql, int defaultValue) throws SQLException {
		int v = 0;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = con.prepareStatement(sql);
			rs = st.executeQuery();
			v = (rs.next() ? rs.getInt(1) : defaultValue);
		} finally {
			close(null, st, rs);
		}
		return v;
	}

	/**
	 * 执行一个SELECT语句，获取第一行第一列的long值。
	 * 
	 * @param con
	 *            连接。
	 * @param sql
	 *            SELECT语句。
	 * @param defaultValue
	 *            如果无结果集时，返回什么默认值。
	 * @return 第一行第一列的long值。
	 * @throws SQLException
	 *             查询失败。
	 */
	public static final long getUniqueLongValue(Connection con, String sql, long defaultValue) throws SQLException {
		long v = 0;
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			v = (rs.next() ? rs.getLong(1) : defaultValue);
		} finally {
			close(null, st, rs);
		}
		return v;
	}

	/**
	 * 关闭ResultSet 发生异常时，方法内记日志，不抛出
	 * 
	 * @param rs
	 */
	public static final void close(ResultSet rs) {
		if (rs == null)
			return;

		try {
			rs.close();
		} catch (Exception e) {
			LOG.warn("关闭ResultSet时发生异常。", e);
		}
	}

	/**
	 * 关闭Statement 发生异常时，方法内记日志，不抛出
	 * 
	 * @param stmt
	 */
	public static final void close(Statement stmt) {
		if (stmt == null)
			return;

		try {
			stmt.close();
		} catch (Exception e) {
			LOG.warn("关闭Statement时发生异常。", e);
		}
	}

	/**
	 * 关闭连接 发生异常时，方法内记日志，不抛出
	 * 
	 * @param conn
	 */
	public static final void close(Connection conn) {
		if (conn == null)
			return;

		try {
			conn.close();
		} catch (Exception e) {
			LOG.warn("关闭Connection时发生异常。", e);
		}
	}

	/**
	 * 连接ResultSet,Statement,Connection 发生异常时，方法内记日志，不抛出
	 * 
	 * @param con
	 * @param st
	 * @param rs
	 */
	public static final void close(Connection con, Statement st, ResultSet rs) {
		close(rs);
		close(st);
		close(con);
	}

	/**
	 * 根据数据库的驱动名称获取验证sql
	 * 
	 * @param DBDriver
	 * @return
	 */
	public static String getMyValidationQuery(String DBDriver) {
		// oracle.jdbc.dirver.OracleDriver
		if (DBDriver.toLowerCase().indexOf("oracle") > -1)
			return "select 1 from dual";
		// com.mysql.jdbc.Driver
		if (DBDriver.toLowerCase().indexOf("mysql") > -1)
			return "select 1";
		// com.microsoft.jdbc.sqlserver.SQLServerDriver
		if (DBDriver.toLowerCase().indexOf("sqlserver") > -1)
			return "select 1";
		// net.sourceforge.jtds.jdbc.Driver
		if (DBDriver.toLowerCase().indexOf("jtds") > -1)
			return "select 1";
		// com.sybase.jdbc.SybDriver
		if (DBDriver.toLowerCase().indexOf("sybase") > -1)
			return "select 1";
		// com.ibm.db2.jdbc.app.DB2Driver
		if (DBDriver.toLowerCase().indexOf("db2") > -1)
			return "select 1 from sysibm.sysdummy1";
		// com.informix.jdbc.IfxDriver
		if (DBDriver.toLowerCase().indexOf("informix") > -1)
			return "select count(*) from systables";
		// org.postgresql.Driver
		if (DBDriver.toLowerCase().indexOf("postgresql") > -1)
			return "select version()";
		return "";
	}
}

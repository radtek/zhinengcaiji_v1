package lte;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;
import framework.SystemConfig;

/**
 * db操作 临时util
 * 
 * @author yuy
 * 
 */
public class DbUtil {

	private static Logger LOGGER = LogMgr.getInstance().getSystemLogger();

	/**
	 * 表结构缓存
	 */
	public static Map<String/* tableName */, List<String>/* columns */> tableMap = new HashMap<String, List<String>>();

	/**
	 * 创建表
	 * 
	 * @param record
	 * @return
	 */
	public static boolean createTable(String tableName, Map<String, String> columnMap) {
		columnMap = new TreeMap<String, String>(columnMap);
		StringBuilder sql = new StringBuilder();
		sql.append("create table ").append(tableName).append(" (");
		Iterator<String> it = columnMap.keySet().iterator();
		while (it.hasNext()) {
			String column = it.next();
			String type = columnMap.get(column);
			if (column.length() > 30)
				column = column.substring(column.length() - 30);
			sql.append(column).append(" " + type + ",");
		}
		if (sql.charAt(sql.length() - 1) == ',') {
			sql.deleteCharAt(sql.length() - 1);
		}
		sql.append(")");
		try {
			CommonDB.executeUpdate(sql.toString());
			return true;
		} catch (SQLException e) {
			LOGGER.error("建表时异常：" + sql, e);
			return false;
		}

	}

	/**
	 * 查看表是否存在
	 * 
	 * @param con
	 * @param tableName
	 * @param taskId
	 * @return
	 * @throws SQLException
	 */
	public static boolean tableExists(String tableName, long taskId) throws SQLException {
		if (Util.isNull(tableName)) {
			return false;
		}
		Connection con = null;
		con = DbPool.getConn();
		if (con == null) {
			LOGGER.error("查找表是否存在:获取数据库连接失败！");
			return false;
		}

		String prefix = taskId < 0 ? "" : (taskId + " - ");

		String sql = "select * from " + tableName + " where 1=2";
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			if (!con.isClosed()) {
				st = con.prepareStatement(sql);
				st.setQueryTimeout(SystemConfig.getInstance().getQueryTimeout());
				rs = st.executeQuery();

				// 把表结构加载到map中，缓存起来
				putMap(tableName, rs);
			}
		} catch (SQLException e) {
			int code = e.getErrorCode();
			// 表或视图不存在，oracle错误码为924，sysbase与sqlserver的错误码为208
			if (code == 942 || code == 208) {
				LOGGER.debug(prefix + "表或视图不存在,测试语句:" + sql + ",出现的异常信息:" + e.getMessage().trim());
				return false;
			}
			LOGGER.debug(prefix + "测试表或视图是否存在时,发生异常,测试语句:" + sql + ",出现的异常信息:" + e.getMessage().trim());
			return true;
		} catch (Exception e) {
			LOGGER.debug(prefix + "测试表或视图是否存在时,发生异常,测试语句:" + sql + ",出现的异常信息:" + e.getMessage().trim());
			return true;
		} finally {
			CommonDB.close(rs, st, con);
		}
		return true;
	}

	/**
	 * 初始化输出SQL
	 * 
	 * @return
	 */
	public static String createInsertSql(String tableName, List<String> columnList) {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(tableName).append(" (");
		for (int i = 0; i < columnList.size(); i++) {
			String col = columnList.get(i);
			if (col.length() > 30) {
				col = col.substring(col.length() - 30);
			}
			sb.append(col);
			if (i < columnList.size() - 1)
				sb.append(",");
		}
		sb.append(")values(");
		for (int i = 0; i < columnList.size(); i++) {
			sb.append("?");
			if (i < columnList.size() - 1)
				sb.append(",");
		}
		sb.append(")");
		return sb.toString();
	}

	/**
	 * 把表结构加载到map中，缓存起来
	 * 
	 * @param tableName
	 * @param rs
	 * @throws SQLException
	 */
	public static void putMap(String tableName, ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int count = rsmd.getColumnCount();
		List<String> list = null;
		for (int n = 1; n <= count; n++) {
			if (list == null) {
				list = new ArrayList<String>();
			}
			String name = rsmd.getColumnName(n);
			list.add(name.toUpperCase());
		}
		tableMap.put(tableName, list);
	}

	/**
	 * 把表结构加载到map中，缓存起来
	 * 
	 * @param tableName
	 * @param rs
	 * @throws SQLException
	 */
	public static void putMap(String tableName, Collection<String> columnList) throws SQLException {
		List<String> list = null;
		Object[] objs = columnList.toArray();
		for (int n = 0; n < objs.length; n++) {
			if (list == null) {
				list = new ArrayList<String>();
			}
			String name = (String) objs[n];
			list.add(name.toUpperCase());
		}
		tableMap.put(tableName, list);
	}

	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

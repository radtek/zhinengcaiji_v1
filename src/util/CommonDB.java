package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.jsp.jstl.sql.Result;
import javax.servlet.jsp.jstl.sql.ResultSupport;

import org.apache.log4j.Logger;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import framework.SystemConfig;

/**
 * 数据库操作工具类
 * 
 * @author IGP TDT
 * @since 1.0
 */
public class CommonDB {

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public static List<String> loadCols(String tableName) throws Exception {
		String sql = "select * from " + tableName + " where 1=2";
		List<String> cols = new ArrayList<String>();
		Connection con = DbPool.getConn();
		PreparedStatement st = null;
		ResultSet rs = null;
		ResultSetMetaData meta = null;
		try {
			st = con.prepareStatement(sql);
			rs = st.executeQuery();
			meta = rs.getMetaData();
			int count = meta.getColumnCount();
			for (int i = 0; i < count; i++) {
				cols.add(meta.getColumnName(i + 1));
			}
		} catch (Exception e) {
			throw e;
		} finally {
			close(rs, st, con);
		}
		return cols;
	}

	public static Map<Integer, String> GetTableColumns(String strTableName) {
		String strDriver = SystemConfig.getInstance().getDbDriver();
		if (strDriver.contains("oracle")) // oracle 数据库
		{
			return null;
		}

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			String strSQL = "select t1.name as COLUMN_NAME,t1.colid as COLUMN_ID from sysobjects t,syscolumns t1 where t.id=t1.id and t.name='"
					+ strTableName + "'";

			pstmt = conn.prepareStatement(strSQL);
			rs = pstmt.executeQuery();

			Map<Integer, String> columns = new HashMap<Integer, String>();
			while (rs.next()) {
				columns.put(rs.getInt("COLUMN_ID"), rs.getString("COLUMN_NAME"));
			}
			rs.close();
			pstmt.close();

			return columns;

		} catch (Exception e) {
			log.error("CommonDB: GetTableColumns", e);
		} finally {
			try {
				if (pstmt != null)
					pstmt.close();
				if (conn != null)
					conn.close();
			} catch (Exception e) {
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return "CommonDB";
	}

	public static boolean isReAdoptObj(CollectObjInfo taskInfo) {
		boolean flag = false;
		if (taskInfo != null && taskInfo instanceof RegatherObjInfo) {
			flag = true;
		}
		return flag;
	}

	public static void closeDbConnection() {
		DbPool.close();
	}

	public static Connection getConnection(long taskId, String OracleDriver, String OracleUrl, String OracleUser, String OraclePassword) {
		log.debug(taskId + " - 连接到数据库(driver=" + OracleDriver + ",url=" + OracleUrl + ",user=" + OracleUser + ",pwd=" + OraclePassword + ")");
		Connection conn = null;

		try {
			Class.forName(OracleDriver);

			conn = DriverManager.getConnection(OracleUrl, OracleUser, OraclePassword);
		} catch (Exception ex) {
			log.error(taskId + " - 获取连接失败(driver=" + OracleDriver + ",url=" + OracleUrl + ",user=" + OracleUser + ",pwd=" + OraclePassword + "),原因:",
					ex);
		}

		return conn;
	}

	/**
	 * @param taskId
	 * @param driver
	 * @param url
	 * @param user
	 * @param password
	 * @return
	 */
	public static Connection getConnection(CollectObjInfo task) {
		log.debug(task.getTaskID() + " - 连接到数据库(driver=" + task.getDBDriver() + ",url=" + task.getDBUrl() + ",user="
				+ task.getDevInfo().getHostUser() + ")");
		Connection conn = null;
		if (SystemConfig.getInstance().getPoolMaxActiveRemoteEnable()) {
			conn = DbPoolManager.getConnection(task.getDBDriver(), task.getDBUrl(), task.getDevInfo().getHostUser(), task.getDevInfo().getHostPwd());
		} else {
			conn = getConnection(task.getTaskID(), task.getDBDriver(), task.getDBUrl(), task.getDevInfo().getHostUser(), task.getDevInfo()
					.getHostPwd());
		}

		// 建立多次重连机制
		if (conn == null) {
			String strLog = "Task-" + task.getTaskID();

			log.error(strLog + ": 获取对方数据库连接失败,尝试重连 ... ");

			byte tryTimes = 0;
			int sleepTime = 5000;
			while (tryTimes < 3 && conn == null) {
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					break;
				}

				if (SystemConfig.getInstance().getPoolMaxActiveRemoteEnable()) {
					conn = DbPoolManager.getConnection(task.getDBDriver(), task.getDBUrl(), task.getDevInfo().getHostUser(), task.getDevInfo()
							.getHostPwd());
				} else {
					conn = getConnection(task.getTaskID(), task.getDBDriver(), task.getDBUrl(), task.getDevInfo().getHostUser(), task.getDevInfo()
							.getHostPwd());
				}

				tryTimes++;

				if (conn == null) {
					log.error(strLog + ": 尝试数据库重连失败 (" + tryTimes + ") ... ");
				}

				sleepTime += sleepTime * 1;
			}

			if (conn == null) {
				log.error(strLog + ": 多次获取对方数据库连接失败.");
			} else {
				log.info(strLog + ": 数据库重连成功(" + tryTimes + ").");
			}
		}

		return conn;
	}

	/**
	 * 获取数据库连接,如果获取失败则进行指定最大次数的重连
	 * 
	 * @param task
	 * @param iSleepTime
	 *            每次重连的间隔时间
	 * @param maxTryTimes
	 *            最大重连次数
	 * @return
	 */
	public static Connection getConnection(CollectObjInfo task, int iSleepTime, byte maxTryTimes) {
		if (task == null)
			return null;

		Connection conn = null;

		conn = getConnection(task.getTaskID(), task.getDBDriver(), task.getDBUrl(), task.getDevInfo().getHostUser(), task.getDevInfo().getHostPwd());

		// 建立多次重连机制
		if (conn == null) {
			String strLog = "Task-" + task.getTaskID();

			log.error(strLog + ": 获取对方数据库连接失败,尝试重连 ... ");

			byte tryTimes = 0;
			int sleepTime = iSleepTime;
			while (tryTimes < maxTryTimes && conn == null) {
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					break;
				}

				conn = getConnection(task.getTaskID(), task.getDBDriver(), task.getDBUrl(), task.getDevInfo().getHostUser(), task.getDevInfo()
						.getHostPwd());

				tryTimes++;

				if (conn == null) {
					log.error(strLog + ": 尝试数据库重连失败 (" + tryTimes + ") ... ");
				}

				sleepTime += sleepTime * 1;
			}

			if (conn == null) {
				log.error(strLog + ": 多次获取对方数据库连接失败.");
			} else {
				log.info(strLog + ": 数据库重连成功(" + tryTimes + ").");
			}
		}

		return conn;
	}

	public static Connection getConnection() {
		return DbPool.getConn();
	}

	// 设置最后成功导入的时间和位置
	public static void LastImportTimePos(long TaskID, Timestamp Timestp, int nPos) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String strTime = formatter.format(Timestp);

			conn = getConnection();
			StringBuffer m_Build = new StringBuffer();

			if (Util.isOracle()) // oracle 数据库
			{
				m_Build.append("update IGP_CONF_TASK set suc_data_time=to_date('" + strTime + "','YYYY-MM-DD HH24:MI:SS'),suc_data_pos=" + nPos
						+ " where TASK_ID =" + TaskID);
			} else if (Util.isSybase()) // sybase 数据库
			{
				m_Build.append("update IGP_CONF_TASK set suc_data_time=convert(datetime,'" + strTime + "'),suc_data_pos=" + nPos + " where TASK_ID ="
						+ TaskID);
			} else if (Util.isMysql()) // mysql 数据库
			{
				m_Build.append("update IGP_CONF_TASK set suc_data_time='" + strTime + "',suc_data_pos=" + nPos + " where TASK_ID =" + TaskID);
			}

			pstmt = conn.prepareStatement(m_Build.toString());
			pstmt.executeUpdate();
		} catch (Exception e) {
			log.error("Telnet: 更新最后导入时间出错:", e);
		} finally {
			try {
				if (pstmt != null)
					pstmt.close();
				if (conn != null)
					conn.close();
			} catch (Exception e) {
			}
		}
	}

	public static int executeUpdate(String sql) throws SQLException {
		int count = -1;

		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			count = ps.executeUpdate();
		} finally {
			close(null, ps, con);
		}

		return count;
	}

	/**
	 * 执行select数据
	 * 
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	public static Result queryForResult(String sql) throws Exception {
		Result result = null;
		ResultSet resultSet = null;
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			connection = DbPool.getConn();
			preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();
			result = ResultSupport.toResult(resultSet);
		} finally {
			close(resultSet, preparedStatement, connection);
		}

		return result;

	}

	/**
	 * 执行select数据
	 * 
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	public static ResultSet queryForResultSet(String sql) throws Exception {
		ResultSet resultSet = null;
		Connection connection = null;
		PreparedStatement preparedStatement = null;

		connection = DbPool.getConn();
		preparedStatement = connection.prepareStatement(sql);
		resultSet = preparedStatement.executeQuery();

		return resultSet;

	}

	/**
	 * 向数据库批量添加新数据
	 * 
	 * @param sql
	 * @return 返回批量提交受影响的行数
	 */
	public static int[] executeBatch(List<String> sqlList) throws SQLException {
		int[] result = null;
		Connection con = null;
		Statement stm = null;
		con = DbPool.getConn();
		if (con == null) {
			log.error("批量提交获取数据库连接失败！");
			return result;
		}
		try {
			if (sqlList != null && !sqlList.isEmpty()) {
				con.setAutoCommit(false);
				stm = con.createStatement();

				for (String sql : sqlList) {
					stm.addBatch(sql);
				}
				result = stm.executeBatch();
				con.commit();
			}
		}

		finally {
			close(null, stm, con);
		}
		return result;
	}

	/**
	 * 关闭所有连接
	 */
	public static void close(ResultSet rs, Statement stm, Connection conn) {
		if (rs != null) {
			try {
				rs.close();
			} catch (Exception unused) {
			}
		}
		if (stm != null) {
			try {
				stm.close();
			} catch (Exception unused) {
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception unused) {
			}
		}

	}

	public static void main(String[] args) throws Exception {
		try {
			List<String> list = new ArrayList<String>();
			list.add(" insert into igp_conf_task (aaa) values (1) ");
			CommonDB.executeBatch(list);
		} catch (SQLException e) {
			System.out.println(e.getErrorCode());// TODO: handle exception
		}
	}

	public static boolean tableExists(Connection con, String tableName, long taskId) throws SQLException {
		if (Util.isNull(tableName)) {
			return false;
		}
		if (con == null)
			return true;

		String prefix = taskId < 0 ? "" : (taskId + " - ");

		String sql = "select 1 from " + tableName + " where 1=2";
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			if (!con.isClosed()) {
				st = con.prepareStatement(sql);
				st.setQueryTimeout(SystemConfig.getInstance().getQueryTimeout());
				rs = st.executeQuery();
			}
		} catch (SQLException e) {
			int code = e.getErrorCode();
			// 表或视图不存在，oracle错误码为924，sysbase与sqlserver的错误码为208
			if (code == 942 || code == 208) {
				log.debug(prefix + "表或视图不存在,测试语句:" + sql + ",出现的异常信息:" + e.getMessage().trim());
				return false;
			}
			log.debug(prefix + "测试表或视图是否存在时,发生异常,测试语句:" + sql + ",出现的异常信息:" + e.getMessage().trim());
			return true;
		} catch (Exception e) {
			log.debug(prefix + "测试表或视图是否存在时,发生异常,测试语句:" + sql + ",出现的异常信息:" + e.getMessage().trim());
			return true;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}
			} catch (Exception e) {
			}
		}
		return true;
	}

	/**
	 * 检查表是否存在
	 * 
	 * @param con
	 * @param tableName
	 *            表名必须大写
	 * @return
	 */
	public static boolean tableExists(Connection con, String tableName) throws SQLException {
		return tableExists(con, tableName, -1);
	}

	/**
	 * 根据传入的sql语句解析出表名
	 * 
	 * @param sql
	 * @return
	 */
	public static String getTableName(String sql) {
		String s = "";
		String str = sql.toLowerCase();
		s = str.substring(str.indexOf(" from ") + 5, str.length()).trim();
		int i = s.indexOf(" ");
		if (i > -1) {
			s = s.substring(0, i);
		}

		return s;
	}

	/**
	 * 获取一条select语句的记录条数
	 * 
	 * @param con
	 * @param selectStatement
	 * @return
	 * @throws Exception
	 */
	public static int getRowCount(Connection con, String selectStatement) throws Exception {

		String sql = selectStatement.toLowerCase();
		int selectIndex = sql.indexOf("select ") + 7;
		int fromIndex = sql.indexOf(" from ");

		StringBuilder buffer = new StringBuilder();
		char[] chars = selectStatement.toCharArray();
		boolean flag = false;
		for (int i = 0; i < chars.length; i++) {
			if (i >= selectIndex && i <= fromIndex && !flag) {
				buffer.append(" count(*) ");
				flag = true;
			} else if (i < selectIndex || i > fromIndex) {
				buffer.append(chars[i]);
			}
		}

		PreparedStatement st = con.prepareStatement(buffer.toString());
		st.setQueryTimeout(SystemConfig.getInstance().getQueryTimeout());
		ResultSet rs = st.executeQuery();
		rs.next();
		int c = rs.getInt(1);
		try {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
		} catch (Exception e) {
		}
		return c;

	}

	/**
	 * <p>
	 * 关闭数据库连接，及相关资源。
	 * </p>
	 * 
	 * @param con
	 *            数据库连接
	 * @param st
	 *            SQL语句对象
	 * @param rs
	 *            结果集对象
	 * @author ChenSijiang 2011-01-04 18:36
	 */
	public static void closeDBConnection(Connection con, Statement st, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (Exception unused) {
			}
		}
		if (st != null) {
			try {
				st.close();
			} catch (Exception unused) {
			}
		}
		if (con != null) {
			try {
				con.close();
			} catch (Exception unused) {
			}
		}
	}

}

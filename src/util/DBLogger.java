package util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

/**
 * <p>
 * 记录数据库日志，用于前台显示 。<br />
 * 数据库中对应的表是: LOG_CLT_INSERT <br />
 * 其中的字段： <br />
 * OMCID omcid <br />
 * CLT_TBNAME clt原始表名<br />
 * STAMPTIME 入库几点的数据<br />
 * VSYSDATE 当前系统时间<br />
 * INSERT_COUNTNUM 入库条数<br />
 * IS_CAL 是否汇总,默认为0<br />
 * TASKID 任务编号<br />
 * </p>
 * 
 * @author 陈思江 2010.02.04
 */
public final class DBLogger {

	private static final String INSERT_SQL;

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final DBLogger INSTANCE = new DBLogger();

	// 20120502 chensj.
	// 标记当前库中的log_clt_insert表，是否有group_id,parsed_count,sqlldr_start_time,sqlldr_exit_code这几列。
	private static boolean hasNewCols = false;

	static {
		String sql = "select count(1) from user_tab_columns t where  COLUMN_NAME in ('GROUP_ID','PARSERD_COUNT','SQLLDR_START_TIME','SQLLDR_EXIT_CODE') and  TABLE_NAME='LOG_CLT_INSERT'";
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			st = con.prepareStatement(sql);
			rs = st.executeQuery();
			if (!rs.next())
				hasNewCols = false;
			else {
				int ret = rs.getInt(1);
				hasNewCols = (ret == 4);
			}
		} catch (Exception e) {
			logger.error("Error in DBLogger.static{}", e);
		} finally {
			CommonDB.close(rs, st, con);
		}

		if (hasNewCols)
			INSERT_SQL = "INSERT INTO LOG_CLT_INSERT" + " (OMCID,CLT_TBNAME,STAMPTIME,VSYSDATE,INSERT_COUNTNUM,IS_CAL,TASKID,GROUP_ID,"
					+ "PARSERD_COUNT,SQLLDR_START_TIME,SQLLDR_EXIT_CODE) VALUES " + "(?,UPPER(?),?,sysdate,?,0,?,?,?,?,?)";
		else
			INSERT_SQL = "INSERT INTO LOG_CLT_INSERT" + " (OMCID,CLT_TBNAME,STAMPTIME,VSYSDATE,INSERT_COUNTNUM,IS_CAL,TASKID) VALUES "
					+ "(?,UPPER(?),?,sysdate,?,0,?)";
	}

	/**
	 * 构造方法。
	 */
	private DBLogger() {
		super();
	}

	public synchronized static DBLogger getInstance() {
		return INSTANCE;
	}

	/**
	 * 记录一条数据库日志到LOG_CLT_INSERT表中。
	 * 
	 * @param omcId
	 *            omcId
	 * @param tableName
	 *            clt原始表名
	 * @param stampTime
	 *            入库时间
	 * @param count
	 *            入库条数
	 * @param taskID
	 *            任务编号
	 */
	public synchronized void log(int omcId, String tableName, long stampTime, int count, long taskID) {
		log(omcId, tableName, new Date(stampTime), count, taskID);
	}

	public synchronized void log(int omcId, String tableName, String stampTime, int count, long taskID) {
		log(omcId, tableName, stampTime, count, taskID, 0, 0, null, 0);
	}

	/**
	 * 记录一条数据库日志到LOG_CLT_INSERT表中。
	 * 
	 * @param omcId
	 *            omcId
	 * @param tableName
	 *            clt原始表名
	 * @param stampTime
	 *            入库时间
	 * @param count
	 *            入库条数
	 * @param taskID
	 *            任务编号
	 */
	public synchronized void log(int omcId, String tableName, String stampTime, int count, long taskID, int groupId, int parserdCount,
			Timestamp sqlldrStartTime, int sqlldrExitCode) {
		PreparedStatement preparedStatement = null;

		String sql = INSERT_SQL;
		Connection conn = null;
		try {
			conn = DbPool.getConn();
			preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setInt(1, omcId);
			preparedStatement.setString(2, tableName == null ? "" : tableName);
			String ss = stampTime;
			if (ss != null && ss.length() == 13)
				ss = ss + ":00:00";
			if (ss != null && ss.length() == 10)
				ss = ss + " 00:00:00";
			Timestamp t = (ss == null ? null : new Timestamp(Util.getDate1(ss).getTime()));
			preparedStatement.setTimestamp(3, t);
			preparedStatement.setInt(4, count);
			preparedStatement.setLong(5, taskID);
			if (hasNewCols) {
				preparedStatement.setInt(6, groupId);
				preparedStatement.setInt(7, parserdCount);
				preparedStatement.setTimestamp(8, sqlldrStartTime);
				preparedStatement.setInt(9, sqlldrExitCode);
			}
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("记录数据库日志时异常,sql:" + sql, e);
		} finally {
			CommonDB.close(null, preparedStatement, conn);
		}
	}

	public synchronized void log(int omcId, String tableName, Timestamp stampTime, int count, long taskID, int groupId, int parserdCount,
			Timestamp sqlldrStartTime, int sqlldrExitCode) {
		String s = Util.getDateString(stampTime);
		log(omcId, tableName, s, count, taskID, groupId, parserdCount, sqlldrStartTime, sqlldrExitCode, true);
	}

	/**
	 * 记录一条数据库日志到LOG_CLT_INSERT表中。
	 * 
	 * @param omcId
	 *            omcId
	 * @param tableName
	 *            clt原始表名
	 * @param stampTime
	 *            入库时间
	 * @param count
	 *            入库条数
	 * @param taskID
	 *            任务编号
	 * @param groupId
	 *            组编号
	 * @param parserdCount
	 *            解析条数
	 * @param sqlldrStartTime
	 *            开始执行SQLLDR的时间
	 * @param sqlldrExitCode
	 *            SQLLDR返回代码
	 * @param isRe
	 *            出现异常是否重新插入一次 true:是 false:否定
	 */
	public synchronized void log(int omcId, String tableName, String stampTime, int count, long taskID, int groupId, int parserdCount,
			Timestamp sqlldrStartTime, int sqlldrExitCode, boolean isRe) {

		PreparedStatement preparedStatement = null;

		String sql = INSERT_SQL;
		Connection conn = null;
		try {
			conn = DbPool.getConn();
			preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setInt(1, omcId);
			preparedStatement.setString(2, tableName == null ? "" : tableName);
			String ss = stampTime;
			if (ss != null && ss.length() == 13)
				ss = ss + ":00:00";
			if (ss != null && ss.length() == 10)
				ss = ss + " 00:00:00";
			Timestamp t = (ss == null ? null : new Timestamp(Util.getDate1(ss).getTime()));
			preparedStatement.setTimestamp(3, t);
			preparedStatement.setInt(4, count);
			preparedStatement.setLong(5, taskID);
			if (hasNewCols) {
				preparedStatement.setInt(6, groupId);
				preparedStatement.setInt(7, parserdCount);
				preparedStatement.setTimestamp(8, sqlldrStartTime);
				preparedStatement.setInt(9, sqlldrExitCode);
			}
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("记录数据库日志时异常,sql:" + sql, e);
			// if ( isRe )
			// {
			// log(omcId, tableName, stampTime, count, taskID, groupId,
			// parserdCount, sqlldrStartTime, sqlldrExitCode, false);
			// }
		} finally {
			CommonDB.close(null, preparedStatement, conn);
		}
	}

	/**
	 * 记录一条数据库日志到LOG_CLT_INSERT表中。
	 * 
	 * @param omcId
	 *            omcId
	 * @param tableName
	 *            clt原始表名
	 * @param stampTime
	 *            入库时间
	 * @param count
	 *            入库条数
	 * @param taskID
	 *            任务编号
	 */
	public synchronized void log(int omcId, String tableName, Date stampTime, int count, long taskID) {
		log(omcId, tableName, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(stampTime), count, taskID, 0, 0, null, 0);
	}

	/**
	 * 记录一条数据库日志到LOG_CLT_INSERT表中。
	 * 
	 * @param omcId
	 *            omcId
	 * @param tableName
	 *            clt原始表名
	 * @param stampTime
	 *            入库时间
	 * @param count
	 *            入库条数
	 * @param taskID
	 *            任务编号
	 */
	public synchronized void log(int omcId, String tableName, Date stampTime, int count, long taskID, boolean isRe) {
		log(omcId, tableName, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(stampTime), count, taskID, 0, 0, null, 0, isRe);
	}

	public static void main(String[] args) throws Exception {
		final DBLogger d = LogMgr.getInstance().getDBLogger();
		// d.log(123, "clt_pm_w_eric_utranrelation",
		// Util.getDate1("2010-11-21 00:00:00"), 33, 789);
		d.logForHour(123, "clt_pm_w_eric_utranrelation", Util.getDate1("2010-11-21 01:00:00"), 33, 789, true);
		// logger.debug("start");
		// for (int i = 0; i < 10; i++)
		// {
		// Thread th = new Thread(new Runnable()
		// {
		//
		// @Override
		// public void run()
		// {
		// for (int j = 0; j < 1000; j++)
		// {
		// d.log(123, "clt_pm_w_eric_utranrelation", new Date(), j, 789);
		// }
		// logger.debug("end");
		// }
		// });
		// th.start();
		// }
	}

	/**
	 * 记录一条数据库日志到LOG_CLT_INSERT表中。
	 * 
	 * @param omcId
	 *            omcId
	 * @param tableName
	 *            clt原始表名
	 * @param stampTime
	 *            入库时间
	 * @param count
	 *            入库条数
	 * @param taskID
	 *            任务编号
	 */
	public synchronized void logForHour(int omcId, String tableName, Date stampTime, int count, long taskID) {
		log(omcId, tableName, new SimpleDateFormat("yyyy-MM-dd HH").format(stampTime), count, taskID, 0, 0, null, 0);
	}

	public synchronized void logForHour(int omcId, String tableName, Date stampTime, int count, long taskID, int groupId, int parserdCount,
			Timestamp sqlldrStartTime, int sqlldrExitCode) {
		log(omcId, tableName, new Timestamp(stampTime.getTime()), count, taskID, groupId, parserdCount, sqlldrStartTime, sqlldrExitCode);
	}

	/**
	 * 记录一条数据库日志到LOG_CLT_INSERT表中。
	 * 
	 * @param omcId
	 *            omcId
	 * @param tableName
	 *            clt原始表名
	 * @param stampTime
	 *            入库时间
	 * @param count
	 *            入库条数
	 * @param taskID
	 *            任务编号
	 */
	public synchronized void logForHour(int omcId, String tableName, Date stampTime, int count, long taskID, boolean isRe) {
		log(omcId, tableName, new SimpleDateFormat("yyyy-MM-dd HH").format(stampTime), count, taskID, 0, 0, null, 0, isRe);
	}

	public synchronized void dispose() {
	}
}

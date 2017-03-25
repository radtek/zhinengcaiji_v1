package access;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import task.CollectObjInfo;
import task.TaskMgr;
import task.TaskMgr.RedoSQL;
import util.CommonDB;
import util.DBLogger;
import util.LogMgr;
import util.Parsecmd;
import util.Util;
import util.exception.NullDBConnException;
import alarm.AlarmMgr;
import datalog.DataLogInfo;
import framework.ConstDef;

/**
 * 数据库接入抽象类
 * 
 * @author ltp Jul 2, 2010
 * @since 3.1
 */
public abstract class AbstractDBAccessor extends AbstractAccessor {

	/** 数据库采集方式时,连接到对方数据库失败重试最大次数 */
	protected static final byte MAX_TRY_TIMES = 2;

	protected static final int SLEEP_TIME = 5000;

	// 需要添加到补采表的集合 <表名>
	protected List<RedoSQL> redoSqlList = null;

	protected DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	public AbstractDBAccessor() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see access.AbstractAccessor#access()
	 */
	@Override
	public abstract boolean access() throws Exception;

	/*
	 * (non-Javadoc)
	 * 
	 * @see access.AbstractAccessor#configure()
	 */
	@Override
	public void configure() throws Exception {
		// do nothing
	}

	@Override
	public boolean doAfterAccess() throws Exception {
		// 采集之后执行的Shell命令
		String cmd = taskInfo.getShellCmdFinish();
		if (Util.isNotNull(cmd)) {
			Parsecmd.ExecShellCmdByFtp(cmd, taskInfo.getLastCollectTime());
		}

		return true;
	}

	/**
	 * 获取数据库连接,如果失败则重连,如获取为Null时会抛异常
	 * 
	 * @return
	 * @throws Exception
	 */
	protected Connection getConnection() throws NullDBConnException {
		// 获取数据库连接,如果失败则重连
		Connection conn = CommonDB.getConnection(taskInfo);
		if (conn == null) {
			// 通知告警
			AlarmMgr.getInstance().insert(taskInfo.getTaskID(), "多次获取数据库连接失败", taskInfo.getDBUrl(), name, 1507);
			throw new NullDBConnException("多次获取数据库连接失败");
		}

		return conn;
	}

	
	/**
	 * 获取数据库连接,如果失败则重连,如获取为Null时会抛异常
	 * 
	 * @return
	 * @throws Exception
	 */
	protected Connection getConnection(CollectObjInfo taskInfo) throws NullDBConnException {
		// 获取数据库连接,如果失败则重连
		Connection conn = CommonDB.getConnection(taskInfo);
		if (conn == null) {
			// 通知告警
			AlarmMgr.getInstance().insert(taskInfo.getTaskID(), "多次获取数据库连接失败", taskInfo.getDBUrl(), name, 1507);
			throw new NullDBConnException("多次获取数据库连接失败");
		}

		return conn;
	}

	
	/**
	 * 采集之前执行的Shell命令
	 */
	protected void execShellBeforeAccess(CollectObjInfo taskInfo) throws Exception {
		String preCmd = taskInfo.getShellCmdPrepare();
		if (Util.isNotNull(preCmd)) {
			boolean b = execSql(preCmd);
			if (!b)
				throw new Exception("命令(" + preCmd + ")执行失败");
		}
	}
	
	/**
	 * 采集之前执行的Shell命令
	 */
	protected void execShellBeforeAccess() throws Exception {
		String preCmd = taskInfo.getShellCmdPrepare();
		if (Util.isNotNull(preCmd)) {
			boolean b = execSql(preCmd);
			if (!b)
				throw new Exception("命令(" + preCmd + ")执行失败");
		}
	}
	

	/** 利用数据库采集之前后执行的命令 */
	private boolean execSql(String strSQLList) {
		PreparedStatement pstmt = null;
		Connection conn = null;
		boolean bSuccesed = true;
		try {
			conn = CommonDB.getConnection();
			String[] strSQL = strSQLList.split(";");
			for (int i = 0; i < strSQL.length; i++) {
				if (strSQL[i] != null && !strSQL[i].equals("")) {
					String strSql = ConstDef.ParseFilePath(strSQL[i], taskInfo.getLastCollectTime());
					synchronized (conn) {
						pstmt = conn.prepareStatement(strSql);
						pstmt.execute(strSQL[i]);
					}
				}
			}
		} catch (Exception e) {
			String logStr = name + ": 执行SQL语句失败,原因:";
			log.error(logStr, e);
			taskInfo.log(DataLogInfo.STATUS_START, logStr);
			bSuccesed = false;
		} finally {
			CommonDB.close(null, pstmt, conn);
		}

		return bSuccesed;
	}

	/**
	 * 检查表是否存在，如果不存在就返回false
	 * 
	 * @param conn
	 * @param sqlTName
	 *            匹配日期后的表名
	 * @return
	 */
	protected boolean tablesExists(Connection conn, String sqlTName, CollectObjInfo taskInfo) {
		boolean flag = true;
		String logStr = null;
		if (sqlTName == null || sqlTName.equals("")) {
			return flag;
		}

		try {
			if (!CommonDB.tableExists(conn, sqlTName, taskInfo.getTaskID())) {
				logStr = taskInfo.getTaskID() + ": omc_id=" + taskInfo.getDevInfo().getOmcID() + " 跳过此表的采集,原因:表(" + sqlTName + ")不存在" + " 数据时间： "
						+ taskInfo.getLastCollectTime();

				log.error(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);
				flag = false;
			}
		} catch (SQLException e) {
			logStr = taskInfo.getTaskID() + ": 检查表(" + sqlTName + ")是否存在时异常," + " 数据时间： " + taskInfo.getLastCollectTime() + ", 原因:";
			log.error(logStr, e);
			taskInfo.log(DataLogInfo.STATUS_START, logStr, e);
			// 虽然抛异常，但还是试着把代码执行下去
		}

		return flag;
	}
	
	/**
	 * 检查表是否存在，如果不存在就返回false
	 * 
	 * @param conn
	 * @param sqlTName
	 *            匹配日期后的表名
	 * @return
	 */
	protected boolean tablesExists(Connection conn, String sqlTName) {
		
		return this.tablesExists(conn, sqlTName, taskInfo);
	

	}

	/**
	 * 拼凑采集SQL语句
	 * 
	 * @param tableName
	 *            表名
	 * @param condition
	 *            where条件
	 * @return
	 */
	protected String toSql(String tableName, String condition) {
		String sql = null;
		if (Util.isNotNull(tableName)) {
			sql = "select * from " + tableName;
			if (Util.isNotNull(condition)) {
				condition = ConstDef.ParseFilePathForDB(condition, taskInfo.getLastCollectTime());
				sql += " where " + condition;
			}
		}
		return sql;
	}
	
	
	protected String toSql(String tableName, String condition, CollectObjInfo taskInfo) {
		String sql = null;
		if (Util.isNotNull(tableName)) {
			sql = "select * from " + tableName;
			if (Util.isNotNull(condition)) {
				condition = ConstDef.ParseFilePathForDB(condition, taskInfo.getLastCollectTime());
				sql += " where " + condition;
			}
		}
		return sql;
	}
	

	/**
	 * 提交采集失败的sql，在这里用表名表示，因为数据采集是基于表的采集
	 */
	protected void commitFailSql() {
		// 为null的情况必须全部补采,这里注意不为null但size为空在时候为不需要补采
		if (redoSqlList == null) {
			TaskMgr.getInstance().newRegather(taskInfo, "", "数据库采集失败(还未执行select语句)， 补采所有表");
			return;
		}

		// 添加补采任务,表名之间用逗号隔开
		if (redoSqlList.size() > 0) {
			StringBuilder sb = new StringBuilder();
			StringBuilder cause = new StringBuilder();
			String sql = null;
			for (int i = 0; i < redoSqlList.size() - 1; i++) {
				sql = redoSqlList.get(i).sql;
				if (Util.isNull(sql))
					continue;
				sb.append(sql + ";");
				cause.append("\"").append(sql).append("\"补采原因为:").append(redoSqlList.get(i).cause).append("\n\n");
			}
			sb.append(redoSqlList.get(redoSqlList.size() - 1).sql);
			cause.append("\"").append(redoSqlList.get(redoSqlList.size() - 1).sql).append("\"补采原因为:")
					.append(redoSqlList.get(redoSqlList.size() - 1).cause).append("\n\n");
			String str = sb.toString();
			String logStr = name + " 本次要添加补采的表：" + str;
			log.info(logStr);
			taskInfo.log(DataLogInfo.STATUS_START, logStr);

			TaskMgr.getInstance().newRegather(taskInfo, str, cause.toString());
			sb.delete(0, sb.length());
			redoSqlList.clear();
			redoSqlList = null;
		}
	}
}

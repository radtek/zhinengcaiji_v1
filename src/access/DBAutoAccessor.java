package access;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import parser.DBAutoParser;
import task.IgnoresInfo;
import task.IgnoresMgr;
import task.TaskMgr.RedoSQL;
import templet.DBAutoTempletP;
import templet.DBAutoTempletP.Templet;
import templet.GenericSectionHeadD;
import templet.TempletBase;
import util.CommonDB;
import util.JDBCConnectionMonitor;
import util.Util;
import alarm.AlarmMgr;
import datalog.DataLogInfo;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 数据库方式接入(不需要配置采集SQL语句，只需要配置解析/分发模板即可)
 * 
 * @author ltp Jul 2, 2010
 * @since 3.1
 */
public class DBAutoAccessor extends AbstractDBAccessor {

	// 所有需要采集表的集合
	private Collection<Templet> templets = null;

	private IgnoresMgr ignoresMgr = IgnoresMgr.getInstance();

	public DBAutoAccessor() {
		super();
	}

	@Override
	public boolean validate() {
		String logStr = null;

		if (taskInfo == null)
			return false;

		// 检查最后一次采集时间是否正确
		try {
			strLastGatherTime = Util.getDateString(taskInfo.getLastCollectTime());
		} catch (Exception e) {
			logStr = name + "> 时间格式错误,原因:";
			log.error(logStr, e);
			taskInfo.log(DataLogInfo.STATUS_START, logStr, e);
			return false;
		}

		TempletBase tBase = taskInfo.getParseTemplet();
		if (!(tBase instanceof DBAutoTempletP))
			return false;

		if (!(parser instanceof DBAutoParser))
			return false;

		return true;
	}

	@Override
	public boolean access() throws Exception {
		String logStr = null;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		int maxRecltTime = taskInfo.getMaxReCollectTime();
		JDBCConnectionMonitor monitor = null;
		try {
			// 获取数据库连接,如果失败则重连
			conn = getConnection();
			monitor = new JDBCConnectionMonitor(taskInfo.getTaskID(), conn, SystemConfig.getInstance().getQueryTimeout() * 1000);
			monitor.startMonit();
			// 采集之前执行的Shell命令
			execShellBeforeAccess();

			redoSqlList = new ArrayList<RedoSQL>();
			DBAutoParser myParser = (DBAutoParser) parser;
			Map<String, Templet> templetsP = ((DBAutoTempletP) taskInfo.getParseTemplet()).getTemplets();

			// 设置需要采集的模板
			setTemplets(templetsP);
			// 遍历所有需要解析的模板
			for (Templet t : templets) {
				GenericSectionHeadD.Templet templetD = ((GenericSectionHeadD) taskInfo.getDistributeTemplet()).getTemplet(t.getId());
				// 查看是否需要要采集此模板对应的表
				if (t == null || !t.isUsed())
					continue;

				Timestamp lastCollectTime = taskInfo.getLastCollectTime();
				// 原表名
				String tableName = t.getTableName();
				// 要查询的表名
				String sqlTName = ConstDef.ParseFilePathForDB(tableName, lastCollectTime);

				// 如果有表不存在，那么此语句将不执行
				boolean flag = tablesExists(conn, sqlTName);
				if (!flag) {
					dbLogger.log(taskInfo.getDevInfo().getOmcID(), templetD.getTables().get(0).getName(), taskInfo.getLastCollectTime(), -1,
							taskInfo.getTaskID());
					if (t.isOccur()) {
						IgnoresInfo ignoresInfo = ignoresMgr.checkIgnore(taskInfo.getTaskID(), sqlTName, taskInfo.getLastCollectTime());
						if (ignoresInfo == null) {
							redoSqlList.add(new RedoSQL(tableName, " 表(" + tableName + ")不存在."));
						} else {
							log.warn(name + " " + sqlTName + "不存在,但igp_conf_ignores表中设置了忽略此路径(" + ignoresInfo + "),不加入补采表.");
						}

					}
					continue;
				} else {
					IgnoresInfo ignoresInfo = ignoresMgr.checkIgnore(taskInfo.getTaskID(), sqlTName, taskInfo.getLastCollectTime());
					if (ignoresInfo != null) {
						log.warn(name + " " + sqlTName + ",igp_conf_ignores表中设置了忽略此路径(" + ignoresInfo + "),  但本次发现其存在,以后将不再忽略此路径.");
						ignoresInfo.setNotUsed();
					}
				}

				String sql = getSql(lastCollectTime, t, sqlTName);

				boolean exceptionFlag = false;
				// 开始查询
				int recordCount = 0;
				double curr1 = System.currentTimeMillis();
				try {
					ps = conn.prepareStatement(sql);
					ps.setQueryTimeout(SystemConfig.getInstance().getQueryTimeout());
					rs = ps.executeQuery();
					// 解析数据
					recordCount = myParser.parseData(rs, t);
					CommonDB.close(rs, ps, null);
					if (recordCount == 0 && maxRecltTime > -1) {
						redoSqlList.add(new RedoSQL(tableName, "select出的记录数为0"));
						dbLogger.log(taskInfo.getDevInfo().getOmcID(), templetD.getTables().get(0).getName(), taskInfo.getLastCollectTime(), 0,
								taskInfo.getTaskID());
					}
				} catch (Exception e) {
					log.error(name + " - 数据库采集出错", e);
					exceptionFlag = true;
				}
				// 如果发生异常就重试
				if (exceptionFlag)
					recordCount = reCollect(myParser, t, templetD, conn, sql, tableName, maxRecltTime);
				// 如果返回结果为-1就表示采集失败,否则表示成功
				if (recordCount == -1)
					continue;
				double curr2 = System.currentTimeMillis();
				double resultTime = (curr2 - curr1) / 1000;
				logStr = name + ": SQL采集成功，select耗时" + resultTime + "秒，数量= " + recordCount + " SQL=" + sql;
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);
			}
		} finally {
			if (monitor != null)
				monitor.endMonit();
			commitFailSql(); // 提交补采任务
			CommonDB.close(rs, ps, conn);
		}

		return true;
	}

	private String getSql(Timestamp lastCollectTime, Templet t, String sqlTName) {
		// 如果有sql语句，那么就直接用sql语句去查询而不需要用表名去查询
		String sql = t.getSql();

		if (sql != null && !sql.equals("")) {
			// 格式转换后的语句
			sql = ConstDef.ParseFilePathForDB(sql, lastCollectTime);
		} else {
			String condition = t.getCondition();
			sql = toSql(sqlTName, condition);
		}
		return sql;
	}

	/**
	 * 如果当前重试次数还没有达到最大重试次数并且采集采集失败时，则一直重试到最大次数
	 * 
	 * @param myParser
	 * @param t
	 * @param sql
	 * @param tableName
	 * @param maxRecltTime
	 * @return
	 */
	private int reCollect(DBAutoParser myParser, Templet t, GenericSectionHeadD.Templet templetD, Connection conn, String sql, String tableName,
			int maxRecltTime) {
		int recordCount = -1;
		PreparedStatement ps = null;
		ResultSet rs = null;

		int currentTry = 1;
		String logStr = null;

		while (currentTry <= MAX_TRY_TIMES) {
			JDBCConnectionMonitor monitor = null;
			try {
				Thread.sleep(SLEEP_TIME);
				logStr = name + ":开始第" + currentTry + "次从数据库重试进行采集!";
				log.info(logStr);
				conn = getConnection();
				monitor = new JDBCConnectionMonitor(taskInfo.getTaskID(), conn, SystemConfig.getInstance().getQueryTimeout() * 1000);
				monitor.startMonit();
				// 采集之前执行的Shell命令
				execShellBeforeAccess();
				ps = conn.prepareStatement(sql);
				ps.setQueryTimeout(SystemConfig.getInstance().getQueryTimeout());
				rs = ps.executeQuery();
				// 解析数据
				recordCount = myParser.parseData(rs, t);
				if (recordCount == 0 && maxRecltTime > -1) {
					redoSqlList.add(new RedoSQL(tableName, "select出的记录数为0"));
					dbLogger.log(taskInfo.getDevInfo().getOmcID(), templetD.getTables().get(0).getName(), taskInfo.getLastCollectTime(), 0,
							taskInfo.getTaskID());
				}
				logStr = name + ":第" + currentTry + "次从数据库重试成功!";
				log.info(logStr);
				break;
			} catch (Exception e) {
				if (currentTry == MAX_TRY_TIMES) {
					redoSqlList.add(new RedoSQL(tableName, "执行select时异常，异常信息为:" + e.getMessage()));
					logStr = name + ": SQL执行失败,并进行了" + currentTry + "次重试,sql=(" + sql + "),原因: ";
					log.error(logStr, e);
					taskInfo.log(DataLogInfo.STATUS_START, logStr, e);
					// 通知告警
					AlarmMgr.getInstance().insert(getTaskID(), "SQL执行失败", name, sql + " " + e.getMessage(), 1507);
					dbLogger.log(taskInfo.getDevInfo().getOmcID(), templetD.getTables().get(0).getName(), taskInfo.getLastCollectTime(), 0,
							taskInfo.getTaskID());
				}
			} finally {
				if (monitor != null)
					monitor.endMonit();
				CommonDB.close(rs, ps, conn);
			}
			currentTry++;
		}

		return recordCount;
	}

	/**
	 * 获取我们需要用到的模板
	 * 
	 * @param templetsP
	 */
	private void setTemplets(Map<String, Templet> templetsP) {
		templets = new ArrayList<Templet>();
		boolean isRegatherObj = CommonDB.isReAdoptObj(taskInfo);
		if (isRegatherObj) {
			// 利用补采表中Collect_Path 保存要补采的表名
			String[] paths = (this.getDataSourceConfig() != null ? this.getDataSourceConfig().getDatas() : null);
			// 如果Collect_Path为空，将采集所有
			if (paths == null || paths.length == 0) {
				templets = templetsP.values();
			} else
			// 如果不为空，那只采列出的数据
			{
				List<Templet> list = new ArrayList<Templet>();
				for (String tableName : paths) {
					if (Util.isNull(tableName))
						continue;
					list.add(templetsP.get(tableName));
				}
				templets.addAll(list);
			}
		} else {
			templets = templetsP.values();
		}
	}
}

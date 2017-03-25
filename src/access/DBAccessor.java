package access;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import parser.LineParser;
import task.RegatherObjInfo;
import task.TaskMgr.RedoSQL;
import util.CommonDB;
import util.Util;
import alarm.AlarmMgr;
import datalog.DataLogInfo;
import distributor.DistributeTemplet;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 数据库接入类
 * 
 * @author IGP TDT
 * @since 3.0
 */
public class DBAccessor extends AbstractDBAccessor {

	public DBAccessor() {
		super();
	}

	@Override
	public boolean access() throws Exception {
		boolean bSucceed = false;

		DistributeTemplet templetD = (DistributeTemplet) taskInfo.getDistributeTemplet();
		int maxRecltTime = taskInfo.getMaxReCollectTime();
		String currentTableName = null;

		String logStr = null;

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		ResultSetMetaData rsm = null;
		List<RedoSQL> redoSqlList = null;
		// 当采集路径中在执行某个语句而发生异常导致退出时，记录下这个语名的索引，
		// 索引之前采集成功的语句就不用加入到补采表中
		int sqlIndex = 0;
		// 用于标识是否发生异常
		boolean exception = false;
		String exceptionMsg = null;
		// 是否为补采
		boolean isRedoFlag = false;
		try {
			// 获取数据库连接,如果失败则重连
			conn = getConnection();
			// 利用Collect_Path 保存sql语句，支持多种表,SQL语句之间用;分隔
			String[] strNeedGatherFileNames = this.getDataSourceConfig().getDatas();
			// 采集之前执行的Shell命令
			execShellBeforeAccess();
			isRedoFlag = CommonDB.isReAdoptObj(taskInfo);
			redoSqlList = new ArrayList<RedoSQL>();
			for (int k = 0; k < strNeedGatherFileNames.length; k++) {
				sqlIndex = k;
				if (Util.isNull(strNeedGatherFileNames[k]))
					continue;

				currentTableName = templetD.tableTemplets.get(k).tableName;
				parser.setFileName("TABLE_" + k);
				String strNewSQL = null;
				strNewSQL = ConstDef.ParseFilePathForDB(strNeedGatherFileNames[k].trim(), taskInfo.getLastCollectTime());

				String tableName = CommonDB.getTableName(strNewSQL);

				// add by 2010-04-08
				int tempIndex = strNewSQL.indexOf("@");
				int redoTableIndex = -1;
				if (isRedoFlag) {
					if (tempIndex != -1) {
						redoTableIndex = Integer.parseInt(strNewSQL.substring(tempIndex + 1).trim());
						strNewSQL = strNewSQL.substring(0, tempIndex);
						//
						((RegatherObjInfo) taskInfo).addTableIndex(redoTableIndex);
						//
						parser.setFileName("TABLE_" + redoTableIndex);
						logStr = name + " : reAdopt table index ： TABLE_" + redoTableIndex;
						log.debug(logStr);
						taskInfo.log(DataLogInfo.STATUS_START, logStr);
					}
					// 如果是补采任务但是filepath字段中不含@,(如为空，则全部数据源进行补采)则
					else {
						((RegatherObjInfo) taskInfo).addTableIndex(k);
					}
				}
				// end

				// 开始查询
				boolean errorFlag = false;
				String sqlEx = null;
				try {
					stmt = conn.prepareStatement(strNewSQL);
					stmt.setQueryTimeout(SystemConfig.getInstance().getQueryTimeout());
					rs = stmt.executeQuery();
					rsm = rs.getMetaData();
				} catch (Exception e) {
					if (maxRecltTime <= -2 && !CommonDB.tableExists(conn, tableName, taskInfo.getTaskID())) {
						logStr = "表" + tableName + "不存在，且MAXCLTTIME<=-2，忽略此表";
						log.warn(logStr);
						taskInfo.log(DataLogInfo.STATUS_START, logStr);
					} else {
						errorFlag = true;
						sqlEx = e.getMessage() == null ? "" : e.getMessage();
						logStr = name + ": SQL执行失败:" + strNewSQL + " Cause: ";
						log.error(logStr, e);
						taskInfo.log(DataLogInfo.STATUS_START, logStr, e);
						// TODO
						// 这里如果直接返回，会导致下面的语句无法进行，其中有一个局点就有出现了一个问题，一批sql执行成功了，直到sql执行失败了后就会有没有clt文件的现象，这里要修改

						// 通知告警
						AlarmMgr.getInstance().insert(taskInfo.getTaskID(), "SQL执行失败", name, strNewSQL + " " + e.getMessage(), 1507);
					}
				}

				if (errorFlag) {
					String redoSql = null;
					// 任务表中
					if (tempIndex == -1 && !isRedoFlag) {
						redoSql = strNewSQL + "@" + k;
						logStr = name + ": add reAdapt SQL :" + strNewSQL + "@" + k;
						log.debug(logStr);
						taskInfo.log(DataLogInfo.STATUS_START, logStr);
					}
					// 补采表中
					else {
						redoSql = strNewSQL + "@" + redoTableIndex;
						logStr = name + ": add reAdapt SQL :" + strNewSQL + "@" + redoTableIndex;
						log.debug(logStr);
						taskInfo.log(DataLogInfo.STATUS_START, logStr);
					}
					if (redoSql != null) {
						redoSqlList.add(new RedoSQL(redoSql, "执行select语句时异常，异常信息为：" + sqlEx));
					}
					dbLogger.log(taskInfo.getDevInfo().getOmcID(), currentTableName, taskInfo.getLastCollectTime(), -1, taskInfo.getTaskID());
					continue;
				}

				logStr = name + ": SQL执行成功: " + strNewSQL;
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);

				StringBuffer buf = new StringBuffer();
				int nLineIndex = 0;
				long recordCount = 0;
				try {

					while (rs.next()) {
						// 获取列数
						int nColumnCount = rsm.getColumnCount();
						for (int i = 0; i < nColumnCount; ++i) {
							// 获取每列的数据
							String strValue = rs.getString(i + 1);
							// 如果是日期内省，截取19位数据 yyyy-mm-dd hh:mi:ss
							if (rsm.getColumnType(i + 1) == 91 || rsm.getColumnType(i + 1) == 92 || rsm.getColumnType(i + 1) == 93) {
								if (strValue == null)
									strValue = "";
								else
									strValue = strValue.substring(0, 19);
							}
							if (strValue == null)
								strValue = "";

							// 去除字段中的噪音字
							strValue = removeNoise(strValue);

							if (i < nColumnCount - 1)
								buf.append(strValue + ";");
							else
								buf.append(strValue + ";0\n");
						}
						nLineIndex++;
						++recordCount;

						if (nLineIndex % 1000 == 0) {
							// 数据库采集都按行分析
							((LineParser) parser).BuildData(buf.toString().toCharArray(), buf.length());
							buf.delete(0, buf.length());
							nLineIndex = 0;
						}
					}
					// 最后一条数据强制加一条数据
					buf.append("**FILEEND**\n");
					((LineParser) parser).BuildData(buf.toString().toCharArray(), buf.length());

					logStr = name + ": " + strNewSQL + " 数据采集完成.此SQL语句共采集数据:" + recordCount;
					log.debug(logStr);
					taskInfo.log(DataLogInfo.STATUS_START, logStr);

				} catch (Exception e) {
					errorFlag = true;
					sqlEx = e.getMessage() == null ? "" : e.getMessage();
					logStr = name + ": 数据库采集失败.ResultSet获取数据异常! Cause: ";
					log.error(logStr, e);
					taskInfo.log(DataLogInfo.STATUS_START, logStr, e);
					// 通知告警
					AlarmMgr.getInstance().insert(taskInfo.getTaskID(), "ResultSet获取数据异常!", name, strNewSQL + " " + e.getMessage(), 1508);
				}
				if (errorFlag) {
					String redoSql = null;
					// 任务表中
					if (tempIndex == -1 && !isRedoFlag) {
						redoSql = strNewSQL + "@" + k;
						logStr = name + ": add reAdapt SQL :" + strNewSQL + "@" + k;
						log.debug(logStr);
						taskInfo.log(DataLogInfo.STATUS_START, logStr);
					}
					// 补采表中
					else {
						redoSql = strNewSQL + "@" + redoTableIndex;
						logStr = name + ": add reAdapt SQL :" + strNewSQL + "@" + redoTableIndex;
						log.debug(logStr);
						taskInfo.log(DataLogInfo.STATUS_START, logStr);
					}
					if (redoSql != null) {
						redoSqlList.add(new RedoSQL(redoSql, "从ResultSet（select出的结果集）中获取数据时异常，异常信息为：" + sqlEx));
					}
					dbLogger.log(taskInfo.getDevInfo().getOmcID(), currentTableName, taskInfo.getLastCollectTime(), -1, taskInfo.getTaskID());
					continue;
				}

				// add on 2010-04-14
				// 对结果集为空的sql语句添加到集合中
				if (recordCount == 0 && maxRecltTime > -1)//
				{
					String redoSql = null;

					// 任务表中
					if (tempIndex == -1) {
						redoSql = strNewSQL + "@" + k;
						logStr = name + ": add reAdapt SQL :" + strNewSQL + "@" + k;
						log.debug(logStr);
						taskInfo.log(DataLogInfo.STATUS_START, logStr);
					}
					// 补采表中
					else {
						redoSql = strNewSQL + "@" + redoTableIndex;
						logStr = name + ": add reAdapt SQL :" + strNewSQL + "@" + redoTableIndex;
						log.debug(logStr);
						taskInfo.log(DataLogInfo.STATUS_START, logStr);
					}
					if (redoSql != null) {
						redoSqlList.add(new RedoSQL(redoSql, "select出来的记录数为0"));
					}
					dbLogger.log(taskInfo.getDevInfo().getOmcID(), currentTableName, taskInfo.getLastCollectTime(), -1, taskInfo.getTaskID());
				}
				// end

			} // for 循环查询多个表

			bSucceed = true;

		} catch (Exception e) {
			logStr = name + ": 数据库采集失败. Cause: ";
			log.error(logStr, e);
			taskInfo.log(DataLogInfo.STATUS_START, logStr, e);
			bSucceed = false;
			exception = true;
			exceptionMsg = e.getMessage() == null ? "" : e.getMessage();
			// 通知告警
			AlarmMgr.getInstance().insert(taskInfo.getTaskID(), "数据库采集失败", name, "原因:" + e.getMessage(), 1508);
		} finally {
			// 如果发生异常就将发生异常的语句添加到redoSqlList中
			if (exception) {

				int sqlLen = getDataSourceConfig().getDatas().length;
				if (sqlIndex < sqlLen) {
					for (int i = sqlIndex; i < sqlLen; i++) {
						String redoSql = null;
						// 如果是补采
						if (isRedoFlag) {

							redoSql = getDataSourceConfig().getDatas()[i];
						} else {
							redoSql = getDataSourceConfig().getDatas()[i] + "@" + i;
						}
						redoSqlList.add(new RedoSQL(redoSql, "数据库采集时发生异常，异常信息:" + exceptionMsg));
					}
				}
				// dbLogger.log(taskInfo.getDevInfo().getOmcID(),
				// currentTableName == null ? "" : currentTableName,
				// taskInfo.getLastCollectTime(), -1, taskInfo.getTaskID());

			}
			// 添加补采任务
			if (redoSqlList.size() > 0) {
				StringBuilder sb = new StringBuilder();
				StringBuilder cause = new StringBuilder();
				String sql = null;
				for (int i = 0; i < redoSqlList.size() - 1; i++) {
					sql = redoSqlList.get(i).sql;
					if (sql == null)
						continue;
					sb.append(sql + ";");
					cause.append("语句\"").append(sql).append("\"补采原因为:").append(redoSqlList.get(i).cause).append("\n\n");
				}
				sb.append(redoSqlList.get(redoSqlList.size() - 1).sql);
				cause.append("语句\"").append(redoSqlList.get(redoSqlList.size() - 1).sql).append("\"补采原因为:")
						.append(redoSqlList.get(redoSqlList.size() - 1).cause).append("\n\n");
				logStr = name + " add  reAdapt filepath ,the SQL is :" + sb.toString();
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);

				task.TaskMgr.getInstance().newRegather(taskInfo, sb.toString(), cause.toString());
				sb.delete(0, sb.length());
				redoSqlList.clear();
			}
			try {
				if (rs != null)
					rs.close();
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			} catch (Exception ex) {
			}
		}

		return bSucceed;
	}

	private String removeNoise(String content) {
		// 字段中不能出现；
		String strValue = content.replaceAll(";", " ");
		// 字段中不能出现换行符号
		strValue = strValue.replaceAll("\r\n", " ");
		strValue = strValue.replaceAll("\n", " ");
		strValue = strValue.replaceAll("\r", " ");
		strValue = strValue.trim();

		return strValue;
	}

}

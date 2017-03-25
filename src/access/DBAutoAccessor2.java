package access;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import parser.DBAutoParser2;
import parser.DBAutoParser3;
import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import task.IgnoresInfo;
import task.IgnoresMgr;
import task.TaskMgr.RedoSQL;
import templet.DBAutoTempletP2;
import templet.DBAutoTempletP2.Templet;
import templet.GenericSectionHeadD;
import templet.TempletBase;
import util.CommonDB;
import util.JDBCConnectionMonitor;
import util.Util;
import util.exception.NullDBConnException;
import alarm.AlarmMgr;
import datalog.DataLogInfo;
import distributor.DistributeTemplet;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 数据库接入类，自动识别数据类型，无需配置字段类型
 * 
 * @author liuwx 2010-7-21
 * @since 1.0
 * @see DBAutoParser2
 * @see DBAutoTempletP2
 */
public class DBAutoAccessor2 extends AbstractDBAccessor {

	// 所有需要采集表的集合
	private Collection<Templet> templets = null;

	private IgnoresMgr ignoresMgr = IgnoresMgr.getInstance();

	private boolean special = false;

	@Override
	public boolean validate() {
		if (taskInfo == null)
			return false;

		// 检查最后一次采集时间是否正确
		try {
			strLastGatherTime = Util.getDateString(taskInfo.getLastCollectTime());
		} catch (Exception e) {
			log.error(name + "> 时间格式错误,原因:", e);
			return false;
		}

		TempletBase tBase = taskInfo.getParseTemplet();
		if (!(tBase instanceof DBAutoTempletP2)) {
			log.error(taskInfo.getTaskID() + " - 请检查模板配置是否正确。");
			return false;
		}
		if (!(parser instanceof DBAutoParser2)) {
			log.error(taskInfo.getTaskID() + " - 请检查模板配置是否正确。");
			return false;
		}

		return true;
	}

	@Override
	public boolean access() throws Exception {

		//DBAutoParser2 myParser = (DBAutoParser2) parser;

		DBAutoTempletP2 temp = (DBAutoTempletP2) taskInfo.getParseTemplet();
		special = temp.getSpecial();

		Map<String, Templet> templetsP = temp.getTemplets();
		// 设置需要采集的模板
		setTemplets(templetsP);

		int temSize = templets.size();
		String key = String.format("[taskId-%s][%s]", taskInfo.getTaskID(), Util.getDateString(taskInfo.getLastCollectTime()));
		log.debug(key+"开始使用多线进行数据库采集,模板个数: " + temSize);

		ExecutorService dsExecutor; // 任务线程池
		int size = SystemConfig.getInstance().getMaxCltSelectParallelCount();
		dsExecutor = Executors.newFixedThreadPool(size);
		CountDownLatch latch = new CountDownLatch(temSize);

		long begin = System.currentTimeMillis();

		try {
			for (Templet t : templets) {
				QueryThread td = new DBAutoAccessor2().new QueryThread(latch, t.getId(),t, parser ,taskInfo,redoSqlList,name);
				log.debug(name+"模板id为("+t.getId()+")的表("+t.getFromTableName()+")加入新线程准备处理.");
				// 开始执行任务操作
				dsExecutor.execute(new Thread(td, " TD -" + td.getId()));

			}
			dsExecutor.shutdown();
			int reportTime = SystemConfig.getInstance().getMaxCltSelectTime();
			if (reportTime <= 0)
				latch.await();
			else
				latch.await(reportTime, TimeUnit.MINUTES);

			long end = System.currentTimeMillis();
			log.debug(key+ "采集完毕,一共消耗时间" + (end - begin)+",还没有结束的任务为:"+latch.getCount());
		} catch (InterruptedException e) {
			log.error("执行sql脚本意外结束,原因:线程被外部意外中断!({})", e);
		} catch (Exception e) {
			log.error("执行sql时发生错误!({})", e);
		} finally {
			if (!dsExecutor.isShutdown())
				dsExecutor.shutdown();
		}

		return true;
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
	private int reCollect(DBAutoParser2 myParser, Templet t, Connection conn, String sql, String tableName, int maxRecltTime,CollectObjInfo taskInfo,List<RedoSQL> redoSqlList) {
		int recordCount = -1;
		PreparedStatement ps = null;
		ResultSet rs = null;

		int currentTry = 1;
		String logStr = null;
		while (currentTry <= MAX_TRY_TIMES) {
			JDBCConnectionMonitor monitor = null;
			try {
				Thread.sleep(SLEEP_TIME);
				logStr = name + ":开始第" + currentTry + "次从数据库重试进行采集!" + " 数据时间： " + taskInfo.getLastCollectTime();
				log.info(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);
				conn = getConnection(taskInfo);
				monitor = new JDBCConnectionMonitor(taskInfo.getTaskID(), conn, SystemConfig.getInstance().getQueryTimeout() * 1000);
				monitor.startMonit();
				// 采集之前执行的Shell命令
				execShellBeforeAccess(taskInfo);
				ps = conn.prepareStatement(sql);
				ps.setQueryTimeout(SystemConfig.getInstance().getQueryTimeout());
				rs = ps.executeQuery();

				// 解析数据
				if (!special) {
					recordCount = myParser.parseData(rs, t);
				} else {
					DBAutoParser3 myParser3 = new DBAutoParser3();
					myParser3.setCollectObjInfo(myParser.getCollectObjInfo());
					recordCount = myParser3.parseData(rs, t);
				}

				if (recordCount == 0 && maxRecltTime > -1) {
					redoSqlList.add(new RedoSQL(tableName, "select出的记录数为0"));
					dbLogger.log(taskInfo.getDevInfo().getOmcID(), t.getDestTableName(), taskInfo.getLastCollectTime(), 0, taskInfo.getTaskID());
				}
				logStr = name + ":第" + currentTry + "次从数据库重试成功!";
				log.info(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);
				break;
			} catch (Exception e) {
				if (currentTry == MAX_TRY_TIMES) {
					redoSqlList.add(new RedoSQL(tableName, "执行select时异常，异常信息为:" + e.getMessage()));
					logStr = name + ": SQL执行失败,并进行了" + currentTry + "次重试,sql=(" + sql + "),原因: ";
					log.error(logStr, e);
					taskInfo.log(DataLogInfo.STATUS_START, logStr, e);
					// 通知告警
					AlarmMgr.getInstance().insert(getTaskID(), "SQL执行失败", name, sql + " " + e.getMessage(), 1507);
					dbLogger.log(taskInfo.getDevInfo().getOmcID(), t.getDestTableName(), taskInfo.getLastCollectTime(), 0, taskInfo.getTaskID());
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
	
	private String getSql(Timestamp lastCollectTime, Templet t, String sqlTName, CollectObjInfo taskInfo) {
		// 如果有sql语句，那么就直接用sql语句去查询而不需要用表名去查询
		String sql = t.getSql();

		if (sql != null && !sql.equals("")) {
			// 格式转换后的语句
			sql = ConstDef.ParseFilePathForDB(sql, lastCollectTime);
		} else {
			String condition = t.getCondition();
			sql = toSql(sqlTName, condition, taskInfo);
		}
		return sql;
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

	class QueryThread extends Thread {

		CountDownLatch latch = null;

		templet.DBAutoTempletP2.Templet t = null;

		DBAutoParser2 myParser = null;
		
		CollectObjInfo taskInfo = null;

		List<RedoSQL> redoSqlList = null;

		String name = null;

		int index = 0;

		public QueryThread(CountDownLatch latch, int index, templet.DBAutoTempletP2.Templet t, Parser  myParser, CollectObjInfo taskInfo,
				List<RedoSQL> redoSqlList, String name) {
			this.latch = latch;
			this.index = index;
			this.t = t;
			this.myParser =( DBAutoParser2)  myParser;

			this.taskInfo = taskInfo;
			this.redoSqlList = redoSqlList;

			this.name = name;
		}

		@Override
		public void run() {
			String sql = null;
			try {
				// 查看是否需要要采集此模板对应的表
				if (t == null || !t.isUsed()){
					log.debug(name+"模板id为("+index+")的表中止处理，模板为空.");
					return;
				}
				String tableName = t.getFromTableName();
				log.debug(name+"模板id为("+index+")的表("+tableName+")处理开始.");
				Thread.sleep(1000);
				Connection conn = null;
				PreparedStatement ps = null;
				ResultSet rs = null;
				int maxRecltTime = taskInfo.getMaxReCollectTime();
				JDBCConnectionMonitor monitor = null;
				try {
					// 获取数据库连接,如果失败则重连
					conn = getConnection(taskInfo);
					monitor = new JDBCConnectionMonitor(taskInfo.getTaskID(), conn, SystemConfig.getInstance().getQueryTimeout() * 1000);
					monitor.startMonit();
					// 采集之前执行的Shell命令
					execShellBeforeAccess(taskInfo);

					redoSqlList = new ArrayList<RedoSQL>();

					// DBAutoParser2 myParser = (DBAutoParser2) parser;
					//
					// DBAutoTempletP2 temp = (DBAutoTempletP2) taskInfo.getParseTemplet();
					// special = temp.getSpecial();
					//
					// Map<String, Templet> templetsP = temp.getTemplets();
					// // 设置需要采集的模板
					// setTemplets(templetsP);
					// 遍历所有需要解析的模板

					Timestamp lastCollectTime = taskInfo.getLastCollectTime();

					// 要查询的表名
					String sqlTName = ConstDef.ParseFilePathForDB(tableName, lastCollectTime);

					// 如果有表不存在，那么此语句将不执行
					boolean flag = tablesExists(conn, sqlTName,taskInfo);
					if (!flag) {
						dbLogger.log(taskInfo.getDevInfo().getOmcID(), t.getDestTableName(), taskInfo.getLastCollectTime(), -1, taskInfo.getTaskID());
						if (t.isOccur()) {
							IgnoresInfo ignoresInfo = ignoresMgr.checkIgnore(taskInfo.getTaskID(), sqlTName, taskInfo.getLastCollectTime());
							if (ignoresInfo == null) {
								redoSqlList.add(new RedoSQL(tableName, " 表(" + tableName + ")不存在添加到补采表."));
							} else {
								log.warn(name + " " + sqlTName + "不存在,但igp_conf_ignores表中设置了忽略此路径(" + ignoresInfo + "),不加入补采表.");
							}
						}
						log.debug(name+"模板id为("+index+")的表("+tableName+")中止处理，表不存在.");
						return;
					} else {
						IgnoresInfo ignoresInfo = ignoresMgr.checkIgnore(taskInfo.getTaskID(), sqlTName, taskInfo.getLastCollectTime());
						if (ignoresInfo != null) {
							log.warn(name + " " + sqlTName + ",igp_conf_ignores表中设置了忽略此路径(" + ignoresInfo + "),  但本次发现其存在,以后将不再忽略此路径.");
							ignoresInfo.setNotUsed();
						}
					}

					sql = getSql(lastCollectTime, t, sqlTName,taskInfo);

					// 开始查询
					int recordCount = 0;
					double curr1 = System.currentTimeMillis();
					try {
						ps = conn.prepareStatement(sql);
						ps.setQueryTimeout(SystemConfig.getInstance().getQueryTimeout());
						ps.setFetchSize(500);
						rs = ps.executeQuery();

						// 解析数据
						if (!special) {
							log.debug(name + "开始parseDate,templetid:"+t.getId());
							recordCount = myParser.parseData(rs, t);
						} else {
							DBAutoParser3 myParser3 = new DBAutoParser3();
							myParser3.setCollectObjInfo(myParser.getCollectObjInfo());

							recordCount = myParser3.parseData(rs, t);
						}
						if (recordCount == 0 && maxRecltTime > -1) {
							redoSqlList.add(new RedoSQL(tableName, "select出来的记录数为0"));
							dbLogger.log(taskInfo.getDevInfo().getOmcID(), t.getDestTableName(), taskInfo.getLastCollectTime(), 0,
									taskInfo.getTaskID());
						}

					} catch (Exception e) {
						log.error(taskInfo.getTaskID() + " 出现异常，目标表=" + t.getDestTableName() + "，sql=" + sql + "，原因:", e);

						recordCount = reCollect(myParser, t, conn, sql, tableName, maxRecltTime,taskInfo, redoSqlList);

					} finally {
						// liangww modify 2012-09-10 当出现socketException时，解决ps没关的问题
						CommonDB.close(rs, ps, null);
					}

					// 如果返回结果为-1就表示采集失败,否则表示成功
					if (recordCount == -1){
						log.debug(name+"模板id为("+index+")的表结束处理，采集失败.");
						return;
					}
					double curr2 = System.currentTimeMillis();
					double resultTime = (curr2 - curr1) / 1000;
					String logStr = name + ": SQL采集入库成功，查询到本地文件产生完毕，一共耗时" + resultTime + "秒，数量= " + recordCount + " SQL=" + sql;
					log.debug(logStr);
					taskInfo.log(DataLogInfo.STATUS_START, logStr);

				} catch (NullDBConnException e) {
					log.error(name + "多次获取数据库连接失败:", e);
				} finally {
					if (monitor != null)
						monitor.endMonit();
					commitFailSql(); // 提交补采任务
					CommonDB.close(rs, ps, conn);
				}

			} catch (Exception e) {
				log.error("执行查询脚本发生错误!{} ,sql" + sql, e);
			} finally {
				latch.countDown();
			}
		}
	}

	// 单元测试
	public static void main(String[] args) {
		CollectObjInfo obj = new CollectObjInfo(755123);
		DBAutoTempletP2 sect = new DBAutoTempletP2();
		try {
			sect.parseTemp("dbauto2_parse_hw.xml");
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		DistributeTemplet dis = new GenericSectionHeadD();
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setMaxReCollectTime(3);
		obj.setDevInfo(dev);
		obj.setParseTemplet(sect);
		obj.setDistributeTemplet(dis);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));
		obj.setTaskID(1212);
		// obj.setDBDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		// obj.setDBUrl("jdbc:sqlserver://192.168.0.170:1433;databaseName=uway");
		// obj.setDevPort(1521);
		// obj.getDevInfo().setHostUser("sa");
		// obj.getDevInfo().setHostPwd("sa");

		// obj.setDBDriver("net.sourceforge.jtds.jdbc.Driver");
		// obj.setDBUrl("jdbc:jtds:sqlserver://192.168.0.170:1433/uway;charset=gb2312");

		// obj.setDevPort(1521);
		// obj.getDevInfo().setHostUser("sa");
		// obj.getDevInfo().setHostPwd("sa");

		// obj.setDBDriver("com.jnetdirect.jsql.JSQLDriver");
		// obj.setDBUrl("jdbc:JSQLConnect://192.168.0.170:1433/uway");
		// obj.setDevPort(1521);
		// obj.getDevInfo().setHostUser("sa");
		// obj.getDevInfo().setHostPwd("sa");
		obj.setDBDriver("oracle.jdbc.driver.OracleDriver");
		obj.setDBUrl("jdbc:oracle:thin:@192.168.0.111:1521:uway");
		obj.setDevPort(1521);
		obj.getDevInfo().setHostUser("uway");
		obj.getDevInfo().setHostPwd("uway");

		DBAutoParser2 xml = new DBAutoParser2();
		xml.setCollectObjInfo(obj);
		DBAutoAccessor2 dc = new DBAutoAccessor2();
		dc.setParser(xml);
		dc.taskInfo = obj;
		try {
			dc.access();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

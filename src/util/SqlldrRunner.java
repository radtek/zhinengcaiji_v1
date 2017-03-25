package util;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

import task.CollectObjInfo;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

public class SqlldrRunner {
	
	public static String RUN_CMD = "sqlldr userid=%s/%s@%s skip=%s control=%s bad=%s log=%s errors=999999 bindsize=20000000 rows=5000 readsize=20000000";

	/**
	 * 命令执行器
	 */
	private ExternalCmd executor;

	/**
	 * oracle服务名
	 */
	private String serviceName;

	/**
	 * oracle用户名
	 */
	private String userName;

	/**
	 * oracle密码
	 */
	private String password;

	/**
	 * sqlldr 控件文件路径
	 */
	private String cltPath;

	/**
	 * bad文件路径
	 */
	private String badpath;

	/**
	 * log文件路径
	 */
	private String logPath;

	/**
	 * 跳过的行数
	 */
	private int skip;

	/**
	 * 命令的执行结果
	 */
	private SqlldrResult result;

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	public SqlldrRunner() {
		super();
	}

	public SqlldrRunner(String serviceName, String userName, String password, String cltPath, String badpath, String logPath, int skip) {
		this();
		this.serviceName = serviceName;
		this.userName = userName;
		this.password = password;
		this.cltPath = cltPath;
		this.badpath = badpath;
		this.logPath = logPath;
		this.skip = skip;
	}

	/**
	 * 开始执行sqlldr命令
	 */
	public void runSqlldr(CollectObjInfo colInfo) {
		long taskID = colInfo.getTaskID();
		long keyID = colInfo.getKeyID();

		Timestamp sqlldrStartTime = new Timestamp(System.currentTimeMillis());
		//10g的数据库有readszie不大于20MB的限定，11g没有限制
		String cmd = RUN_CMD;
		cmd = String.format(cmd, userName, password, serviceName, skip, cltPath, badpath, logPath);
		logger.debug("要执行的sqlldr命令为：" + cmd.replace(userName, "*").replace(password, "*"));

		executor = new ExternalCmd();
		executor.setCmd(cmd);

		int retCode = -1; // 执行sqlldr后的返回码

		try {
			retCode = executor.execute();
			if (retCode == 0 || retCode == 2) {
				logger.debug("Task-" + taskID + "-" + keyID + ": sqldr OK. retCode=" + retCode);
			} else if (retCode != 0 && retCode != 2) {
				int maxTryTimes = 3;
				int tryTimes = 0;
				long waitTimeout = 30 * 1000;
				while (tryTimes < maxTryTimes) {
					retCode = executor.execute();
					if (retCode == 0 || retCode == 2) {
						break;
					}

					tryTimes++;
					waitTimeout = 2 * waitTimeout;

					logger.error("Task-" + taskID + "-" + keyID + ": 第" + tryTimes + "次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode);

					Thread.sleep(waitTimeout);
				}

				// 如果重试超过 maxTryTimes 次还失败则记录日志
				if (retCode == 0 || retCode == 2) {
					logger.info("Task-" + taskID + "-" + keyID + ": " + tryTimes + "次Sqlldr尝试入库后成功. retCode=" + retCode);
				} else {
					logger.error("Task-" + taskID + "-" + keyID + " : " + tryTimes + "次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode);
				}
			}
		} catch (Exception e) {
			logger.error("执行sqlldr时发生异常，原因： " + e.getMessage());
		}

		File logFile = new File(logPath);
		if (!logFile.exists() || !logFile.isFile()) {
			logger.info(logPath + "不存在，任务ID：" + taskID);
			return;
		}
		SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
		try {
			SqlldrResult result = analyzer.analysis(new FileInputStream(logPath));
			if (result == null)
				return;

			logger.debug("Task-" + taskID + "-" + colInfo.getKeyID() + ": SQLLDR日志分析结果: omcid=" + colInfo.getDevInfo().getOmcID() + " 表名="
					+ result.getTableName() + " 数据时间=" + Util.getDateString(colInfo.getLastCollectTime()) + " 入库成功条数=" + result.getLoadSuccCount()
					+ " sqlldr日志=" + logPath);

			dbLogger.log(colInfo.getDevInfo().getOmcID(), result.getTableName(), colInfo.getLastCollectTime(), result.getLoadSuccCount(), taskID,
					colInfo.getGroupId(), result.getLoadFailCount() + result.getLoadSuccCount(), sqlldrStartTime, retCode);
			if (SystemConfig.getInstance().isDeleteLog()) {
				new File(badpath).delete();
				new File(cltPath).delete();
				new File(badpath.replace(".bad", ".txt")).delete();
				new File(logPath).delete();
			}
		} catch (Exception e) {
			logger.error("Task-" + taskID + "-" + keyID + ": sqlldr日志分析失败，文件名：" + logPath + "，原因: ", e);
		}
	}

	public ExternalCmd getExecutor() {
		return executor;
	}

	public void setExecutor(ExternalCmd executor) {
		this.executor = executor;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getCltPath() {
		return cltPath;
	}

	public void setCltPath(String cltPath) {
		this.cltPath = cltPath;
	}

	public SqlldrResult getResult() {
		return result;
	}

	public String getBadpath() {
		return badpath;
	}

	public void setBadpath(String badpath) {
		this.badpath = badpath;
	}

	public String getLogPath() {
		return logPath;
	}

	public void setLogPath(String logPath) {
		this.logPath = logPath;
	}

	/**
	 * 删除txt,log,bad,ctl文件
	 */
	public void delLogs() {
		new File(logPath).delete();
		new File(badpath).delete();
		new File(cltPath).delete();
	}

	public void printResult(SqlldrResult result) {
		logger.info("===============sqlldr结果分析=================");

		logger.info("日志位置：" + logPath);
		logger.info("表名：" + result.getTableName());
		logger.info("载入成功的行数：" + result.getLoadSuccCount());
		logger.info("因数据错误而没有加载的行数：" + result.getData());
		logger.info("因when子句失败页没有加载的行数：" + result.getWhen());
		logger.info("null字段行数：" + result.getNullField());
		logger.info("跳过的逻辑记录总数：" + result.getSkip());
		logger.info("读取的逻辑记录总数：" + result.getRead());
		logger.info("拒绝的逻辑记录总数：" + result.getRefuse());
		logger.info("废弃的逻辑记录总数：" + result.getAbandon());
		logger.info("开始运行时间：" + result.getStartTime());
		logger.info("结束运行时间：" + result.getEndTime());

		logger.info("==============================================");
	}

}

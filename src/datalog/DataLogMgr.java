package datalog;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.Util;
import framework.SystemConfig;

/**
 * 管理数据库日志，即igp_data_log表
 * 
 * @author ChenSijiang 2010-07-23
 * @since 1.1
 */
public class DataLogMgr {

	/**
	 * 存放尚未提交的日志
	 */
	private List<DataLogInfo> logs = new ArrayList<DataLogInfo>();

	/**
	 * 提交入库的间隔
	 */
	private static final int INTERVAL = SystemConfig.getInstance().getDataLogInterval();

	/**
	 * 是否开启数据库日志
	 */
	private static final boolean IS_ENABLE = SystemConfig.getInstance().isEnableDataLog();

	private static DataLogMgr instance;

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	/**
	 * 以单例方式获取此类实例。
	 * 
	 * @return 此类实例
	 */
	public synchronized static DataLogMgr getInstance() {
		if (instance == null) {
			instance = new DataLogMgr();
		}
		return instance;
	}

	private DataLogMgr() {
	}

	/**
	 * 添加一条日志记录，但并不一定立即提交到数据库，仅当已添加的日志达到间隔数时才会提交。 注意：logTime（日志时间）不必设置，由此方法设置日志时间。
	 * 
	 * @param info
	 *            日志信息
	 */
	public void addLog(DataLogInfo info) {
		if (!IS_ENABLE) {
			return;
		}
		if (info == null) {
			logger.error("要添加的日志为null");
			return;
		}
		synchronized (logs) {
			info.setLogTime(new Date());
			logs.add(info);
			if (logs.size() >= INTERVAL) {
				commit();
			}
		}
	}

	/**
	 * 立即将尚未入库的日志，提交至数据库。
	 */
	public void commit() {
		if (!IS_ENABLE) {
			return;
		}
		synchronized (logs) {
			if (SystemConfig.getInstance().isSqlldrDataLog()) {
				sqlldrCommit();
			} else {
				jdbcCommit();
			}
		}
	}

	private void sqlldrCommit() {
		try {
			String currenDate = Util.getDateString_yyyyMMddHHmmssSSS(new Date());
			File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "data_log");
			if (!dir.exists()) {
				dir.mkdir();
			}
			File ctl = new File(dir.getAbsoluteFile(), "data_log_" + currenDate + ".ctl");
			File txt = new File(dir.getAbsoluteFile(), "data_log_" + currenDate + ".txt");
			File log = new File(dir.getAbsoluteFile(), "data_log_" + currenDate + ".log");
			File bad = new File(dir.getAbsoluteFile(), "data_log_" + currenDate + ".bad");

			StringBuilder bufferCtl = new StringBuilder();
			bufferCtl.append("load data\nCHARACTERSET ").append(SystemConfig.getInstance().getSqlldrCharset()).append("\n");
			bufferCtl.append("infile '").append(txt.getAbsolutePath()).append("'");
			bufferCtl.append(" append into table IGP_DATA_LOG\n").append("FIELDS TERMINATED BY \";\"\n");
			bufferCtl.append("TRAILING NULLCOLS\n").append("(LOG_TIME DATE 'YYYY-MM-DD HH24:MI:SS',TASK_ID,TASK_DESCRIPTION,TASK_TYPE,");
			bufferCtl.append("TASK_STATUS,TASK_DETAIL CHAR(4000),TASK_EXCEPTION CHAR(4000),");
			bufferCtl.append("DATA_TIME DATE 'YYYY-MM-DD HH24:MI:SS',COST_TIME,TASK_RESULT)");
			PrintWriter pw = new PrintWriter(ctl);
			pw.print(bufferCtl);
			pw.flush();
			pw.close();

			pw = new PrintWriter(txt);
			int index = 0;
			for (DataLogInfo d : logs) {
				index++;
				StringBuilder tmp = new StringBuilder();
				tmp.append(d.getLogTime() == null ? "" : Util.getDateString(d.getLogTime())).append(";");
				tmp.append(d.getTaskId()).append(";");
				tmp.append(handleStringField(d.getTaskDescription(), 254)).append(";");
				tmp.append(handleStringField(d.getTaskType(), 50)).append(";");
				tmp.append(handleStringField(d.getTaskStatus(), 50)).append(";");
				tmp.append(handleStringField(d.getTaskDetail(), 4000)).append(";");
				tmp.append(handleStringField(d.getTaskException(), 4000)).append(";");
				tmp.append(d.getDataTime() == null ? "" : Util.getDateString(d.getDataTime())).append(";");
				tmp.append(d.getCostTime()).append(";");
				tmp.append(handleStringField(d.getTaskResult(), 50));
				pw.println(tmp);
			}
			pw.flush();
			pw.close();

			String cmd = String.format("sqlldr userid=%s/%s@%s skip=0 control=%s bad=%s log=%s errors=9999", SystemConfig.getInstance()
					.getDbUserName(), SystemConfig.getInstance().getDbPassword(), SystemConfig.getInstance().getDbService(), ctl.getAbsoluteFile(),
					bad.getAbsoluteFile(), log.getAbsoluteFile());
			logger.debug("开始提交数据库日志: "
					+ cmd.replace(SystemConfig.getInstance().getDbUserName(), "*").replace(SystemConfig.getInstance().getDbPassword(), "*"));
			int code = new ExternalCmd().execute(cmd);
			logger.debug("数据库日志提交完毕，返回码为: " + code);
			if (SystemConfig.getInstance().isDelDataLogTmpFile() && code == 0) {
				ctl.delete();
				log.delete();
				bad.delete();
				txt.delete();
			}
		} catch (Exception e) {
			logger.error("提交数据库日志时异常", e);
			return;
		} finally {
			logs.clear();
		}
	}

	private void jdbcCommit() {
		Connection con = DbPool.getConn();
		Statement st = null;
		try {
			con.setAutoCommit(false);
			st = con.createStatement();
			for (DataLogInfo d : logs) {
				String insert = "insert into igp_data_log (log_time,task_id,task_description,task_type,"
						+ "task_status,task_detail,task_exception,data_time,cost_time,task_result) values ("
						+ "%s,%s,'%s','%s','%s','%s','%s',%s,%s,'%s')";
				insert = String.format(insert, d.getLogTime() == null ? "null" : "to_date('" + Util.getDateString(d.getLogTime())
						+ "','yyyy-mm-dd hh24:mi:ss')", d.getTaskId(), handleStringField(d.getTaskDescription(), 255).replace("'", "''"),
						handleStringField(d.getTaskType(), 50), handleStringField(d.getTaskStatus(), 50), handleStringField(d.getTaskDetail(), 4000),
						handleStringField(d.getTaskException(), 4000).replace("'", "''"),
						d.getDataTime() == null ? "null" : "to_date('" + Util.getDateString(d.getDataTime()) + "','yyyy-mm-dd hh24:mi:ss')",
						d.getCostTime(), handleStringField(d.getTaskResult(), 50));
				st.addBatch(insert);
			}
			st.executeBatch();
			con.commit();
			logs.clear();
		} catch (Exception e) {
			logger.error("提交数据库日志时异常，" + logs.size() + "条日志未提交。", e);
			return;
		} finally {
			try {
				if (st != null) {
					st.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
			}
		}
	}

	private String handleStringField(String s, int maxSize) {
		byte[] bs = s.getBytes();
		if (bs.length > maxSize) {
			s = new String(bs, 0, maxSize);
		}
		if (SystemConfig.getInstance().isSqlldrDataLog()) {
			s = s.replaceAll("\n", " ").replaceAll("\r", " ").replaceAll(";", " ");
		}
		return s;
	}

	public static void main(String[] args) throws Exception {
		final DataLogInfo info = new DataLogInfo(
				new Timestamp(Util.getDate1("2010-07-23 17:32:23").getTime()),
				4476,
				"测'试任务",
				"正常任务",
				"解析",
				"测;;;\n\r试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测'试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下日志模块测试一下",
				"eeeeeee", new Timestamp(Util.getDate1("2010-07-10 00:00:00").getTime()), 654654321, "sdf");

		// for (int i = 0; i < 3; i++)
		// {
		// new Thread(new Runnable()
		// {
		// public void run()
		// {
		// for (int j = 0; j < 2; j++)
		// {
		DataLogMgr.getInstance().addLog(info);
		// }
		// }
		// }).start();
		// }
		DataLogMgr.getInstance().commit();
	}
}

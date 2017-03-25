package parser.hw.pm.w.xml;

import java.io.Closeable;
import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import tools.socket.SocketClientHelper;
import util.DBLogger;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

class SqlldrTool {

	private Map<String, List<String>> tableCols;

	private CollectObjInfo task;

	private String logKey;

	private SystemConfig cfg;

	private SqlLdrLogAnalyzer an = new SqlLdrLogAnalyzer();

	// 表名 - SQLLDR信息
	private Map<String, SqlldrInfo> infos = new HashMap<String, SqlldrInfo>();

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	public SqlldrTool(Map<String, List<String>> tableCols, CollectObjInfo task) {
		super();
		this.tableCols = tableCols;
		this.task = task;
		this.logKey = String.format("[%s][%s]", task.getTaskID(), Util.getDateString(task.getLastCollectTime()));
		this.cfg = SystemConfig.getInstance();
	}

	public void init() {
		String time = Util.getDateString_yyyyMMddHHmmss(this.task.getLastCollectTime());
		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "hw_w_pm_xml" + File.separator
				+ task.getTaskID() + File.separator);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		long rnd = System.currentTimeMillis();
		Iterator<String> it = tableCols.keySet().iterator();
		while (it.hasNext()) {
			String tableName = it.next();
			List<String> cols = tableCols.get(tableName);
			String baseName = task.getTaskID() + "_" + tableName + "_" + time + "_" + rnd;
			SqlldrInfo info = new SqlldrInfo(new File(dir, baseName + ".txt"), new File(dir, baseName + ".log"), new File(dir, baseName + ".bad"),
					new File(dir, baseName + ".ctl"));
			infos.put(tableName, info);
			info.writerForCtl.println("LOAD DATA");
			info.writerForCtl.println("CHARACTERSET " + cfg.getSqlldrCharset());
			info.writerForCtl.println("INFILE '" + info.txt.getAbsolutePath() + "' APPEND INTO TABLE " + tableName);
			info.writerForCtl.println("FIELDS TERMINATED BY \"|\"");
			info.writerForCtl.println("TRAILING NULLCOLS");
			info.writerForCtl.print("(");
			for (int i = 0; i < cols.size(); i++) {
				String colName = cols.get(i);
				info.writerForCtl.print("\"" + colName + "\"");
				info.writerForTxt.print(colName);
				if (colName.equals("COLLECTTIME") || colName.equals("STAMPTIME")) {
					info.writerForCtl.print(" DATE 'YYYY-MM-DD HH24:MI:SS'");
				}
				if (i < cols.size() - 1) {
					info.writerForCtl.print(",");
					info.writerForTxt.print("|");
				}
			}
			info.writerForCtl.print(")");
			info.writerForTxt.println();
			info.writerForCtl.flush();
			info.writerForCtl.close();
			info.writerForTxt.flush();
		}
	}

	public void writeRow(List<String> row, String tableName) {
		SqlldrInfo info = infos.get(tableName);
		for (int i = 0; i < row.size(); i++) {
			String val = row.get(i);
			val = Util.nvl(val, "");
			if (val.trim().equalsIgnoreCase("nil"))
				val = "";
			info.writerForTxt.print(val);
			if (i < row.size() - 1) {
				info.writerForTxt.print("|");
			}
		}
		info.writerForTxt.println();
		info.writerForTxt.flush();
	}

	/**
	 * @return socket message
	 */
	public String getMessages(String vendor, String rncName) {
		StringBuffer bs = new StringBuffer();
		Iterator<String> it = infos.keySet().iterator();
		String time = Util.getDateString_yyyyMMddHHmm(task.getLastCollectTime());
		while (it.hasNext()) {
			String tableName = it.next();
			SqlldrInfo sq = infos.get(tableName);
			sq.close();
			// if (!sq.txt.getName().startsWith(rncName)) {
			// File dest = new File(sq.txt.getParent(), rncName + "_" + sq.txt.getName());
			// sq.txt.renameTo(dest);
			// sq.txt = dest;
			// }
			bs.append(time).append(SocketClientHelper.splitSign);
			bs.append(sq.txt.getAbsoluteFile()).append(SocketClientHelper.splitSign);
			bs.append(task.getPeriodTime() / 1000 / 60).append(SocketClientHelper.splitSign);
			bs.append((task instanceof RegatherObjInfo) ? 1 : 0).append(SocketClientHelper.splitSign);
			bs.append(vendor).append(SocketClientHelper.endSign);
			// delete ctl file
			sq.ctl.delete();
		}
		return bs.toString();
	}

	public boolean runSqlldr() {
		Iterator<String> it = infos.keySet().iterator();
		while (it.hasNext()) {
			String tableName = it.next();
			SqlldrInfo info = infos.get(tableName);
			info.close();
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=%s control=%s bad=%s log=%s errors=9999999", cfg.getDbUserName(),
					cfg.getDbPassword(), cfg.getDbService(), 1, info.ctl.getAbsoluteFile(), info.bad.getAbsoluteFile(), info.log.getAbsoluteFile());
			int ret = -1;
			try {
				logger.debug(logKey + "执行sqlldr - " + cmd.replace(cfg.getDbPassword(), "*"));
				ret = new ExternalCmd().execute(cmd);
				SqlldrResult result = an.analysis(info.log.getAbsolutePath());
				dbLogger.log(this.task.getDevInfo().getOmcID(), result.getTableName(), this.task.getLastCollectTime(), result.getLoadSuccCount(),
						this.task.getTaskID());
				logger.debug(logKey
						+ String.format("ret=%s,入库条数=%s,task_id=%s,表名=%s,时间点=%s", ret, result.getLoadSuccCount(), this.task.getTaskID(),
								result.getTableName(), Util.getDateString(this.task.getLastCollectTime())));
				if (ret == 0 && cfg.isDeleteLog()) {
					info.txt.delete();
					info.log.delete();
					info.ctl.delete();
					info.bad.delete();
				}
			} catch (Exception e) {
				logger.error(logKey + "sqlldr时异常", e);
				return false;
			}
		}
		return true;
	}

	private class SqlldrInfo implements Closeable {

		File txt;

		File log;

		File bad;

		File ctl;

		PrintWriter writerForTxt;

		PrintWriter writerForCtl;

		public SqlldrInfo(File txt, File log, File bad, File ctl) {
			super();
			this.txt = txt;
			this.log = log;
			this.bad = bad;
			this.ctl = ctl;
			try {
				writerForTxt = new PrintWriter(txt);
				writerForCtl = new PrintWriter(ctl);
			} catch (Exception unused) {
			}
		}

		@Override
		public void close() {
			IOUtils.closeQuietly(writerForCtl);
			IOUtils.closeQuietly(writerForTxt);

		}

	}
}

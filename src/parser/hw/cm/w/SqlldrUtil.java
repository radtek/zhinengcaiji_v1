package parser.hw.cm.w;

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
import util.DBLogger;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

public class SqlldrUtil {

	private Map<String, List<String>> tableCols;

	private CollectObjInfo task;

	private String logKey;

	private SystemConfig cfg;

	private SqlLdrLogAnalyzer an = new SqlLdrLogAnalyzer();

	// 表名 - SQLLDR信息
	private Map<String, SqlldrInfo> infos = new HashMap<String, SqlldrInfo>();

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	public SqlldrUtil(Map<String, List<String>> tableCols, CollectObjInfo task) {
		super();
		this.tableCols = tableCols;
		this.task = task;
		this.logKey = String.format("[%s][%s]", task.getTaskID(), Util.getDateString(task.getLastCollectTime()));
		this.cfg = SystemConfig.getInstance();
	}

	public void init() {
		File dir = new File(cfg.getCurrentPath() + File.separator + "hw_w_cm_xml" + File.separator + this.task.getTaskID() + File.separator);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		long rnd = System.currentTimeMillis();
		Iterator<String> it = tableCols.keySet().iterator();
		while (it.hasNext()) {
			String tableName = it.next();
			List<String> cols = tableCols.get(tableName);
			String baseName = this.task.getTaskID() + "_" + tableName + "_" + Util.getDateString_yyyyMMddHHmmss(this.task.getLastCollectTime()) + "_"
					+ rnd;
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
				info.writerForCtl.print(colName);
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
			info.writerForTxt.print(Util.nvl(val, ""));
			if (i < row.size() - 1) {
				info.writerForTxt.print("|");
			}
		}
		info.writerForTxt.println();
		info.writerForTxt.flush();
	}

	public void runSqlldr() {
		Iterator<String> it = infos.keySet().iterator();
		while (it.hasNext()) {
			String tableName = it.next();
			SqlldrInfo info = infos.get(tableName);
			info.close();
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=%s control=%s bad=%s log=%s errors=9999999 bindsize=20000000 rows=5000 readsize=20000000", cfg.getDbUserName(),
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
			}
		}
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

package parser.hw.spas;

import java.io.Closeable;
import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import util.CommonDB;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

class HW_XMLSqlldr {

	CollectObjInfo task;

	Map<String, SQLLDR_INFO> infos;

	static ConcurrentMap<String, List<String>> COLS_MAP = new ConcurrentHashMap<String, List<String>>();

	static final Logger log = LogMgr.getInstance().getSystemLogger();

	static final SystemConfig cfg = SystemConfig.getInstance();

	HW_XMLSqlldr(CollectObjInfo task) {
		super();
		this.task = task;
		this.infos = new HashMap<String, SQLLDR_INFO>();
	}

	public void add(HW_XML_MO_Entry entry, String table, String strStamptime, String strCollecttime) {
		List<String> cols = null;
		if (COLS_MAP.containsKey(table)) {
			cols = COLS_MAP.get(table);
		} else {
			try {
				cols = CommonDB.loadCols(table);
				COLS_MAP.put(table, cols);
			} catch (Exception e) {
				log.error(task.getTaskID() + " 载入" + table + "表时出错，可能此表不存在，或数据库连接失败。", e);
				return;
			}
		}
		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "hw_cm_xml_cdma"
				+ File.separator + task.getTaskID() + File.separator);
		dir.mkdirs();

		SQLLDR_INFO info = null;

		if (!infos.containsKey(table)) {
			String baseName = this.task.getTaskID() + "_" + table + "_" + Util.getDateString_yyyyMMddHHmmss(this.task.getLastCollectTime()) + "_"
					+ System.currentTimeMillis();
			info = new SQLLDR_INFO(new File(dir, baseName + ".txt"), new File(dir, baseName + ".log"), new File(dir, baseName + ".bad"), new File(
					dir, baseName + ".ctl"));
			infos.put(table, info);
			info.writerForCtl.println("LOAD DATA");
			info.writerForCtl.println("CHARACTERSET " + cfg.getSqlldrCharset());
			info.writerForCtl.println("INFILE '" + info.txt.getAbsolutePath() + "' APPEND INTO TABLE " + table);
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
		} else {
			info = infos.get(table);
		}

		for (int i = 0; i < cols.size(); i++) {
			String col = cols.get(i);
			if (col.equals("OMCID")) {
				info.writerForTxt.print(task.spasOmcId);
			} else if (col.equals("STAMPTIME")) {
				info.writerForTxt.print(strStamptime);
			} else if (col.equals("COLLECTTIME")) {
				info.writerForTxt.print(strCollecttime);
			} else {
				String val = entry.getAttrs().get(col);
				if (Util.isNotNull(val))
					info.writerForTxt.print(val);
				else
					info.writerForTxt.print("");
			}
			if (i < cols.size() - 1)
				info.writerForTxt.print("|");
		}
		info.writerForTxt.println();
		info.writerForTxt.flush();
	}

	public void commit() {
		Iterator<String> it = infos.keySet().iterator();
		while (it.hasNext()) {
			String tableName = it.next();
			SQLLDR_INFO info = infos.get(tableName);
			info.close();
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=%s control=%s bad=%s log=%s errors=9999999", cfg.getDbUserName(),
					cfg.getDbPassword(), cfg.getDbService(), 1, info.ctl.getAbsoluteFile(), info.bad.getAbsoluteFile(), info.log.getAbsoluteFile());
			int ret = -1;
			try {
				log.debug(task.getTaskID() + " 执行sqlldr - " + cmd.replace(cfg.getDbPassword(), "*"));
				ret = new ExternalCmd().execute(cmd);
				SqlldrResult result = new SqlLdrLogAnalyzer().analysis(info.log.getAbsolutePath());
				LogMgr.getInstance()
						.getDBLogger()
						.log(Integer.parseInt(this.task.spasOmcId), result.getTableName(), this.task.getLastCollectTime(), result.getLoadSuccCount(),
								this.task.getTaskID());
				log.debug(task.getTaskID()
						+ String.format(" ret=%s,入库条数=%s,task_id=%s,表名=%s,时间点=%s", ret, result.getLoadSuccCount(), this.task.getTaskID(),
								result.getTableName(), Util.getDateString(this.task.getLastCollectTime())));
				if (ret == 0 && cfg.isDeleteLog()) {
					info.txt.delete();
					info.log.delete();
					info.ctl.delete();
					info.bad.delete();
				}
			} catch (Exception e) {
				log.error(task.getTaskID() + " sqlldr时异常", e);
			}
		}
	}

}

class SQLLDR_INFO implements Closeable {

	File txt;

	File log;

	File bad;

	File ctl;

	PrintWriter writerForTxt;

	PrintWriter writerForCtl;

	public SQLLDR_INFO(File txt, File log, File bad, File ctl) {
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

package parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.jsp.jstl.sql.Result;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import util.CommonDB;
import util.DBLogger;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.LogAnalyzerException;
import util.loganalyzer.SqlLdrLogAnalyzer;
import access.special.luc.LucSubTemplet;
import access.special.luc.LucTemplet;
import framework.SystemConfig;

public class SmartLucTelnetParser extends Parser {

	/* 缓存阿朗.SCT模板 */
	public static final Map<String/* 小写模板名，如/home/omp6/cdhfl.sct，带目录 */, Map<Integer/* 字段位置 */, String/* 字段名 */>> SCT_CACHE = new HashMap<String, Map<Integer, String>>();

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final DBLogger dblogger = LogMgr.getInstance().getDBLogger();

	private static final SystemConfig cfg = SystemConfig.getInstance();

	private static final String SQLLDR_SEP = ";";

	/* sqlldr files. */
	private File txt;

	private File ctl;

	private File log;

	private File bad;

	private PrintWriter txtWriter;

	private PrintWriter ctlWriter;

	private Map<Integer, String> indexField = new HashMap<Integer, String>();

	private Map<String, Integer> fieldIndex = new HashMap<String, Integer>();

	private Map<Integer, String> sctMap;

	private LucSubTemplet subTemplet;

	String strCollecttime;

	String strStamptime;

	@Override
	public boolean parseData() throws Exception {
		return true;
	}

	private StringBuilder cache = new StringBuilder();

	public void parse(final String data) {
		if (data == null)
			return;

		String content = data;
		boolean end = false;
		if (collectObjInfo.getDevInfo().getHostSign() != null)
			content = content.replace(collectObjInfo.getDevInfo().getHostSign().trim(), "");
		if (content.contains("**FILEEND**")) {
			end = true;
			content = content.substring(0, content.indexOf("**FILEEND**"));
		}
		cache.append(content);
		if (subTemplet == null || sctMap == null)
			return;

		if (end) {
			BufferedReader br = new BufferedReader(new StringReader(cache.toString()));

			String line = null;
			List<String> vals = new ArrayList<String>(indexField.size() + 3);
			try {
				while ((line = br.readLine()) != null) {
					if (Util.isNull(line))
						continue;

					String[] sp = line.split(subTemplet.getSeparator(), 999);
					if (sp.length != sctMap.size()) {
						continue;
					}
					vals.clear();
					for (int j = 0; j < indexField.size() + 3; j++) {
						vals.add("");
					}
					vals.set(0, collectObjInfo.getDevInfo().getOmcID() + "");
					vals.set(1, strCollecttime);
					vals.set(2, strStamptime);
					for (int j = 0; j < sp.length; j++) {
						String rawname = sctMap.get(j).toLowerCase();
						String colName = subTemplet.getFileds().get(rawname);
						if (colName != null) {
							Integer index = fieldIndex.get(colName) + 3;
							vals.set(index, sp[j]);
						}
					}
					for (int j = 0; j < vals.size(); j++) {
						String writerVal = vals.get(j);
						writerVal = (writerVal != null ? writerVal.trim() : "");
						txtWriter.print(writerVal);
						if (j < vals.size() - 1)
							txtWriter.print(SQLLDR_SEP);
					}
					txtWriter.println();
					txtWriter.flush();
				}
			} catch (Exception e) {
				logger.error(collectObjInfo.getTaskID() + " 写sqlldr文件异常", e);
			} finally {
				IOUtils.closeQuietly(br);
				dispose();
			}

			dispose();
			String cmd = "sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=99999999";
			cmd = String.format(cmd, cfg.getDbUserName(), cfg.getDbPassword(), cfg.getDbService(), ctl.getAbsolutePath(), bad.getAbsolutePath(),
					log.getAbsolutePath());
			logger.debug(collectObjInfo.getTaskID() + " 当前执行的sqlldr命令为 - " + cmd);
			int ret = -1;
			try {
				ret = new ExternalCmd().execute(cmd);
			} catch (Exception e) {
				logger.error(collectObjInfo.getTaskID() + " 执行sqlldr命令时发生异常 - " + cmd);
			}
			try {
				SqlldrResult sr = new SqlLdrLogAnalyzer().analysis(log.getAbsolutePath());
				logger.debug(collectObjInfo.getTaskID() + " SQLLDR结果：ret=" + ret + ", omcid=" + collectObjInfo.getDevInfo().getOmcID() + ", 表名="
						+ sr.getTableName() + ", 入库成功条数=" + sr.getLoadSuccCount());
				dblogger.log(collectObjInfo.getDevInfo().getOmcID(), sr.getTableName(), collectObjInfo.getLastCollectTime(), sr.getLoadSuccCount(),
						collectObjInfo.getTaskID());
			} catch (LogAnalyzerException e) {
				logger.error(collectObjInfo.getTaskID() + " 分析SQLLDR日志时异常 - " + log, e);
				dblogger.log(collectObjInfo.getDevInfo().getOmcID(), "", collectObjInfo.getLastCollectTime(), 0, collectObjInfo.getTaskID());
			}
			if (ret == 0) {
				txt.delete();
				log.delete();
				bad.delete();
				ctl.delete();
			} else if (ret == 2) {
				bad.delete();
				txt.delete();
			}
		}
	}

	public void dispose() {

		cache.setLength(0);
		IOUtils.closeQuietly(txtWriter);
		IOUtils.closeQuietly(ctlWriter);

	}

	public void setSctInfo(String sctName, String logName, Map<Integer, String> s) {
		synchronized (SCT_CACHE) {

			sctMap = s;
		}
		strCollecttime = Util.getDateString(new Date());
		strStamptime = Util.getDateString(collectObjInfo.getLastCollectTime());

		// init sqlldr
		SystemConfig sys = SystemConfig.getInstance();

		int tmpId = collectObjInfo.getParseTmpID();
		String sql = "select tempfilename as TNAME from igp_conf_templet where tmpid = " + tmpId;
		Result rs = null;
		try {
			rs = CommonDB.queryForResult(sql);
			if (rs == null || rs.getRows().length < 1)
				throw new SQLException("Empty ResultSet.");
		} catch (Exception e) {
			logger.error(collectObjInfo.getTaskID() + " 获取解析模板失败 " + sql);
		}

		File tfile = new File(sys.getTempletPath() + File.separator + rs.getRows()[0].get("TNAME"));
		LucTemplet templet = LucTemplet.parse(new File(sys.getTempletPath() + File.separator + rs.getRows()[0].get("TNAME")));
		if (templet == null) {
			logger.error(collectObjInfo.getTaskID() + " 解析模板失败 " + tfile);
			return;
		}

		File baseDir = new File(sys.getCurrentPath() + File.separator + "ldrlog" + File.separator + "cdma_luc_telnet" + File.separator
				+ collectObjInfo.getTaskID() + File.separator);
		if (!baseDir.exists() || !baseDir.isDirectory())
			baseDir.mkdirs();

		subTemplet = templet.findBySctLog(sctName, logName);
		if (subTemplet == null) {
			logger.error(collectObjInfo.getTaskID() + " 未在解析模板(" + tfile + ")中找到对应的项 - sct=" + sctName + ",log=" + logName);
			return;
		}

		String baseName = subTemplet.getId() + "_" + subTemplet.getTable() + "_" + Util.getDateString_yyyyMMddHH(collectObjInfo.getLastCollectTime())
				+ "_" + System.currentTimeMillis();
		txt = new File(baseDir, baseName + ".txt");
		ctl = new File(baseDir, baseName + ".ctl");
		bad = new File(baseDir, baseName + ".bad");
		log = new File(baseDir, baseName + ".log");
		try {
			txtWriter = new PrintWriter(txt);
			ctlWriter = new PrintWriter(ctl);
		} catch (Exception e) {
			logger.error(collectObjInfo.getTaskID() + " 创建SQLLDR文件时出错", e);
			return;
		}

		ctlWriter.println("LOAD DATA");
		ctlWriter.println("CHARACTERSET " + sys.getSqlldrCharset());
		ctlWriter.println("INFILE '" + txt.getAbsolutePath() + "' APPEND INTO TABLE " + subTemplet.getTable());
		ctlWriter.println("FIELDS TERMINATED BY \"" + SQLLDR_SEP + "\"");
		ctlWriter.println("TRAILING NULLCOLS (");
		ctlWriter.println("OMCID,");
		ctlWriter.println("COLLECTTIME DATE 'YYYY-MM-DD HH24:MI:SS',");
		ctlWriter.println("STAMPTIME DATE 'YYYY-MM-DD HH24:MI:SS',");
		txtWriter.print("OMCID" + SQLLDR_SEP + "COLLECTTIME" + SQLLDR_SEP + "STAMPTIME" + SQLLDR_SEP);
		List<String> fields = new ArrayList<String>(subTemplet.getFileds().values());
		int index = 0;
		for (int i = 0; i < fields.size(); i++) {
			int ii = index++;
			indexField.put(ii, fields.get(i));
			fieldIndex.put(fields.get(i), ii);
			ctlWriter.print(fields.get(i));
			txtWriter.print(fields.get(i));
			if (i < fields.size() - 1) {
				ctlWriter.print(",");
				txtWriter.print(SQLLDR_SEP);
			}
			ctlWriter.println();
		}
		ctlWriter.println(")");
		txtWriter.println();
		ctlWriter.flush();
		IOUtils.closeQuietly(ctlWriter);
		txtWriter.flush();
	}
}

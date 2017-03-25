package parser.c.ft;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * C网投诉数据。
 * 
 * @author ChenSijiang 2011-4-8 上午09:40:23
 */
public class CV1ASCII extends Parser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final int DEFAULT_BUFFER_SIZE = 1024;

	private static final String DEFAULT_ROW_SEP = "~$~"; // 记录分隔符

	private static final String DEFAULT_FIELD_SEP = "~!~"; // 字段分隔符

	private String logKey;

	private String stamptime;

	private String omcid;

	private String collecttime;

	private Timestamp tsStamptime;

	private Timestamp tsCollecttime;

	private String rowSep;

	private String fieldSep;

	@Override
	public boolean parseData() throws Exception {

		this.stamptime = Util.getDateString(collectObjInfo.getLastCollectTime());
		this.tsCollecttime = new Timestamp(System.currentTimeMillis());
		this.tsStamptime = new Timestamp(collectObjInfo.getLastCollectTime().getTime());
		this.collecttime = Util.getDateString(this.tsCollecttime);
		this.omcid = String.valueOf(collectObjInfo.getDevInfo().getOmcID());
		this.logKey = String.format("[%s][%s]", collectObjInfo.getTaskID(), this.stamptime);

		Map<String, FTTemplet> templets = null;
		FTTemplet templet = null;
		SqlldrInfo sqlldr = null;
		File ftFile = new File(getFileName());
		InputStream in = null;
		InputStreamReader isr = null;
		BufferedReader br = null;

		try {
			logger.info(logKey + "开始解析 - " + ftFile.getAbsolutePath());
			templets = Helper.readTemplet(getCollectObjInfo().getParseTmpID());
			Iterator<String> tkeys = templets.keySet().iterator();
			while (tkeys.hasNext()) {
				String pat = tkeys.next();
				if (FilenameUtils.wildcardMatch(ftFile.getName().toLowerCase(), pat.toLowerCase()))
					templet = templets.get(pat);
			}
			if (templet == null)
				throw new Exception("未能找到相应的模板，要解析的文件 - " + ftFile.getAbsolutePath());
			else
				sqlldr = buildSqlldr(templet);

			if (Util.isNotNull(templet.getFieldSep()))
				fieldSep = templet.getFieldSep();
			else
				fieldSep = DEFAULT_FIELD_SEP;

			if (Util.isNotNull(templet.getRowSep()))
				rowSep = templet.getRowSep();
			else
				rowSep = DEFAULT_ROW_SEP;

			in = new FileInputStream(ftFile);
			isr = new InputStreamReader(in);
			br = new BufferedReader(isr);

			StringBuilder cachedString = new StringBuilder();
			char[] cs = new char[DEFAULT_BUFFER_SIZE];
			int readCount = -1;
			while ((readCount = br.read(cs)) != -1) {
				cachedString.append(new String(cs, 0, readCount).replace("\n", " ").replace("\r", " "));
				List<String> raws = splitRaw(cachedString);
				for (String raw : raws) {
					Record record = createRecord(raw);
					if (record != null) {
						StringBuilder txtLine = new StringBuilder();
						txtLine.append(this.omcid).append(";");
						txtLine.append(this.collecttime).append(";");
						txtLine.append(this.stamptime).append(";");
						for (int i = 0; i < record.vals.size(); i++) {
							txtLine.append(HtmlRegexpUtil.filterHtml(record.vals.get(i)));
							if (i < record.vals.size() - 1)
								txtLine.append(";");
						}
						sqlldr.writerForTxt.println(txtLine);
						txtLine.setLength(0);
						txtLine = null;
						record = null;
					}
				}
			}
			logger.info(logKey + "解析完毕 - " + ftFile.getAbsolutePath());

			logger.info(logKey + "开始入库 - " + ftFile.getAbsolutePath());
			store(sqlldr);
			logger.info(logKey + "入库完毕 - " + ftFile.getAbsolutePath());

		} catch (Exception e) {
			logger.error(logKey + "解析/入库投诉数据时发生异常 - " + ftFile.getAbsolutePath(), e);
			return false;
		} finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(isr);
			IOUtils.closeQuietly(in);
		}
		return true;
	}

	private void store(SqlldrInfo sqlldr) {
		String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", SystemConfig.getInstance()
				.getDbUserName(), SystemConfig.getInstance().getDbPassword(), SystemConfig.getInstance().getDbService(),
				sqlldr.ctl.getAbsoluteFile(), sqlldr.bad.getAbsoluteFile(), sqlldr.log.getAbsoluteFile());
		sqlldr.close();
		logger.debug(logKey + "执行 "
				+ cmd.replace(SystemConfig.getInstance().getDbPassword(), "*").replace(SystemConfig.getInstance().getDbUserName(), "*"));
		ExternalCmd execute = new ExternalCmd();
		try {
			int ret = execute.execute(cmd);
			SqlldrResult result = new SqlLdrLogAnalyzer().analysis(sqlldr.log.getAbsolutePath());
			logger.debug(logKey + "exit=" + ret + " omcid=" + omcid + " 入库成功条数=" + result.getLoadSuccCount() + " 表名=" + result.getTableName()
					+ " 数据时间=" + stamptime + " sqlldr日志=" + sqlldr.log.getAbsolutePath());
			LogMgr.getInstance()
					.getDBLogger()
					.log(collectObjInfo.getDevInfo().getOmcID(), result.getTableName(), tsStamptime, result.getLoadSuccCount(),
							collectObjInfo.getTaskID());
			if (ret == 0 && SystemConfig.getInstance().isDeleteLog()) {
				sqlldr.txt.delete();
				sqlldr.ctl.delete();
				sqlldr.log.delete();
				sqlldr.bad.delete();
			}
		} catch (Exception ex) {
			logger.error(logKey + "sqlldr时异常", ex);
		}
	}

	private SqlldrInfo buildSqlldr(FTTemplet templet) {
		SqlldrInfo info = null;
		File baseDir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "c_ft" + File.separator
				+ getCollectObjInfo().getTaskID() + File.separator);
		baseDir.mkdirs();

		String baseName = templet.getTable() + "_" + Util.getDateString_yyyyMMddHH(tsStamptime) + "_" + System.currentTimeMillis();

		info = new SqlldrInfo(new File(baseDir, baseName + ".txt"), new File(baseDir, baseName + ".log"), new File(baseDir, baseName + ".bad"),
				new File(baseDir, baseName + ".ctl"));

		info.writerForCtl.println("LOAD DATA");
		info.writerForCtl.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
		info.writerForCtl.println("INFILE '" + info.txt.getAbsolutePath() + "' APPEND INTO TABLE " + templet.getTable());
		info.writerForCtl.println("FIELDS TERMINATED BY \";\"");
		info.writerForCtl.println("TRAILING NULLCOLS");
		info.writerForCtl.println("(");
		info.writerForCtl.println("OMCID,");
		info.writerForCtl.println("COLLECTTIME DATE 'YYYY-MM-DD HH24:MI:SS',");
		info.writerForCtl.println("STAMPTIME DATE 'YYYY-MM-DD HH24:MI:SS',");
		info.writerForTxt.print("OMCID;COLLECTTIME;STAMPTIME;");
		List<FTField> cols = new ArrayList<FTField>(templet.getFields().values());
		for (int i = 0; i < cols.size(); i++) {
			info.writerForCtl.println(cols.get(i) + (Util.isNull(cols.get(i).getType()) ? "" : " " + cols.get(i).getType())
					+ (i < cols.size() - 1 ? "," : ""));
			info.writerForTxt.print(cols.get(i) + (i < cols.size() - 1 ? ";" : ""));
		}
		info.writerForTxt.println();
		info.writerForCtl.println(")");
		info.writerForCtl.flush();
		info.writerForTxt.flush();
		return info;
	}

	// 从缓存到的一串字符中，取出一条原始记录（文件中每条原始记录之间，是用"~$~"分隔的）
	private List<String> splitRaw(StringBuilder s) {
		List<String> result = new ArrayList<String>();

		int index = s.indexOf(rowSep);
		while (index > -1) {
			result.add(s.substring(0, index));
			s.delete(0, index + rowSep.length());
			index = s.indexOf(rowSep);
		}

		return result;
	}

	private Record createRecord(String raw) {
		if (Util.isNull(raw))
			return null;
		Record r = null;
		try {
			String[] sp = raw.split(fieldSep, 999);
			List<String> vals = new ArrayList<String>();
			for (String s : sp)
				vals.add(s);
			r = new Record(raw, vals);
		} catch (Exception e) {
			logger.error(logKey + "解析一条原始数据时异常 - " + raw, e);
			return null;
		}
		return r;
	}

	class Record {

		String raw;// 原始数据内容

		List<String> vals; // 解析出的所有值

		public Record(String raw, List<String> vals) {
			super();
			this.raw = raw;
			this.vals = vals;
		}

		@Override
		public String toString() {
			return "Record [vals=" + vals + "]";
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
			} catch (Exception ex) {
				logger.error(logKey + "创建txt/ctl文件时发生异常", ex);
			}

		}

		@Override
		public void close() {
			IOUtils.closeQuietly(writerForCtl);
			IOUtils.closeQuietly(writerForTxt);

		}

	}

	public static void main(String[] args) {
		CollectObjInfo obj = new CollectObjInfo(31601);
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setParseTmpID(11040801);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));

		CV1ASCII w = new CV1ASCII();
		w.fileName = "C:\\Users\\ChenSijiang\\Desktop\\数据样例\\vw_nbi_tbFTFaultTicket.txt";
		w.collectObjInfo = obj;
		try {
			w.parseData();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

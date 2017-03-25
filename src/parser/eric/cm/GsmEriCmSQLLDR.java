package parser.eric.cm;

import java.io.Closeable;
import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.eric.cm.EricssonV1CmParserImp.ColInfo;
import util.CommonDB;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.LogAnalyzerException;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

public class GsmEriCmSQLLDR {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final Map<String, List<String>> TB_COLS = new HashMap<String, List<String>>();

	private static final SystemConfig CFG = SystemConfig.getInstance();

	private String logKey;

	private long taskId;

	private int omcId;

	private Timestamp time;

	private String strTime;

	private String strCt;

	private String bscName;

	private Map<String, SLInfo> infos = new HashMap<String, GsmEriCmSQLLDR.SLInfo>();

	public GsmEriCmSQLLDR(long taskId, Timestamp time, int omcId, String bscName) {
		super();
		this.taskId = taskId;
		this.time = time;
		this.omcId = omcId;
		this.bscName = bscName;
		this.strCt = Util.getDateString(new Date());
		this.strTime = Util.getDateString(time);
		this.logKey = taskId + "[gsm爱立信参数]";

	}

	public void readyForSqlldr() {
		if (TB_COLS.size() == 0)
			loadCols();
		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "gsm_eri_ccd" + File.separator);
		dir.mkdirs();
		logger.debug(logKey + "临时文件路径为 - " + dir.getAbsolutePath());
		String baseName = taskId + "_" + Util.getDateString_yyyyMMddHH(this.time) + "_" + System.currentTimeMillis() + "_";
		Iterator<Entry<String, List<String>>> it = TB_COLS.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, List<String>> en = it.next();
			String tn = en.getKey();
			List<String> cols = en.getValue();
			String fn = baseName + tn;
			SLInfo info = new SLInfo(new File(dir, fn + ".txt"), new File(dir, fn + ".log"), new File(dir, fn + ".ctl"), new File(dir, fn + ".bad"));
			info.writerForCtl.println("load data");
			info.writerForCtl.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
			info.writerForCtl.println("infile '" + info.txt.getAbsolutePath() + "' append into table " + tn);
			info.writerForCtl.println("FIELDS TERMINATED BY \"|\"");
			info.writerForCtl.println("TRAILING NULLCOLS");
			info.writerForCtl.println("(");
			for (int i = 0; i < cols.size(); i++) {
				String cn = "\"" + cols.get(i) + "\"";
				info.writerForTxt.print(cn);
				if (i < cols.size() - 1)
					info.writerForTxt.print("|");
				if (cn.equals("\"STAMPTIME\"") || cn.equals("\"COLLECTTIME\""))
					cn = cn + " date 'yyyy-mm-dd hh24:mi:ss'";
				info.writerForCtl.print(cn);
				if (i < cols.size() - 1)
					info.writerForCtl.println(",");
			}
			info.writerForCtl.println(")");
			info.writerForCtl.flush();
			info.writerForTxt.println();
			info.writerForTxt.flush();
			infos.put(tn, info);
		}
	}

	public boolean sqlldr() {
		Iterator<Entry<String, SLInfo>> it = infos.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, SLInfo> en = it.next();
			SLInfo info = en.getValue();
			info.close();
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", CFG.getDbUserName(),
					CFG.getDbPassword(), CFG.getDbService(), info.ctl.getAbsoluteFile(), info.bad.getAbsoluteFile(), info.log.getAbsoluteFile());
			logger.debug(logKey + "执行sqlldr - " + cmd);
			int ret = -1;
			try {
				ret = new ExternalCmd().execute(cmd);
			} catch (Exception e) {
				logger.error(logKey + "执行sqlldr异常 " + cmd, e);
				return false;
			}
			try {
				SqlldrResult result = new SqlLdrLogAnalyzer().analysis(info.log.getAbsolutePath());
				LogMgr.getInstance().getDBLogger().log(this.omcId, result.getTableName(), this.strTime, result.getLoadSuccCount(), taskId);
				logger.debug(logKey + "exit=" + ret + " omcid=" + this.omcId + " 入库成功条数=" + result.getLoadSuccCount() + " 表名="
						+ result.getTableName() + " 数据时间=" + this.strTime + " sqlldr日志=" + info.log.getAbsolutePath());
			} catch (LogAnalyzerException e) {
				logger.error(logKey + "分析sqlldr日志异常 - " + info.log.getAbsolutePath(), e);
				return false;
			}
			if (CFG.isDeleteLog()) {
				if (ret == 0) {
					info.txt.delete();
					info.ctl.delete();
					info.log.delete();
					info.bad.delete();
				} else if (ret == 2) {
					info.bad.delete();
					info.txt.delete();
				}
			}
		}
		return true;
	}

	public void writeRow(String tableName, List<ColInfo> row, List<String> values) {
		if (TB_COLS.containsKey(tableName) && infos.containsKey(tableName)) {
			List<ColInfo> tmpRow = new ArrayList<EricssonV1CmParserImp.ColInfo>();
			tmpRow.addAll(row);
			tmpRow.add(new ColInfo("OMCID", 0, 0));
			tmpRow.add(new ColInfo("COLLECTTIME", 0, 0));
			tmpRow.add(new ColInfo("STAMPTIME", 0, 0));
			tmpRow.add(new ColInfo("BSC_NAME", 0, 0));

			List<String> tmpVal = new ArrayList<String>();
			tmpVal.addAll(values);
			tmpVal.add(String.valueOf(omcId));
			tmpVal.add(strCt);
			tmpVal.add(strTime);
			tmpVal.add(bscName);

			List<String> tcols = TB_COLS.get(tableName);
			List<String> writeVals = new ArrayList<String>(tmpVal.size());
			for (int i = 0; i < tcols.size(); i++)
				writeVals.add("");

			for (int i = 0; i < tmpRow.size(); i++) {
				String ctxt = tmpRow.get(i).colText;
				if (tcols.contains(ctxt)) {

					writeVals.set(tcols.indexOf(ctxt), tmpVal.get(i));

				}
			}

			SLInfo info = infos.get(tableName);
			StringBuilder buff = new StringBuilder();
			// buff.append(omcId).append("|").append(strCt).append("|").append(strTime).append("|").append(bscName).append("|");
			for (int i = 0; i < writeVals.size(); i++) {
				String val = writeVals.get(i);
				if (val.equalsIgnoreCase("null"))
					val = "";
				buff.append(val).append("|");
			}
			buff.delete(buff.length() - 1, buff.length());
			info.writerForTxt.println(buff);
			info.writerForTxt.flush();
			buff.setLength(0);
			buff = null;
		}

		// List<Integer> del = new ArrayList<Integer>();
		// if ( TB_COLS.containsKey(tableName) )
		// {
		// List<String> cols = TB_COLS.get(tableName);
		//
		// for (int i = 0; i < row.size(); i++)
		// {
		// ColInfo ci = row.get(i);
		// if ( !cols.contains(ci.colText.toUpperCase()) )
		// {
		// del.add(i);
		// }
		// }
		//
		// }
		//
		// if ( infos.containsKey(tableName) )
		// {
		// SLInfo info = infos.get(tableName);
		// StringBuilder buff = new StringBuilder();
		// buff.append(omcId).append("|").append(strCt).append("|").append(strTime).append("|").append(bscName).append("|");
		// for (int i = 0; i < values.size(); i++)
		// {
		// if ( del.contains(i) )
		// {
		// buff.append("|");
		// }
		// else
		// {
		// String val = values.get(i);
		// if ( val.equalsIgnoreCase("null") )
		// val = "";
		// buff.append(val).append("|");
		// }
		// }
		// buff.delete(buff.length() - 1, buff.length());
		// info.writerForTxt.println(buff);
		// info.writerForTxt.flush();
		// buff.setLength(0);
		// buff = null;
		// }
	}

	private synchronized static void loadCols() {
		TB_COLS.clear();
		String sql = "select TABLE_NAME, COLUMN_NAME " + "  from user_tab_cols " + " where TABLE_NAME like 'CLT_CM_ERIR12_%'";
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			st = con.prepareStatement(sql);
			logger.debug("执行SQL - " + sql);
			rs = st.executeQuery();
			while (rs.next()) {
				String tn = rs.getString("TABLE_NAME");
				String cn = rs.getString("COLUMN_NAME");
				if (TB_COLS.containsKey(tn)) {
					if (!cn.equals("OMCID") && !cn.equals("COLLECTTIME") && !cn.equals("STAMPTIME") && !cn.equals("BSC_NAME"))
						TB_COLS.get(tn).add(cn);
				} else {
					List<String> list = new ArrayList<String>();
					list.add("OMCID");
					list.add("COLLECTTIME");
					list.add("STAMPTIME");
					list.add("BSC_NAME");
					if (!cn.equals("OMCID") && !cn.equals("COLLECTTIME") && !cn.equals("STAMPTIME") && !cn.equals("BSC_NAME"))
						list.add(cn);
					TB_COLS.put(tn, list);
				}
			}
		} catch (Exception e) {
			logger.error("执行SQL时异常 - " + sql, e);
		} finally {
			CommonDB.close(rs, st, con);
		}
	}

	private static class SLInfo implements Closeable {

		File txt;

		File log;

		File ctl;

		File bad;

		PrintWriter writerForTxt;

		PrintWriter writerForCtl;

		private SLInfo(File txt, File log, File ctl, File bad) {
			super();
			this.txt = txt;
			this.log = log;
			this.ctl = ctl;
			this.bad = bad;
			try {
				writerForTxt = new PrintWriter(txt);
				writerForCtl = new PrintWriter(ctl);
			} catch (Exception e) {
				logger.error("创建文件时异常", e);
			}
		}

		@Override
		public void close() {
			IOUtils.closeQuietly(writerForCtl);
			IOUtils.closeQuietly(writerForTxt);
		}

	}

	public static void main(String[] args) throws Exception {
		GsmEriCmSQLLDR s = new GsmEriCmSQLLDR(9981, new Timestamp(Util.getDate1("2011-07-01 00:00:00").getTime()), 12, "gzbsc1");
		s.readyForSqlldr();
	}
}

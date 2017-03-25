package parser.dt.dingli201;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * 鼎利路测数据，201版本。
 * 
 * @author ChenSijiang 2011-3-16 上午09:08:35
 */
public class CV201ASCII extends Parser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private String logKey;

	private String stamptime;

	private String omcid;

	private String collecttime;

	private Timestamp tsCollecttime;

	private Map<String, List<String>> fileFieldsKeys = new HashMap<String, List<String>>();

	private Map<String, String> templet = null;

	private DateStruct fileDate = new DateStruct();;

	private Map<String, Long> preFileDate = new HashMap<String, Long>();

	private static final Map<String, List<String>> TB_COLS = new HashMap<String, List<String>>();

	private int gpsSeq = 0;

	private String rcuId = "";

	private static final String SEQ_SQL = "select seq_dt_dingli_201.nextval as seq from dual";

	static {
		loadCols();
	}

	private static void loadCols() {
		String sql = "select t.TABLE_NAME tn,t.COLUMN_NAME cn from user_tab_columns t where t.TABLE_NAME like 'CLT_DT_DINGLI_201_%'";
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			st = con.prepareStatement(sql);
			rs = st.executeQuery();
			while (rs.next()) {
				String tn = rs.getString("tn");
				String cn = rs.getString("cn");
				if (TB_COLS.containsKey(tn)) {
					TB_COLS.get(tn).add(cn);
				} else {
					List<String> list = new ArrayList<String>();
					list.add(cn);
					TB_COLS.put(tn, list);
				}
			}
		} catch (Exception e) {
			logger.error("载入路测CLT表列名时出错 - " + sql, e);
		} finally {
			CommonDB.close(rs, st, con);
		}
	}

	@SuppressWarnings("unused")
	private synchronized static int getSeq() {
		int seq = -1;

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			st = con.prepareStatement(SEQ_SQL);
			rs = st.executeQuery();
			if (rs.next())
				seq = rs.getInt("seq");
		} catch (Exception e) {
			logger.error("执行sql异常 - " + SEQ_SQL, e);
		} finally {
			CommonDB.close(rs, st, con);
		}
		return seq;
	}

	/**
	 * 文件是否已经解析过
	 * 
	 * @param fileName
	 * @return
	 */
	// private synchronized boolean isParsed(String fileName)
	// {
	// boolean b = false;
	// String sql = "select * from clt_dt_dingli_201_file_cfg "
	// + "where collector_name=? and file_name=?";
	// Connection con = null;
	// PreparedStatement ps = null;
	// ResultSet rs = null;
	// try
	// {
	// con = DbPool.getConn();
	// ps = con.prepareStatement(sql);
	// ps.setString(1, Util.getHostName());
	// ps.setString(2, fileName);
	// rs = ps.executeQuery();
	// if ( rs.next() )
	// b = true;
	// else
	// {
	// rs.close();
	// ps.close();
	// sql = "insert into clt_dt_dingli_201_file_cfg "
	// + "(collector_name,file_name,insert_date) values (?,?,?)";
	// ps = con.prepareStatement(sql);
	// ps.setString(1, Util.getHostName());
	// ps.setString(2, fileName);
	// ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
	// ps.executeUpdate();
	// b = false;
	// }
	// }
	// catch (Exception e)
	// {
	// log.error("[dt_dingli]执行sql异常,sql=" + sql, e);
	// }
	// finally
	// {
	// CommonDB.close(rs, ps, con);
	// }
	// return b;
	// }

	@Override
	public boolean parseData() throws Exception {

		this.stamptime = Util.getDateString(collectObjInfo.getLastCollectTime());
		this.tsCollecttime = new Timestamp(System.currentTimeMillis());
		this.collecttime = Util.getDateString(this.tsCollecttime);
		this.omcid = String.valueOf(collectObjInfo.getDevInfo().getOmcID());
		this.logKey = String.format("[%s][%s]", collectObjInfo.getTaskID(), this.stamptime);

		templet = Tools.readTemplet(collectObjInfo.getParseTmpID());
		Map<String, SqlldrInfo> sqlldrInfos = null;
		File dtFile = new File(fileName);

		// if ( isParsed(dtFile.getAbsolutePath()) )
		// {
		// log.debug(logKey + "此DT日志已解析过，跳过 - " + fileName);
		// return true;
		// }

		// 得到目录中的设备号
		try {
			String dirs = FilenameUtils.getPath(fileName);
			String[] sdirs = dirs.split("\\\\");
			rcuId = sdirs[sdirs.length - 1];
			rcuId = rcuId.replace("_", "-");
			rcuId = rcuId.substring(1, rcuId.length() - 1);
		} catch (Exception e) {
			logger.error(logKey + "获取设备号(rcu_id)失败 " + fileName);
		}

		InputStream in = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		try {
			in = new FileInputStream(dtFile);
			isr = new InputStreamReader(in);
			br = new BufferedReader(isr);

			String firstLine = null;
			String line = null;
			logger.debug(logKey + "开始解析 - " + dtFile.getAbsolutePath());
			gpsSeq = 0;
			int lineNum = 0;
			while ((line = br.readLine()) != null) {
				lineNum++;
				if (firstLine == null) {
					firstLine = line;
					int drIndex = firstLine.indexOf("DataReport\t");
					if (drIndex > -1) {
						firstLine = firstLine.substring(drIndex, firstLine.length());
						String[] splitedLine = firstLine.split("\t");
						String key = getKey(splitedLine);
						if (!templet.containsKey(key) && !isDateKey(key))
							continue;
						if (templet.containsKey(key)) {
							fileFieldsKeys.put(key, getFileds(splitedLine));
						}
					}
				}
				line = line.trim();
				if (line.length() == 0)
					continue;

				String[] splitedLine = line.split("\t");
				String key = getKey(splitedLine);
				String firstWord = splitedLine[0].trim();
				if (isDataLine(splitedLine)) {
					if (sqlldrInfos == null)
						sqlldrInfos = buildSqlldr();

					if (firstWord.equals("GPS"))
						gpsSeq = 0;

					SqlldrInfo info = sqlldrInfos.get(templet.get(firstWord));
					if (info == null)
						continue;

					StringBuilder buffer = new StringBuilder();
					buffer.append(omcid).append(";");
					buffer.append(collecttime).append(";");
					buffer.append(stamptime).append(";");
					buffer.append(fileDate.toSqlldrString()).append(";");
					buffer.append(rcuId).append(";");
					buffer.append(gpsSeq).append(";");
					for (int i = 1; i < splitedLine.length; i++) {
						if (info.ignore.contains(i - 1)) {
							// if ( i < splitedLine.length - 1 )
							// buffer.append(";");
							continue;
						}
						buffer.append(splitedLine[i]);
						if (i < splitedLine.length - 1)
							buffer.append(";");
					}
					String toStr = buffer.toString();
					if (firstWord.equals("EVT") && toStr.contains("Call_Complete")) {
						log.debug("[Call_Complete]文件='" + fileName + "', content='" + toStr + "', line_num='" + lineNum + "', sqlldr='"
								+ info.txt.getAbsolutePath() + "'");
					}
					info.writerForTxt.println(toStr);
					info.writerForTxt.flush();

				} else if (line.startsWith("DataReport\t")) {
					if (!templet.containsKey(key) && !isDateKey(key))
						continue;
					if (templet.containsKey(key)) {
						fileFieldsKeys.put(key, getFileds(splitedLine));
					}

				} else if (firstWord.equals("DAY")) {
					fileDate.year = Integer.parseInt(splitedLine[1].trim());
					fileDate.month = Integer.parseInt(splitedLine[2].trim());
					fileDate.day = Integer.parseInt(splitedLine[3].trim());
					// if ( beginWithoutTime == null )
					// beginWithoutTime = (DateStruct) fileDate.clone();
				} else if (firstWord.equals("HOUR")) {
					fileDate.hour = Integer.parseInt(splitedLine[1].trim());
				} else if (firstWord.equals("MIN")) {
					fileDate.minute = Integer.parseInt(splitedLine[1].trim());
				} else if (firstWord.equals("SEC")) {
					fileDate.second = Integer.parseInt(splitedLine[1].trim());
					fileDate.millisecond = Integer.parseInt(splitedLine[2].trim());
					// if ( begin == null )
					// begin = (DateStruct) fileDate.clone();

				}
			}
			lineNum = 0;
			logger.debug(logKey + "解析完毕 - " + dtFile.getAbsolutePath());
			// end = (DateStruct) fileDate.clone();
			fileDate = new DateStruct();
			if (sqlldrInfos != null) {
				Iterator<SqlldrInfo> it = sqlldrInfos.values().iterator();
				while (it.hasNext())
					it.next().close();
				logger.debug(logKey + "开始入库");
				store(sqlldrInfos);
				logger.debug(logKey + "入库完毕");

				/* 以下是MOD_DT_COLLECT_STATE日志的相关处理。 */
				// List<CalLogStruct> calList = null;
				// if ( tmpLogs.containsKey(rcuId) )
				// {
				// calList = tmpLogs.get(rcuId);
				// }
				// else
				// {
				// calList = new ArrayList<CV201ASCII.CalLogStruct>();
				// tmpLogs.put(rcuId, calList);
				// }
				// calList.add(CalLogStruct.create(rcuId, (begin != null ?
				// begin.toTimestamp() : beginWithoutTime.toTimestamp()),
				// end.toTimestamp()));
				// Collections.sort(calList);
				//
				// List<CalLogStruct> tmpList = new
				// ArrayList<CV201ASCII.CalLogStruct>(calList);
				// calList.clear();
				//
				// for (int i = 0; i < tmpList.size(); i++)
				// {
				//
				// }
				//
				// logsToFile(tmpLogs);
			}
		} finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(isr);
			IOUtils.closeQuietly(in);
			fileFieldsKeys.clear();
			templet = null;
			preFileDate.clear();
			gpsSeq = 0;
		}

		return true;
	}

	private void store(Map<String, SqlldrInfo> sqlldrInfos) {
		Iterator<Entry<String, SqlldrInfo>> it = sqlldrInfos.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, SqlldrInfo> e = it.next();
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", SystemConfig.getInstance()
					.getDbUserName(), SystemConfig.getInstance().getDbPassword(), SystemConfig.getInstance().getDbService(), e.getValue().ctl
					.getAbsoluteFile(), e.getValue().bad.getAbsoluteFile(), e.getValue().log.getAbsoluteFile());
			e.getValue().close();
			logger.debug(logKey + "执行 "
					+ cmd.replace(SystemConfig.getInstance().getDbPassword(), "*").replace(SystemConfig.getInstance().getDbUserName(), "*"));
			ExternalCmd execute = new ExternalCmd();
			try {
				int ret = execute.execute(cmd);
				SqlldrResult result = new SqlLdrLogAnalyzer().analysis(e.getValue().log.getAbsolutePath());
				logger.debug(logKey + "exit=" + ret + " omcid=" + omcid + " 入库成功条数=" + result.getLoadSuccCount() + " 表名=" + result.getTableName()
						+ " 数据时间=" + stamptime + " sqlldr日志=" + e.getValue().log.getAbsolutePath());
				if (ret == 0 && SystemConfig.getInstance().isDeleteLog()) {
					e.getValue().txt.delete();
					e.getValue().ctl.delete();
					e.getValue().log.delete();
					e.getValue().bad.delete();
				} else if (ret == 2 && SystemConfig.getInstance().isDeleteLog()) {
					e.getValue().txt.delete();
					e.getValue().ctl.delete();
					e.getValue().bad.delete();
				}
			} catch (Exception ex) {
				logger.error(logKey + "sqlldr时异常", ex);
			}
		}

	}

	private String getKey(String[] splitedLine) {
		if (splitedLine.length > 1)
			return splitedLine[1].trim().toUpperCase();
		return null;
	}

	private boolean isDataLine(String[] splitedLine) {
		if (splitedLine.length < 1)
			return false;

		String first = splitedLine[0].trim().toUpperCase();

		Iterator<String> it = templet.keySet().iterator();
		while (it.hasNext()) {
			String s = it.next();
			if (s.equals(first))
				return true;
		}

		return false;
	}

	private List<String> getFileds(String[] splitedLine) {
		if (splitedLine.length > 2) {
			List<String> fields = new ArrayList<String>();
			for (int i = 2; i < splitedLine.length; i++) {
				String col = splitedLine[i].trim().replace("/", "");
				fields.add(col);
			}
			return fields;
		}

		return null;
	}

	private boolean isDateKey(String key) {
		if (key.equals("DAY") || key.equals("HOUR") || key.equals("MIN") || key.equals("SEC")) {
			return true;
		}

		return false;
	}

	private Map<String, SqlldrInfo> buildSqlldr() throws Exception {
		Map<String, SqlldrInfo> map = new HashMap<String, SqlldrInfo>();

		File baseDir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "cdma_dt_dingli" + File.separator);
		if (!baseDir.exists())
			baseDir.mkdirs();

		long curr = System.currentTimeMillis();

		Iterator<Entry<String, List<String>>> it = fileFieldsKeys.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, List<String>> e = it.next();
			String tb = templet.get(e.getKey()).toUpperCase();
			List<String> cols = e.getValue();

			String baseFilename = baseDir.getAbsolutePath() + File.separator + collectObjInfo.getTaskID() + "_"
					+ Util.getDateString_yyyyMMddHH(collectObjInfo.getLastCollectTime()) + "_" + tb.toUpperCase() + "_" + curr;
			File txt = new File(baseFilename + ".txt");
			File log = new File(baseFilename + ".log");
			File bad = new File(baseFilename + ".bad");
			File ctl = new File(baseFilename + ".ctl");
			SqlldrInfo info = new SqlldrInfo(txt, log, bad, ctl);
			map.put(tb.toUpperCase(), info);

			info.writerForCtl.println("load data");
			info.writerForCtl.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
			info.writerForCtl.println("INFILE '" + txt.getAbsolutePath() + "' APPEND INTO TABLE " + tb);
			info.writerForCtl.println("FIELDS TERMINATED BY \";\"");
			info.writerForCtl.println("TRAILING NULLCOLS");
			info.writerForCtl.println("(");
			info.writerForTxt.print("OMCID;COLLECTTIME;STAMPTIME;FILE_DATE;RCU_ID;GPS_LINK_SEQ;");
			info.writerForCtl.println("OMCID,");
			info.writerForCtl.println("COLLECTTIME DATE 'YYYY-MM-DD HH24:MI:SS',");
			info.writerForCtl.println("STAMPTIME DATE 'YYYY-MM-DD HH24:MI:SS',");
			info.writerForCtl.println("FILE_DATE TIMESTAMP 'YYYY-MM-DD HH24:MI:SS:FF',");
			info.writerForCtl.println("RCU_ID,GPS_LINK_SEQ,");
			StringBuilder ctlBuff = new StringBuilder();
			StringBuilder txtBuff = new StringBuilder();
			for (int i = 0; i < cols.size(); i++) {
				String col = cols.get(i).toUpperCase().trim();
				if (!TB_COLS.get(tb).contains(col)) {
					info.ignore.add(i);

					continue;
				}
				txtBuff.append(col).append(";");
				ctlBuff.append(col).append(",");

			}
			txtBuff.delete(txtBuff.length() - 1, txtBuff.length());
			ctlBuff.delete(ctlBuff.length() - 1, ctlBuff.length());
			info.writerForTxt.print(txtBuff);
			info.writerForCtl.print(ctlBuff);
			info.writerForCtl.print(")");
			info.writerForCtl.flush();
			info.writerForTxt.println();
			info.writerForTxt.flush();

		}

		return map;
	}

	private static final SimpleDateFormat FMT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private class DateStruct implements Cloneable {

		int year = 1970;

		int month = 1;

		int day = 1;

		int hour = 0;

		int minute = 0;

		int second = 0;

		int millisecond = 0;

		Timestamp ts;

		private StringBuilder buffer = new StringBuilder();

		public Timestamp toTimestamp() throws Exception {
			ts = new Timestamp(Util.getDate(toString(), "yyyy-MM-dd HH:mm:ss.SSS").getTime());
			if (Tools.timezone != 0) {
				long modMil = Tools.timezone * 60 * 60 * 1000;
				ts.setTime(ts.getTime() + modMil);
			}
			return ts;
		}

		public String toSqlldrString() {
			try {
				return FMT.format(toTimestamp());
			} catch (Exception e) {
				return toString();
			}

		}

		@Override
		public String toString() {
			buffer.setLength(0);
			buffer.append(year).append("-");
			buffer.append(month).append("-");
			buffer.append(day).append(" ");
			buffer.append(hour).append(":");
			buffer.append(minute).append(":");
			buffer.append(second).append(".");
			String mill = String.valueOf(millisecond);
			if (mill.length() == 1)
				mill = ("00" + mill);
			else if (mill.length() == 2)
				mill = ("0" + mill);
			buffer.append(mill);
			return buffer.toString();
		}

		@Override
		protected Object clone() throws CloneNotSupportedException {
			return super.clone();
		}
	}

	private class SqlldrInfo implements Closeable {

		File txt;

		File log;

		File bad;

		File ctl;

		PrintWriter writerForTxt;

		PrintWriter writerForCtl;

		/* 要忽略的列索引 */
		List<Integer> ignore = new ArrayList<Integer>();

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

	private static class CalLogStruct implements Serializable, Comparable<CalLogStruct> {

		private static final long serialVersionUID = 6398002653363741607L;

		String rcuId;

		Timestamp min;

		Timestamp max;

		public static CalLogStruct create(String rcuId, Timestamp min, Timestamp max) {
			CalLogStruct instance = new CalLogStruct();
			instance.rcuId = rcuId;
			instance.min = min;
			instance.max = max;
			return instance;
		}

		@Override
		public String toString() {
			return "CalLogStruct [rcuId=" + rcuId + ", min=" + min + ", max=" + max + "]";
		}

		@Override
		public int compareTo(CalLogStruct o) {
			return max.compareTo(o.max);
		}

	}

	public static void main(String[] args) {
		CollectObjInfo obj = new CollectObjInfo(31601);
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setParseTmpID(3160101);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));

		CV201ASCII w = new CV201ASCII();
		w.fileName = "C:\\Users\\ChenSijiang\\Desktop\\2011-12-20 02-03-41-4_loc";
		w.collectObjInfo = obj;
		try {
			w.parseData();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// try
		// {
		// w.parseData();
		// }
		// catch (Exception e)
		// {
		// e.printStackTrace();
		// }
	}

}

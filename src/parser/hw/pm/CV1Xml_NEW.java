package parser.hw.pm;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import task.TaskMgr.RedoSQL;
import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

public class CV1Xml_NEW extends Parser {

	private Timestamp stamptime;

	private String strStamptime;

	private Timestamp collecttime;

	private String strCollecttime;

	private long omcId;

	private long taskId;

	private String logKey = null;

	/* 整个文件的公共字段 */
	private String strBeginTime;

	private String elementType;

	private String bsc;

	/* 单个表的公共字段 */
	private String measInfoId;

	private String granDuration;

	private String repDuration;

	private String strEndTime;

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private final Map<String, HWSqlldrInfo> SQLLDR_INFOS = new HashMap<String, HWSqlldrInfo>();

	private static final Map<String, List<String>> TAB_COLS = new HashMap<String, List<String>>();

	private static final String TERMINATED_FOR_SQLLDR = "|";

	@Override
	public boolean parseData() {
		this.stamptime = collectObjInfo.getLastCollectTime();
		this.strStamptime = Util.getDateString(this.stamptime);
		this.collecttime = new Timestamp(System.currentTimeMillis());
		this.taskId = collectObjInfo.getTaskID();
		this.omcId = collectObjInfo.getDevInfo().getOmcID();
		this.strCollecttime = Util.getDateString(this.collecttime);
		this.logKey = "[" + this.taskId + "][" + this.strStamptime + "]";
		try {
			parse();
		} catch (Exception e) {
			logger.error("解析数据时发生异常", e);
			return false;
		}
		return true;
	}

	public void parse() throws HuaweiPmParseException {

		InputStream in = null;
		XMLStreamReader reader = null;
		try {
			in = new FileInputStream(fileName);
			logger.debug(logKey + "开始解析 - " + fileName);
			double curr = System.currentTimeMillis();
			reader = XMLInputFactory.newInstance().createXMLStreamReader(in);
			int type = -1;
			List<String> currTypes = new ArrayList<String>();
			List<String> currValues = new ArrayList<String>();
			String currLdn = null;
			String currTableName = null;
			while (reader.hasNext()) {
				type = reader.next();
				String tagName = null;

				if (type == XMLStreamConstants.START_ELEMENT || type == XMLStreamConstants.END_ELEMENT)
					tagName = reader.getLocalName();

				if (tagName == null) {
					continue;
				}
				switch (type) {
					case XMLStreamConstants.START_ELEMENT :
						if (tagName.equals("measResults")) {
							getCounterValues(currValues, reader.getElementText());
							writeSqlldr(currTableName, currTypes, currValues, currLdn);
						} else if (tagName.equals("measValue")) {
							currLdn = reader.getAttributeValue(null, "measObjLdn");
							if (currLdn != null)
								currLdn = currLdn.replace(TERMINATED_FOR_SQLLDR, " ");
						} else if (tagName.equals("measTypes")) {
							getCounterTypes(currTypes, reader.getElementText());
						} else if (tagName.equals("repPeriod"))
							this.repDuration = getNum(reader.getAttributeValue(null, "duration"));
						else if (tagName.equals("granPeriod")) {
							this.granDuration = getNum(reader.getAttributeValue(null, "duration"));
							this.strEndTime = Util.getDateString(strToDate(reader.getAttributeValue(null, "endTime")));
						} else if (tagName.equals("measInfo")) {
							this.measInfoId = reader.getAttributeValue(null, "measInfoId");
							currTableName = DefaultValueConstant.tableNameHead + this.measInfoId;
							synchronized (TAB_COLS) {
								if (!TAB_COLS.containsKey(currTableName)) {
									List<String> currTbCols = loadTableCols(currTableName);
									if (currTbCols == null) {
										logger.warn(logKey + "需要的表" + currTableName + "不存在，measInfo=" + this.measInfoId + "的counter类型将无法入库");
									} else
										TAB_COLS.put(currTableName, currTbCols);
								}

							}
							if (!SQLLDR_INFOS.containsKey(currTableName)) {
								readySqlldr(currTableName);
							}
						} else if (tagName.equals("managedElement"))
							this.bsc = reader.getAttributeValue(null, "userLabel");
						else if (tagName.equals("measCollec")) {
							String time = reader.getAttributeValue(null, "beginTime");
							this.strBeginTime = Util.getDateString(strToDate(time == null ? this.strBeginTime : time));
						} else if (tagName.equals("fileSender"))
							this.elementType = reader.getAttributeValue(null, "elementType");

						break;
					case XMLStreamConstants.END_ELEMENT :

						break;
					default :
						break;
				}
			}
			double end = System.currentTimeMillis();
			logger.debug(logKey + "解析完成（耗时" + ((end - curr) / 1000) + "秒），开始执行sqlldr入库");
			curr = System.currentTimeMillis();
			// store();
			store2();
			end = System.currentTimeMillis();
			logger.debug(logKey + "入库完成（耗时" + ((end - curr) / 1000) + "秒）");
			SQLLDR_INFOS.clear();
		} catch (Exception e) {
			throw new HuaweiPmParseException(logKey + "解析华为性能文件时异常", e);
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (Exception e) {
			}
			IOUtils.closeQuietly(in);
		}
	}

	private static List<String> loadTableCols(String tableName) throws Exception {
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		String sql = "select * from " + tableName + " where 1=2";
		try {
			con = DbPool.getConn();
			st = con.prepareStatement(sql);
			rs = st.executeQuery();
			ResultSetMetaData meta = rs.getMetaData();
			List<String> list = new ArrayList<String>();
			for (int i = 0; i < meta.getColumnCount(); i++) {
				list.add(meta.getColumnName(i + 1));
			}
			return list;
		} catch (Exception e) {
			if (e instanceof SQLException) {
				SQLException sqlex = (SQLException) e;
				if (sqlex.getErrorCode() == 942)
					return null;
			}
			throw new Exception("执行sql时出错 - " + sql, e);
		} finally {
			CommonDB.close(rs, st, con);
		}

	}

	/* 把"2010-02-03T10:30:00+08:00"这类格式的字符串转为日期 */
	private static Date strToDate(String time) throws Exception {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, Integer.parseInt(time.substring(0, 4)));
		calendar.set(Calendar.MONTH, Integer.parseInt(time.substring(5, 7)) - 1);
		calendar.set(Calendar.DATE, Integer.parseInt(time.substring(8, 10)));
		calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(time.substring(11, 13)));
		calendar.set(Calendar.MINUTE, Integer.parseInt(time.substring(14, 16)));
		calendar.set(Calendar.SECOND, 0);
		return calendar.getTime();
	}

	/* 从"PT1800S"这种字符串中取得中间的数字部分 */
	private static String getNum(String duration) throws Exception {
		return duration.substring(duration.indexOf("PT") + 2, duration.indexOf("S"));
	}

	private void store2() {
		int temSize = SQLLDR_INFOS.size();
		ExecutorService dsExecutor; // 任务线程池
		int size = SystemConfig.getInstance().getMaxCltSelectParallelCount() ;
		dsExecutor = Executors.newFixedThreadPool(size);
		CountDownLatch latch = new CountDownLatch(temSize);

		long begin = System.currentTimeMillis();

		try {

			Iterator<String> it = SQLLDR_INFOS.keySet().iterator();
			while (it.hasNext()) {
				HWSqlldrInfo info = SQLLDR_INFOS.get(it.next());
				info.close();

				RunSqlldrThread td = new CV1Xml_NEW().new RunSqlldrThread(latch, info, this.collectObjInfo,  logKey );
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
			log.debug(taskId + "一共消耗时间" + (end - begin));

		} catch (InterruptedException e) {
			log.error("执行sql脚本意外结束,原因:线程被外部意外中断!({})", e);
		} catch (Exception e) {
			log.error("执行sql时发生错误!({})", e);
		} finally {
			if (!dsExecutor.isShutdown())
				dsExecutor.shutdown();
		}
	}

	class RunSqlldrThread extends Thread {

		CountDownLatch latch = null;

		HWSqlldrInfo info = null;

		CollectObjInfo taskInfo = null;

		List<RedoSQL> redoSqlList = null;
		String logKey = null;

		public RunSqlldrThread(CountDownLatch latch, HWSqlldrInfo info, CollectObjInfo taskInfo, String logKey) {
			this.latch = latch;
			this.info = info;

			this.taskInfo = taskInfo;
			
			this.logKey = logKey; 

		}

		@Override
		public void run() {
			String sql = null;
			try {
				Thread.sleep(1000);

				SystemConfig cfg = SystemConfig.getInstance();
				String cmd = String.format(
						"sqlldr userid=%s/%s@%s skip=%s control=%s bad=%s log=%s errors=9999999 bindsize=20000000 rows=5000 readsize=20000000 ",
						cfg.getDbUserName(), cfg.getDbPassword(), cfg.getDbService(), 1, info.cltFile.getAbsoluteFile(),
						info.badFile.getAbsoluteFile(), info.logFile.getAbsoluteFile());
				int ret = -1;
				try {
					logger.debug(logKey + "执行sqlldr - " + cmd.replace(cfg.getDbPassword(), "*"));
					ret = new ExternalCmd().execute(cmd);
					SqlldrResult result = new SqlLdrLogAnalyzer().analysis(info.logFile.getAbsolutePath());
					dbLogger.log((int) taskInfo.getDevInfo().getOmcID(), result.getTableName(), taskInfo.getLastCollectTime(),
							result.getLoadSuccCount(), taskInfo.getTaskID());
					logger.debug(logKey
							+ String.format("ret=%s,入库条数=%s,task_id=%s,表名=%s,时间点=%s", ret, result.getLoadSuccCount(), taskInfo.getTaskID(),
									result.getTableName(), util.Util.getDateString_yyyyMMddHHmm(taskInfo.getLastCollectTime())));
					if (cfg.isDeleteLog()) {
						if (ret == 0) {
							info.txtFile.delete();
							info.logFile.delete();
							info.cltFile.delete();
							info.badFile.delete();
						} else if (ret == 2) {
							info.txtFile.delete();
							info.badFile.delete();
						}
					}
				} catch (Exception e) {
					logger.error(logKey + "sqlldr时异常", e);

				}

			} catch (Exception e) {
				log.error("执行查询脚本发生错误!{} ,sql" + sql, e);
			} finally {
				latch.countDown();
			}
		}
	}

	private void store() {
		Iterator<String> it = SQLLDR_INFOS.keySet().iterator();
		while (it.hasNext()) {
			HWSqlldrInfo info = SQLLDR_INFOS.get(it.next());
			info.close();
			SystemConfig cfg = SystemConfig.getInstance();
			String cmd = String.format(
					"sqlldr userid=%s/%s@%s skip=%s control=%s bad=%s log=%s errors=9999999 direct=true streamsize=10485760 date_cache=5000",
					cfg.getDbUserName(), cfg.getDbPassword(), cfg.getDbService(), 1, info.cltFile.getAbsoluteFile(), info.badFile.getAbsoluteFile(),
					info.logFile.getAbsoluteFile());
			int ret = -1;
			try {
				logger.debug(logKey + "执行sqlldr - " + cmd.replace(cfg.getDbPassword(), "*"));
				ret = new ExternalCmd().execute(cmd);
				SqlldrResult result = new SqlLdrLogAnalyzer().analysis(info.logFile.getAbsolutePath());
				dbLogger.log((int) this.omcId, result.getTableName(), this.stamptime, result.getLoadSuccCount(), this.taskId);
				logger.debug(logKey
						+ String.format("ret=%s,入库条数=%s,task_id=%s,表名=%s,时间点=%s", ret, result.getLoadSuccCount(), this.taskId, result.getTableName(),
								this.strStamptime));
				if (cfg.isDeleteLog()) {
					if (ret == 0) {
						info.txtFile.delete();
						info.logFile.delete();
						info.cltFile.delete();
						info.badFile.delete();
					} else if (ret == 2) {
						info.txtFile.delete();
						info.badFile.delete();
					}
				}
			} catch (Exception e) {
				logger.error(logKey + "sqlldr时异常", e);

			}
		}
	}

	private void writeSqlldr(String tn, List<String> types, List<String> values, String ldn) {
		if (!SQLLDR_INFOS.containsKey(tn))
			return;

		List<String> nTypes = new ArrayList<String>(types);
		nTypes.add("OMCID");
		values.add(String.valueOf(this.omcId));
		nTypes.add("COLLECTTIME");
		values.add(this.strCollecttime);
		nTypes.add("STAMPTIME");
		values.add(this.strStamptime);
		nTypes.add("STARTTIME");
		values.add(this.strBeginTime);
		nTypes.add("MEASOBJLDN");
		values.add(ldn);

		List<String> tbCols = TAB_COLS.get(tn);
		List<String> writeValues = new ArrayList<String>(tbCols.size());
		List<String> list = DefaultValueConstant.map.get(tn);
		for (int i = 0; i < tbCols.size(); i++) {
			// 1.不在原始文件中，而在map中，也要设为默认值 --added by yuy 2013-07-09
			if (list != null && list.contains(tbCols.get(i)) && !nTypes.contains(tbCols.get(i))) {
				writeValues.add(DefaultValueConstant.defaultValue);
				continue;
			}
			writeValues.add("");
		}

		for (int i = 0; i < nTypes.size(); i++) {
			String type = nTypes.get(i);
			if (tbCols.contains(type)) {
				// 2.如果等于“”，设为默认值 --added by yuy 2013-07-09
				if (list != null && list.size() > 0) {
					if (list.contains(type) && "".equals(values.get(i))) {
						values.set(i, DefaultValueConstant.defaultValue);
					}
				}
				writeValues.set(tbCols.indexOf(type), values.get(i));
			}
		}

		HWSqlldrInfo sl = SQLLDR_INFOS.get(tn);
		for (int i = 0; i < writeValues.size(); i++) {
			sl.txtWriter.print(writeValues.get(i));
			if (i < writeValues.size() - 1)
				sl.txtWriter.print(TERMINATED_FOR_SQLLDR);
			else {
				sl.txtWriter.println();
			}
		}
		sl.txtWriter.flush();
		nTypes.clear();
		nTypes = null;
		writeValues.clear();
		writeValues = null;
	}

	private void readySqlldr(String tn) {
		if (!TAB_COLS.containsKey(tn))
			return;
		List<String> tnCols = TAB_COLS.get(tn);
		File baseDir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "cdma_hw_pm_file_new"
				+ File.separator + "task_" + this.taskId + File.separator + Util.getDateString_yyyyMMddHHmmss(this.stamptime) + File.separator);
		if (!baseDir.exists())
			baseDir.mkdirs();
		String baseName = taskId + "_" + collectObjInfo.getKeyID() + "_" + tn + "_" + Util.getDateString_yyyyMMddHHmmss(stamptime) + "_"
				+ collecttime.getTime();
		File txt = new File(baseDir, baseName + ".txt");
		File ctl = new File(baseDir, baseName + ".ctl");
		File bad = new File(baseDir, baseName + ".bad");
		File log = new File(baseDir, baseName + ".log");
		HWSqlldrInfo sl = new HWSqlldrInfo(txt, log, bad, ctl);
		SQLLDR_INFOS.put(tn, sl);
		sl.ctlWriter.println("LOAD DATA");
		sl.ctlWriter.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
		sl.ctlWriter.println("INFILE '" + txt.getAbsolutePath() + "' APPEND INTO TABLE " + tn);
		sl.ctlWriter.println("FIELDS TERMINATED BY \"" + TERMINATED_FOR_SQLLDR + "\"");
		sl.ctlWriter.println("TRAILING NULLCOLS");
		sl.ctlWriter.println("(");
		for (int i = 0; i < tnCols.size(); i++) {
			String colName = tnCols.get(i);
			sl.ctlWriter.print("\t" + colName);
			sl.txtWriter.print(colName);
			if (colName.equals("STAMPTIME") || colName.equals("COLLECTTIME") || colName.equals("STARTTIME") || colName.equals("END_TIME"))
				sl.ctlWriter.print(" DATE 'YYYY-MM-DD HH24:MI:SS'");
			if (i < tnCols.size() - 1) {
				sl.ctlWriter.print(",");
				sl.txtWriter.print(TERMINATED_FOR_SQLLDR);
			}
			sl.ctlWriter.println();
		}
		sl.txtWriter.println();
		sl.ctlWriter.println(")");
		sl.ctlWriter.flush();
		sl.ctlWriter.flush();
		IOUtils.closeQuietly(sl.ctlWriter);
	}

	private void getCounterTypes(List<String> target, String source) {
		target.clear();

		String[] items = source.split(" ");
		for (String s : items) {
			if (Util.isNotNull(s)) {
				target.add(DefaultValueConstant.columnNameHead + s);
			}
		}
	}

	/* 从文本中解析出counter值 */
	private void getCounterValues(List<String> counterValues, String s) {
		counterValues.clear();

		String[] splited = s.split(" ");
		for (String item : splited) {
			if (Util.isNotNull(item)) {
				if (item.trim().equals("NIL")) {
					counterValues.add("");
				} else {
					counterValues.add(item);
				}
			}
		}

	}

	class HWSqlldrInfo implements Closeable {

		File txtFile;

		File logFile;

		File badFile;

		File cltFile;

		PrintWriter txtWriter;

		PrintWriter ctlWriter;

		public HWSqlldrInfo(File txtFile, File logFile, File badFile, File cltFile) {
			super();
			this.txtFile = txtFile;
			this.logFile = logFile;
			this.badFile = badFile;
			this.cltFile = cltFile;
			try {
				txtWriter = new PrintWriter(txtFile);
				ctlWriter = new PrintWriter(cltFile);
			} catch (Exception e) {
				logger.error(logKey + "创建txt/ctl文件时出错", e);
			}
		}

		@Override
		public void close() {
			IOUtils.closeQuietly(txtWriter);
			IOUtils.closeQuietly(ctlWriter);
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println(Util.getDateString(strToDate("2013-07-07T18:30:00+08:00")));
		CV1Xml_NEW parser = new CV1Xml_NEW();
		CollectObjInfo info = new CollectObjInfo(1020111);
		DevInfo dev = new DevInfo();
		dev.setOmcID(7);
		info.setDevInfo(dev);
		info.setLastCollectTime(new Timestamp(11));
		parser.setCollectObjInfo(info);
		try {
			List<String> list = new ArrayList<String>();
			list.add("D:\\hw.xml");
			for (int i = 0; i < list.size(); i++) {
				parser.setFileName(list.get(i));
				parser.parseData();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("end");
		// List<String> type = new ArrayList<String>();
		// type.add("COUNTER_1157632715");
		// type.add("COUNTER_1157632716");
		// type.add("Counter_1157632675");
		// type.add("Counter_1157632676");
		// type.add("Counter_1157632701");
		// type.add("Counter_1157632702");
		// type.add("COUNTER_1157632828");
		// List<String> values = new ArrayList<String>();
		// values.add("1");
		// values.add("2");
		// values.add("");
		// values.add("");
		// values.add("");
		// values.add("");
		// values.add("3");
		//
		// List<String> writeValues = new ArrayList<String>(7);
		// for (int i = 0; i < 7; i++)
		// writeValues.add("");
		// for (int i = 0; i < type.size(); i++)
		// {
		// List<String> list = DefaultValueConstant.map.get("CLT_PM_C_HW_1157630812");
		// if(list != null && list.size()>0){
		// if(list.contains((String)type.get(i)) && "".equals(values.get(i))){
		// values.set(i, DefaultValueConstant.defaultValue);
		// }
		// }
		// writeValues.set(i, values.get(i));
		// }
	}
}

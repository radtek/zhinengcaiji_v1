package parser.c.wlan;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.log4j.Logger;

import task.CollectObjInfo;
import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import exception.StoreException;
import framework.SystemConfig;

public abstract class AbstractWlanParser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private static final Map<String, List<Col>> COLS = new HashMap<String, List<Col>>();

	private String key;

	private CollectObjInfo taskInfo;

	private String file;

	private String omcId;

	private String stamptime;

	private String collectTime;

	private File ctl;

	private File txt;

	private File bad;

	private File log;

	private PrintWriter txtWriter;

	protected AbstractWlanParser(CollectObjInfo taskInfo, String file) {
		this.taskInfo = taskInfo;
		this.file = file;
	}

	public boolean parse() throws Exception {
		key = String.format("[taskId-%s][%s]", taskInfo.getTaskID(), Util.getDateString(taskInfo.getLastCollectTime()));
		logger.debug(key + "开始解析入库:" + file);
		omcId = String.valueOf(taskInfo.getDevInfo().getOmcID());
		stamptime = Util.getDateString(taskInfo.getLastCollectTime());
		collectTime = Util.getDateString(new Date());
		loadCols();

		File xml = new File(file);
		InputStream in = null;
		XMLStreamReader reader = null;
		try {
			boolean isFindRowFlag = false;
			in = new FileInputStream(xml);
			XMLInputFactory fac = XMLInputFactory.newInstance();
			fac.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
			fac.setProperty("javax.xml.stream.isReplacingEntityReferences", false);
			reader = fac.createXMLStreamReader(in);
			Row row = null;
			int type = -1;
			readyForSqlldr();
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
						if (isFindRowFlag) {
							Col c = new Col(false, tagName, 0);
							if (COLS.get(getTableName()).contains(c)) {
								String content = reader.getElementText();
								if (c.colName.equals("ADDRESS"))
									content = content.replace("null", "&nbsp;");
								row.addToVals(content, tagName);
							}
						} else if (tagName.equalsIgnoreCase(getRowFlag())) {
							isFindRowFlag = true;
							row = new Row();
						}
						break;
					case XMLStreamConstants.END_ELEMENT :
						if (tagName.equalsIgnoreCase(getRowFlag())) {
							isFindRowFlag = false;
							addForSqlldr(row);
							row = null;
						}
						break;
					default :
						break;
				}
			}
			runSqlldr();
			logger.debug(key + "解析入库完毕:" + file);
		} catch (Exception e) {
			logger.error(key + "解析入库时异常:" + file, e);
			return false;
		}

		return true;
	}

	private void loadCols() {
		synchronized (COLS) {
			if (!COLS.containsKey(getTableName())) {
				List<Col> list = new ArrayList<Col>();
				String sql = "select * from " + getTableName() + " where 1=2";
				Connection con = null;
				PreparedStatement st = null;
				ResultSet rs = null;
				try {
					con = DbPool.getConn();
					st = con.prepareStatement(sql);
					rs = st.executeQuery();
					ResultSetMetaData meta = rs.getMetaData();
					for (int i = 0; i < meta.getColumnCount(); i++) {   // 只适用于oracle
																		// 11以上版本，date&timestamp:93
																		// 2013-05-13
																		// modify
																		// by
																		// yuy
						list.add(new Col(meta.getColumnType(i + 1) == Types.TIMESTAMP, meta.getColumnName(i + 1), i));
					}
					COLS.put(getTableName(), list);
				} catch (Exception e) {
					logger.error(key + "读取CLT表时异常:" + sql, e);
				} finally {
					CommonDB.close(rs, st, con);
				}
			}
		}
	}

	private void readyForSqlldr() throws Exception {
		Date now = new Date();
		String today = Util.getDateString_yyyyMMdd(now);
		// 存放wlan数据临时文件的文件夹
		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "c_wlan" + File.separator
				+ today);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		String mainFileName = taskInfo.getTaskID() + "_" + getTableName() + "_" + Util.getDateString_yyyyMMddHHmmss(taskInfo.getLastCollectTime())
				+ "_" + Util.getDateString_yyyyMMddHHmmssSSS(new Date());
		txt = new File(dir, mainFileName + ".txt");
		ctl = new File(dir, mainFileName + ".ctl");
		log = new File(dir, mainFileName + ".log");
		bad = new File(dir, mainFileName + ".bad");
		PrintWriter ctlWriter = new PrintWriter(ctl);
		txtWriter = new PrintWriter(txt);
		try {
			ctlWriter.println("load data");
			ctlWriter.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
			ctlWriter.println("infile '" + txt.getAbsolutePath() + "' append into table " + getTableName());
			ctlWriter.println("FIELDS TERMINATED BY \"|\"");
			ctlWriter.println("TRAILING NULLCOLS");
			ctlWriter.print("(");
			List<Col> cols = COLS.get(getTableName());
			for (int i = 0; i < cols.size(); i++) {
				Col c = cols.get(i);
				ctlWriter.print(c.getColName());
				txtWriter.print(c.getColName());
				if (c.isDate()) {
					ctlWriter.print(" Date 'YYYY-MM-DD HH24:MI:SS'");
				}
				if (i < cols.size() - 1) {
					ctlWriter.print(",");
					txtWriter.print("|");
				}
			}
			txtWriter.println();
			ctlWriter.print(")");
		} catch (Exception e) {
			throw e;
		} finally {
			if (ctlWriter != null) {
				ctlWriter.flush();
				ctlWriter.close();
			}
			if (txtWriter != null) {
				txtWriter.flush();
			}
		}
	}

	private void addForSqlldr(Row row) {
		txtWriter.println(row);
		txtWriter.flush();
	}

	private void runSqlldr() throws Exception {
		if (txtWriter != null) {
			txtWriter.close();
		}
		String serviceName = SystemConfig.getInstance().getDbService();
		String uid = SystemConfig.getInstance().getDbUserName();
		String pwd = SystemConfig.getInstance().getDbPassword();
		String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", uid, pwd, serviceName,
				ctl.getAbsoluteFile(), bad.getAbsoluteFile(), log.getAbsoluteFile());
		ExternalCmd externalCmd = new ExternalCmd();
		logger.debug(key + "当前执行的SQLLDR命令为：" + cmd.replace(uid, "*").replace(pwd, "*"));
		int ret = -1;
		try {
			ret = externalCmd.execute(cmd);
		} catch (Exception e) {
			throw new StoreException(key + "执行sqlldr命令失败(" + cmd + ")", e);
		}

		SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
		try {
			SqlldrResult result = analyzer.analysis(new FileInputStream(log));
			if (result == null)
				return;

			logger.debug(key + " SQLLDR日志分析结果: 退出码=" + ret + " omcid=" + omcId + " 入库成功条数=" + result.getLoadSuccCount() + " 表名="
					+ result.getTableName() + " 数据时间=" + stamptime + " sqlldr日志=" + log.getAbsolutePath());

			dbLogger.log(taskInfo.getDevInfo().getOmcID(), result.getTableName(), taskInfo.getLastCollectTime(), result.getLoadSuccCount(),
					taskInfo.getTaskID());
		} catch (Exception e) {
			logger.error(key + " sqlldr日志分析失败，文件名：" + log.getAbsolutePath() + "，原因: ", e);
		}
		if (ret == 0 && SystemConfig.getInstance().isDeleteLog()) {
			txt.delete();
			ctl.delete();
			bad.delete();
			log.delete();
		}
	}

	class Row {

		Row() {
			values.add(omcId);
			values.add(collectTime);
			values.add(stamptime);
			for (int i = 0; i < COLS.get(getTableName()).size() - 3; i++) {
				values.add("");
			}
		}

		private List<String> values = new ArrayList<String>();

		public void addToVals(String val, String colName) {
			List<Col> tableCols = COLS.get(getTableName());
			int index = -1;
			for (int i = 0; i < tableCols.size(); i++) {
				if (tableCols.get(i).colName.equalsIgnoreCase(colName)) {
					index = tableCols.get(i).getIndex();
					break;
				}
			}
			if (index > -1)
				values.set(index, val);
		}

		@Override
		public String toString() {
			StringBuilder line = new StringBuilder();
			for (int i = 0; i < values.size(); i++) {
				line.append(values.get(i));
				if (i < values.size() - 1) {
					line.append("|");
				}
			}
			return line.toString();
		}
	}

	class Col {

		private boolean isDate;

		private String colName;

		private int index;// 字段在表中的索引位置

		public Col(boolean isDate, String colName, int index) {
			super();
			this.isDate = isDate;
			this.colName = colName;
			this.index = index;
		}

		boolean isDate() {
			return isDate;
		}

		String getColName() {
			return colName;
		}

		public int getIndex() {
			return index;
		}

		@Override
		public int hashCode() {
			return colName.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			Col c = (Col) obj;
			return c.getColName().equalsIgnoreCase(this.getColName());
		}

		@Override
		public String toString() {
			return "Col [colName=" + colName + ", index=" + index + ", isDate=" + isDate + "]";
		}

	}

	/**
	 * 需入库的表名
	 * 
	 * @return
	 */
	public abstract String getTableName();

	/**
	 * 每一行记录，是包含在哪个标签内的。 比如，HP_CM_6_2_20101115.xml这类文件，一行数据，是在&lt;HpCMData&gt;标签中的 ，其所有子标签，可组成库表中的一行记录。
	 * 
	 * @return
	 */
	public abstract String getRowFlag();
}

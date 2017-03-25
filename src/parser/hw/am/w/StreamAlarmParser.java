package parser.hw.am.w;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.jsp.jstl.sql.Result;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import parser.Parser;
import task.CollectObjInfo;
import task.RegatherObjInfo;
import task.TaskMgr;
import util.CommonDB;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.LogAnalyzerException;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * W网华为告警采集，字符流方式。
 * 
 * @author ChenSijiang
 */
public class StreamAlarmParser extends Parser implements Runnable {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	/* 等待数据的超时时间（毫秒）。 */
	// private static final int DATA_WAIT_TIMEOUT = 10 * 60 * 1000;

	/* 重试次数。 */
	// private static final int RETRY_TIMES = 3;

	/* 重试间隔时间。 */
	// private static final int RETRY_DELAY = 5 * 1000;

	/* 接收到一条完整报文后的休眠时间（毫秒）。 */
	// private static final int RECV_SLEEP = 10;

	/* 报文开始标记。 */
	private static final String START_FLAG = "<+++>";

	/* 报文结束标记。 */
	private static final String END_FLAG = "<--->";

	/* 报文件中的键值分隔符。 */
	private static final String KEY_VAL_SPLIT = "=";

	/* sqlldr线程扫描间隔时间（毫秒）。 */
	private static final int SQLLDR_SCAN_TIME = 2000;

	/* sqlldr数据文件分隔符号。 */
	private static final String SQLLDR_SPLIT = "|";

	/* 入库间隔条数。 */
	private static final int DEFAULT_SQLLDR_PERIOD = 100;

	private CollectObjInfo task;

	private String name;

	private String host;

	private int port;

	private Socket socket;

	private InputStream inputstream;

	private Reader reader;

	private BufferedReader bufferedReader;

	private AlarmTemplet templet;

	private StringBuilder buffer;

	private List<String> records;

	private SQLLDRThread sqlldrThread;

	public StreamAlarmParser() {
		super();
		this.buffer = new StringBuilder();
		this.records = new LinkedList<String>();
	}

	public StreamAlarmParser(CollectObjInfo task, String name) {
		this();
		this.task = task;
		this.name = name;
		this.host = task.getDevInfo().getIP();
		this.port = task.getDevPort();
	}

	@Override
	public void run() {
		try {
			work();
		} finally {
			TaskMgr.getInstance().delActiveTask(task.getTaskID(), task instanceof RegatherObjInfo);
			logger.info(name + " M2000告警采集线程已退出。");
		}
	}

	@Override
	public boolean parseData() throws Exception {
		return true;
	}

	private void work() {
		if (Util.isNull(host) || port <= 0) {
			logger.error(name + " 服务器IP或端口不合法，M2000告警采集退出（host=" + host + ",port=" + port + "）。");
			return;
		}

		if ((templet = parseTemplet(task.getParseTmpID())) == null) {
			logger.error(name + " 模板加载失败，M2000告警采集退出。");
			return;
		}

		if (!createSocket())
			return;

		try {
			inputstream = socket.getInputStream();
			if (Util.isNotNull(templet.encoding))
				reader = new InputStreamReader(inputstream, templet.encoding);
			else
				reader = new InputStreamReader(inputstream);
			this.bufferedReader = new BufferedReader(reader);
		} catch (Exception e) {
			clearUp();
			logger.error(name + " 获取网络输入流失败，M2000告警采集退出。", e);
			return;
		}

		/* 启动sqlldr线程。 */
		sqlldrThread = new SQLLDRThread();
		sqlldrThread.start();

		/* 开始逐行读取数据，读到null后，认为此连接已不可用，退出。 */
		String line = null;
		boolean findStart = false;
		try {
			while ((line = bufferedReader.readLine()) != null) {
				line = line.trim();
				if (line.equals(END_FLAG)) {
					synchronized (this.records) {
						this.records.add(buffer.toString());
					}
					clearBuffer();
					findStart = false;
					// try
					// {
					// Thread.sleep(RECV_SLEEP);
					// }
					// catch (InterruptedException e)
					// {
					// }
				} else if (findStart) {
					buffer.append(line).append("\n");
				} else if (line.equals(START_FLAG)) {
					findStart = true;
				}

			}
		} catch (Exception e) {
			logger.error(name + " 读取SOCKET数据时发生异常。", e);
		} finally {
			sqlldrThread.interrupt();
			try {
				sqlldrThread.join();
			} catch (InterruptedException e) {
			}
			clearUp();
		}
	}

	private void clearBuffer() {
		this.buffer.delete(0, this.buffer.length());
		this.buffer.setLength(0);
	}

	/* 释放所有资源。 */
	private void clearUp() {
		try {
			IOUtils.closeQuietly(this.bufferedReader);
			IOUtils.closeQuietly(this.reader);
			IOUtils.closeQuietly(this.inputstream);
			clearBuffer();
			if (this.socket != null)
				this.socket.close();
			this.bufferedReader = null;
			this.reader = null;
			this.inputstream = null;
			this.socket = null;
			this.templet = null;
			this.buffer = null;
			this.records = null;
			this.sqlldrThread = null;
		} catch (Exception e) {
		}
	}

	/* 创建socket，连接到服务端。 */
	private boolean createSocket() {
		try {
			logger.debug(name + " 开始连接到 - " + host + ":" + port);
			this.socket = new Socket(host, port);
			logger.debug(name + " 连接成功 - " + host + ":" + port);
		} catch (UnknownHostException e) {
			log.error(name + " 未知的主机名 - " + host, e);
			return false;
		} catch (Exception e) {
			log.error(name + " Socket连接失败 - " + host + ":" + port, e);
			return false;
		}

		return true;
	}

	/* 解析采集模板。 */
	private static AlarmTemplet parseTemplet(int tmpid) {
		String sql = "select tempfilename from igp_conf_templet " + "where tempfilename is not null and tmpid=" + tmpid;
		Result result = null;
		try {
			result = CommonDB.queryForResult(sql);
			if (result == null || result.getRowCount() < 1) {
				logger.error("未找到模板记录 - " + sql);
				return null;
			}
			String filename = result.getRows()[0].get("tempfilename").toString().trim();
			File f = new File(SystemConfig.getInstance().getTempletPath(), filename);
			SAXReader r = new SAXReader();
			Document doc = r.read(f);
			Element elTable = doc.getRootElement().element("table");
			String tableName = elTable.attributeValue("name");
			String encoding = elTable.attributeValue("encoding");
			int sqlldrPeriod = 0;
			try {
				sqlldrPeriod = Integer.parseInt(elTable.attributeValue("sqlldrPeriod"));
			} catch (Exception e) {
			}
			if (sqlldrPeriod <= 0)
				sqlldrPeriod = DEFAULT_SQLLDR_PERIOD;
			Map<String, AlarmColumn> cols = new HashMap<String, AlarmColumn>();
			List<?> colEls = elTable.elements("column");
			for (Object obj : colEls) {
				if (obj instanceof Element) {
					Element e = (Element) obj;
					String attType = e.attributeValue("type");
					String attFormat = e.attributeValue("format");
					cols.put(e.attributeValue("src"),
							new AlarmColumn(e.attributeValue("to"), e.attributeValue("src"), attType != null && attType.equals("3"), attFormat));
				}
			}
			return new AlarmTemplet(tableName, encoding, cols, sqlldrPeriod);
		} catch (Exception e) {
			logger.error("模板解析失败 - " + sql, e);
		} finally {
			result = null;
		}
		return null;
	}

	private class SQLLDRThread extends Thread {

		List<String> tmpRecords = new LinkedList<String>();

		StringBuilder txtBuffer = new StringBuilder();

		int bufferLinesCount = 0;

		@Override
		public void run() {
			/* 写控制文件。 */
			File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "wcdma_m2000_stream_alarm"
					+ File.separator);
			if (!dir.exists())
				dir.mkdirs();
			String fname = task.getTaskID() + "_" + Util.getDateString_yyyyMMddHHmmss(new Date()) + ".ctl";
			File ctlFile = new File(dir, fname);// control file
			File txtFile = new File(dir, fname.replace(".ctl", ".txt")); // data
																			// file
			PrintWriter pw = null;
			try {
				pw = new PrintWriter(ctlFile);
				pw.println("LOAD DATA");
				pw.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
				pw.println("INFILE '" + txtFile.getAbsolutePath() + "' APPEND INTO TABLE " + templet.tableName);
				pw.println("FIELDS TERMINATED BY \"" + SQLLDR_SPLIT + "\"");
				pw.println("TRAILING NULLCOLS");
				pw.println("(");
				pw.println("\tOMCID,");
				pw.println("\tCOLLECTTIME DATE 'YYYY-MM-DD HH24:MI:SS',");
				for (int i = 0; i < templet.sortedColsList.size(); i++) {
					String str = "\t" + templet.sortedColsList.get(i).to;
					if (templet.sortedColsList.get(i).isDate)
						str = str + " DATE '" + templet.sortedColsList.get(i).format + "'";
					if (i < templet.sortedColsList.size() - 1)
						str += ",";
					pw.println(str);
				}
				pw.println(")");
				pw.flush();
			} catch (Exception e) {
			} finally {
				IOUtils.closeQuietly(pw);
				pw = null;
			}

			while (true) {
				try {
					Thread.sleep(SQLLDR_SCAN_TIME);
				} catch (InterruptedException e) {
					ctlFile.delete();
					return;
				}
				synchronized (records) {
					if (records.size() > 0) {
						tmpRecords.addAll(records);
						records.clear();
					}
				}

				if (tmpRecords.size() == 0)
					continue;

				for (String aRec : tmpRecords) {
					if (aRec.contains("告警握手") || Util.isNull(aRec))
						continue;
					String[] spRec = aRec.trim().split("\n");
					List<String> writeVals = new ArrayList<String>();
					for (int i = 0; i < templet.sortedColsList.size(); i++)
						writeVals.add("");
					for (int i = 0; i < spRec.length; i++) {
						if (Util.isNotNull(spRec[i])) {
							String[] spCol = spRec[i].trim().split(KEY_VAL_SPLIT, 2);
							String key = spCol[0].trim();
							String val = (spCol.length == 2 ? spCol[1].trim() : "");
							for (int j = 0; j < templet.sortedColsList.size(); j++) {
								if (key.equalsIgnoreCase(templet.sortedColsList.get(j).src))
									writeVals.set(j, val);
							}
						}
					}
					txtBuffer.append(task.getDevInfo().getOmcID()).append(SQLLDR_SPLIT).append(Util.getDateString(new Date())).append(SQLLDR_SPLIT);
					for (int i = 0; i < writeVals.size(); i++) {
						txtBuffer.append(writeVals.get(i));
						if (i < writeVals.size() - 1)
							txtBuffer.append(SQLLDR_SPLIT);
					}
					txtBuffer.append("\n");
					bufferLinesCount++;
				}
				tmpRecords.clear();

				if (bufferLinesCount >= templet.sqlldrPeriod) {
					PrintWriter txtPw = null;
					try {
						txtPw = new PrintWriter(txtFile);
						txtPw.print(txtBuffer);
						txtPw.flush();
					} catch (Exception e) {
					} finally {
						IOUtils.closeQuietly(txtPw);
						txtPw = null;
						txtBuffer.delete(0, txtBuffer.length());
						txtBuffer.setLength(0);
					}
					bufferLinesCount = 0;

					/* 执行sqlldr */
					File logFile = new File(txtFile.getAbsolutePath().replace(".txt", ".log"));
					File badFile = new File(txtFile.getAbsolutePath().replace(".txt", ".bad"));
					String cmd = String.format("sqlldr userid=%s/%s@%s skip=0 control=%s bad=%s log=%s errors=9999999", SystemConfig.getInstance()
							.getDbUserName(), SystemConfig.getInstance().getDbPassword(), SystemConfig.getInstance().getDbService(), ctlFile
							.getAbsolutePath(), badFile.getAbsolutePath(), logFile.getAbsolutePath());
					int ret = -1;
					try {
						ret = new ExternalCmd().execute(cmd);
					} catch (Exception e) {
						log.error(name + " 执行sqlldr时异常。", e);
					}
					try {
						SqlldrResult sr = new SqlLdrLogAnalyzer().analysis(logFile.getAbsolutePath());
						logger.debug(name + " 入库完成(ret=" + ret + ")：omcid=" + task.getDevInfo().getOmcID() + ",表名=" + sr.getTableName() + ",入库成功条数="
								+ sr.getLoadSuccCount());
					} catch (LogAnalyzerException e) {
					}
					txtFile.delete();
					badFile.delete();
					if (ret == 0) {
						logFile.delete();
					} else {
						logFile.renameTo(new File(logFile.getAbsolutePath() + ".bak" + System.currentTimeMillis()));
					}
				}
			}
		}
	}

	public static void main(String[] args) {

	}
}

class AlarmTemplet {

	String tableName;

	String encoding;

	Map<String/* 源字段名 */, AlarmColumn> cols;

	List<AlarmColumn> sortedColsList;

	int sqlldrPeriod;

	public AlarmTemplet(String tableName, String encoding, Map<String, AlarmColumn> cols, int sqlldrPeriod) {
		super();
		this.tableName = tableName;
		this.encoding = encoding;
		this.cols = cols;
		this.sortedColsList = new ArrayList<AlarmColumn>();
		this.sqlldrPeriod = sqlldrPeriod;
		Iterator<AlarmColumn> it = cols.values().iterator();
		while (it.hasNext()) {
			this.sortedColsList.add(it.next());
		}
		it = null;
	}

}

class AlarmColumn {

	String to;

	String src;

	boolean isDate;

	String format;

	public AlarmColumn(String to, String src, boolean isDate, String format) {
		super();
		this.to = to;
		this.src = src;
		this.isDate = isDate;
		this.format = format;
	}

}

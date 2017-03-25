package parser.al.w.pm;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import task.DevInfo;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * W网阿朗性能数据解析（XML文件）。
 * 
 * @author ChenSijiang 2012-5-2
 */
public class WCDMA_AlcateLucentPerformanceParser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private CollectObjInfo task;

	private String logKey;

	private long taskId;

	private Timestamp stamptime;

	private Timestamp collecttime;

	private String strStamptime;

	private String strCollecttime;

	private String strOmcId;

	private String subnetwork;

	private String managedelement;

	private String rncName;

	private static final String TABLE_NAME_PREFIX = "CLT_PM_W_AL_";

	private static final Map<String/* 表名 */, Map<String/* 原始counter名 */, String/* counter短名 */>> MAPPING_TABLE = new HashMap<String, Map<String, String>>();

	private static final Map<String, List<String>> REAL_COLS = new HashMap<String, List<String>>();

	private Map<String/* 表名 */, AlSqlldrInfo/* sqlldr入库信息 */> sqlldrs; // 记录入库信息，表名对应sqlldr信息

	public WCDMA_AlcateLucentPerformanceParser(CollectObjInfo task) {
		super();
		this.task = task;
		this.taskId = task.getTaskID();
		this.stamptime = task.getLastCollectTime();
		this.collecttime = new Timestamp(System.currentTimeMillis());
		this.strStamptime = Util.getDateString(this.stamptime);
		this.strCollecttime = Util.getDateString(this.collecttime);
		this.logKey = String.format("[%s][%s]", this.taskId, this.strStamptime);
		this.strOmcId = String.valueOf(task.getDevInfo().getOmcID());
		this.sqlldrs = new HashMap<String, WCDMA_AlcateLucentPerformanceParser.AlSqlldrInfo>();
	}

	private synchronized static void loadMaps() {
		try {
			if (MAPPING_TABLE.isEmpty()) {
				Helper.loadMap(MAPPING_TABLE);
				if (!MAPPING_TABLE.isEmpty()) {
					logger.debug("成功加载了clt_pm_w_al_map表，表数量：" + MAPPING_TABLE.size());
				}
			}
		} catch (Exception e) {
			logger.error("加载clt_pm_w_al_map表失败", e);
		}
		if (MAPPING_TABLE.isEmpty()) {
			logger.error("clt_pm_w_al_map表未加载成功，或表中无数据，解析不进行");
			return;
		}

		try {
			Helper.loadRealCols(MAPPING_TABLE, REAL_COLS);
		} catch (Exception e) {
			logger.error("阿朗性能列信息加载失败，解析不进行");
			return;
		}
	}

	public void parse(File fd) {
		logger.debug(taskId + " 准备解析文件：" + fd);
		loadMaps();
		logger.debug(taskId + " 开始解析文件：" + fd);

		InputStream in = null;
		/* STAX解析器，解析xml原始文件 */
		XMLStreamReader reader = null;
		try {
			in = new FileInputStream(fd);

			/* 创建STAX解析器 */
			XMLInputFactory fac = XMLInputFactory.newInstance();
			fac.setProperty("javax.xml.stream.supportDTD", false);
			fac.setProperty("javax.xml.stream.isValidating", false);
			reader = fac.createXMLStreamReader(in);

			/* type记录stax解析器每次读到的对象类型，是element，还是attribute等等…… */
			int type = -1;
			/* 保存当前的xml标签名 */
			String tagName = null;
			/* 当前的moid */
			List<String[]> currMOID = null;
			/* 当前mt列表 */
			List<String> currMT = new ArrayList<String>();
			/* 当前r列表 */
			List<String> currR = new ArrayList<String>();
			/* 开始迭代读取xml文件 */
			while (reader.hasNext()) {
				type = reader.next();
				/*
				 * 判断读取到的xml对象，只对START_ELEMENT和END_ELEMENT进行getLocalName()操作，在jdk1 .6023环境中，如果对CHARACTER进行getLocalName()操作，会抛异常。
				 */
				if (type == XMLStreamConstants.START_ELEMENT || type == XMLStreamConstants.END_ELEMENT)
					tagName = reader.getLocalName();
				if (tagName == null) {
					continue;
				}

				switch (type) {
					case XMLStreamConstants.START_ELEMENT :
						if (tagName.equals("r")) { /* 处理r标签，读取counter值 */
							String rVal = reader.getElementText();
							if (rVal != null) {
								if (rVal.trim().equalsIgnoreCase("null"))
									rVal = "";
							}
							currR.add(rVal);
						} else if (tagName.equals("mt")) {
							/* 处理mt标签，读取counter名 */
							currMT.add(reader.getElementText());
						} else if (tagName.equals("moid")) { /* 处理moid标签，读取counter类别、公共信息 */
							currMOID = Helper.parseMOID(reader.getElementText());
						} else if (tagName.equals("sn")) {
							/* 处理sn标签，读取rnc名、nodeb名等信息。 */
							String[] list = Helper.parseSN(reader.getElementText());
							this.rncName = list[0];
							this.subnetwork = list[1];
							this.managedelement = list[2];
						}
						break;
					case XMLStreamConstants.END_ELEMENT :
						/* 遇到mv结束标签，应处理并清空r列表和当前moid */
						if (tagName.equals("mv")) {
							List<String> tmpR = new ArrayList<String>();
							tmpR.addAll(currR);
							currR.clear();
							List<String> tmpMT = new ArrayList<String>();
							tmpMT.addAll(currMT);
							MOID moid = new MOID(currMOID);
							Record record = new Record(moid, tmpMT, tmpR);
							writeToSqlldrFile(record);
							record.dispose();
							record = null;
						}
						/* 遇到mi结束标签，应处理并清空mt列表 */
						else if (tagName.equals("mi")) {
							currMT.clear();
						}
						break;
					default :
						break;
				}
			}
		} catch (Exception e) {
			logger.error(logKey + "解析时异常 - " + (fd != null ? fd.getAbsolutePath() : ""), e);
		} finally {
			try {
				reader.close();
			} catch (Exception exForReader) {
			}
			IOUtils.closeQuietly(in);
		}
	}

	protected void writeToSqlldrFile(Record record) {
		String[] tableNames = null;
		String tmpTableName = record.getTableName();
		if (tmpTableName != null && (tmpTableName.startsWith("CLT_PM_W_AL_UTRANCELL") || tmpTableName.startsWith("CLT_PM_W_AL_RNCFUNCTION"))) {
			if (tmpTableName.startsWith("CLT_PM_W_AL_UTRANCELL")) {
				tableNames = new String[]{"CLT_PM_W_AL_UTRANCELL1", "CLT_PM_W_AL_UTRANCELL2", "CLT_PM_W_AL_UTRANCELL3", "CLT_PM_W_AL_UTRANCELL4",
						"CLT_PM_W_AL_UTRANCELL5"};
			} else if (tmpTableName.startsWith("CLT_PM_W_AL_RNCFUNCTION")) {
				tableNames = new String[]{"CLT_PM_W_AL_RNCFUNCTION1", "CLT_PM_W_AL_RNCFUNCTION2"};
			}
		} else if (tmpTableName != null) {
			tableNames = new String[]{tmpTableName};
		}
		for (String tableName : tableNames) {
			Map<String, String> mapping = MAPPING_TABLE.get(tableName);
			AlSqlldrInfo sqlldrInfo = sqlldrs.get(tableName);
			if (mapping == null)
				return;
			record.tableName = tableName;
			if (sqlldrInfo == null) {
				sqlldrInfo = makeSqlldr(record, mapping);
				if (sqlldrInfo == null)
					return;
				sqlldrs.put(tableName, sqlldrInfo);
			}

			Map<String, Integer> fieldIndex = sqlldrInfo.fieldIndex;
			List<String> sqlldrRecord = new ArrayList<String>();
			for (int i = 0; i < fieldIndex.size(); i++)
				sqlldrRecord.add("");
			addToRecord("RNC_NAME", sqlldrRecord, this.rncName, fieldIndex);
			addToRecord("SUBNETWORK", sqlldrRecord, this.subnetwork, fieldIndex);
			addToRecord("MANAGEDELEMENT", sqlldrRecord, this.managedelement, fieldIndex);
			for (String[] s : record.moid.values) {
				String moidName = null;
				String moidValue = null;
				try {
					moidName = s[0].toUpperCase();
					moidValue = s[1];
				} catch (Exception e) {
					logger.debug("厂家数据存在问题，" + moidName + " = " + moidValue, e);
				}
				addToRecord(moidName, sqlldrRecord, moidValue, fieldIndex);
			}
			for (int i = 0; i < record.mtList.size(); i++) {
				String colName = mapping.get(record.mtList.get(i));
				if (colName == null || i >= record.rList.size())
					continue;
				addToRecord(colName, sqlldrRecord, record.rList.get(i), fieldIndex);
			}
			addToRecord("OMCID", sqlldrRecord, this.strOmcId, fieldIndex);
			addToRecord("COLLECTTIME", sqlldrRecord, this.strCollecttime, fieldIndex);
			addToRecord("STAMPTIME", sqlldrRecord, this.strStamptime, fieldIndex);
			for (int i = 0; i < sqlldrRecord.size(); i++) {
				sqlldrInfo.writerForTxt.print(sqlldrRecord.get(i));
				if (i < sqlldrRecord.size() - 1)
					sqlldrInfo.writerForTxt.print(AlSqlldrInfo.SQLLDR_SEPARATOR);
			}
			sqlldrInfo.writerForTxt.println();
			sqlldrInfo.writerForTxt.flush();
		}
	}

	public boolean startSqlldr() {
		boolean b = true;
		Iterator<Entry<String, AlSqlldrInfo>> it = sqlldrs.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, AlSqlldrInfo> en = it.next();
			File txt = en.getValue().txt;
			File ctl = en.getValue().ctl;
			File bad = en.getValue().bad;
			File log = en.getValue().log;
			en.getValue().close();
			String serviceName = SystemConfig.getInstance().getDbService();
			String uid = SystemConfig.getInstance().getDbUserName();
			String pwd = SystemConfig.getInstance().getDbPassword();
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", uid, pwd, serviceName,
					ctl.getAbsoluteFile(), bad.getAbsoluteFile(), log.getAbsoluteFile());
			ExternalCmd externalCmd = new ExternalCmd();
			logger.debug(logKey + "当前执行的SQLLDR命令为：" + cmd.replace(uid, "*").replace(pwd, "*"));
			int ret = -1;
			try {
				ret = externalCmd.execute(cmd);
			} catch (Exception e) {
				logger.error(logKey + "执行sqlldr命令失败(" + cmd + ")", e);
				b = false;
			}
			SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
			try {
				SqlldrResult result = analyzer.analysis(new FileInputStream(log));
				logger.debug(logKey + "ret=" + ret + " SQLLDR日志分析结果: omcid=" + strOmcId + " 入库成功条数=" + result.getLoadSuccCount() + " 表名="
						+ result.getTableName() + " 数据时间=" + stamptime + " sqlldr日志=" + log.getAbsolutePath());

				LogMgr.getInstance()
						.getDBLogger()
						.log(task.getDevInfo().getOmcID(), result.getTableName(), task.getLastCollectTime(), result.getLoadSuccCount(),
								task.getTaskID());
			} catch (Exception e) {
				logger.error(logKey + " sqlldr日志分析失败，文件名：" + log.getAbsolutePath() + "，原因: ", e);
				b = false;
			}
			if (SystemConfig.getInstance().isDeleteLog()) {
				if (ret == 0) {
					txt.delete();
					ctl.delete();
					bad.delete();
					log.delete();
				} else if (ret == 2) {
					txt.delete();
					bad.delete();
				}
			}

		}

		// liangww add 2012-06-26 执行完sqlldr后要清除
		sqlldrs.clear();
		return b;
	}

	static void addToRecord(String colName, List<String> sqlldrRecord, String value, Map<String, Integer> fieldIndex) {
		Integer i = fieldIndex.get(colName);
		if (i != null && i < sqlldrRecord.size()) {
			sqlldrRecord.set(i, value);
		}
	}

	protected AlSqlldrInfo makeSqlldr(Record record, Map<String, String> mapping) {
		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "al_w_pm" + File.separator
				+ taskId + File.separator + Util.getDateString_yyyyMMddHHmmss(stamptime) + File.separator);
		dir.mkdirs();
		long flag = System.currentTimeMillis();
		String tn = record.getTableName();
		List<String> realCols = REAL_COLS.get(tn);
		File txt = new File(dir, tn + "_" + flag + ".txt");
		File ctl = new File(dir, tn + "_" + flag + ".ctl");
		File log = new File(dir, tn + "_" + flag + ".log");
		File bad = new File(dir, tn + "_" + flag + ".bad");
		AlSqlldrInfo sq = new AlSqlldrInfo(txt, log, bad, ctl);
		int index = 0;
		sq.fieldIndex.put("RNC_NAME", index++);
		sq.fieldIndex.put("SUBNETWORK", index++);
		sq.fieldIndex.put("MANAGEDELEMENT", index++);
		sq.writerForCtl.println("LOAD DATA");
		sq.writerForCtl.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
		sq.writerForCtl.println("INFILE '" + txt.getAbsolutePath() + "' APPEND INTO TABLE " + tn);
		sq.writerForCtl.println("FIELDS TERMINATED BY \"" + AlSqlldrInfo.SQLLDR_SEPARATOR + "\"");
		sq.writerForCtl.println("TRAILING NULLCOLS (");
		sq.writerForCtl.println("\tRNC_NAME,");
		sq.writerForCtl.println("\tSUBNETWORK,");
		sq.writerForCtl.println("\tMANAGEDELEMENT,");
		sq.writerForTxt.print("RNC_NAME" + AlSqlldrInfo.SQLLDR_SEPARATOR);
		sq.writerForTxt.print("SUBNETWORK" + AlSqlldrInfo.SQLLDR_SEPARATOR);
		sq.writerForTxt.print("MANAGEDELEMENT" + AlSqlldrInfo.SQLLDR_SEPARATOR);
		for (String[] s : record.moid.values) {
			String moidName = s[0].toUpperCase();
			if (realCols.contains(moidName)) {
				sq.fieldIndex.put(moidName, index++);
				sq.writerForCtl.println("\t" + moidName + ",");
				sq.writerForTxt.print(moidName + AlSqlldrInfo.SQLLDR_SEPARATOR);
			}
		}
		for (String mt : record.mtList) {
			String colName = mapping.get(mt);
			if (colName == null)
				continue;
			sq.fieldIndex.put(colName, index++);
			sq.writerForCtl.println("\t" + colName + ",");
			sq.writerForTxt.print(colName + AlSqlldrInfo.SQLLDR_SEPARATOR);
		}
		sq.fieldIndex.put("OMCID", index++);
		sq.fieldIndex.put("COLLECTTIME", index++);
		sq.fieldIndex.put("STAMPTIME", index++);
		sq.writerForCtl.println("\tOMCID,");
		sq.writerForCtl.println("\tCOLLECTTIME DATE 'YYYY-MM-DD HH24:MI:SS',");
		sq.writerForCtl.println("\tSTAMPTIME DATE 'YYYY-MM-DD HH24:MI:SS'");
		sq.writerForTxt.print("OMCID" + AlSqlldrInfo.SQLLDR_SEPARATOR);
		sq.writerForTxt.print("COLLECTTIME" + AlSqlldrInfo.SQLLDR_SEPARATOR);
		sq.writerForTxt.print("STAMPTIME");
		sq.writerForCtl.println(")");
		sq.writerForCtl.flush();
		sq.writerForCtl.close();
		sq.writerForTxt.println();
		sq.writerForTxt.flush();
		return sq;
	}

	static class MOID {

		List<String[]> values;

		int hash = -1;

		public MOID(List<String[]> values) {
			super();
			this.values = values;
		}

		@Override
		public int hashCode() {
			if (hash != -1)
				return hash;

			for (String[] array : values) {
				hash += array[0].hashCode();
				hash += array[1].hashCode();
			}

			return hash;
		}

	}

	class Record {

		MOID moid;

		List<String> mtList;

		List<String> rList;

		String tableName;

		String type;

		public Record(MOID moid, List<String> mtList, List<String> rList) {
			super();
			this.moid = moid;
			this.mtList = mtList;
			this.rList = rList;
		}

		String findVal(String rawCounterName) {
			for (int i = 0; i < mtList.size(); i++) {
				String mt = mtList.get(i);
				if (mt.equalsIgnoreCase(rawCounterName)) {
					return rList.get(i);
				}
			}
			return null;
		}

		/* 获取moid中最后一个key的名字 */
		String getMOIDType() {
			if (this.type == null)
				this.type = moid.values.get(moid.values.size() - 1)[0];
			return this.type;
		}

		/* 获取表名 */
		String getTableName() {
			if (this.tableName == null) {
				this.tableName = TABLE_NAME_PREFIX + getMOIDType().toUpperCase();
				if (this.tableName.length() > 30)
					this.tableName = this.tableName.substring(0, 30);
			}
			return this.tableName;
		}

		/* 向本记录追加mt和r，即moid完全一样的情况下，是要合并在一起的，才是一条完整的记录。 */
		void appendRecord(Record record) {
			for (int i = 0; i < record.mtList.size(); i++) {
				this.mtList.add(record.mtList.get(i));
				this.rList.add(record.rList.get(i));
			}
		}

		/* 释放资源 */
		void dispose() {
			this.moid = null;
			if (this.mtList != null)
				this.mtList.clear();
			if (this.rList != null)
				this.rList.clear();
			this.mtList = null;
			this.rList = null;
		}

		@Override
		public String toString() {
			return "Record [moid=" + moid + ", mtList=" + mtList + ", rList=" + rList + ", tableName=" + tableName + ", type=" + type + "]";
		}

	}

	class AlSqlldrInfo implements Closeable {

		/* sqlldr分隔符，即数据文件的字段分隔符。 */
		static final String SQLLDR_SEPARATOR = "|";

		File txt;

		File log;

		File bad;

		File ctl;

		PrintWriter writerForTxt;

		PrintWriter writerForCtl;

		Map<String/* 字段大写名 */, Integer/* 在sqlldr控制文件中的字段索引位置 */> fieldIndex;

		String table;

		public AlSqlldrInfo(File txt, File log, File bad, File ctl) {
			super();
			this.fieldIndex = new HashMap<String, Integer>();
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

	public static void main(String[] args) throws Exception {
		CollectObjInfo obj = new CollectObjInfo(7515123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(201);
		obj.setDevPort(21);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2011-07-10 19:45:00").getTime()));

		WCDMA_AlcateLucentPerformanceParser parser = new WCDMA_AlcateLucentPerformanceParser(obj);
		// C:\Documents and
		// Settings\Administrator\桌面\A20120401.0400+0800-0415+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=DGRNC06,MeContext=NC_XinJi-BuXingJieBanGongShi_6512_statsfile.xml
		// parser.parse(new
		// File("C:\\Users\\ChenSijiang\\Desktop\\A20120103.1000+0800-1100+0800_NodeB-DQ2_0457longfengdishuiW"));
		parser.parse(new File("E:\\uway\\bug\\igp1\\广西\\W网阿朗性能\\A20141230.1000+0800-1100+0800_RNCCN-GLRNC01.xml"));
		parser.startSqlldr();
	}
}

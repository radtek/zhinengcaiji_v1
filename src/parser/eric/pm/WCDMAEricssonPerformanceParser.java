package parser.eric.pm;

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

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.axis.utils.StringUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.eric.pm.DBFunction.CalCfgItem;
import task.CollectObjInfo;
import task.DevInfo;
import task.RegatherObjInfo;
import tools.socket.SocketClientHelper;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * WCDMA爱立信性能数据解析入库。
 * 
 * @author ChenSijiang 2011-07-27
 */
public class WCDMAEricssonPerformanceParser {

	private CollectObjInfo task;

	private String logKey;

	private long taskId;

	private Timestamp stamptime;

	private Timestamp collecttime;

	private String strStamptime;

	private String strCollecttime;

	private String strOmcId;

	private String subnetworkRoot;

	private String subnetwork;

	private String meContext;

	private String rncName;

	private boolean isNodeB;

	private String isDeal;

	private static final String TABLE_NAME_PREFIX = "CLT_PM_W_ERIC_";

	private String vendor = "ZY0801";

	/**
	 * 需要分拆的mt计数器（DC支撑情况）
	 */
	private String splitMT = "pmTotNoRrcConnectUeCapability";
	private static Map<String/* 原始counter名 */,Integer/* counter长度 */> SPLIT_MT_MAP = new HashMap<String,Integer>();

	/**
	 * 分拆mt计数器的个数（DC支撑情况）
	 */
	private int splitCount = 23;

	/* clt_pm_w_eric_map映射表内容 */
	private static final Map<String/* 表名 */, Map<String/* 原始counter名 */, String/* counter短名 */>> MAPPING_TABLE = new HashMap<String, Map<String, String>>();

	private Map<String/* 表名 */, EricssonSqlldrInfo/* sqlldr入库信息 */> sqlldrs; // 记录入库信息，表名对应sqlldr信息

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();
	static{
		SPLIT_MT_MAP.put("pmTotNoRrcConnectUeCapability", 23);
		SPLIT_MT_MAP.put("pmNoOfHsUsersPerTti", 5);
		SPLIT_MT_MAP.put("pmNoOfHsUsersPerTtiHsFach", 5);
		SPLIT_MT_MAP.put("pmNoOfMcSecUsersPerTti", 5);
	}

	public WCDMAEricssonPerformanceParser(CollectObjInfo task) {
		this.task = task;
		this.taskId = task.getTaskID();
		this.stamptime = task.getLastCollectTime();
		this.collecttime = new Timestamp(System.currentTimeMillis());
		this.strStamptime = Util.getDateString(this.stamptime);
		this.strCollecttime = Util.getDateString(this.collecttime);
		this.logKey = String.format("[%s][%s]", this.taskId, this.strStamptime);
		this.strOmcId = String.valueOf(task.getDevInfo().getOmcID());
		this.isDeal = SystemConfig.getInstance().getEric_w_pm_isDeal();
	}

	/**
	 * 开始解析一个原始文件。
	 * 
	 * @param fileName
	 */
	/**
	 * @param fd
	 */
	/**
	 * @param fd
	 */
	public void parse(File fd) {
		synchronized (MAPPING_TABLE) {
			try {
				if (MAPPING_TABLE.isEmpty()) {
					HelperForWCDMAEricssonPerformanceParser.loadMappingTable(MAPPING_TABLE);
					if (!MAPPING_TABLE.isEmpty()) {
						logger.debug("成功加载了clt_pm_w_eric_map表，表数量：" + MAPPING_TABLE.size());
					}
				}
			} catch (Exception e) {
				logger.error("加载clt_pm_w_eric_map表失败", e);
			}
			if (MAPPING_TABLE.isEmpty()) {
				logger.error("clt_pm_w_eric_map表未加载成功，或表中无数据，解析不进行");
				return;
			}
		}
		if (!initSqlldrs()) {
			logger.error(logKey + "初始化sqlldr失败，此文件不进行解析。");
			return;
		}
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
			/* 存储所有解析出的记录，以MOID作为key. */
			Map<MOID, Record> records = new HashMap<MOID, Record>();
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
							currR.add(reader.getElementText());
						} else if (tagName.equals("mt")) {
							/* 处理mt标签，读取counter名 */
							currMT.add(reader.getElementText());
						} else if (tagName.equals("moid")) { /* 处理moid标签，读取counter类别、公共信息 */
							currMOID = HelperForWCDMAEricssonPerformanceParser.parseMOID(reader.getElementText());
						} else if (tagName.equals("sn")) {
							/* 处理sn标签，读取rnc名、nodeb名等信息。 */
							List<String[]> list = HelperForWCDMAEricssonPerformanceParser.parseSN(reader.getElementText());
							this.subnetworkRoot = HelperForWCDMAEricssonPerformanceParser.findByName(list, "SubNetworkRoot");
							this.subnetwork = HelperForWCDMAEricssonPerformanceParser.findByName(list, "SubNetwork");
							this.meContext = HelperForWCDMAEricssonPerformanceParser.findByName(list, "MeContext");
							this.rncName = this.subnetwork;
							this.isNodeB = (!this.subnetwork.equals(this.meContext));// SubNetwork和MeContext的值不一样的，表示解析的是一个NodeB级别的文件。
						}
						break;
					case XMLStreamConstants.END_ELEMENT :
						/* 遇到mv结束标签，应处理并清空r列表和当前moid */
						if (tagName.equals("mv")) {
							List<String> tmpR = new ArrayList<String>();
							tmpR.addAll(currR);
							List<String> tmpMT = new ArrayList<String>();
							tmpMT.addAll(currMT);
							MOID moid = new MOID(currMOID);
							Record record = new Record(moid, tmpMT, tmpR);
							if (records.containsKey(moid))
								records.get(moid).appendRecord(record);
							else
								records.put(moid, record);
							currR.clear();
							currMOID = null;
						}
						/* 遇到md结束标签，应处理并清空mt列表 */
						else if (tagName.equals("md")) {
							currMT.clear();
						}
						break;
					default :
						break;
				}
			}
			/* 开始合并同moid的记录及开始写入sqlldr文件。 */
			currMOID = null;
			currMT = null;
			currR = null;
			tagName = null;
			Iterator<MOID> keyIt = records.keySet().iterator();
			while (keyIt.hasNext()) {
				MOID key = keyIt.next();
				Record record = records.get(key);
				String tn = record.getTableName();
				if (!MAPPING_TABLE.containsKey(tn) || !sqlldrs.containsKey(tn)) {
					record.dispose();
					continue;
				}

				Map<String, String> rawToCol = MAPPING_TABLE.get(tn);

				// 形成可写入到sqlldr的一行数据。
				EricssonSqlldrInfo sq = sqlldrs.get(tn);
				Map<String, Integer> fieldIndex = sq.fieldIndex;
				List<String> sqlldrRecord = new ArrayList<String>(sq.fieldIndex.size());
				for (int i = 0; i < sq.fieldIndex.size(); i++)
					sqlldrRecord.add("");
				addToRecord("OMCID", sqlldrRecord, this.strOmcId, fieldIndex);
				addToRecord("COLLECTTIME", sqlldrRecord, this.strCollecttime, fieldIndex);
				addToRecord("STAMPTIME", sqlldrRecord, this.strStamptime, fieldIndex);
				addToRecord("RNC_NAME", sqlldrRecord, this.rncName, fieldIndex);
				addToRecord("SUBNETWORKROOT", sqlldrRecord, this.subnetworkRoot, fieldIndex);
				addToRecord("SUBNETWORK", sqlldrRecord, this.subnetwork, fieldIndex);
				addToRecord("MECONTEXT", sqlldrRecord, this.meContext, fieldIndex);

				// pdf计数器附加列
				List<CalCfgItem> calCfgs = DBFunction.findNeedCal(tn);
				if (calCfgs != null && !calCfgs.isEmpty()) {
					for (int i = 0; i < calCfgs.size(); i++) {
						CalCfgItem calCfg = calCfgs.get(i);
						if (calCfg == null || calCfg.colName == null || calCfg.counterName == null)
							continue;
						Double calVal = null;
						if(calCfg.calType != 0){
							calVal = DBFunction.getValueFromCounter(record.findVal(calCfg.counterName),
									calCfg.counterGroup, calCfg);
							addToRecord(calCfg.colName, sqlldrRecord, (calVal != null ? String.format("%.5f", calVal)
									: ""), fieldIndex);
							continue;
						}
						if(!StringUtils.isEmpty(calCfg.additionColAvg))
						{
							// avg
							calVal = DBFunction.getMaxAvgCounter(record.findVal(calCfg.counterName), 2,
									calCfg.counterGroup, calCfg);
							addToRecord(calCfg.additionColAvg, sqlldrRecord,
									(calVal != null ? String.format("%.5f", calVal) : ""), fieldIndex);
						}
						
						if(!StringUtils.isEmpty(calCfg.additionColMax))
						{
							// max
							calVal = DBFunction.getMaxAvgCounter(record.findVal(calCfg.counterName), 1,
									calCfg.counterGroup, calCfg);
							addToRecord(calCfg.additionColMax, sqlldrRecord,
									(calVal != null ? String.format("%.5f", calVal) : ""), fieldIndex);
						}
						
					}
				}

				// cqi附加列
				if (tn.equalsIgnoreCase("CLT_PM_W_ERIC_HSDSCHRESOURCES")) {
					// linagww modify 2013-01-31 修改cqi值
					int[] counts = new int[32];
					String rawCQI = record.findVal("pmReportedCqi");
					boolean flag = false;
					if (Util.isNotNull(rawCQI)) {
						flag = true;
						String[] spCQI = rawCQI.split(",", 999);
						for (int i = 0; i < 32 && i < spCQI.length; i++) {
							try {
								counts[i] += Integer.valueOf(spCQI[i]);
							} catch (Exception e) {
							}
						}
					}

					rawCQI = record.findVal("pmReportedCqi64Qam");
					if (Util.isNotNull(rawCQI)) {
						flag = true;
						String[] spCQI = rawCQI.split(",", 999);
						for (int i = 0; i < 32 && i < spCQI.length; i++) {
							try {
								counts[i] += Integer.valueOf(spCQI[i]);
							} catch (Exception e) {
							}
						}
					}
					// 判断是否个性化处理
					if (!this.isDeal.equals("true"))
						flag = true;

					for (int i = 0; flag && i < 32; i++) {
						addToRecord("PMREPORTEDCQI_" + i, sqlldrRecord, Integer.toString(counts[i]), fieldIndex);
					}
				}

				// moid
				List<String[]> moidList = record.moid.values;
				if (moidList != null && !moidList.isEmpty()) {
					for (String[] moid : moidList) {
						String name = moid[0];
						name = rawToCol.get(name);
						if (Util.isNotNull(name))
							addToRecord(name, sqlldrRecord, moid[1], fieldIndex);
					}
				}

				// 普通counter
				for (int i = 0; i < record.mtList.size(); i++) {
					String name = record.mtList.get(i);
					String colName = rawToCol.get(name);
					String colValue = record.rList.get(i);
					if (Util.isNotNull(colName)) {
						// 计数器pmTotNoRrcConnectUeCapability的附加字段（表CLT_PM_W_ERIC_UTRANCELL）
						if("pmNoOfHsUsersPerTtiHsFach".equals(name)){
							System.out.println("11");
						}
						if(SPLIT_MT_MAP.containsKey(name)){
							int count = SPLIT_MT_MAP.get(name);
							String[] array = Util.split(colValue, ",");
							for(int n = 0; n < count; n++){
								//如果长度不够，置空
								if (n >= array.length) {
									addToRecord(colName + "_" + n, sqlldrRecord, "", fieldIndex);
									continue;
								}
								addToRecord(colName + "_" + n, sqlldrRecord, array[n], fieldIndex);
							}
						}else{
						addToRecord(colName, sqlldrRecord, colValue, fieldIndex);
						}
					}
				}

				for (int i = 0; i < sqlldrRecord.size(); i++) {
					sq.writerForTxt.print(sqlldrRecord.get(i));
					if (i < sqlldrRecord.size() - 1)
						sq.writerForTxt.print(EricssonSqlldrInfo.SQLLDR_SEPARATOR);
				}
				sq.writerForTxt.println();
				sq.writerForTxt.flush();
				sqlldrRecord.clear();
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

	/**
	 * 开始解析一个原始文件。
	 * 
	 * @param fileName
	 */
	public void parse(String fileName) {
		this.parse(new File(fileName));
	}

	/**
	 * @return socket message
	 */
	public String getMessages() {
		if (sqlldrs == null)
			return "";
		StringBuffer bs = new StringBuffer();
		Iterator<String> it = sqlldrs.keySet().iterator();
		String time = Util.getDateString_yyyyMMddHHmm(stamptime);
		while (it.hasNext()) {
			String tn = it.next();
			EricssonSqlldrInfo sq = sqlldrs.get(tn);
			sq.close();
			if (!sq.txt.getName().startsWith(rncName)) {
				File dest = new File(sq.txt.getParent(), rncName + "_" + sq.txt.getName());
				sq.txt.renameTo(dest);
				sq.txt = dest;
			}
			bs.append(time).append(SocketClientHelper.splitSign);
			bs.append(sq.txt.getAbsoluteFile()).append(SocketClientHelper.splitSign);
			bs.append(task.getPeriodTime() / 1000 / 60).append(SocketClientHelper.splitSign);
			bs.append((task instanceof RegatherObjInfo) ? 1 : 0).append(SocketClientHelper.splitSign);
			bs.append(vendor).append(SocketClientHelper.endSign);
			// delete ctl file
			sq.ctl.delete();
		}
		sqlldrs.clear();
		sqlldrs = null;
		return bs.toString();
	}

	public void startSqlldr() {
		if (sqlldrs == null)
			return;
		Iterator<String> it = sqlldrs.keySet().iterator();
		while (it.hasNext()) {
			String tn = it.next();
			EricssonSqlldrInfo sq = sqlldrs.get(tn);
			sq.close();
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", SystemConfig.getInstance()
					.getDbUserName(), SystemConfig.getInstance().getDbPassword(), SystemConfig.getInstance().getDbService(),
					sq.ctl.getAbsoluteFile(), sq.bad.getAbsoluteFile(), sq.log.getAbsoluteFile());
			logger.debug(logKey + "执行 "
					+ cmd.replace(SystemConfig.getInstance().getDbPassword(), "*").replace(SystemConfig.getInstance().getDbUserName(), "*"));
			ExternalCmd execute = new ExternalCmd();
			try {
				int ret = execute.execute(cmd);
				SqlldrResult result = new SqlLdrLogAnalyzer().analysis(sq.log.getAbsolutePath());
				logger.debug(logKey + "exit=" + ret + " omcid=" + this.strOmcId + " 入库成功条数=" + result.getLoadSuccCount() + " 表名="
						+ result.getTableName() + " 数据时间=" + stamptime + " sqlldr日志=" + sq.log.getAbsolutePath());
				LogMgr.getInstance().getDBLogger()
						.log(task.getDevInfo().getOmcID(), result.getTableName(), stamptime, result.getLoadSuccCount(), taskId);
				if (ret == 0 && SystemConfig.getInstance().isDeleteLog()) {
					sq.txt.delete();
					sq.ctl.delete();
					sq.log.delete();
					sq.bad.delete();
				} else if (ret == 2 && SystemConfig.getInstance().isDeleteLog()) {
					sq.txt.delete();
					sq.ctl.delete();
					sq.bad.delete();
				}
			} catch (Exception ex) {
				logger.error(logKey + "sqlldr时异常", ex);
			}
		}// end of while (it.hasNext())

		// liangww add 2012-04-09
		// 解决配置任务多个路径时，只解析入库第一个的bug
		sqlldrs.clear();
		sqlldrs = null;

	}

	protected boolean initSqlldrs() {
		if (sqlldrs != null)
			return true;
		String time = Util.getDateString_yyyyMMddHHmmss(this.task.getLastCollectTime());
		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "eric_w_pm" + File.separator
				+ taskId + File.separator);
		long flag = System.currentTimeMillis();
		if (!dir.mkdirs() && !dir.exists()) {
			logger.error(logKey + "创建临时目录失败：" + dir);
			return false;
		}
		sqlldrs = new HashMap<String, EricssonSqlldrInfo>();
		Iterator<String> it = MAPPING_TABLE.keySet().iterator();
		while (it.hasNext()) {
			String tn = it.next();
			String baseName = tn + "_" + time + "_" + flag;
			List<String> fs = new ArrayList<String>(MAPPING_TABLE.get(tn).values());
			File txt = new File(dir, baseName + ".txt");
			File ctl = new File(dir, baseName + ".ctl");
			File log = new File(dir, baseName + ".log");
			File bad = new File(dir, baseName + ".bad");
			EricssonSqlldrInfo sq = new EricssonSqlldrInfo(txt, log, bad, ctl);
			int index = 0;
			sq.fieldIndex.put("OMCID", index++);
			sq.fieldIndex.put("COLLECTTIME", index++);
			sq.fieldIndex.put("STAMPTIME", index++);
			sq.fieldIndex.put("RNC_NAME", index++);
			sq.fieldIndex.put("SUBNETWORKROOT", index++);
			sq.fieldIndex.put("SUBNETWORK", index++);
			sq.fieldIndex.put("MECONTEXT", index++);
			sq.writerForCtl.println("LOAD DATA");
			sq.writerForCtl.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
			sq.writerForCtl.println("INFILE '" + txt.getAbsolutePath() + "' APPEND INTO TABLE " + tn);
			sq.writerForCtl.println("FIELDS TERMINATED BY \"" + EricssonSqlldrInfo.SQLLDR_SEPARATOR + "\"");
			sq.writerForCtl.println("TRAILING NULLCOLS (");
			sq.writerForCtl.println("OMCID,");
			sq.writerForCtl.println("COLLECTTIME DATE 'YYYY-MM-DD HH24:MI:SS',");
			sq.writerForCtl.println("STAMPTIME DATE 'YYYY-MM-DD HH24:MI:SS',");
			sq.writerForCtl.println("RNC_NAME,");
			sq.writerForCtl.println("SUBNETWORKROOT,");
			sq.writerForCtl.println("SUBNETWORK,");
			sq.writerForCtl.println("MECONTEXT,");
			sq.writerForTxt.print("OMCID" + EricssonSqlldrInfo.SQLLDR_SEPARATOR);
			sq.writerForTxt.print("COLLECTTIME" + EricssonSqlldrInfo.SQLLDR_SEPARATOR);
			sq.writerForTxt.print("STAMPTIME" + EricssonSqlldrInfo.SQLLDR_SEPARATOR);
			sq.writerForTxt.print("RNC_NAME" + EricssonSqlldrInfo.SQLLDR_SEPARATOR);
			sq.writerForTxt.print("SUBNETWORKROOT" + EricssonSqlldrInfo.SQLLDR_SEPARATOR);
			sq.writerForTxt.print("SUBNETWORK" + EricssonSqlldrInfo.SQLLDR_SEPARATOR);
			sq.writerForTxt.print("MECONTEXT" + EricssonSqlldrInfo.SQLLDR_SEPARATOR);

			List<String> addedFileds = new ArrayList<String>();
			/* cqi的附加字段 */
			if (tn.equalsIgnoreCase("CLT_PM_W_ERIC_HSDSCHRESOURCES")) {
				for (int i = 0; i < 32; i++) {
					sq.writerForTxt.print("PMREPORTEDCQI_" + i + EricssonSqlldrInfo.SQLLDR_SEPARATOR);
					sq.writerForCtl.println("PMREPORTEDCQI_" + i + ",");
					sq.fieldIndex.put("PMREPORTEDCQI_" + i, index++);
					addedFileds.add("PMREPORTEDCQI_" + i);
				}
			}
			/* 计数器pmTotNoRrcConnectUeCapability的附加字段 */
			if (tn.equalsIgnoreCase("CLT_PM_W_ERIC_UTRANCELL")) {
				String colNamePrefix = MAPPING_TABLE.get(tn).get(this.splitMT);
				for (int i = 0; i < this.splitCount; i++) {
					sq.writerForTxt.print(colNamePrefix + "_" + i + EricssonSqlldrInfo.SQLLDR_SEPARATOR);
					sq.writerForCtl.println(colNamePrefix + "_" + i + ",");
					sq.fieldIndex.put(colNamePrefix + "_" + i, index++);
					addedFileds.add(colNamePrefix + "_" + i);
				}
			}
			/* pdf计数器的附加字段 */
			List<CalCfgItem> calCfg = DBFunction.findNeedCal(tn);
			if (calCfg != null && !calCfg.isEmpty()) {
				for (int i = 0; i < calCfg.size(); i++) {
					
					if(calCfg.get(i).calType != 0){
						sq.writerForTxt.print(calCfg.get(i).colName + EricssonSqlldrInfo.SQLLDR_SEPARATOR);
						sq.writerForCtl.println(calCfg.get(i).colName + ",");
						sq.fieldIndex.put(calCfg.get(i).colName, index++);
						addedFileds.add(calCfg.get(i).colName);
						continue;
					}
					
					if(!StringUtils.isEmpty(calCfg.get(i).additionColAvg))
					{
						// avg
						sq.writerForTxt.print(calCfg.get(i).additionColAvg + EricssonSqlldrInfo.SQLLDR_SEPARATOR);
						sq.writerForCtl.println(calCfg.get(i).additionColAvg + ",");
						sq.fieldIndex.put(calCfg.get(i).additionColAvg, index++);
						addedFileds.add(calCfg.get(i).additionColAvg);
					}
					
					if(!StringUtils.isEmpty(calCfg.get(i).additionColMax))
					{
						// max
						sq.writerForTxt.print(calCfg.get(i).additionColMax + EricssonSqlldrInfo.SQLLDR_SEPARATOR);
						sq.writerForCtl.println(calCfg.get(i).additionColMax + ",");
						sq.fieldIndex.put(calCfg.get(i).additionColMax, index++);
						addedFileds.add(calCfg.get(i).additionColMax);
					}
					
				}
			}
			for (int i = 0; i < fs.size(); i++) {
				String fieldName = fs.get(i);
				
				if (addedFileds.contains(fieldName))
					continue;
				sq.fieldIndex.put(fieldName, index++);
				sq.writerForTxt.print(fieldName);
				sq.writerForCtl.print(fieldName + " char(512)");
				if (i < fs.size() - 1) {
					sq.writerForTxt.print(EricssonSqlldrInfo.SQLLDR_SEPARATOR);
					sq.writerForCtl.print(",");
				}
				sq.writerForCtl.println();
			}
			sq.writerForTxt.println();
			sq.writerForTxt.flush();
			sq.writerForCtl.println(")");
			sq.writerForCtl.close();
			sqlldrs.put(tn, sq);
		}

		return true;
	}

	static void addToRecord(String name, List<String> sqlldrRecord, String value, Map<String, Integer> fieldIndex) {
		Integer i = fieldIndex.get(name);
		if (i != null && i < sqlldrRecord.size()) {
			sqlldrRecord.set(i, value);
		}
	}

	class MOID {

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

		@Override
		public boolean equals(Object obj) {
			MOID m = (MOID) obj;
			if (m.values.size() != this.values.size())
				return false;
			for (int i = 0; i < this.values.size(); i++) {
				if (!HelperForWCDMAEricssonPerformanceParser.compareMOID(this.values, m.values))
					return false;
			}
			return true;
		}
	}

	/**
	 * 表示一条记录，包括的元素：MOID、mt列表、r列表。
	 * 
	 * @author ChenSijiang
	 */
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
				this.type = HelperForWCDMAEricssonPerformanceParser.lastMOIDEntry(this.moid.values)[0];
			return this.type;
		}

		/* 获取表名 */
		String getTableName() {
			if (this.tableName == null) {
				this.tableName = TABLE_NAME_PREFIX + getMOIDType().toUpperCase();
				if (this.tableName.length() > 30)
					this.tableName = this.tableName.substring(0, 30);
				if (this.tableName.equalsIgnoreCase("CLT_PM_W_ERIC_VCLTP") && isNodeB)
					this.tableName = "CLT_PM_W_ERIC_NODEBVCLTP";
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

		// @Override
		// public boolean equals(Object obj)
		// {
		// Record r = (Record) obj;
		// return HelperForWCDMAEricssonPerformanceParser.compareMOID(this.moid,
		// r.moid);
		// }
	}

	/**
	 * sqlldr入库信息，记录txt、ctl等文件路径，及写txt、ctl文件的Writer.
	 * 
	 * @author ChenSijiang
	 */
	private class EricssonSqlldrInfo implements Closeable {

		/* sqlldr分隔符，即数据文件的字段分隔符。 */
		static final String SQLLDR_SEPARATOR = "|";

		File txt;

		File log;

		File bad;

		File ctl;

		PrintWriter writerForTxt;

		PrintWriter writerForCtl;

		Map<String/* 字段大写名 */, Integer/* 在sqlldr控制文件中的字段索引位置 */> fieldIndex;

		public EricssonSqlldrInfo(File txt, File log, File bad, File ctl) {
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

		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(201);
		obj.setDevPort(21);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2011-07-10 19:45:00").getTime()));

		WCDMAEricssonPerformanceParser parser = new WCDMAEricssonPerformanceParser(obj);
		// C:\Documents and
		// Settings\Administrator\桌面\A20120401.0400+0800-0415+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=DGRNC06,MeContext=NC_XinJi-BuXingJieBanGongShi_6512_statsfile.xml
		//		parser.parse("E:\\IGPV1\\爱立信性能应答变更\\A20140310.1000+0800-1015+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=ECR05,MeContext=ECR05_statsfile.xml");
		parser.parse("E:\\IGPV1\\爱立信性能应答变更\\A20140310.1030+0800-1045+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=SWRNC04,MeContext=SWPinQing_4208_B_statsfile.xml");
		parser.startSqlldr();
	}
}

package parser.lucent.cm;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.SqlldrRunner;
import util.Util;
import util.loganalyzer.LogAnalyzerException;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * w网阿朗参数解析。
 * 
 * @author ChenSijiang
 * 
 *         V8版本变更需求： 1,原始文件中<Class3CellReconfParams>下的所有节点移至FDDCELL节点下，提取出来单独入表CLT_CM_W_AL_CLASS3C74
 *         2,FDDCell节点下新增<cellSelectionProfileId>标签，根据此标签值去找<CellProfiles>下对应的<CellSelectionInfo>并入表CLT_CM_W_AL_CELLSEL77。
 *         替代V7版本的直接在FDDCell下对应的<CellSelectionInfo>下的值，然后入库 added by yuy , 2013-07-17
 * 
 */
public class WcdmaALParamParser extends Parser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final String TABLE_NAME_PREFIX = "CLT_CM_W_AL_";

	private static final String MAP_TABLE_NAME = TABLE_NAME_PREFIX + "MAP";

	private static final Map<String/* 节点名 */, String/* 表名(未加前缀) */> SPEC_MAP = new HashMap<String, String>();

	private static final Map<String/* 表名(带前缀) */, Map<String/* 原始参数名 */, String/* 列名 */>> MAP_TABLE = new HashMap<String, Map<String, String>>();

	private static final String SQLLDR_DE = ";";

	private static final int PID_COUNT = 8;

	private DBLogger dblogger = LogMgr.getInstance().getDBLogger();

	private Timestamp collecttime;

	private String strCollecttime;

	private String strStamptime;

	private int omcid;

	private long taskId;

	private String loggerKey;

	private Map<String, Map<String, String>> tempObjMap;

	private Map<String, AlColumnObj> tempCellMap;

	private String cellSelectionProfileId = null;

	private String flagBit = "uwaysoft";

	private Map<String, ALParamSqlldrInfo> sqlldrs = new HashMap<String, ALParamSqlldrInfo>();

	static {
		/* 这是为了兼容以前的解析，很多MAP表丢失的对应关系，这里写死。 */
		SPEC_MAP.put("Class3CellReconfParams", "CLASS3C74");// 此标签已不存在，其下的字段在FDDCell标签下
		SPEC_MAP.put("UlOuterLoopPowerCtrl", "ULOUTER242");
		SPEC_MAP.put("UlIrmCEMParameters", "ULIRMCE399");
		SPEC_MAP.put("UlInnerLoopConf", "ULINNER407");
		SPEC_MAP.put("UeTimerCstIdleMode", "UETIMER519");
		SPEC_MAP.put("UeTimerCstConnectedMode", "UETIMER305");
		SPEC_MAP.put("RachTxParameters", "RACHTXP96");
		SPEC_MAP.put("ReferenceEtfciConfList", "REFEREN373");
		SPEC_MAP.put("RadioAccessService", "RADIOAC167");
		SPEC_MAP.put("UmtsNeighbouringRelation", "UMTSNEI529");
		SPEC_MAP.put("UMTSFddNeighbouringCell", "UMTSFDD72");
		SPEC_MAP.put("IrmPreemptionCacParams", "IRMPREE455");
		SPEC_MAP.put("IrmOnCellColourParameters", "IRMONCE456");
		SPEC_MAP.put("InterFreqMeasConf", "INTERFR411");
		SPEC_MAP.put("MissingMeasurement", "MISSING437");
		SPEC_MAP.put("ManualActivation", "MANUALA325");
		SPEC_MAP.put("DlIrmCEMParameters", "DLIRMCE403");
		SPEC_MAP.put("DlBlerQualityList", "DLBLERQ283");
		SPEC_MAP.put("PreemptionQueuingReallocation", "PREEMPT351");
		SPEC_MAP.put("PowerPartConfClass", "POWERPA444");
		SPEC_MAP.put("DynamicParameterPerDch", "DYNAMIC254");
		SPEC_MAP.put("FastAlarmHardHoConf", "FASTALA508");
		SPEC_MAP.put("AutomatedCellBarring", "AUTOMAT324");
		SPEC_MAP.put("FullEventHOConfHhoMgtEvent2D", "FULLEVE507");
		SPEC_MAP.put("FullEventHOConfHhoMgtEvent2F", "FULLEVE503");
		SPEC_MAP.put("FullEventHOConfShoMgtEvent1D", "FULLEVE490");
		SPEC_MAP.put("FullEventHOConfShoMgtEvent1C", "FULLEVE489");
		SPEC_MAP.put("CellSelectionInfo", "CELLSEL77");
		SPEC_MAP.put("CellAccessRestrictionConnectedMode", "CELLACC103");
		SPEC_MAP.put("GsmNeighbouringCell", "GSMNEIG73");
		SPEC_MAP.put("HsdpaUserService", "HSDPAUS526");
		SPEC_MAP.put("HsPdschDynamicManagement", "HSPDSCH446");
		SPEC_MAP.put("FullEventHOConfShoMgtEvent1B", "FULLEVE488");
		SPEC_MAP.put("FullEventHOConfShoMgtEvent1F", "FULLEVE487");
		SPEC_MAP.put("FullEventHOConfShoMgtEvent1E", "FULLEVE485");
		SPEC_MAP.put("FullEventHOConfShoMgtEvent1A", "FULLEVE484");
		SPEC_MAP.put("FullEventRepCritEvent1AWithoutIur", "FULLEVE415");
		SPEC_MAP.put("FddNeighCellSelectionInfoConnectedMode", "FDDNEIG532");
		SPEC_MAP.put("FddIntelligentMultiCarrierTrafficAllocation", "FDDINTE81");
		SPEC_MAP.put("CallAccessPerformanceConf", "CALLACC237");
		SPEC_MAP.put("DlInnerLoopConf", "DLINNER406");
		SPEC_MAP.put("UlIrmRadioLoadParameters", "ULIRMRA452");
		SPEC_MAP.put("FullEventRepCritShoMgtEvent1A", "FULLEVE422");
		SPEC_MAP.put("FullEventRepCritShoMgtEvent1B", "FULLEVE426");
		SPEC_MAP.put("FullEventRepCritShoMgtEvent1C", "FULLEVE420");
		SPEC_MAP.put("FullEventRepCritShoMgtEvent1D", "FULLEVE421");
		SPEC_MAP.put("FullEventRepCritShoMgtEvent1E", "FULLEVE423");
		SPEC_MAP.put("FullEventRepCritShoMgtEvent1F", "FULLEVE425");
		SPEC_MAP.put("CellAccessRestrictionConnectedMode", "CELLACC101");

		/* 加载MAP表 */
		String sql = "select * from " + MAP_TABLE_NAME + " where tab_name in (select table_name from user_tables where table_name like '"
				+ TABLE_NAME_PREFIX + "%')";
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			logger.debug("正在加载阿朗参数映射表 - " + sql);
			con = DbPool.getConn();
			st = con.createStatement();
			rs = st.executeQuery(sql);
			while (rs.next()) {
				//全转化为大写，确保一张表的字段名称唯一，避免由于clt_cm_w_al_map中字段重复导致问题
				String colName = rs.getString("col_name").toUpperCase();
				String shortColName = rs.getString("short_col_name").toUpperCase();
				String tabName = rs.getString("tab_name").toUpperCase();
				Map<String, String> colMap = null;
				if (MAP_TABLE.containsKey(tabName)) {
					colMap = MAP_TABLE.get(tabName);
				} else {
					colMap = new HashMap<String, String>();
					MAP_TABLE.put(tabName, colMap);
				}
				colMap.put(colName, shortColName);
			}
		} catch (Exception e) {
			logger.error("加载阿朗参数映射表时出现异常。", e);
		} finally {
			logger.debug("阿朗参数映射表加载完毕。");
			CommonDB.close(rs, st, con);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean parseData() throws Exception {
		this.collecttime = new Timestamp(System.currentTimeMillis());
		this.strCollecttime = Util.getDateString(this.collecttime);
		this.strStamptime = Util.getDateString(super.getCollectObjInfo().getLastCollectTime());
		this.omcid = super.getCollectObjInfo().getDevInfo().getOmcID();
		this.taskId = super.getCollectObjInfo().getTaskID();
		this.loggerKey = "[" + this.taskId + "][" + this.strStamptime + "]";

		Date begin = new Date();
		List<File> files = FileSplit.splitFull(new File(fileName));

		if (files == null) {
			files = new ArrayList<File>();
			files.add(new File(fileName));
		}

		// File[] files = new File(fileName).listFiles();

		initSqlldrs();
		initPids(pids);

		for (File oneFile : files) {
			InputStream in = null;
			try {
				in = new FileInputStream(oneFile);
				SAXReader r = new SAXReader();
				logger.debug(loggerKey + "解析xml - " + oneFile);
				Document doc = r.read(in);
				logger.debug(loggerKey + "xml解析完成 - " + oneFile);
				Element rootElement = doc.getRootElement();
				List<Element> rncElements = rootElement.elements();
				rncid1 = findRncID(rncElements);
				for (Element rnc : rncElements) {
					parseElement(rnc);
				}

				// CellSelectionInfo 处理
				if (tempObjMap != null && tempCellMap != null) {
					cellSelectionInfoHandler();
				}

			} catch (Exception e) {
				logger.error(loggerKey + "解析文件出错 - " + fileName, e);
			} finally {
				IOUtils.closeQuietly(in);
				id = "";
				pid = "";
				ptablename = "";
				rncid1 = "";
				rncidForCI = "";
				rncname = "";
				nodebid1 = "";
				fddcellid1 = "";
				ci1 = "";
				lac1 = "";
				initPids(pids);
				buffer.setLength(0);
				oneFile.delete();
			}
		}
		log.info(this.loggerKey + fileName + " 文件解析结束，解析耗时：" + (int) (new Date().getTime() - begin.getTime()) / 1000 + "秒");

		begin = new Date();

		if (files != null)
			startSqlldr();

		log.info(this.loggerKey + fileName + " 文件入库结束，入库耗时：" + (int) (new Date().getTime() - begin.getTime()) / 1000 + "秒");

		sqlldrs.clear();

		return true;
	}

	private void cellSelectionInfoHandler() {
		String tableName = getTableName("CellSelectionInfo");
		Iterator<String> it = tempCellMap.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			Map<String, String> map = tempObjMap.get(key.substring(0, key.indexOf(flagBit)));
			AlColumnObj obj = tempCellMap.get(key);
			if (tableName != null && MAP_TABLE.containsKey(tableName)) {
				ALParamSqlldrInfo sq = sqlldrs.get(tableName);
				Map<Integer, String> indexCol = sq.indexCol();
				Map<String, String> filedMap = MAP_TABLE.get(tableName);
				int totalFieldsCount = filedMap.size() + 11 + PID_COUNT;
				int startIndex = 11 + PID_COUNT;
				StringBuilder buffer = new StringBuilder();
				buffer.append(obj.getOmcId()).append(SQLLDR_DE).append(obj.getStrCollecttime()).append(SQLLDR_DE);
				buffer.append(obj.getStrStamptime()).append(SQLLDR_DE).append("0").append(SQLLDR_DE).append(obj.getPid());
				buffer.append(SQLLDR_DE).append(obj.getPtablename()).append(SQLLDR_DE).append(obj.getRncid());
				buffer.append(SQLLDR_DE).append(obj.getNodebid()).append(SQLLDR_DE).append(obj.getFddcellid());
				buffer.append(SQLLDR_DE).append(obj.getCi()).append(SQLLDR_DE).append(obj.getLac()).append(SQLLDR_DE);

				String[] strs = fillIdToPids(obj.getPids(), obj.getFddcellid());
				for (String s : strs) {
					buffer.append(s).append(SQLLDR_DE);
				}
				for (int i = startIndex; i < totalFieldsCount; i++) {
					String name = indexCol.get(i);
					String value = map.get(name);
					value = (value == null ? "" : value);
					value.replace(SQLLDR_DE, " ");
					buffer.append(value);
					if (i < totalFieldsCount - 1)
						buffer.append(SQLLDR_DE);
				}
				sq.txtWriter.println(buffer.toString());
				sq.flush();
				buffer.setLength(0);
			}
		}
		tempObjMap = null;
		tempCellMap = null;
	}

	StringBuilder buffer = new StringBuilder();

	String id = "";

	String pid = "";

	String ptablename = "";

	String rncid1 = "";

	String rncidForCI = "";

	String rncname = "";

	String nodebid1 = "";

	String fddcellid1 = "";

	String ci1 = "";

	String lac1 = "";

	String btsId = "";

	String[] pids = new String[PID_COUNT];

	@SuppressWarnings("unchecked")
	void parseElement(Element el) throws Exception {
		String elementName = el.getName();
		String tableName = getTableName(elementName);

		id = el.attributeValue("id");
		Element pe = el.getParent();
		if (pe != null) {
			ptablename = getTableName(pe.getName());
			ptablename = (ptablename != null ? ptablename : "");
		}

		initPids(pids);
		int pidCount = 0;
		while (pe != null && pidCount < pids.length) {
			String peId = pe.attributeValue("id");
			peId = (peId != null ? peId : "");
			String peName = pe.getName();
			if (peName.equalsIgnoreCase("nodeb")) {
				nodebid1 = peId;
			} else if (peName.equalsIgnoreCase("fddcell")) {
				fddcellid1 = peId;
			}
			pids[pidCount++] = peId;
			pe = pe.getParent();
		}
		pid = pids[0];

		// key
		String key = null;
		if ("CellSelectionInfo".equals(elementName)) {
			key = pids[1] + "_" + pids[0];
		}
		reversePids(pids);

		List<Element> subEls = el.elements();

		/** elementName.attributes */
		Element attsEl = findAttsEl(subEls);
		if (attsEl != null) {
			if (tableName != null && MAP_TABLE.containsKey(tableName)) {
				ALParamSqlldrInfo sq = sqlldrs.get(tableName);

				Map<Integer, String> indexCol = sq.indexCol();
				Map<String, String> filedMap = MAP_TABLE.get(tableName);
				int size = filedMap.size();
				int totalFieldsCount = size + 11 + PID_COUNT/*
															 * 公共字段个数+ pid个数
															 */;
				int startIndex = 11 + PID_COUNT/* 公共字段个数+pid个数 */;

				// 把CLT_CM_W_AL_CLASS3C74表的字段加进去
				if ("FDDCell".equals(elementName)) {
					filedMap.putAll(MAP_TABLE.get(getTableName("Class3CellReconfParams")));
				}

				SortedMap<String, String> vals = getAttVals(attsEl, filedMap);
				buffer.setLength(0);
				buffer.append(omcid).append(SQLLDR_DE).append(strCollecttime).append(SQLLDR_DE).append(strStamptime);
				buffer.append(SQLLDR_DE).append(id).append(SQLLDR_DE).append(pid).append(SQLLDR_DE);
				buffer.append(ptablename).append(SQLLDR_DE).append(rncid1).append(SQLLDR_DE);
				buffer.append(nodebid1).append(SQLLDR_DE).append(fddcellid1).append(SQLLDR_DE);
				buffer.append(ci1).append(SQLLDR_DE).append(lac1).append(SQLLDR_DE);
				for (String s : pids) {
					buffer.append(s).append(SQLLDR_DE);
				}

				/*
				 * V8变更处理 /* [begin]
				 */
				if ("FDDCell".equals(elementName)) {
					// Class3CellReconfParams 处理
					class3CellReconfParamsHandler(tableName, filedMap, vals);

					// CellSelectionInfo 处理--获取cellSelectionProfileId值和公共值
					setTempCellMap(tableName);
				}
				// CellSelectionInfo 处理--获取标签<CellSelectionInfo>下的值
				if ("CellSelectionInfo".equals(elementName)) {
					setTempObjMap(key, indexCol, totalFieldsCount, startIndex, vals);
					return;
				}
				/* [end] */

				/** BTSEquipment:三个特殊字段处理 --20141222 */
				BTSEquipmentOtherHandler(elementName, subEls, vals);

				for (int i = startIndex; i < totalFieldsCount; i++) {
					String name = indexCol.get(i);
					String value = vals.get(name);
					value = (value == null ? "" : value);
					value.replace(SQLLDR_DE, " ");
					buffer.append(value);
					if (i < totalFieldsCount - 1)
						buffer.append(SQLLDR_DE);
				}

				sq.txtWriter.println(buffer);
				sq.flush();
				buffer.setLength(0);
				filedMap = null;
			}
		} else {

		}
		for (Element e : subEls) {
			parseElement(e);
		}
	}

	/**
	 * BTSEquipment:三个字段特殊处理，见需求excel中注意事项(W0018.WCDMA贝尔(阿朗)参数采集需求_V1.0_20141117.xlsx)
	 * 
	 * @param elementName
	 * @param subEls
	 */
	@SuppressWarnings("unchecked")
	public void BTSEquipmentOtherHandler(String elementName, List<Element> subEls, SortedMap<String, String> vals) {
		if (!elementName.equalsIgnoreCase("BTSEquipment"))
			return;

		/** CLT_CM_W_AL_BTSEQUIPMENT.ulBandwidth */
		String ulBandwidth = null;
		/** CLT_CM_W_AL_BTSEQUIPMENT.imaPcmlist可能存在多行数据，多行数据用逗号隔开。 */
		String imaPcmList_val = null;
		/** 在imaPcmList_val中找到最大的PCM/X，则X就是字段imaPcmlist_MAX的值 */
		String imaPcmList_max = null;

		// BTSEquipment:三个字段特殊处理，见需求excel中注意事项
		// EthernetPort.ulBandwidth
		Element ulBandwidth_Ele = null;
		// Class0ImaGroupParams.ImaGroup
		Element imaGroup_Ele = null;
		for (int i = 0; i < subEls.size(); i++) {
			if (subEls.get(i).getName().equalsIgnoreCase("EthernetPort")) {
				ulBandwidth_Ele = subEls.get(i);
				continue;
			}
			if (subEls.get(i).getName().equalsIgnoreCase("ImaGroup")) {
				imaGroup_Ele = subEls.get(i);
				continue;
			}
			if (ulBandwidth_Ele != null && imaGroup_Ele != null)
				break;
		}
		// ulBandwidth字段值
		ulBandwidth = getUlbandwidth(ulBandwidth, ulBandwidth_Ele);

		if (imaGroup_Ele != null) {
			List<Element> els = imaGroup_Ele.elements();
			for (int i = 0; i < els.size(); i++) {
				if (els.get(i).getName().equalsIgnoreCase("Class0ImaGroupParams")) {
					List<Element> subEls_ = els.get(i).elements();
					Element attsEl_ = findAttsEl(subEls_);
					if (attsEl_ != null) {
						List<Element> es = attsEl_.elements();
						for (int n = 0; n < es.size(); n++) {
							Element imaPcmEl = es.get(n);
							if (imaPcmEl.getName().equalsIgnoreCase("imaPcmList")) {
								List<Element> valEl = imaPcmEl.elements();
								StringBuilder sb = new StringBuilder();
								int max = 0;
								for (int m = 0; m < valEl.size(); m++) {
									if (valEl.get(m).getName().equalsIgnoreCase("value")) {
										String val = Util.nvl(valEl.get(m).getText(), "");
										sb.append(",").append(val);
										int index = val.lastIndexOf("/");
										if (index > -1) {
											int c = Integer.parseInt(val.substring(index + 1));
											if (c > max) {
												max = c;
											}
										}
									}
								}
								if (sb.toString().length() > 0) {
									imaPcmList_val = sb.toString().substring(1);
									imaPcmList_max = max == 0 ? "" : (max + "");
									break;
								}
							}
						}
					}
				}
			}
		}
		// result : set values
		vals.put("ULBANDWIDTH", Util.nvl(ulBandwidth, ""));
		vals.put("IMAPCMLIST", Util.nvl(imaPcmList_val, ""));
		vals.put("IMAPCMLIST_MAX", Util.nvl(imaPcmList_max, ""));
	}

	/**
	 * @param ulBandwidth
	 * @param ulBandwidth_Ele
	 * @return ulBandwidth
	 */
	@SuppressWarnings("unchecked")
	public String getUlbandwidth(String ulBandwidth, Element ulBandwidth_Ele) {
		if (ulBandwidth_Ele != null) {
			List<Element> els = ulBandwidth_Ele.elements();
			Element attsEl_u = findAttsEl(els);
			List<Element> els_ = attsEl_u.elements();
			for (int i = 0; i < els_.size(); i++) {
				if (els_.get(i).getName().equalsIgnoreCase("ulBandwidth")) {
					ulBandwidth = els_.get(i).getText();
					break;
				}
			}
		}
		return ulBandwidth;
	}

	private void class3CellReconfParamsHandler(String tableName, Map<String, String> filedMap, SortedMap<String, String> vals) {
		String tn = getTableName("Class3CellReconfParams");
		ALParamSqlldrInfo sq0 = sqlldrs.get(tn);

		Map<Integer, String> indexCol0 = sq0.indexCol();
		Map<String, String> filedMap0 = MAP_TABLE.get(tn);
		int totalFieldsCount0 = filedMap0.size() + 11 + PID_COUNT;
		int startIndex0 = 11 + PID_COUNT;
		StringBuilder buffer0 = new StringBuilder();
		buffer0.append(omcid).append(SQLLDR_DE).append(strCollecttime).append(SQLLDR_DE).append(strStamptime);
		buffer0.append(SQLLDR_DE).append("0").append(SQLLDR_DE).append(id).append(SQLLDR_DE);
		buffer0.append(tableName).append(SQLLDR_DE).append(rncid1).append(SQLLDR_DE);
		buffer0.append(nodebid1).append(SQLLDR_DE).append(id).append(SQLLDR_DE);
		buffer0.append(ci1).append(SQLLDR_DE).append(lac1).append(SQLLDR_DE);

		String[] strs = fillIdToPids(pids, id);
		for (String s : strs) {
			buffer0.append(s).append(SQLLDR_DE);
		}
		for (int i = startIndex0; i < totalFieldsCount0; i++) {
			String name = indexCol0.get(i);
			String value = vals.get(name);
			value = (value == null ? "" : value);
			value.replace(SQLLDR_DE, " ");
			buffer0.append(value);
			if (i < totalFieldsCount0 - 1)
				buffer0.append(SQLLDR_DE);
			filedMap.remove(name);
		}
		sq0.txtWriter.println(buffer0.toString());
		sq0.flush();
		buffer0.setLength(0);
	}

	private void setTempCellMap(String tableName) {
		if (cellSelectionProfileId == null)
			return;

		if (tempCellMap == null) {
			tempCellMap = new HashMap<String, AlColumnObj>();
		}
		StringBuilder key0 = new StringBuilder(cellSelectionProfileId.substring(cellSelectionProfileId.indexOf("/") + 1,
				cellSelectionProfileId.indexOf(" ")));
		key0.append("_").append(cellSelectionProfileId.substring(cellSelectionProfileId.lastIndexOf("/") + 1));
		AlColumnObj cell = new AlColumnObj();
		cell.setOmcId(omcid);
		cell.setStrCollecttime(strCollecttime);
		cell.setStrStamptime(strStamptime);
		cell.setPid(id);
		cell.setPtablename(tableName);
		cell.setRncid(rncid1);
		cell.setNodebid(nodebid1);
		cell.setFddcellid(id);
		cell.setCi(ci1);
		cell.setLac(lac1);
		cell.setPids(pids);
		tempCellMap.put(key0.toString() + flagBit + id, cell);

		cellSelectionProfileId = null;
	}

	private void setTempObjMap(String key, Map<Integer, String> indexCol, int totalFieldsCount, int startIndex, SortedMap<String, String> vals) {
		Map<String, String> tempMap = null;
		for (int i = startIndex; i < totalFieldsCount; i++) {
			String name = indexCol.get(i);
			String value = vals.get(name);
			value = (value == null ? "" : value);
			value.replace(SQLLDR_DE, " ");
			if (tempMap == null) {
				tempMap = new HashMap<String, String>();
			}
			tempMap.put(name, value);
		}
		if (tempObjMap == null) {
			tempObjMap = new HashMap<String, Map<String, String>>();
		}
		tempObjMap.put(key, tempMap);
	}

	void initSqlldrs() {
		File baseDir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "w_al_cm" + File.separator
				+ taskId + "_" + Util.getDateString_yyyyMMdd(super.getCollectObjInfo().getLastCollectTime()) + File.separator);
		if (!baseDir.exists() || !baseDir.isDirectory())
			baseDir.mkdirs();

		String basename = "_" + System.currentTimeMillis();

		Iterator<Entry<String, Map<String, String>>> mapTableEntrySet = MAP_TABLE.entrySet().iterator();
		while (mapTableEntrySet.hasNext()) {
			Entry<String, Map<String, String>> next = mapTableEntrySet.next();
			String tableName = next.getKey();
			List<String> cols = new ArrayList<String>(next.getValue().values());
			File txt = new File(baseDir, tableName + basename + ".txt");
			File ctl = new File(baseDir, tableName + basename + ".ctl");
			File bad = new File(baseDir, tableName + basename + ".bad");
			File log = new File(baseDir, tableName + basename + ".log");
			ALParamSqlldrInfo sq = new ALParamSqlldrInfo(txt, ctl, bad, log);
			sqlldrs.put(tableName, sq);

			int index = 0;

			sq.ctlWriter.println("load data");
			sq.ctlWriter.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
			sq.ctlWriter.println("INFILE '" + txt.getAbsolutePath() + "' APPEND INTO TABLE " + tableName);
			sq.ctlWriter.println("FIELDS TERMINATED BY \"" + SQLLDR_DE + "\"");
			sq.ctlWriter.println("TRAILING NULLCOLS");
			sq.ctlWriter.println("(");
			sq.ctlWriter.println("OMCID,");
			sq.ctlWriter.println("COLLECTTIME DATE 'YYYY-MM-DD HH24:MI:SS',");
			sq.ctlWriter.println("STAMPTIME DATE 'YYYY-MM-DD HH24:MI:SS',");
			sq.ctlWriter.println("ID,");
			sq.ctlWriter.println("PID,");
			sq.ctlWriter.println("PTABLENAME,");
			sq.ctlWriter.println("RNCID1,");
			sq.ctlWriter.println("NODEBID1,");
			sq.ctlWriter.println("FDDCELLID1,");
			sq.ctlWriter.println("CI1,");
			sq.ctlWriter.println("LAC1,");
			for (int i = 0; i < PID_COUNT; i++) {
				sq.ctlWriter.println("PID_" + i + ",");
			}

			sq.txtWriter.print("OMCID" + SQLLDR_DE + "COLLECTTIME" + SQLLDR_DE + "STAMPTIME" + SQLLDR_DE + "ID" + SQLLDR_DE + "PID" + SQLLDR_DE
					+ "PTABLENAME" + SQLLDR_DE + "RNCID1" + SQLLDR_DE + "NODEBID1" + SQLLDR_DE + "FDDCELLID1" + SQLLDR_DE + "CI1" + SQLLDR_DE
					+ "LAC1" + SQLLDR_DE);
			for (int i = 0; i < PID_COUNT; i++) {
				sq.txtWriter.print("PID_" + i + SQLLDR_DE);
			}
			sq.colIndex.put("OMCID", index++);
			sq.colIndex.put("COLLECTTIME", index++);
			sq.colIndex.put("STAMPTIME", index++);
			sq.colIndex.put("ID", index++);
			sq.colIndex.put("PID", index++);
			sq.colIndex.put("PTABLENAME", index++);
			sq.colIndex.put("RNCID1", index++);
			sq.colIndex.put("NODEBID1", index++);
			sq.colIndex.put("FDDCELLID1", index++);
			sq.colIndex.put("CI1", index++);
			sq.colIndex.put("LAC1", index++);
			for (int i = 0; i < PID_COUNT; i++) {
				sq.colIndex.put("PID_" + i, index++);
			}

			for (int i = 0; i < cols.size(); i++) {
				String col = cols.get(i);
				sq.colIndex.put(col, index++);
				if (i < cols.size() - 1) {
					sq.txtWriter.print(col + SQLLDR_DE);
					sq.ctlWriter.println(col + ",");
				} else {
					sq.txtWriter.print(col);
					sq.ctlWriter.println(col);
				}
			}
			sq.txtWriter.println();
			sq.ctlWriter.println(")");
			sq.flush();
		}
	}

	@SuppressWarnings("unchecked")
	SortedMap<String, String> getAttVals(Element attsEl, Map<String, String> fieldsMap) {
		SortedMap<String, String> sm = new TreeMap<String, String>();

		List<Element> els = attsEl.elements();
		for (Element e : els) {
			String name = e.getName().toUpperCase();
			String val = "";
			List<Element> subs = e.elements();
			if (subs.isEmpty()) {
				val = e.getText();
			} else if (e.element("unset") != null) {
				val = "unset";
			} else if (!subs.isEmpty()) {
				val = subs.get(0).getText();
			}

			if (name.equals("ASSOCIATEDBTSCELL")) {
				val = parseBtsCell(val);
			}

			String colName = fieldsMap.get(name);
			if (colName != null && !sm.containsKey(colName)) {
				sm.put(colName, val);
			}
			if (name.equalsIgnoreCase("ci")) {
				ci1 = val;
			} else if (name.equalsIgnoreCase("cellid")) {
				ci1 = val;
			} else if (name.equalsIgnoreCase("localCellId")) {
				if (Util.isNotNull(rncidForCI)) {
					int rncLen = rncidForCI.length();
					String left = null;
					if (val != null && val.length() >= rncLen)
						left = val.substring(0, rncLen);
					if (left != null && left.equals(rncidForCI)) {
						ci1 = val.substring(rncLen);
					} else {
						ci1 = makeCI(val);
					}
				} else {
					ci1 = makeCI(val);
				}
			} else if (name.equalsIgnoreCase("locationAreaCode")) {
				lac1 = val;
			} else if (name.equalsIgnoreCase("rncid")) {
				rncidForCI = val;
			} else if (name.equalsIgnoreCase("cellSelectionProfileId")) {
				cellSelectionProfileId = val;
			}
		}

		return sm;
	}

	private static String makeCI(String content) {
		if (content.length() <= 5)
			return content;

		String result = "";
		int i = 0;
		while (result.length() != 5) {
			result = content.substring(i++, content.length());
		}

		return result;
	}

	void startSqlldr() {
		Iterator<Entry<String, ALParamSqlldrInfo>> it = sqlldrs.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, ALParamSqlldrInfo> en = it.next();
			ALParamSqlldrInfo sq = en.getValue();
			sq.close();                   
			String cmd = String.format(SqlldrRunner.RUN_CMD, 
					SystemConfig.getInstance().getDbUserName(), SystemConfig.getInstance().getDbPassword(), 
					SystemConfig.getInstance().getDbService(),1,
					sq.ctl.getAbsoluteFile(), sq.bad.getAbsoluteFile(), sq.log.getAbsoluteFile());
			int ret = -1;
			try {
				log.debug(loggerKey + "执行sqlldr - " + cmd);
				ret = new ExternalCmd().execute(cmd);
			} catch (Exception e) {
				logger.error(loggerKey + "执行sqlldr时异常。", e);
			}
			SqlldrResult sr = null;
			try {
				sr = new SqlLdrLogAnalyzer().analysis(sq.log.getAbsolutePath());
			} catch (LogAnalyzerException e) {
				logger.error(loggerKey + "分析sqlldr日志时异常。", e);
			}
			dblogger.log(omcid, en.getKey(), super.getCollectObjInfo().getLastCollectTime(), sr.getLoadSuccCount(), taskId);
			logger.debug(loggerKey + "ret=" + ret + "，表名=" + sr.getTableName() + "，入库成功条数=" + sr.getLoadSuccCount() + "，sqlldr日志="
					+ sq.log.getAbsolutePath());
			if (ret == 0) {
				sq.delete(true, true, true, true);
			} else if (ret == 2) {
				sq.delete(true, false, false, true);
			}
		}
	}

	static String findRncID(List<Element> rncElements) {
		for (Element e : rncElements) {
			Element att = e.element("attributes");
			if (att != null) {
				String rnc = att.elementText("rncId");
				return rnc != null ? rnc : "";
			}
		}
		return "";
	}

	static String getTableName(String elementName) {
		if (SPEC_MAP.containsKey(elementName)) {
			return TABLE_NAME_PREFIX + SPEC_MAP.get(elementName);
		} else {
			String tableName = TABLE_NAME_PREFIX + elementName.toUpperCase();
			if (MAP_TABLE.containsKey(tableName)) {
				return tableName;
			}
			return null;
		}
	}

	static void initPids(String[] pids) {
		for (int i = 0; i < pids.length; i++) {
			pids[i] = "";
		}
	}

	static void reversePids(String[] pids) {
		String[] tmp = new String[PID_COUNT];
		System.arraycopy(pids, 0, tmp, 0, PID_COUNT);
		initPids(pids);
		int index = 0;
		for (int i = PID_COUNT - 1; i >= 0; i--) {
			if (Util.isNotNull(tmp[i])) {
				pids[index++] = tmp[i];
			}
		}
	}

	static Element findAttsEl(List<Element> subEls) {
		if (subEls == null || subEls.isEmpty())
			return null;

		Element attsEl = null;
		int index = -1;
		for (int i = 0; i < subEls.size(); i++) {
			if (subEls.get(i).getName().equals("attributes")) {
				index = i;
				attsEl = subEls.get(i);
				break;
			}
		}

		if (index > -1)
			subEls.remove(index);

		return attsEl;
	}

	static String parseBtsCell(String value) {
		// "BTSEquipment/JDZ_LengDongChang BTSCell/2"
		try {
			String s = value;
			int index = s.indexOf("BTSCell");
			s = s.substring(index + 8, s.length());
			return s;
		} catch (Exception e) {
			return value;
		}
	}

	static String[] fillIdToPids(String[] pids, String pid) {
		String[] strs = new String[pids.length];
		System.arraycopy(pids, 0, strs, 0, pids.length);
		for (int n = 0; n < strs.length; n++) {
			if ("".equals(strs[n])) {
				strs[n] = pid;
				break;
			}
		}
		return strs;
	}

	public static void main(String[] args) throws Exception {
		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(100);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2015-7-13 00:00:00").getTime()));

		WcdmaALParamParser w = new WcdmaALParamParser();
		w.collectObjInfo = obj;
		w.fileName = "E:\\company\\问题及其资源\\wcdma贝尔参数升级\\UTRAN-SNAP20150713000023.xml";
		w.parseData();
		// List<File> files = FileSplit.splitFull(new File("F:\\yy\\原始数据\\阿朗\\UTRAN-SNAP20130328000005.xml_split\\UTRAN-SNAP20130328000005.xml"));
	}

}

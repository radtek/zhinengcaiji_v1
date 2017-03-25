package parser.eric.cm;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import sqlldr.SqlldrImpl;
import sqlldr.SqlldrInterface;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;

/**
 * <p>
 * 爱立信参数解析实现(dom4j方式)。
 * </p>
 * <p>
 * 解析思路：每个vsDataType标签内容，都是一种对象类型，以此类型区分表。每种类型的VsDataContainer标签下的属性不同，也就相当于不同的列 。
 * </p>
 * 
 * @author chensj 2010-3-5
 * 
 *         modified by yuy 2013-6-21 13:50 2 points: first, code reconstructed; second, replacing the way of data warehousing from jdbc(batch
 *         processing) to sqlldr
 * 
 */
public class EricssonCmParserImp implements EricssonCmParser {

	// 解析锁，避免多个线程在同时解析参数文件.
	private static final Object PARSE_LOCK = new Object();

	private long taskId;

	private String omcId;

	private String stampTime;

	private String thisTime;

	private String rncname = "";

	private String rncid = "";

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	private final Map<String, String> BUFFER = new HashMap<String, String>();

	private static final String DATA_TYPE = "vsDataType";

	private static final String USER_LABEL = "userLabel";

	private static final String DATA_FORMAT_VERSION = "vsDataFormatVersion";

	private static final String GSM_RELATION = "GsmRelation";

	private static final String UTRAN_RELATION = "UtranRelation";

	private static final String UTRAN_CELL = "UtranCell";

	private static final String IUB_LINK = "IubLink";

	private static final String DATA_CHANNEL_SWITCHING = "vsDataChannelSwitching";

	private static final String MECONTEXT = "MeContext";

	private static final String COL_MAPPING_TABLE = "CLT_CM_W_ERIC_COL_MAPPING";

	private static final String SEQ_NAME = "SEQ_ERIC_CM_COL_MAPPING";

	private static final String HW_FLAG = "HWR";

	private static final String FEMTOFLAG = "Femto";

	private static final String regex = "MeContext=(\\w*),?";

	private static final String nodebregex = "NodeBFunction=(\\w*),?";

	private static final String SPLIT_FLAG_1 = "-";

	private static final String SPLIT_FLAG_2 = "_";

	private static final String SUBNETWORK = "SubNetwork";

	private static final String MANAGEDELEMENT = "ManagedElement";

	private static final String RNCFUNCTION = "RncFunction";

	private static final String IUBLINK = "IubLink";

	/**
	 * 之前版本是IubLink，升13版本后变为vsDataIubLink
	 */
	private static final String IUBLINK_updated = "vsDataIubLink";

	private static final String EXTERNALGSMCELL = "ExternalGsmCell";

	private static final String EXTERNALUTRANCELL = "ExternalUtranCell";

	private Element rncFunctionElement = null;

	private List<Element> nodebElements = null;

	private List<Element> manageElements = null;

	private String subRootId = null;

	private String subid = null;

	private String rncFunction = null;

	private SqlldrInterface sqlldr = null;

	private Map<String, List<String>> tableCols;

	private static List<String> tableList = new ArrayList<String>();

	/**
	 * 特殊表处理 在sqlldr初始化时加上字段类型和长度
	 */
	private static Map<String/* tableName */, Map<String/* columnName */, String/* type */>> specialTableMap = null;

	static {
		specialTableMap = new HashMap<String, Map<String, String>>();

		Map<String, String> specialColumnMap = new HashMap<String, String>();
		specialColumnMap.put("DBCCDEVICEREF", "char(300)");

		specialTableMap.put("CLT_CM_W_ERIC_CARRIER", specialColumnMap);
		specialTableMap.put("CLT_CM_W_ERIC_MANAGED", specialColumnMap);
		specialTableMap.put("CLT_CM_W_ERIC_NODEB", specialColumnMap);
	}

	static {
		tableList.add("CLT_CM_W_ERIC_MECONTEXT");
		tableList.add("CLT_CM_W_ERIC_CARRIER");
		tableList.add("CLT_CM_W_ERIC_SECTOR");
		tableList.add("CLT_CM_W_ERIC_RBS");
		tableList.add("CLT_CM_W_ERIC_MANAGED");
		tableList.add("CLT_CM_W_ERIC_RNC");
		tableList.add("CLT_CM_W_ERIC_CHANNEL");
		tableList.add("CLT_CM_W_ERIC_CELL");
		tableList.add("CLT_CM_W_ERIC_IUB");
		tableList.add("CLT_CM_W_ERIC_GSM_RELATION");
		tableList.add("CLT_CM_W_ERIC_UTRAN_RELATION");
		tableList.add("CLT_CM_W_ERIC_NODEB");
		tableList.add("CLT_CM_W_ERIC_MANAGEDELEMENT");
	}

	@SuppressWarnings("unchecked")
	@Override
	public void parse(String file, int omcId, Timestamp stampTime, long taskId) throws Exception {
		synchronized (PARSE_LOCK) {
			this.taskId = taskId;
			this.omcId = String.valueOf(omcId);
			this.stampTime = Util.getDateString(stampTime);
			this.thisTime = Util.getDateString(new Date());

			Document doc = null;
			try {
				File f = new File(file);
				// 大于200mb，作为大文件处理，进入单独的分支
				if (f.length() > (200 * 1024 * 1024) && !f.getName().startsWith(CMFileSpliter.eriCmSplitFileNamePrefix)) {
					largeFileHandle(file, omcId, stampTime);
					return;
				}
				if (sqlldr == null) {
					sqlldr = new SqlldrImpl(taskId, omcId, stampTime, "eric_w_cm", specialTableMap);
					tableCols = sqlldr.loadDataStructrue(tableList);
					sqlldr.initSqlldr();
				}

				doc = new SAXReader().read(f);
				nodebElements = new ArrayList<Element>();
				manageElements = new ArrayList<Element>();
				Element subnetworkRoot = doc.getRootElement().element("configData").element("SubNetwork");
				subRootId = subnetworkRoot.attributeValue("id");
				Element sub = subnetworkRoot.element("SubNetwork");
				subid = sub.attributeValue("id");

				// 设置rncname
				setRncName(sub, f);

				// MeContext
				List<Element> mecontexts = meContextHandler(tableList.get(0), sub);
				
				managedElementHandler(tableList.get(12), mecontexts);

				// RncFunction 抓取
				if (mecontexts != null)
					rncFunctionExtractor(sub, mecontexts);

				// dataCarrier
				String label = "es:vsDataCarrier";
				String ref = "trDeviceRef";
				List<Element> carrElements = doc.selectNodes("//" + label);
				commonHandler(tableList.get(1), carrElements, label, ref);

				carrElements.clear();
				carrElements = null;

				// dataSector
				label = "es:vsDataSector";
				ref = "sectorAntennasRef";
				List<Element> sectorElements = doc.selectNodes("//" + label);
				commonHandler(tableList.get(2), sectorElements, label, ref);

				sectorElements.clear();
				sectorElements = null;

				// rbs
				label = "es:vsDataRbsLocalCell";
				ref = "carrierRef";
				String otherRef = "carriersRef";
				List<Element> rbsElements = doc.selectNodes("//" + label);
				commonHandler(tableList.get(3), rbsElements, label, ref, otherRef);

				rbsElements.clear();
				rbsElements = null;

				// management
				managedHanlder(tableList.get(4));

				if (rncFunctionElement != null) {
					rncFunction = rncFunctionElement.attributeValue("id");
					Element managedment = rncFunctionElement.getParent();
					String managedId = managedment.attributeValue("id");
					Element mecontextElement = managedment.getParent();
					String mecontext = mecontextElement.attributeValue("id");

					// rnc
					rncHandler(tableList.get(5), managedId, mecontext);

					// data channel
					channelHandler(tableList.get(6), managedId, mecontext);

					// cell
					List<Element> cellElements = cellHandler(tableList.get(7));

					// iub
					iubHandler(tableList.get(8));

					if (cellElements != null) {
						// gsm relation
						relationHandler(tableList.get(9), cellElements);
						// utran relation
						utranHandler(tableList.get(10), cellElements);
					}
				}
				// NodeB
				if (nodebElements != null)
					nodebHandler(tableList.get(11));
			} catch (Exception e) {
				logger.error("解析爱立信参数时异常，文件:" + file);
				throw e;
			}

			// 入库
			logger.debug(this.taskId + " - 开始入库 " + this.stampTime);
			sqlldr.runSqlldr();
		}
	}
	
	@SuppressWarnings("unchecked")
	private void managedElementHandler(String tableName, List<Element> mecontexts) throws Exception{
		List<Pair> pairs = new ArrayList<Pair>();
		for(Element e : mecontexts){
			if(e.element("ManagedElement") != null){
				List<Element> elements = e.element("ManagedElement").element("attributes").elements();
				addPublic(pairs);
				pairs.add(new Pair("SUBNETWORK_ROOT", subRootId));
				pairs.add(new Pair("SUBNETWORK", subid));
				pairs.add(new Pair("MECONTEXT", e.attributeValue("id")));
				pairs.add(new Pair("MANAGEDELEMENT", e.element("ManagedElement").attributeValue("id")));
				for (Element e1 : elements) {
					Pair pair = new Pair(e1.getName(), e1.getTextTrim());
					pairs.add(pair);
				}
				sqlldr.writeRows(buildRowData(pairs, tableName), tableName);
				pairs.clear();
			}
		}
		logger.debug(this.taskId + " - 数据解析完，准备入库 - " + tableName);
	}

	private void rncFunctionExtractor(Element sub, List<Element> mecontexts) throws Exception {
		rncFunctionElement = sub.element("MeContext").element("ManagedElement").element("RncFunction");
		if (rncFunctionElement == null) {
			for (Element e : mecontexts) {
				if (e.attributeValue("id").equalsIgnoreCase(rncname)) {
					if (e.element("ManagedElement").element("RncFunction") != null) {
						rncFunctionElement = e.element("ManagedElement").element("RncFunction");
						break;
					} else if (e.element("ManagedElement").element("NodeBFunction") != null) {
						rncFunctionElement = e.element("ManagedElement").element("NodeBFunction");
						break;
					}
				}
			}
		}
	}

	private void nodebHandler(String tableName) throws Exception {
		List<Pair> nodeBPairs = new ArrayList<Pair>();
		for (Element e : nodebElements) {
			addPublic(nodeBPairs);
			String mx = e.getParent().getParent().attributeValue("id");
			String nodebid = e.attributeValue("id");
			handleNodebElement(e, nodeBPairs, stringNullHandler(mx), nodebid);
			sqlldr.writeRows(buildRowData(nodeBPairs, tableName), tableName);
			nodeBPairs.clear();
		}
		logger.debug(this.taskId + " - 数据解析完，准备入库 - " + tableName);
	}

	@SuppressWarnings("unchecked")
	private void utranHandler(String tableName, List<Element> cellElements) throws Exception {
		List<Pair> utranPairs = new ArrayList<Pair>();
		for (Element e : cellElements) {
			List<Element> utranElements = e.elements(UTRAN_RELATION);
			for (Element ee : utranElements) {
				addPublic(utranPairs);
				handleUtranElement(ee, utranPairs);
				sqlldr.writeRows(buildRowData(utranPairs, tableName), tableName);
				utranPairs.clear();
			}
		}
		logger.debug(this.taskId + " - 数据解析完，准备入库 - " + tableName);
	}

	@SuppressWarnings("unchecked")
	private void relationHandler(String tableName, List<Element> cellElements) throws Exception {
		List<Pair> gsmPairs = new ArrayList<Pair>();
		for (Element e : cellElements) {
			List<Element> gsmElements = e.elements(GSM_RELATION);
			for (Element ee : gsmElements) {
				addPublic(gsmPairs);
				handleGsmElement(ee, gsmPairs);
				sqlldr.writeRows(buildRowData(gsmPairs, tableName), tableName);
				gsmPairs.clear();
			}
		}
		logger.debug(this.taskId + " - 数据解析完，准备入库 - " + tableName);
	}

	@SuppressWarnings("unchecked")
	private void iubHandler(String tableName) throws Exception {
		List<Element> iubElements = rncFunctionElement.elements(IUB_LINK);
		if (iubElements == null)
			return;
		List<Pair> iubPairs = new ArrayList<Pair>();
		for (Element e : iubElements) {
			addPublic(iubPairs);
			handleIubElement(e, iubPairs);
			iubId = "";
			sqlldr.writeRows(buildRowData(iubPairs, tableName), tableName);
			iubPairs.clear();
		}
		logger.debug(this.taskId + " - 数据解析完，准备入库 - " + tableName);
	}

	@SuppressWarnings("unchecked")
	private List<Element> cellHandler(String tableName) throws Exception {
		List<Element> cellElements = rncFunctionElement.elements(UTRAN_CELL);
		if (cellElements == null)
			return null;
		List<Pair> cellPairs = new ArrayList<Pair>();
		for (Element e : cellElements) {
			addPublic(cellPairs);
			handleCellElement(e, cellPairs);
			cellname = "";
			sqlldr.writeRows(buildRowData(cellPairs, tableName), tableName);
			cellPairs.clear();
		}
		logger.debug(this.taskId + " - 数据解析完，准备入库 - " + tableName);
		return cellElements;
	}

	private void channelHandler(String tableName, String managedId, String mecontext) throws Exception {
		Element dataChannelElement = findChannel(rncFunctionElement);
		if (dataChannelElement == null)
			return;
		List<Pair> channelPairs = new ArrayList<Pair>();
		addPublic(channelPairs);
		handleDataChannelElement(dataChannelElement, channelPairs, managedId, mecontext);
		sqlldr.writeRows(buildRowData(channelPairs, tableName), tableName);
		channelPairs.clear();
		logger.debug(this.taskId + " - 数据解析完，准备入库 - " + tableName);
	}

	private void rncHandler(String tableName, String managedId, String mecontext) throws Exception {
		List<Pair> rncPairs = new ArrayList<Pair>();
		addPublic(rncPairs);
		handleRncElement(rncFunctionElement, rncPairs, managedId, mecontext);
		sqlldr.writeRows(buildRowData(rncPairs, tableName), tableName);
		rncPairs.clear();
		logger.debug(this.taskId + " - 数据解析完，准备入库 - " + tableName);
	}

	private void managedHanlder(String tableName) throws Exception {
		List<Pair> managedPairs = new ArrayList<Pair>();
		for (Element e : manageElements) {
			addPublic(managedPairs);
			String mx = e.getParent().attributeValue("id");
			String managedmentId = e.attributeValue("id");
			handleManagedElement(e, managedPairs, stringNullHandler(mx), managedmentId);
			sqlldr.writeRows(buildRowData(managedPairs, tableName), tableName);
			managedPairs.clear();
		}
		logger.debug(this.taskId + " - 数据解析完，准备入库 - " + tableName);
	}
	
	Pair p11 = new Pair("vsDataRbsSlot", "");
	
	List<Pair> pairs11 = new ArrayList<Pair>();
	

	@SuppressWarnings("unchecked")
	private void commonHandler(String tableName, List<Element> elements, String label, String ref) throws Exception {
		if (elements == null || elements.isEmpty()) {
			logger.warn(this.taskId + " 未能找到" + label + "节点");
			return;
		}
		List<Pair> pairs = new ArrayList<Pair>();
		for (Element e0 : elements) {
			addPublic(pairs);
			List<Element> rbsSub = e0.elements();
			for (Element e1 : rbsSub) {
				String name = e1.getName();
				String val = e1.getTextTrim();
				if (Util.isNotNull(name)) {
					String sn = getShortName(name);
					if (Util.isNotNull(sn)) {
						Pair pp = new Pair(sn, stringNullHandler(val));
						if (!pairs.contains(pp))
							pairs.add(pp);

						if (name.equalsIgnoreCase(ref) && Util.isNotNull(val)) {
							List<Pair> refs = splitRef(val);
							for (Pair p : refs) {
								if (!pairs.contains(p))
									pairs.add(p);
							}
							// 把vsDataRbsSlot值赋给vsDataSectorAntenna，以保证数据唯一性----此处变态
							if (ref.equalsIgnoreCase("trDeviceRef")) {
								Pair p1 = new Pair("vsDataSectorAntenna", "");
								if (!pairs.contains(p1)) {
									//pairs11 = pairs;
									Pair p2 = new Pair("vsDataRbsSlot", "");
									//p11=p2;
									if(pairs.indexOf(p2)!=-1)
									  p1.value = pairs.get(pairs.indexOf(p2)).getValue();
									pairs.add(p1);
								}
							}
						}
					}
				}
			}
			pairs.add(new Pair("RNC_NAME", this.rncname));
			sqlldr.writeRows(buildRowData(pairs, tableName), tableName);
			pairs.clear();
		}
		logger.debug(this.taskId + " - 数据解析完，准备入库 - " + tableName);
	}

	@SuppressWarnings("unchecked")
	private void commonHandler(String tableName, List<Element> elements, String label, String ref, String otherRef) throws Exception {
		if (elements == null || elements.isEmpty()) {
			logger.warn(this.taskId + " 未能找到" + label + "节点");
			return;
		}
		List<Pair> pairs = new ArrayList<Pair>();
		for (Element e0 : elements) {
			addPublic(pairs);
			List<Element> rbsSub = e0.elements();
			for (Element e1 : rbsSub) {
				String name = e1.getName();
				String val = e1.getTextTrim();
				if (Util.isNotNull(name)) {
					String sn = getShortName(name);
					if (Util.isNotNull(sn)) {
						Pair pp = new Pair(sn, stringNullHandler(val));
						if (!pairs.contains(pp))
							pairs.add(pp);

						if ((name.equalsIgnoreCase(ref) || name.equalsIgnoreCase(otherRef)) && Util.isNotNull(val)) {
							List<Pair> refs = splitRef(val);
							for (Pair p : refs) {
								if (!pairs.contains(p))
									pairs.add(p);
							}
						}
					}
				}
			}
			pairs.add(new Pair("RNC_NAME", this.rncname));
			sqlldr.writeRows(buildRowData(pairs, tableName), tableName);
			pairs.clear();
		}
		logger.debug(this.taskId + " - 数据解析完，准备入库 - " + tableName);
	}

	@SuppressWarnings("unchecked")
	private List<Element> meContextHandler(String tableName, Element sub) throws Exception {
		List<Element> mecontexts = sub.elements("MeContext");
		if (mecontexts == null)
			return null;

		List<Pair> mecontextPairs = new ArrayList<Pair>();
		for (Element e : mecontexts) {
			addPublic(mecontextPairs);
			if (!e.attributeValue("id").equalsIgnoreCase(rncname)) {
				Element manage = e.element("ManagedElement");
				if (manage == null)
					continue;
				manageElements.add(manage);
				Element nodeb = manage.element("NodeBFunction");
				if (nodeb != null) {
					Element attE = nodeb.element("attributes");
					if (attE != null && Util.isNotNull(attE.elementText("nodeBFunctionIubLink")))
						nodebElements.add(nodeb);
				}
			}
			String mx = e.attributeValue("id");
			handleMeContextElement(e, mecontextPairs, stringNullHandler(mx));
			sqlldr.writeRows(buildRowData(mecontextPairs, tableName), tableName);
			mecontextPairs.clear();
		}
		logger.debug(this.taskId + " - 数据解析完，准备入库 - " + tableName);
		return mecontexts;
	}

	private void setRncName(Element sub, File f) {
		if (sub.attributeValue("id") != null)
			this.rncname = sub.attributeValue("id");
		else
			this.rncname = f.getName().substring(0, f.getName().indexOf("_"));
	}

	private String stringNullHandler(String str) {
		return str == null ? "" : str;
	}

	private List<String> buildRowData(List<Pair> record, String tableName) throws SQLException {
		Map<String, String> map = rebuildRecord(record);
		List<String> valList = new ArrayList<String>();
		List<String> colList = this.tableCols.get(tableName);
		for (int i = 0; i < colList.size(); i++) {
			valList.add(stringNullHandler(map.get(colList.get(i).toUpperCase())));
		}
		return valList;
	}

	private Map<String, String> rebuildRecord(List<Pair> record) {
		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < record.size(); i++) {
			map.put(record.get(i).getKey().toUpperCase(), record.get(i).getValue());
		}
		record.clear();
		record = null;
		return map;
	}

	private void addCol(String shortName, String colName) throws SQLException {
		String sql = "insert into %s (COL_ID,SHORT_NAME,COL_NAME)values (%s.nextval,'%s'||%s.currval,'%s')";
		sql = String.format(sql, COL_MAPPING_TABLE, SEQ_NAME, shortName, SEQ_NAME, colName);

		if (colName.length() <= 30) {
			sql = "insert into %s (COL_ID,SHORT_NAME,COL_NAME) values (%s.nextval,'%s','%s')";
			sql = String.format(sql, COL_MAPPING_TABLE, SEQ_NAME, shortName, colName);
		}

		try {
			CommonDB.executeUpdate(sql);
			BUFFER.put(colName, getShortName(colName));
		} catch (SQLException e) {
			logger.error("执行SQL异常:" + sql);
			throw e;
		}
	}

	private String getShortName(String colName) throws SQLException {
		String shortName = BUFFER.get(colName);

		if (shortName != null) {
			return shortName;
		}

		String sql = "select t.short_name from %s t where t.col_name=?";
		sql = String.format(sql, COL_MAPPING_TABLE);

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;

		try {
			con = DbPool.getConn();
			st = con.prepareStatement(sql);
			st.setString(1, colName);
			rs = st.executeQuery();

			if (rs.next()) {
				shortName = rs.getString(1);
			}
		} catch (SQLException e) {
			logger.error("查询时异常:" + sql);
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
			if (con != null) {
				con.close();
			}
		}

		if (shortName == null) {
			shortName = handleColName(colName);
			addCol(shortName, colName);
			return getShortName(colName);
		} else {
			BUFFER.put(colName, shortName);
		}
		return shortName;
	}

	private void addPublic(List<Pair> pairs) {
		pairs.add(new Pair("OMCID", omcId));
		pairs.add(new Pair("COLLECTTIME", thisTime));
		pairs.add(new Pair("STAMPTIME", stampTime));
	}

	private String handleColName(String colName) {
		if (colName.length() > 30) {
			return colName.substring(0, 20).toUpperCase() + "_";
		}
		return colName.toUpperCase();
	}

	String cellname = "";

	@SuppressWarnings("unchecked")
	private void handleCellElement(Element targetElement, List<Pair> pairs) {

		List<Element> elements = targetElement.elements();
		List<Element> accessClassNBarredEls = targetElement.elements("accessClassNBarred");
		String accessClassNBarred = null;
		if (accessClassNBarredEls != null && !accessClassNBarredEls.isEmpty()) {
			accessClassNBarred = "";
			for (Element e0 : accessClassNBarredEls) {
				accessClassNBarred += e0.getText();
			}
		}
		if (targetElement.attributeValue("id") != null && Util.isNull(cellname)) {
			cellname = targetElement.attributeValue("id");
		}
		for (Element e : elements) {
			if (e.elements().size() > 0 && !e.getName().equalsIgnoreCase(GSM_RELATION) && !e.getName().equalsIgnoreCase(UTRAN_RELATION)) {
				handleCellElement(e, pairs);
			} else {
				if (e.getName().equals("utranCellIubLink")) {
					String iublink = e.getTextTrim();
					String[] values = iublink.split(",");
					if (values.length > 0) {
						String subroot = values[0];

						String[] val = subroot.split("=");
						if (val.length > 0) {
							Pair subRootPair = new Pair("SUBNETWORKROOT", (val.length > 1 ? val[1] : ""));
							if (!pairs.contains(subRootPair)) {
								pairs.add(subRootPair);
							}
						}
					}

					for (int i = 1; i < values.length; i++) {
						String[] map = values[i].split("=");
						if (map[0].equals(SUBNETWORK)) {
							String subid = map[1];
							Pair subPair = new Pair("SUBNETWORK", subid);
							if (!pairs.contains(subPair)) {
								pairs.add(subPair);
							}
						} else if (map[0].equals(MECONTEXT)) {
							String subid = map[1];
							Pair mecontextpair = new Pair("MECONTEXT", subid);
							if (!pairs.contains(mecontextpair)) {
								pairs.add(mecontextpair);
							}
						} else if (map[0].equals(MANAGEDELEMENT)) {
							String subid = map[1];
							Pair managedelementpair = new Pair("MANAGEDELEMENT", subid);
							if (!pairs.contains(managedelementpair)) {
								pairs.add(managedelementpair);
							}
						} else if (map[0].equals(RNCFUNCTION)) {
							String subid = map[1];
							Pair rncfunctionpair = new Pair("RNCFUNCTION", subid);
							if (!pairs.contains(rncfunctionpair)) {
								pairs.add(rncfunctionpair);
							}
						} else if (map[0].equals(IUBLINK)) {
							String subid = map[1];
							Pair iublinkpair = new Pair("IUBLINK", subid);
							if (!pairs.contains(iublinkpair)) {
								pairs.add(iublinkpair);
							}
						} else if (map[0].equals(IUBLINK_updated)) {
							String subid = map[1];
							Pair iublinkpair = new Pair("IUBLINK", subid);
							if (!pairs.contains(iublinkpair)) {
								pairs.add(iublinkpair);
							}
						}
					}

				}
				Pair pair = new Pair(e.getName(), e.getTextTrim());
				if (e.getName().equalsIgnoreCase("accessClassNBarred") && accessClassNBarred != null) {
					pair.value = accessClassNBarred;
				}
				if (!pairs.contains(pair) && !pair.getKey().equalsIgnoreCase(DATA_FORMAT_VERSION) 
						&& !pair.getKey().equalsIgnoreCase(DATA_TYPE)
						&& !pair.getKey().equalsIgnoreCase(GSM_RELATION)
						&& !pair.getKey().equalsIgnoreCase(UTRAN_RELATION)) {
					Pair rncnamepair = new Pair("RNC_NAME", this.rncname);
					Pair cellnamepair = new Pair("CELL_NAME", cellname);
					if (!pairs.contains(rncnamepair)) {
						pairs.add(rncnamepair);
					}
					if (!pairs.contains(cellnamepair)) {
						pairs.add(cellnamepair);
					}
					Pair rncidpair = new Pair("RNC_ID", this.rncid);
					if (!pairs.contains(rncidpair)) {
						pairs.add(rncidpair);
					}
					pairs.add(pair);
				}
			}
		}
	}

	private String iubId = "";

	@SuppressWarnings("unchecked")
	private void handleIubElement(Element targetElement, List<Pair> pairs) {

		List<Element> elements = targetElement.elements();
		if (targetElement.attributeValue("id") != null && Util.isNull(iubId)) {
			iubId = targetElement.attributeValue("id");
		}
		for (Element e : elements) {
			if (e.elements().size() > 0 && !e.getName().equalsIgnoreCase(GSM_RELATION) && !e.getName().equalsIgnoreCase(UTRAN_RELATION)) {
				handleIubElement(e, pairs);
			} else {
				String text = e.getTextTrim();
				if (e.getName().equals("iubLinkUtranCell")) {
					String[] values = text.split(",");
					// String subroot = values[0];
					// Pair subRootPair = new Pair("SUBNETWORKROOT", subroot.split("=")[1]);

					if (values.length > 0) {
						String subroot = values[0];

						String[] val = subroot.split("=");
						if (val.length > 1) {
							Pair subRootPair = new Pair("SUBNETWORKROOT", val[1]);
							if (!pairs.contains(subRootPair)) {
								pairs.add(subRootPair);
							}
						}
					}

					// if ( !pairs.contains(subRootPair) )
					// {
					// pairs.add(subRootPair);
					// }
					for (int i = 1; i < values.length; i++) {
						String[] map = values[i].split("=");
						if (map[0].equals(SUBNETWORK)) {
							String subid = map[1];
							Pair subPair = new Pair("SUBNETWORK", subid);
							if (!pairs.contains(subPair)) {
								pairs.add(subPair);
							}
						} else if (map[0].equals(MECONTEXT)) {
							String subid = map[1];
							Pair mecontextpair = new Pair("MECONTEXT", subid);
							if (!pairs.contains(mecontextpair)) {
								pairs.add(mecontextpair);
							}
						} else if (map[0].equals(MANAGEDELEMENT)) {
							String subid = map[1];
							Pair managedelementpair = new Pair("MANAGEDELEMENT", subid);
							if (!pairs.contains(managedelementpair)) {
								pairs.add(managedelementpair);
							}
						} else if (map[0].equals(RNCFUNCTION)) {
							String subid = map[1];
							Pair rncfunctionpair = new Pair("RNCFUNCTION", subid);
							if (!pairs.contains(rncfunctionpair)) {
								pairs.add(rncfunctionpair);
							}
						} else if (map[0].equals(IUBLINK)) {
							String subid = map[1];
							Pair iublinkpair = new Pair("IUBLINK", subid);
							if (!pairs.contains(iublinkpair)) {
								pairs.add(iublinkpair);
							}
						} else if (map[0].equals(IUBLINK_updated)) {
							String subid = map[1];
							Pair iublinkpair = new Pair("IUBLINK", subid);
							if (!pairs.contains(iublinkpair)) {
								pairs.add(iublinkpair);
							}
						}
					}
				} else if (e.getName().equals("iubLinkNodeBFunction")) {
					String iubnodebmecontext = findByRegex(text, regex, 1);
					iubnodebmecontext = iubnodebmecontext == null ? "" : iubnodebmecontext;
					Pair nodebmecontextpair = new Pair("IUBNODEBMECONTEXT", iubnodebmecontext);
					if (!pairs.contains(nodebmecontextpair)) {
						pairs.add(nodebmecontextpair);
					}

					String nodeb = findByRegex(text, nodebregex, 1);
					nodeb = nodeb == null ? "" : nodeb;
					Pair nodebpair = new Pair("NODEBFUNCTION", nodeb);
					if (!pairs.contains(nodebpair)) {
						pairs.add(nodebpair);
					}
				}
				Pair pair = new Pair(e.getName(), text);
				if (!pairs.contains(pair) && !pair.getKey().equalsIgnoreCase(DATA_FORMAT_VERSION) && !pair.getKey().equalsIgnoreCase(DATA_TYPE)
						&& !pair.getKey().equalsIgnoreCase(USER_LABEL) && !pair.getKey().equalsIgnoreCase(GSM_RELATION)
						&& !pair.getKey().equalsIgnoreCase(UTRAN_RELATION)) {
					Pair rncnamepair = new Pair("RNC_NAME", this.rncname);
					Pair cellnamepair = new Pair("IUB_ID", iubId);
					if (!pairs.contains(rncnamepair)) {
						pairs.add(rncnamepair);
					}
					if (!pairs.contains(cellnamepair)) {
						pairs.add(cellnamepair);
					}
					Pair rncidpair = new Pair("RNC_ID", this.rncid);
					if (!pairs.contains(rncidpair)) {
						pairs.add(rncidpair);
					}
					pairs.add(pair);
				}
			}
		}
	}

	String gsmcellname = "";

	String gsmadjcellname = "";// gsm邻区名

	Pair adjnamepair = null;

	String gsm_cell = "";

	Pair gsm_cell_pair = null;

	@SuppressWarnings("unchecked")
	private void handleGsmElement(Element targetElement, List<Pair> pairs) {
		List<Element> elements = targetElement.elements();
		gsm_cell = targetElement.getParent().attributeValue("id");
		if (targetElement.attributeValue("id") != null && Util.isNull(gsmcellname)) {
			gsmcellname = targetElement.attributeValue("id");

			String sp = SPLIT_FLAG_1;
			if (!gsmcellname.contains(sp)) {
				sp = SPLIT_FLAG_2;
			}

			if (gsmcellname.contains(sp)) {
				String[] adjname = gsmcellname.split(sp);
				if (adjname.length == 2) {
					// gsm_cell = adjname[0];
					gsmadjcellname = adjname[1];
				} else {
					gsmadjcellname = gsmcellname;
				}
				adjnamepair = new Pair("ADJ_CELL_NAME", gsmadjcellname);
				gsm_cell_pair = new Pair("CELL_NAME", gsm_cell);
			} else {
				adjnamepair = new Pair("ADJ_CELL_NAME", "");
				gsm_cell_pair = new Pair("CELL_NAME", gsm_cell);
			}

		}
		for (Element e : elements) {
			if (e.elements().size() > 0 && !e.getName().equalsIgnoreCase(GSM_RELATION) && !e.getName().equalsIgnoreCase(UTRAN_RELATION)) {
				handleGsmElement(e, pairs);
				gsmcellname = "";
			} else {
				String text = e.getTextTrim();
				if (e.getName().equals("adjacentCell")) {
					String[] texts = text.split(",");
					for (String v : texts) {
						String[] map = v.split("=");
						if (map[0].equals(SUBNETWORK)) {
							Pair subpair = new Pair("SUBNETWORKROOT", map[1]);
							if (!pairs.contains(subpair)) {
								pairs.add(subpair);
							}

						} else if (map[0].equals(EXTERNALGSMCELL)) {
							Pair extpair = new Pair("EXTERNALGSMCELL", map[1]);
							if (!pairs.contains(extpair)) {
								pairs.add(extpair);
							}
						}
					}

				}
				Pair pair = new Pair(e.getName(), text);
				if (!pairs.contains(pair) && !pair.getKey().equalsIgnoreCase(DATA_FORMAT_VERSION) && !pair.getKey().equalsIgnoreCase(DATA_TYPE)
						&& !pair.getKey().equalsIgnoreCase(USER_LABEL) && !pair.getKey().equalsIgnoreCase(GSM_RELATION)
						&& !pair.getKey().equalsIgnoreCase(UTRAN_RELATION)) {
					Pair rncnamepair = new Pair("RNC_NAME", this.rncname);
					Pair cellnamepair = new Pair("GSM_RELATION_NAME", gsmcellname);

					if (!pairs.contains(gsm_cell_pair)) {
						pairs.add(gsm_cell_pair);
					}
					if (!pairs.contains(adjnamepair)) {
						pairs.add(adjnamepair);
					}

					if (!pairs.contains(cellnamepair)) {
						pairs.add(cellnamepair);
					}
					if (!pairs.contains(rncnamepair)) {
						pairs.add(rncnamepair);
					}
					Pair rncidpair = new Pair("RNC_ID", this.rncid);
					if (!pairs.contains(rncidpair)) {
						pairs.add(rncidpair);
					}
					pairs.add(pair);
				}
			}
		}
	}

	String utrancellname = "";

	String adjCellName = "";

	Pair adjCellNamePair = null;

	String utrancerncname = "";

	String adjrncname = "";

	@SuppressWarnings("unchecked")
	private void handleUtranElement(Element targetElement, List<Pair> pairs) {

		if (targetElement.attributeValue("id") != null && Util.isNull(utrancellname)) {
			utrancellname = targetElement.attributeValue("id");
			String sp = SPLIT_FLAG_1;
			if (!utrancellname.contains(sp)) {
				sp = SPLIT_FLAG_2;
			}
			if (utrancellname.contains(sp)) {
				String[] adjname = utrancellname.split(sp, 99);

				adjCellName = adjname[1];

				if (adjCellName.contains(FEMTOFLAG)) {
					adjCellName = utrancellname;
				}
			} else {
				adjCellName = utrancellname;
			}
			adjCellNamePair = new Pair("ADJ_CELL_NAME", utrancellname);
		}
		List<Element> elements = targetElement.elements();
		for (Element e : elements) {
			if (e.elements().size() > 0 && !e.getName().equalsIgnoreCase(GSM_RELATION) && !e.getName().equalsIgnoreCase(UTRAN_RELATION)) {
				handleUtranElement(e, pairs);
				utrancellname = "";
			} else {
				String text = e.getTextTrim();
				Pair adjRncNamePair = null;
				Pair pair = new Pair(e.getName(), text);

				String cellname = e.getParent().getParent().getParent().attributeValue("id");
				Pair relationNamePair = new Pair("RELATION_NAME", utrancellname);
				Pair cellnamepair = new Pair("CELL_NAME", cellname);

				if (!pairs.contains(cellnamepair)) {
					pairs.add(cellnamepair);
				}
				if (!pairs.contains(relationNamePair)) {
					pairs.add(relationNamePair);
				}

				if (e.getName().equals("adjacentCell")) {
					adjrncname = findByRegex(text, regex, 1);
					adjrncname = adjrncname == null ? "" : adjrncname;
					String sp = SPLIT_FLAG_1;
					if (!utrancellname.contains(sp)) {
						sp = SPLIT_FLAG_2;
					}
					if (utrancellname.contains(sp)) {
						String[] adjname = utrancellname.split(sp, 99);
						adjCellName = adjname[1];
						if (adjCellName.contains(HW_FLAG) || adjCellName.contains(FEMTOFLAG)) {
							if (adjCellName.contains(HW_FLAG)) {
								adjrncname = adjCellName.substring(0, 5);
							}
							String[] values = text.split(",");
							for (String v : values) {
								String[] map = v.split("=");
								if (map[0].equals(SUBNETWORK)) {
									Pair subrootpair = new Pair("SUBNETWORKROOT", map[1]);
									if (!pairs.contains(subrootpair)) {
										pairs.add(subrootpair);
									}
								} else if (map[0].equals(EXTERNALUTRANCELL)) {
									Pair externalpair = new Pair("EXTERNALUTRANCELL", map[1]);
									if (!pairs.contains(externalpair)) {
										pairs.add(externalpair);
									}
								}
							}
						} else {
							String[] values = text.split(",");
							if (values.length > 1) {
								String subroot = values[0];

								String[] val = subroot.split("=");
								if (val.length > 1) {
									Pair subRootPair = new Pair("SUBNETWORKROOT", val[1]);
									if (!pairs.contains(subRootPair)) {
										pairs.add(subRootPair);
									}
								}
							}
							for (int i = 1; i < values.length; i++) {
								String[] map = values[i].split("=");
								if (map[0].equals(SUBNETWORK)) {
									String subid = map[1];
									Pair subPair = new Pair("SUBNETWORK", subid);
									if (!pairs.contains(subPair)) {
										pairs.add(subPair);
									}
								} else if (map[0].equals(MECONTEXT)) {
									String subid = map[1];
									Pair mecontextpair = new Pair("MECONTEXT", subid);
									if (!pairs.contains(mecontextpair)) {
										pairs.add(mecontextpair);
									}
								} else if (map[0].equals(MANAGEDELEMENT)) {
									String subid = map[1];
									Pair managedelementpair = new Pair("MANAGEDELEMENT", subid);
									if (!pairs.contains(managedelementpair)) {
										pairs.add(managedelementpair);
									}
								} else if (map[0].equals(RNCFUNCTION)) {
									String subid = map[1];
									Pair rncfunctionpair = new Pair("RNCFUNCTION", subid);
									if (!pairs.contains(rncfunctionpair)) {
										pairs.add(rncfunctionpair);
									}
								}
							}
						}

					} else {
						String[] values = text.split(",");
						if (values.length > 1) {
							String subroot = values[0];

							String[] val = subroot.split("=");
							if (val.length > 1) {
								Pair subRootPair = new Pair("SUBNETWORKROOT", val[1]);
								if (!pairs.contains(subRootPair)) {
									pairs.add(subRootPair);
								}
							}
						}
						for (int i = 1; i < values.length; i++) {
							String[] map = values[i].split("=");
							if (map[0].equals(SUBNETWORK)) {
								String subid = map[1];
								Pair subPair = new Pair("SUBNETWORK", subid);
								if (!pairs.contains(subPair)) {
									pairs.add(subPair);
								}
							} else if (map[0].equals(MECONTEXT)) {
								String subid = map[1];
								Pair mecontextpair = new Pair("MECONTEXT", subid);
								if (!pairs.contains(mecontextpair)) {
									pairs.add(mecontextpair);
								}
							} else if (map[0].equals(MANAGEDELEMENT)) {
								String subid = map[1];
								Pair managedelementpair = new Pair("MANAGEDELEMENT", subid);
								if (!pairs.contains(managedelementpair)) {
									pairs.add(managedelementpair);
								}
							} else if (map[0].equals(RNCFUNCTION)) {
								String subid = map[1];
								Pair rncfunctionpair = new Pair("RNCFUNCTION", subid);
								if (!pairs.contains(rncfunctionpair)) {
									pairs.add(rncfunctionpair);
								}
							}
						}

					}
					adjRncNamePair = new Pair("ADJ_RNC_NAME", adjrncname);
					if (!pairs.contains(adjRncNamePair)) {
						pairs.add(adjRncNamePair);
					}

				}
				if (!pairs.contains(pair) && !pair.getKey().equalsIgnoreCase(DATA_FORMAT_VERSION) && !pair.getKey().equalsIgnoreCase(DATA_TYPE)
						&& !pair.getKey().equalsIgnoreCase(USER_LABEL) && !pair.getKey().equalsIgnoreCase(GSM_RELATION)
						&& !pair.getKey().equalsIgnoreCase(UTRAN_RELATION)) {
					Pair rncnamepair = new Pair("RNC_NAME", this.rncname);
					if (!pairs.contains(rncnamepair)) {
						pairs.add(rncnamepair);
					}
					Pair rncidpair = new Pair("RNC_ID", this.rncid);
					if (!pairs.contains(rncidpair)) {
						pairs.add(rncidpair);
					}
					pairs.add(pair);
				}
				if (!pairs.contains(adjCellNamePair)) {
					pairs.add(adjCellNamePair);
				}
			}
		}

	}

	@SuppressWarnings("unchecked")
	private void handleDataChannelElement(Element dataChannelElement, List<Pair> pairs, String managedId, String mecontext) {
		List<Element> childs = dataChannelElement.elements();
		Pair subrootpair = new Pair("SUBNETWORKROOT", this.subRootId);
		if (!pairs.contains(subrootpair)) {
			pairs.add(subrootpair);
		}
		Pair subpair = new Pair("SUBNETWORK", this.subid);
		if (!pairs.contains(subpair)) {
			pairs.add(subpair);
		}
		Pair mecontextpair = new Pair("MECONTEXT", mecontext);
		if (!pairs.contains(mecontextpair)) {
			pairs.add(mecontextpair);
		}
		Pair managedpair = new Pair("MANAGEDELEMENT", managedId);
		if (!pairs.contains(managedpair)) {
			pairs.add(managedpair);
		}
		Pair rncFunctionpair = new Pair("RNCFUNCTION", this.rncFunction);
		if (!pairs.contains(rncFunctionpair)) {
			pairs.add(rncFunctionpair);
		}
		Pair rncpair = new Pair("RNC_NAME", this.rncname);
		if (!pairs.contains(rncpair)) {
			pairs.add(rncpair);
		}

		Pair rncidpair = new Pair("RNC_ID", this.rncid);
		if (!pairs.contains(rncidpair)) {
			pairs.add(rncidpair);
		}
		for (Element e : childs) {
			String val = e.getTextTrim();
			Pair p = new Pair(e.getName(), val);
			if (!pairs.contains(p)) {
				pairs.add(p);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void handleMeContextElement(Element targetElement, List<Pair> pairs, String mecontext) {

		Pair subrootPair = new Pair("SUBNETWORKROOT", this.subRootId);
		Pair subPair = new Pair("SUBNETWORK", this.subid);
		Pair mecontextPair = new Pair("MECONTEXT", mecontext);
		if (!pairs.contains(subrootPair)) {
			pairs.add(subrootPair);
		}
		if (!pairs.contains(subPair)) {
			pairs.add(subPair);
		}
		if (!pairs.contains(mecontextPair)) {
			pairs.add(mecontextPair);
		}

		List<Element> childs = targetElement.elements("VsDataContainer");
		if (childs == null) {
			return;
		}
		for (Element e : childs) {
			List<Element> ats = e.elements("attributes");
			if (ats == null) {
				continue;
			}
			for (Element ee : ats) {
				List<Element> el = ee.elements();
				for (Element eee : el) {
					if (eee.getName().equals("vsDataMeContext"))

					{
						List<Element> e4 = eee.elements();
						for (Element ee5 : e4) {
							String val = ee5.getTextTrim();
							if (val == null || val.equals(""))
								continue;
							Pair p = new Pair(ee5.getName(), val);
							if (!pairs.contains(p)) {
								pairs.add(p);
							}
						}
					} else {
						String val = eee.getTextTrim();
						if (val == null || val.equals(""))
							continue;

						Pair p = new Pair(eee.getName(), val);
						if (!pairs.contains(p)) {
							pairs.add(p);
						}
					}
				}

			}

		}
	}

	@SuppressWarnings("unchecked")
	private void handleRncElement(Element targetElement, List<Pair> pairs, String managedId, String mecontext) {
		List<Element> elements = targetElement.elements();
		for (Element e : elements) {
			if (e.elements().size() > 0 && !e.getName().equalsIgnoreCase(IUB_LINK) && !e.getName().equalsIgnoreCase(UTRAN_CELL)) {
				handleRncElement(e, pairs, managedId, mecontext);
			} else {
				Pair pair = new Pair(e.getName(), e.getTextTrim());
				if (!pairs.contains(pair) && !pair.getKey().equalsIgnoreCase(DATA_FORMAT_VERSION) && !pair.getKey().equalsIgnoreCase(DATA_TYPE)
						&& !pair.getKey().equalsIgnoreCase(USER_LABEL) && !pair.getKey().equalsIgnoreCase(GSM_RELATION)
						&& !pair.getKey().equalsIgnoreCase(UTRAN_RELATION)) {
					if (pair.getKey().equalsIgnoreCase("rncid")) {
						this.rncid = pair.getValue();
					}
					Pair subRootPair = new Pair("SUBNETWORKROOT", subRootId);
					if (!pairs.contains(subRootPair)) {
						pairs.add(subRootPair);
					}
					Pair subworkPair = new Pair("SUBNETWORK", subid);
					if (!pairs.contains(subworkPair)) {
						pairs.add(subworkPair);
					}
					Pair managedmentPair = new Pair("MANAGEDELEMENT", managedId);
					if (!pairs.contains(managedmentPair)) {
						pairs.add(managedmentPair);
					}
					Pair mecontextPair = new Pair("MECONTEXT", mecontext);
					if (!pairs.contains(mecontextPair)) {
						pairs.add(mecontextPair);
					}
					Pair rncFunctionPair = new Pair("RNCFUNCTION", this.rncFunction);
					if (!pairs.contains(rncFunctionPair)) {
						pairs.add(rncFunctionPair);
					}
					Pair rncnamepair = new Pair("RNC_NAME", this.rncname);
					if (!pairs.contains(rncnamepair)) {
						pairs.add(rncnamepair);
					}
					pairs.add(pair);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void handleManagedElement(Element targetElement, List<Pair> pairs, String mecontext, String managedmentId) {
		List<Element> elements = targetElement.elements();
		for (Element e : elements) {
			if (e.elements().size() > 0) {
				handleManagedElement(e, pairs, mecontext, managedmentId);
			} else {
				Pair pair = new Pair(e.getName(), e.getTextTrim());
				if (!pairs.contains(pair) && !pair.getKey().equalsIgnoreCase(DATA_FORMAT_VERSION) && !pair.getKey().equalsIgnoreCase(DATA_TYPE)
						&& !pair.getKey().equalsIgnoreCase(USER_LABEL) && !pair.getKey().equalsIgnoreCase(GSM_RELATION)) {
					Pair rncidpair = new Pair("MANAGEDMENT", managedmentId);
					if (!pairs.contains(rncidpair)) {
						pairs.add(rncidpair);
					}

					Pair mecontextPair = new Pair("MECONTEXT_ID", mecontext);
					if (!pairs.contains(mecontextPair)) {
						pairs.add(mecontextPair);
					}
					pairs.add(pair);
				}
			}
		}
	}

	// end

	@SuppressWarnings("unchecked")
	private void handleNodebElement(Element targetElement, List<Pair> pairs, String mecontext, String nodebid) {
		List<Element> elements = targetElement.elements();
		for (Element e : elements) {
			if (e.elements().size() > 0) {
				handleNodebElement(e, pairs, mecontext, nodebid);
			} else {
				String text = e.getTextTrim();
				if (e.getName().equals("nodeBFunctionIubLink")) {
					if (text.length() > 0) {
						String[] values = text.split(",");
						if (values.length > 1) {
							String subroot = values[0];

							String[] val = subroot.split("=");
							if (val.length > 0) {
								Pair subRootPair = new Pair("SUBNETWORKROOT", val[1]);
								if (!pairs.contains(subRootPair)) {
									pairs.add(subRootPair);
								}
							}
						}
						for (int i = 1; i < values.length; i++) {
							String[] map = values[i].split("=");
							if (map[0].equals(SUBNETWORK)) {
								String subid = map[1];
								Pair subPair = new Pair("SUBNETWORK", subid);
								if (!pairs.contains(subPair)) {
									pairs.add(subPair);
								}
							} else if (map[0].equals(MECONTEXT)) {
								String subid = map[1];
								Pair mecontextpair = new Pair("MECONTEXT", subid);
								if (!pairs.contains(mecontextpair)) {
									pairs.add(mecontextpair);
								}
							} else if (map[0].equals(MANAGEDELEMENT)) {
								String subid = map[1];
								Pair managedelementpair = new Pair("MANAGEDELEMENT", subid);
								if (!pairs.contains(managedelementpair)) {
									pairs.add(managedelementpair);
								}
							} else if (map[0].equals(RNCFUNCTION)) {
								String subid = map[1];
								Pair rncfunctionpair = new Pair("RNCFUNCTION", subid);
								if (!pairs.contains(rncfunctionpair)) {
									pairs.add(rncfunctionpair);
								}
							} else if (map[0].equals(IUBLINK)) {
								String subid = map[1];
								Pair iublinkpair = new Pair("IUBLINK", subid);
								if (!pairs.contains(IUBLINK)) {
									pairs.add(iublinkpair);
								}
							} else if (map[0].equals(IUBLINK_updated)) {
								String subid = map[1];
								Pair iublinkpair = new Pair("IUBLINK", subid);
								if (!pairs.contains(IUBLINK)) {
									pairs.add(iublinkpair);
								}
							}
						}
					}

				}
				Pair pair = new Pair(e.getName(), text);
				if (!pairs.contains(pair) && !pair.getKey().equalsIgnoreCase(DATA_FORMAT_VERSION) 
						&& !pair.getKey().equalsIgnoreCase(DATA_TYPE)
						&& !pair.getKey().equalsIgnoreCase(GSM_RELATION)
						&& !pair.getKey().equalsIgnoreCase(UTRAN_RELATION)) {
					Pair rncnamepair = new Pair("RNC_NAME", this.rncname);
					if (!pairs.contains(rncnamepair)) {
						pairs.add(rncnamepair);
					}
					Pair rncidpair = new Pair("RNC_ID", stringNullHandler(this.rncid));
					if (!pairs.contains(rncidpair)) {
						pairs.add(rncidpair);
					}
					Pair mecontextPair = new Pair("MECONTEXT_ID", mecontext);
					if (!pairs.contains(mecontextPair)) {
						pairs.add(mecontextPair);
					}
					pairs.add(pair);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Element findChannel(Element runcfunctionElement) {
		List<Element> dcs = runcfunctionElement.elements("VsDataContainer");
		if (dcs == null) {
			return null;
		}
		for (Element e : dcs) {
			List<Element> ats = e.elements("attributes");
			if (ats == null) {
				continue;
			}
			for (Element ee : ats) {
				Element channel = ee.element(DATA_CHANNEL_SWITCHING);
				if (channel != null) {
					return channel;
				}
			}
		}
		return null;
	}

	private static List<Pair> splitRef(String val) {
		List<Pair> list = new ArrayList<Pair>();
		String[] sp0 = val.split(",");
		for (String s0 : sp0) {
			if (Util.isNotNull(s0)) {
				String[] sp1 = s0.split("=");
				String name = sp1[0];
				String v = sp1.length > 1 ? sp1[1] : "";
				Pair p = new Pair(name, v);
				if (!list.contains(p))
					list.add(p);
			}
		}
		return list;
	}

	private static class Pair {

		String key;

		String value;

		public Pair(String key, String value) {
			super();
			this.key = key;
			this.value = value;
		}

		public String getKey() {
			return key;
		}

		public String getValue() {
			return value;
		}

		@Override
		public String toString() {
			return String.format("[%s=%s]", key, value);
		}

		@Override
		public boolean equals(Object obj) {
			return ((Pair) obj).getKey().equalsIgnoreCase(key);
		}
	}

	private void largeFileHandle(String file, int omcId, Timestamp stampTime) {
		List<File> files = new CMFileSpliter(new File(file)).split();
		for (File f : files) {
			try {
				new EricssonCmParserImp().parse(f.getAbsolutePath(), omcId, stampTime, this.taskId);
				Thread.sleep(5 * 1000);
			} catch (Exception e) {
				logger.error("处理分隔后的文件时异常:" + f.getAbsolutePath(), e);
			} finally {
				f.delete();
			}
		}
	}

	public static void main(String[] args) throws Exception {
//		EricssonCmParser parser = new EricssonCmParserImp();
//		try {
//			parser.parse("C:\\Users\\Admin\\Desktop\\江苏\\ECR39\\ECR39_cm_exp_20150420_014730.xml", 201, new Timestamp(Util.getDate1("2013-06-24 19:00:00").getTime()),
//					8899);
//		} catch (EricssonCmParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		// try
		// {
		//
		// Thread t1 = new Thread(new Runnable()
		// {
		//
		// @Override
		// public void run()
		// {
		// try
		// {
		// System.out.println("线程开始 - 1");
		// EricssonCmParser parser = new EricssonCmParserImp();
		// parser.parse("C:\\Users\\ChenSijiang\\Desktop\\DGRNC01_BuckCM_export_uway_1.xml",
		// 11, new Timestamp(new Date().getTime()), 989);
		// System.out.println("线程结束 - 1");
		// }
		// catch (Exception e)
		// {
		// e.printStackTrace();
		// }
		// }
		// });
		// t1.start();
		//
		// Thread t2 = new Thread(new Runnable()
		// {
		//
		// @Override
		// public void run()
		// {
		// try
		// {
		// System.out.println("线程开始 - 2");
		// EricssonCmParser parser = new EricssonCmParserImp();
		// parser.parse("C:\\Users\\ChenSijiang\\Desktop\\DGRNC01_BuckCM_export_uway_2.xml",
		// 11, new Timestamp(new Date().getTime()), 989);
		// System.out.println("线程结束 - 2");
		// }
		// catch (Exception e)
		// {
		// e.printStackTrace();
		// }
		// }
		// });
		// t2.start();
		//
		// Thread t3 = new Thread(new Runnable()
		// {
		//
		// @Override
		// public void run()
		// {
		// try
		// {
		// System.out.println("线程开始 - 3");
		// EricssonCmParser parser = new EricssonCmParserImp();
		// parser.parse("C:\\Users\\ChenSijiang\\Desktop\\DGRNC01_BuckCM_export_uway_3.xml",
		// 11, new Timestamp(new Date().getTime()), 989);
		// System.out.println("线程结束 - 3");
		// }
		// catch (Exception e)
		// {
		// e.printStackTrace();
		// }
		// }
		// });
		// t3.start();
		//
		// Thread t4 = new Thread(new Runnable()
		// {
		//
		// @Override
		// public void run()
		// {
		// try
		// {
		// System.out.println("线程开始 - 4");
		// EricssonCmParser parser = new EricssonCmParserImp();
		// parser.parse("C:\\Users\\ChenSijiang\\Desktop\\DGRNC01_BuckCM_export_uway_4.xml",
		// 11, new Timestamp(new Date().getTime()), 989);
		// System.out.println("线程结束 - 4");
		// }
		// catch (Exception e)
		// {
		// e.printStackTrace();
		// }
		// }
		// });
		// t4.start();
		//
		// Thread t5 = new Thread(new Runnable()
		// {
		//
		// @Override
		// public void run()
		// {
		// try
		// {
		// System.out.println("线程开始 - 5");
		// EricssonCmParser parser = new EricssonCmParserImp();
		// parser.parse("C:\\Users\\ChenSijiang\\Desktop\\DGRNC01_BuckCM_export_uway_5.xml",
		// 11, new Timestamp(new Date().getTime()), 989);
		// System.out.println("线程结束 - 5");
		// }
		// catch (Exception e)
		// {
		// e.printStackTrace();
		// }
		// }
		// });
		// t5.start();
		//
		// Thread t6 = new Thread(new Runnable()
		// {
		//
		// @Override
		// public void run()
		// {
		// try
		// {
		// System.out.println("线程开始 - 6");
		// EricssonCmParser parser = new EricssonCmParserImp();
		// parser.parse("C:\\Users\\ChenSijiang\\Desktop\\DGRNC01_BuckCM_export_uway_6.xml",
		// 11, new Timestamp(new Date().getTime()), 989);
		// System.out.println("线程结束 - 6");
		// }
		// catch (Exception e)
		// {
		// e.printStackTrace();
		// }
		// }
		// });
		// t6.start();
		// }
		// catch (Exception e)
		// {
		// e.printStackTrace();
		// }
		//
		// Timer timer = new Timer();
		// timer.scheduleAtFixedRate(new TimerTask()
		// {
		//
		// @Override
		// public void run()
		// {
		// Util.printMemoryStatus();
		// }
		// }, 0, 2000);
	}

	private String findByRegex(String str, String regEx, int group) {
		String resultValue = null;
		if (regEx == null || (regEx != null && "".equals(regEx.trim()))) {
			return resultValue;
		}
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);

		boolean result = m.find();// 查找是否有匹配的结果
		if (result) {
			resultValue = m.group(group);// 找出匹配的结果
		}
		return resultValue;
	}
}

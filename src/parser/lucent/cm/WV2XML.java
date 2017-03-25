package parser.lucent.cm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import parser.Parser;
import store.SqlldrStore;
import store.SqlldrStoreParam;
import task.CollectObjInfo;
import task.DevInfo;
import templet.Table;
import templet.Table.Column;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;
import exception.StoreException;

/**
 * 联通二期W阿朗参数文件解析
 * 
 * @author litp Aug 16, 2010
 * @since 1.0
 */
public class WV2XML extends Parser {

	private static final Object PARSE_LOCK = new Object();

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	/**
	 * 映射表信息 <表名,<原始字段名,映射后的短名>>
	 * <p>
	 * 此类被装载时从爱立信性能映射表中加载所有的列映射信息，如果程序在解析XML文件mt的值的时候，发现mt的值在映射表原始字段列中不存在则不予以处理， 只处理存在的列
	 * <p>
	 */
	public static Map<String, HashMap<String, String>> mapInfo = new HashMap<String, HashMap<String, String>>();

	/**
	 * 因为每次解析的原始数据有可能不一样，所以每个表就可能新增加字段，所以每次解析后就将需要添加的字段存入此map中<表名，<原始列名，短名>>
	 */
	public static Map<String, HashMap<String, String>> needAdd = new HashMap<String, HashMap<String, String>>();

	// 表名的映射关系<原始表名，短表名>
	public static Map<String, String> tableInfo = new HashMap<String, String>();

	// 有新表时，需要添加到数据库的表名映射<原始表名，短表名>
	public static Map<String, String> needAddTabInfo = new HashMap<String, String>();

	// oracle关键字
	private static final String ORACLEKEYWORD = "MODE,ACCESS,";

	// 所有的模板
	private Map<String, Table> templets = null;

	private SqlldrStore sqlldrStore;
	static {
		// loadTables();
		// loadMapInfoFromDB();
	}

	private XmlDefaultHandler saxHandler;

	public int omcID;

	public Timestamp dataTime;

	public long taskID;

	public WV2XML() {
		super();
	}

	public WV2XML(CollectObjInfo obj) {
		super(obj);
	}

	@Override
	public boolean parseData() {
		synchronized (PARSE_LOCK) {
			WcdmaALParamParser w = new WcdmaALParamParser();
			w.setCollectObjInfo(this.collectObjInfo);
			w.setFileName(this.fileName);
			try {
				return w.parseData();
			} catch (Exception e) {
				log.error("解析文件失败,原因:", e);
				return false;
			}
			// omcID = this.collectObjInfo.getDevInfo().getOmcID();
			// dataTime = this.collectObjInfo.getLastCollectTime();
			// taskID = this.collectObjInfo.getTaskID();
			//
			// boolean ret = false;
			// SAXParserFactory sf = SAXParserFactory.newInstance();
			// SAXParser sp = null;
			// try
			// {
			// createSeq();
			// sp = sf.newSAXParser();
			// saxHandler = new XmlDefaultHandler();
			// sp.getXMLReader().setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd",
			// false);
			// sp.getXMLReader().setFeature("http://xml.org/sax/features/validation",
			// false);
			// File f = new File(this.getFileName());
			// List<String> reserv =
			// SystemConfig.getInstance().getReservALRnc();
			// if ( reserv.size() != 0 )
			// {
			// log.debug("开始提取RNC - " + reserv + " file - "
			// + f.getAbsolutePath());
			// File fff = FileSplit.split(f,
			// SystemConfig.getInstance().getReservALRnc());
			// log.debug("提取RNC完成 - " + reserv + " file - "
			// + f.getAbsolutePath());
			// sp.parse(new InputSource(fff != null ? fff.getAbsolutePath() :
			// this.getFileName()), saxHandler);
			// }
			// else
			// {
			// sp.parse(new File(this.getFileName()), saxHandler);
			// }
			// insertTabInfos();
			// createTable();
			// sqlldr();
			// ret = true;
			// }
			// catch (Exception e)
			// {
			// log.error("解析文件失败,原因:", e);
			// }
			// finally
			// {
			// if ( saxHandler != null )
			// saxHandler.clear();
			// }
			//
			// return ret;

		}
	}

	// 创建一个序列
	private void createSeq() {
		String seq = "create sequence lucent_seq start with 1 increment by 1 nocycle";
		try {
			CommonDB.executeUpdate(seq);
		} catch (SQLException e) {
			if (e.getErrorCode() != 955) {
				log.error("创建序列错误：" + seq, e);
			}
		}
	}

	/**
	 * Sqlldr入库数据
	 */
	private void sqlldr() {
		Data datas = saxHandler.getData();
		if (datas == null)
			return;
		Map<String, ArrayList<Node>> datasMap = datas.getDatas();
		Set<String> tableNames = datasMap.keySet();
		for (String tableName : tableNames) {
			Table tableD = templets.get(tableName);
			if (tableD == null)
				continue;
			String splitSign = tableD.getSplitSign();

			ArrayList<Node> nodes = datasMap.get(tableName);
			for (Node node : nodes) {
				StringBuilder lineData = new StringBuilder();
				lineData.append(node.getId()).append(splitSign);
				lineData.append(node.getParentId()).append(splitSign);
				lineData.append(node.getPTBName()).append(splitSign);
				NetStruct netStruct = node.getNetStruct();
				if (netStruct == null) {
					log.debug("没有网元信息：" + tableName);
					lineData.append("").append(splitSign);
					lineData.append("").append(splitSign);
					lineData.append("").append(splitSign);
					lineData.append("").append(splitSign);
					lineData.append("").append(splitSign);
				} else {
					lineData.append(netStruct.getRncId()).append(splitSign);
					lineData.append(netStruct.getNodeBId()).append(splitSign);
					lineData.append(netStruct.getFddCellId()).append(splitSign);
					lineData.append(netStruct.getCi()).append(splitSign);
					lineData.append(netStruct.getLac()).append(splitSign);
				}

				List<Field> fields = node.getDatas();

				// 分发模板的所有列
				Collection<Column> colValues = tableD.getColumns().values();
				for (Column col : colValues) {
					String colName = col.getName();
					// 因为id,pid和pTBname(父节点的表名)都加过了
					if (colName.equals("ID") || colName.equals("PID") || colName.equals("PTABLENAME") || colName.equals("RNCID1")
							|| colName.equals("NODEBID1") || colName.equals("FDDCELLID1") || colName.equals("CI1") || colName.equals("LAC1"))
						continue;
					boolean notFound = true;
					// 这里需要改进
					for (Field f : fields) {
						if (colName.equals(f.getName())) {
							lineData.append(f.getValue()).append(splitSign);
							notFound = false;
							break;
						}
					}
					if (notFound) {
						lineData.append("").append(splitSign);
					}
				}
				try {
					distribute(lineData.toString(), 0, tableD);
				} catch (StoreException e) {
					log.error("提交数据错误：" + lineData.toString(), e);
				}
			}
			commit();
		}

	}

	private void insertTabInfos() {
		// Set<String> set = needAddTabInfo.keySet();
		// for (String rawname : set)
		// {
		// String sql =
		// "insert into CLT_CM_W_AL_TABLESMAP(RAW_TAB_NAME,TAB_NAME) values('"
		// + rawname + "','" + needAddTabInfo.get(rawname) + "')";
		// try
		// {
		// CommonDB.executeUpdate(sql);
		// }
		// catch (SQLException e)
		// {
		// log.error("插入表映射时错误!");
		// }
		// }
	}

	private void createTable() {
		Data datas = saxHandler.getData();
		if (datas == null)
			return;
		templets = new HashMap<String, Table>();
		Map<String, ArrayList<String>> columns = datas.getColumns();
		Set<String> tableNames = columns.keySet();
		int i = 0;
		for (String tableName : tableNames) {
			StringBuilder sql = new StringBuilder("create table ");
			sql.append(tableName).append("(");
			sql.append("OMCID NUMBER,").append("COLLECTTIME DATE,");
			sql.append("STAMPTIME DATE,");
			sql.append("ID varchar2(100),").append("PID varchar2(100),");
			sql.append("PTABLENAME varchar2(100),");
			sql.append("RNCID1 varchar2(100),");
			sql.append("NODEBID1 varchar2(100),");
			sql.append("FDDCELLID1 varchar2(100),");
			sql.append("CI1 varchar2(100),");
			sql.append("LAC1 varchar2(100),");
			// 所有的原始列名
			ArrayList<String> list = columns.get(tableName);

			// 每一个tableName都会生成一个对应的Table对象，用于数据入库
			Table tableD = new Table();
			tableD.setName(tableName);
			tableD.setId(i++);
			tableD.setSplitSign("|");
			// id
			Column idColumn = new Column();
			idColumn.setIndex(0);
			idColumn.setName("ID");
			tableD.getColumns().put(0, idColumn);
			// pid
			Column pidColumn = new Column();
			pidColumn.setIndex(1);
			pidColumn.setName("PID");
			tableD.getColumns().put(1, pidColumn);
			// ptbname
			Column pTNColumn = new Column();
			pTNColumn.setIndex(2);
			pTNColumn.setName("PTABLENAME");
			tableD.getColumns().put(2, pTNColumn);
			// rncid
			Column rncColumn = new Column();
			rncColumn.setIndex(3);
			rncColumn.setName("RNCID1");
			tableD.getColumns().put(3, rncColumn);
			// nodebid
			Column nodebidColumn = new Column();
			nodebidColumn.setIndex(4);
			nodebidColumn.setName("NODEBID1");
			tableD.getColumns().put(4, nodebidColumn);
			// fddcellid
			Column fddColumn = new Column();
			fddColumn.setIndex(5);
			fddColumn.setName("FDDCELLID1");
			tableD.getColumns().put(5, fddColumn);
			// ci
			Column ciColumn = new Column();
			ciColumn.setIndex(6);
			ciColumn.setName("CI1");
			tableD.getColumns().put(6, ciColumn);
			// ptbname
			Column lacColumn = new Column();
			lacColumn.setIndex(7);
			lacColumn.setName("LAC1");
			tableD.getColumns().put(7, lacColumn);

			int index = 8;
			// 遍历原始列
			for (String colName : list) {
				if (colName.equals(""))
					continue;
				// 取出此原始列对应的短名
				// String shortName = getShortName(tableName, rawName);
				sql.append(colName).append(" varchar2(100),");

				Column column = new Column();
				column.setIndex(index);
				column.setName(colName);
				tableD.getColumns().put(index++, column);
			}
			templets.put(tableName, tableD);
			sql.deleteCharAt(sql.length() - 1);
			sql.append(")");
			executeCreate(sql.toString(), tableName);
		}
	}

	/**
	 * 执行创建表：
	 * <p>
	 * 1：如果此语句在CLT_CM_W_AL_MAP中对应的有新列需要添加，那么就先对表CLT_CM_W_AL_MAP执行插入操作
	 * </p>
	 * <p>
	 * 2:如果此表存在，那么就执行新列的插入，如果不存在就新建表
	 * </p>
	 * 
	 * @param sql
	 *            建表语句
	 * @param tableName
	 *            表名
	 */
	private void executeCreate(String sql, String tableName) {
		boolean needAlter = false;
		StringBuilder needAltersql = null;
		try {
			Map<String, String> map = needAdd.get(tableName);
			if (map != null) {
				needAltersql = new StringBuilder("alter table ");
				needAltersql.append(tableName);
				needAltersql.append(" add(");

				Set<String> rawNames = map.keySet();
				for (String rawName : rawNames) {
					String shortName = map.get(rawName);
					String inMap = "insert into CLT_CM_W_AL_MAP values('" + rawName + "','" + shortName + "','" + tableName + "')";
					try {
						// CommonDB.executeUpdate(inMap);
					} catch (Exception e) {
						log.error("执行语句错误：" + inMap, e);
					}
					needAltersql.append("\"" + shortName + "\"").append(" varchar2(100),");
				}
				needAltersql.deleteCharAt(needAltersql.length() - 1).append(")");
			}
			CommonDB.executeUpdate(sql);
		} catch (SQLException e) {

			if (e.getErrorCode() == 955) {
				needAlter = true;
			} else {
				log.error("建表错误:" + sql, e);
			}
		}

		// 如果表存在并且needAltersql不为空
		if (needAlter && needAltersql != null) {
			// try
			// {
			// CommonDB.executeUpdate(needAltersql.toString());
			// }
			// catch (SQLException e)
			// {
			// //log.error("alter table时错误" + needAltersql.toString(), e);
			// }
		}
	}

	private String getNextval() {
		String nextval = null;
		String next = "select lucent_seq.nextval from dual";
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			connection = DbPool.getConn();
			preparedStatement = connection.prepareStatement(next);
			rs = preparedStatement.executeQuery();
			if (rs.next()) {
				nextval = rs.getString("nextval");
			}
		} catch (SQLException e) {
			log.error("获取序列出错：" + next, e);
		} finally {
			CommonDB.close(rs, preparedStatement, connection);
		}
		return nextval;
	}

	// 根据表名和原始列从数据库中取出短名，如果不存在那么就将此列转换为短名并存入一个集合中
	private String getShortName(String tableName, String rawName) {
		String shortName = null;
		Map<String, String> columns = mapInfo.get(tableName);
		// 如果在mapInfo中存在rawName那么就取出，不存在就添加到needAdd中，用于存入数据库
		if (columns != null && columns.containsKey(rawName)) {
			shortName = columns.get(rawName);
		} else {
			return null;
			// shortName = add2NeedAdd(tableName, rawName);
		}
		return shortName;
	}

	private String add2NeedAdd(String tableName, String rawName) {
		String shortName = rawName;
		if (needAdd.containsKey(tableName)) {
			Map<String, String> map = needAdd.get(tableName);
			if (map.containsKey(rawName)) {
				shortName = map.get(rawName);

			} else {
				if (rawName.length() > 30) {
					shortName = "COL_" + getNextval();
				}
				map.put(rawName, shortName);
			}
		} else {
			HashMap<String, String> map = new HashMap<String, String>();
			if (rawName.length() > 30) {
				shortName = "COL_" + getNextval();
			}
			map.put(rawName, shortName);
			needAdd.put(tableName, map);
		}
		return shortName;
	}

	private void distribute(String lineData, int templetId, Table tableD) throws StoreException {
		// 处理sqlldrstore
		if (sqlldrStore == null) {
			sqlldrStore = new SqlldrStore(new SqlldrStoreParam(templetId, tableD));
			sqlldrStore.setCollectInfo(collectObjInfo);
			sqlldrStore.setTaskID(this.collectObjInfo.getTaskID());
			sqlldrStore.setDataTime(this.collectObjInfo.getLastCollectTime());
			sqlldrStore.setOmcID(this.collectObjInfo.getDevInfo().getOmcID());
			sqlldrStore.open();
		}
		sqlldrStore.write(lineData);
	}

	private void commit() {
		if (sqlldrStore != null) {
			try {
				sqlldrStore.flush();
				sqlldrStore.commit();
				sqlldrStore.close();
				sqlldrStore = null;
			} catch (StoreException e) {
				log.error("调用sqlldr入库时异常", e);
			}
		}
	}

	public static synchronized void loadTables() {

		String sql = "SELECT RAW_TAB_NAME,TAB_NAME FROM CLT_CM_W_AL_TABLESMAP";

		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				String rawName = rs.getString("RAW_TAB_NAME").toUpperCase();
				String tbName = rs.getString("TAB_NAME").toUpperCase();

				if (!tableInfo.containsKey(rawName)) {
					tableInfo.put(rawName, tbName);
				}

			}
		} catch (Exception e) {
			// LogMgr.getInstance().getSystemLogger().error("加载表映射信息时异常:" + sql,
			// e);
		} finally {
			CommonDB.close(rs, ps, con);
		}
	}

	/**
	 * 加载所有的列映射信息
	 */
	public static synchronized void loadMapInfoFromDB() {
		Map<String, List<String>> realCols = loadRealCols();
		String sql = "SELECT COL_NAME,SHORT_COL_NAME,TAB_NAME FROM CLT_CM_W_AL_MAP";

		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				String rawName = rs.getString("COL_NAME");
				String shortName = rs.getString("SHORT_COL_NAME").toUpperCase();
				String tbName = rs.getString("TAB_NAME").toUpperCase();

				if (realCols != null) {
					if (realCols.containsKey(tbName)) {
						if (!realCols.get(tbName).contains(shortName))
							continue;// 表中没有这个字段，跳过。
					} else
						continue; // 库中没有这个表，跳过
				}

				if (mapInfo.containsKey(tbName)) {
					HashMap<String, String> fMap = mapInfo.get(tbName);
					// 如果表里有重复的映射定义，只会加载第一个
					if (!fMap.containsKey(rawName)) {
						fMap.put(rawName, shortName);
					}
				} else {
					HashMap<String, String> fMap = new HashMap<String, String>();
					fMap.put(rawName, shortName);
					mapInfo.put(tbName, fMap);
				}
			}
		} catch (Exception e) {
			LogMgr.getInstance().getSystemLogger().error("加载字段映射表信息时异常:" + sql, e);
		} finally {
			CommonDB.close(rs, ps, con);
		}

	}

	private static Map<String, List<String>> realCols = null;

	private static Map<String, List<String>> loadRealCols() {
		if (realCols != null)
			return realCols;

		realCols = new HashMap<String, List<String>>();

		String sql = "select table_name,column_name from user_tab_cols where table_name in( select distinct(tab_name) from clt_cm_w_al_map) order by table_name";
		Connection con = DbPool.getConn();
		ResultSet rs = null;
		PreparedStatement st = null;
		try {
			st = con.prepareStatement(sql);
			rs = st.executeQuery();
			while (rs.next()) {
				String tn = rs.getString("table_name");
				String cn = rs.getString("column_name");
				if (realCols.containsKey(tn))
					realCols.get(tn).add(cn);
				else {
					List<String> cols = new ArrayList<String>();
					cols.add(cn);
					realCols.put(tn, cols);
				}
			}

			return realCols;
		} catch (Exception e) {
			logger.error("执行sql出错 - " + sql, e);
		} finally {
			CommonDB.close(rs, st, con);
		}
		return null;
	}

	// 单元测试
	public static void main(String[] args) throws Exception {
		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(302);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2011-11-21 00:00:00").getTime()));

		WV2XML xml = new WV2XML();
		xml.collectObjInfo = obj;
		// lucent\\UTRAN-SNAP20100801020001.xml
		// nodeb.xml
		// rnc.xml
		xml.setFileName("F:\\资料\\原始数据\\联通第五次PK_原始数据上传\\亿阳\\UTRAN-SNAP20111121020002.xml");
		xml.parseData();
		// System.out.println("Class3CellReconfParams".toUpperCase());
	}

	/**
	 * 自定义XML文件SAX方式处理类
	 * 
	 * @author YangJian
	 * @since 1.0
	 */
	class XmlDefaultHandler extends DefaultHandler {

		private Data datas = null;

		private Stack<String> stack = null;

		// 节点的父id：<当前节点名(原始名称),结构>
		private Map<String, Struct> parents = null;

		private String currentId = null;

		private Node currNode = null;// 当前node

		private String currTBName = null;// 当前在解析的表名

		private String content = null;

		private String currNodename = null;

		private boolean isClosed = false;

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			content = new String(ch, start, length).trim();
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attrs) throws SAXException {
			// if(qName!=null && qName.equals("FDDCell"))
			// {
			// logger.debug("");
			// }
			currentId = attrs.getValue("id");
			// 如果不等于空，就表示读到一个新的节点
			if (currentId == null)
				return;

			// 将获取到的qName压入栈
			stack.push(qName);
			// 设置表名
			setCurrTBname(qName);

			// 如果结构中存在了此表名，那么就删除原来的那个nodename,新增加这个
			if (parents.containsKey(qName)) {
				putNew(qName);
			} else {
				// 如果不存在就直接加上去
				put(qName);
			}

			// 当前的结构
			Struct currS = parents.get(qName);
			// 父结构
			Struct p = currS.getParent();
			currNode = new Node();
			if (p != null) {
				currNode.setParentId(p.getId());
				currNode.setPTBName(p.getTabName());
			}

			currNode.setNetStruct(currS.getNetStruct());
			currNode.setId(currentId);
			//
			// pTBName = currTBName;
			currNodename = qName;
			isClosed = false;
			// 添加数据
			if (Util.isNull((currNode.getNetStruct().getCi())))
				currNode.getNetStruct().setCi(currCi);
			if (Util.isNull(currNode.getNetStruct().getLac()))
				currNode.getNetStruct().setLac(currLac);
			datas.addData(currTBName, currNode);
		}

		/**
		 * 根据节点名称获取表名，并将表名存入currTBName中
		 * 
		 * @param qName
		 * @return
		 */
		private void setCurrTBname(String qName) {
			currTBName = Data.suffixTN + qName.toUpperCase();
			if (qName.equals("Class3CellReconfParams"))
				currTBName = Data.suffixTN + "CLASS3C74";
			else if (qName.equals("UlOuterLoopPowerCtrl"))
				currTBName = Data.suffixTN + "ULOUTER242";
			else if (qName.equals("UlIrmCEMParameters"))
				currTBName = Data.suffixTN + "ULIRMCE399";
			else if (qName.equals("UlInnerLoopConf"))
				currTBName = Data.suffixTN + "ULINNER407";
			else if (qName.equals("UeTimerCstIdleMode"))
				currTBName = Data.suffixTN + "UETIMER519";
			else if (qName.equals("UeTimerCstConnectedMode"))
				currTBName = Data.suffixTN + "UETIMER305";
			else if (qName.equals("RachTxParameters"))
				currTBName = Data.suffixTN + "RACHTXP96";
			else if (qName.equals("ReferenceEtfciConfList"))
				currTBName = Data.suffixTN + "REFEREN373";
			else if (qName.equals("RadioAccessService"))
				currTBName = Data.suffixTN + "RADIOAC167";
			else if (qName.equals("UmtsNeighbouringRelation"))
				currTBName = Data.suffixTN + "UMTSNEI529";
			else if (qName.equals("UMTSFddNeighbouringCell"))
				currTBName = Data.suffixTN + "UMTSFDD72";
			else if (qName.equals("IrmPreemptionCacParams"))
				currTBName = Data.suffixTN + "IRMPREE455";
			else if (qName.equals("IrmOnCellColourParameters"))
				currTBName = Data.suffixTN + "IRMONCE456";
			else if (qName.equals("InterFreqMeasConf"))
				currTBName = Data.suffixTN + "INTERFR411";
			else if (qName.equals("MissingMeasurement"))
				currTBName = Data.suffixTN + "MISSING437";
			else if (qName.equals("ManualActivation"))
				currTBName = Data.suffixTN + "MANUALA325";
			else if (qName.equals("DlIrmCEMParameters"))
				currTBName = Data.suffixTN + "DLIRMCE403";
			else if (qName.equals("DlBlerQualityList"))
				currTBName = Data.suffixTN + "DLBLERQ283";
			else if (qName.equals("PreemptionQueuingReallocation"))
				currTBName = Data.suffixTN + "PREEMPT351";
			else if (qName.equals("PowerPartConfClass"))
				currTBName = Data.suffixTN + "POWERPA444";
			else if (qName.equals("DynamicParameterPerDch"))
				currTBName = Data.suffixTN + "DYNAMIC254";
			else if (qName.equals("FastAlarmHardHoConf"))
				currTBName = Data.suffixTN + "FASTALA508";
			else if (qName.equals("AutomatedCellBarring"))
				currTBName = Data.suffixTN + "AUTOMAT324";
			else if (qName.equals("FullEventHOConfHhoMgtEvent2D"))
				currTBName = Data.suffixTN + "FULLEVE507";
			else if (qName.equals("FullEventHOConfHhoMgtEvent2F"))
				currTBName = Data.suffixTN + "FULLEVE503";
			else if (qName.equals("FullEventHOConfShoMgtEvent1D"))
				currTBName = Data.suffixTN + "FULLEVE490";
			else if (qName.equals("FullEventHOConfShoMgtEvent1C"))
				currTBName = Data.suffixTN + "FULLEVE489";
			else if (qName.equals("CellSelectionInfo"))
				currTBName = Data.suffixTN + "CELLSEL77";
			else if (qName.equals("CellAccessRestrictionConnectedMode"))
				currTBName = Data.suffixTN + "CELLACC103";
			else if (qName.equals("GsmNeighbouringCell"))
				currTBName = Data.suffixTN + "GSMNEIG73";
			else if (qName.equals("HsdpaUserService"))
				currTBName = Data.suffixTN + "HSDPAUS526";
			else if (qName.equals("HsPdschDynamicManagement"))
				currTBName = Data.suffixTN + "HSPDSCH446";
			else if (qName.equals("FullEventHOConfShoMgtEvent1B"))
				currTBName = Data.suffixTN + "FULLEVE488";
			else if (qName.equals("FullEventHOConfShoMgtEvent1F"))
				currTBName = Data.suffixTN + "FULLEVE487";
			else if (qName.equals("FullEventHOConfShoMgtEvent1E"))
				currTBName = Data.suffixTN + "FULLEVE485";
			else if (qName.equals("FullEventHOConfShoMgtEvent1A"))
				currTBName = Data.suffixTN + "FULLEVE484";
			else if (qName.equals("FullEventRepCritEvent1AWithoutIur"))
				currTBName = Data.suffixTN + "FULLEVE415";
			else if (qName.equals("FddNeighCellSelectionInfoConnectedMode"))
				currTBName = Data.suffixTN + "FDDNEIG532";
			else if (qName.equals("FddIntelligentMultiCarrierTrafficAllocation"))
				currTBName = Data.suffixTN + "FDDINTE81";
			else if (qName.equals("CallAccessPerformanceConf"))
				currTBName = Data.suffixTN + "CALLACC237";
			else if (qName.equals("DlInnerLoopConf"))
				currTBName = Data.suffixTN + "DLINNER406";
			else if (qName.equals("UlIrmRadioLoadParameters"))
				currTBName = Data.suffixTN + "ULIRMRA452";

			// 如果之前的数据库中有这个表就直接取出来即可

			if (tableInfo.containsKey(currTBName)) {
				currTBName = tableInfo.get(currTBName);
			} else {
				if (needAddTabInfo.containsKey(currTBName)) {
					currTBName = needAddTabInfo.get(currTBName);
				} else {
					String rawTabName = currTBName;
					// 如果表名超长,就截取
					if (currTBName.length() > 30)
						currTBName = currTBName.substring(0, 23) + getNextval();

					needAddTabInfo.put(rawTabName, currTBName);
				}
			}
		}

		// 将结构直接添加到parents中
		private void put(String qName) {
			// 如果有闭括号,currNodename就是上一个有闭括号的qName,与此时的节点名属于同一级，就需要getParent,
			// 如果没有闭括号,currNodename就是上一个qName,所以就不再需要getParent
			Struct parent = null;
			if (currNodename != null)
				parent = parents.get(currNodename);
			if (isClosed)
				parent = parent.getParent();

			Struct s = new Struct();

			putStruct(parent, s, qName);
		}

		/**
		 * 添加一个新的结构，它的同级结构都将被删掉
		 * 
		 * @param qName
		 */
		private void putNew(String qName) {
			Struct s = parents.get(qName);
			Struct parent = s.getParent();

			removeChild(s);

			s = new Struct();

			putStruct(parent, s, qName);
		}

		private void putStruct(Struct parent, Struct s, String qName) {
			s.setParent(parent);
			s.setTabName(currTBName);
			s.setId(currentId);
			s.setQName(qName);

			NetStruct nsc = null;

			NetStruct ns = null;
			if (parent != null)
				ns = parent.getNetStruct();
			if (ns == null)
				nsc = new NetStruct();
			else
				try {
					nsc = (NetStruct) ns.clone();
				} catch (CloneNotSupportedException e) {
					log.error("克隆网元信息时异常：", e);
				}

			if (qName.equalsIgnoreCase("RNC")) {
				nsc.setRncId(currentId);
			} else if (qName.equalsIgnoreCase("NodeB")) {
				nsc.setNodeBId(currentId);
			} else if (qName.equalsIgnoreCase("FDDCell")) {
				nsc.setFddCellId(currentId);
			}
			s.setNetStruct(nsc);
			parents.put(qName, s);

			// 如果parent不为空，那么就将此节点加到它下面
			if (parent != null) {
				List<Struct> child = parent.getChild();
				if (child == null)
					child = new ArrayList<Struct>();
				child.add(s);
				parent.setChild(child);
			}
		}

		/**
		 * 从parents中删除struct节点以及struct所有的子节点
		 * 
		 * @param struct
		 */
		private void removeChild(Struct struct) {
			if (struct == null)
				return;

			List<Struct> child = struct.getChild();
			if (child != null) {
				for (Struct s : child) {
					removeChild(s);
				}
			}
			parents.remove(struct.getQName());
		}

		private String currLac = "";

		private String currCi = "";

		private String currRncid = "";

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {

			if (stack.peek().equals(qName)) {
				currNodename = stack.pop();
				isClosed = true;
			}
			// 当前节点的一个子节点
			// if ( content.equals("") && stack.contains(qName) )
			// return;

			String columName = qName.toUpperCase();
			if (ORACLEKEYWORD.contains(columName)) {
				columName += 1;
			}

			if (qName.equalsIgnoreCase("localCellId")) {
				if (Util.isNotNull(currRncid)) {
					int rncLen = currRncid.length();
					currCi = content.substring(rncLen);
					currNode.getNetStruct().setCi(currCi);
				}
			}
			if (qName.equalsIgnoreCase("rncid")) {
				currRncid = content;
			} else if (qName.equalsIgnoreCase("ci")) {
				currCi = content;
				currNode.getNetStruct().setCi(currCi);
			} else if (qName.equalsIgnoreCase("locationAreaCode")) {
				currLac = content;
				currNode.getNetStruct().setLac(currLac);
			}

			String shortName = null;
			shortName = getShortName(currTBName, columName);
			if (shortName != null) {
				Field f = new Field(shortName, content);
				// 添加数据;
				currNode.addField(f);
				// 添加列名
				datas.addColumn(currTBName, shortName, content);
			}
			content = "";

		}

		private String makeCI(String content) {
			if (content.length() <= 5)
				return content;

			String result = "";
			int i = 0;
			while (result.length() != 5) {
				result = content.substring(i++, content.length());
			}

			return result;
		}

		@Override
		public void startDocument() throws SAXException {
			log.debug("开始读取XML文件");
			datas = new Data();
			stack = new Stack<String>();
			stack.push("snapshot");
			parents = new HashMap<String, Struct>();
		}

		@Override
		public void endDocument() throws SAXException {
			log.debug("XML文件读取结束");
			parents.clear();
			// display();
		}

		public void display() {
			Map<String, ArrayList<Node>> datasMap = datas.getDatas();
			Set<String> tableM = datasMap.keySet();
			for (String table : tableM) {

				System.out.println("---------------");
				System.out.println("table=" + table);
				for (Node node : datasMap.get(table)) {
					System.out.println("id=" + node.getId());
					System.out.println("pid=" + node.getParentId());
					System.out.println("ptableName=" + node.getPTBName());
					NetStruct ns = node.getNetStruct();
					if (ns != null) {
						System.out.println("ci=" + ns.getCi());
						System.out.println("fddcellid=" + ns.getFddCellId());
						System.out.println("lac=" + ns.getLac());
						System.out.println("nodeid=" + ns.getNodeBId());
						System.out.println("rncid=" + ns.getRncId());
					}
					System.out.print("valus=");
					for (Field f : node.getDatas()) {
						System.out.print("{" + f.getName() + ":" + f.getValue() + "}");
					}
					System.out.println("");

				}
			}

			Map<String, ArrayList<String>> columns = datas.getColumns();
			Set<String> coTB = columns.keySet();
			for (String table : coTB) {
				System.out.println("table=" + table);
				ArrayList<String> list = columns.get(table);
				System.out.print("columns{");
				for (String co : list) {
					System.out.print(co + ",");
				}
				System.out.println("}");
			}

		}

		public Data getData() {
			return this.datas;
		}

		public void clear() {
			if (datas != null)
				datas.clear();
		}
	}

	/**
	 * 每一个大节点(</GsmNeighbouringCell></FDDCell></NodeB>)的对应的值,就相当于一条记录
	 * 
	 * @author litp Aug 16, 2010
	 * @since 1.0
	 */
	class Node {

		// 父节点id
		private String parentId = "";

		// 当前节点id
		private String id = "";

		// 父节点表名
		private String pTBName = "";

		private NetStruct netStruct = null;

		// <数据，每个小节点<key,value>>
		private List<Field> datas = null;

		public Node() {
			super();
			datas = new ArrayList<Field>();
		}

		public void addField(Field data) {
			datas.add(data);
		}

		public List<Field> getDatas() {
			return datas;
		}

		public String getParentId() {
			return parentId;
		}

		public void setParentId(String parentId) {
			this.parentId = parentId;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getPTBName() {
			return pTBName;
		}

		public void setPTBName(String name) {
			pTBName = name;
		}

		public NetStruct getNetStruct() {
			return netStruct;
		}

		public void setNetStruct(NetStruct netStruct) {
			this.netStruct = netStruct;
		}

	}

	class Data {

		// 三个表</GNCell></FDDCELL></NODEB>
		public static final String suffixTN = "CLT_CM_W_AL_";

		// <表名， ArrayList<列名>>
		private Map<String, ArrayList<String>> columns = null;

		// <表名， ArrayList<表节点>>
		private Map<String, ArrayList<Node>> datas = null;

		public Map<String, ArrayList<String>> getColumns() {
			return columns;
		}

		public Map<String, ArrayList<Node>> getDatas() {
			return datas;
		}

		public Data() {
			columns = new HashMap<String, ArrayList<String>>();
			datas = new HashMap<String, ArrayList<Node>>();
		}

		// 将解析出来的原始列名添加到columns<String,List<String>>中
		public void addColumn(String tableName, String column, String content) {
			if (columns.containsKey(tableName)) {
				List<String> list = columns.get(tableName);
				if (!list.contains(column) && !content.equals("")) {
					list.add(column);
				}
			} else {
				ArrayList<String> newList = new ArrayList<String>();
				newList.add("");
				if (!content.equals(""))
					newList.add(column);
				columns.put(tableName, newList);
			}
		}

		// 将解析出来的原始数据以列名和值的形式添加到datas<String,List<Node>>中
		public void addData(String tableName, Node data) {
			if (datas.containsKey(tableName)) {
				List<Node> list = datas.get(tableName);
				list.add(data);
			} else {
				ArrayList<Node> list = new ArrayList<Node>();
				list.add(data);
				datas.put(tableName, list);
			}
		}

		public void clear() {
			if (!columns.isEmpty()) {
				columns.clear();
			}
			if (!datas.isEmpty()) {
				datas.clear();
			}
		}
	}

	/**
	 * 键值对的形式存放每一个节点的值
	 * 
	 * @author litp Aug 16, 2010
	 * @since 1.0
	 */
	class Field {

		String name;

		String value;

		public Field(String name, String value) {
			this.name = name;
			this.value = value;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}

	class Struct {

		// 网元信息
		private NetStruct netStruct;

		private Struct parent;

		private String tabName;

		private String id;

		private String qName;

		private List<Struct> child;

		public Struct getParent() {
			return parent;
		}

		public void setParent(Struct parent) {
			this.parent = parent;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getQName() {
			return qName;
		}

		public void setQName(String name) {
			qName = name;
		}

		public List<Struct> getChild() {
			return child;
		}

		public void setChild(List<Struct> child) {
			this.child = child;
		}

		public String getTabName() {
			return tabName;
		}

		public void setTabName(String tabName) {
			this.tabName = tabName;
		}

		public NetStruct getNetStruct() {
			return netStruct;
		}

		public void setNetStruct(NetStruct netStruct) {
			this.netStruct = netStruct;
		}

	}

	// 网元信息结构
	class NetStruct implements Cloneable {

		private String rncId;

		private String nodeBId;

		private String fddCellId;// 下两个属性都从此表获取

		private String ci;// <localCellId>133311782</localCellId>去掉前面的RNC标识;CI就是11782.

		private String lac;// <locationAreaCode>50513</locationAreaCode>

		public String getRncId() {
			return rncId;
		}

		public void setRncId(String rncId) {
			this.rncId = rncId;
		}

		public String getNodeBId() {
			return nodeBId;
		}

		public void setNodeBId(String nodeBId) {
			this.nodeBId = nodeBId;
		}

		public String getFddCellId() {
			return fddCellId;
		}

		public void setFddCellId(String fddCellId) {
			this.fddCellId = fddCellId;
		}

		public String getCi() {
			return ci;
		}

		public void setCi(String ci) {
			this.ci = ci;
		}

		public String getLac() {
			return lac;
		}

		public void setLac(String lac) {
			this.lac = lac;
		}

		@Override
		protected Object clone() throws CloneNotSupportedException {
			return super.clone();
		}
	}

}

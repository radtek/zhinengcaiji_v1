package parser.eric.pm;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.LogMgr;
import util.Util;

public class EricssonWPmXMLParser {

	private String omcid;

	private String stamptime;

	private String rncName;

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	private DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private final List<String> TABLES = new ArrayList<String>();

	private final Map<String, String> BUFFERS = new HashMap<String, String>();

	private final Map<String, List<String>> COLS = new HashMap<String, List<String>>();

	private final Map<String, String> FIRST_MT_NAME = new HashMap<String, String>();

	private final Map<MOID, Record> MOID_MAPS = new HashMap<MOID, Record>();

	private final Map<String, Integer> COUNT = new HashMap<String, Integer>();

	private static final String TABLE_PREFIX = "CLT_PM_W_ERIC_";

	private String subNetworkRoot = null;

	private final String subNetwork = "SubNetwork";

	private final String meContext = "MeContext";

	private String subNetworkValue = null;

	private String meContextValue = null;

	// private String nodebCell = null;

	private static final int INTERVAL = 500;// 1000;

	/**
	 * 解析入库一个文件。
	 * 
	 * @param file
	 *            原始文件绝对路径
	 * @param omcId
	 *            omc编号
	 * @param stampTime
	 *            数据时间
	 * @param taskID
	 *            任务编号
	 * @return 是否成功
	 */
	@SuppressWarnings("unchecked")
	public boolean parse(String file, int omcId, Timestamp stampTime, long taskID) {
		this.omcid = String.valueOf(omcId);
		this.stamptime = Util.getDateString(stampTime);
		SAXReader reader = new SAXReader();
		reader.setEntityResolver(new EntityResolver() {

			@Override
			public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
				return new InputSource(new ByteArrayInputStream("<?xml version='1.0' encoding='UTF-8'?>".getBytes()));
			}
		});

		Document doc = null;
		try {
			createMapTable();
			createSeq();
			doc = reader.read(new File(file));
			List<Element> mdElements = doc.selectNodes("/mdc/md");
			Element snEl = (Element) doc.selectSingleNode("/mdc/mfh/sn");
			String[] snValue = snEl.getTextTrim().split(",");

			if (snValue.length > 0) {
				subNetworkRoot = snValue[0].split("=")[1];
			}
			for (String text : snValue) {
				String[] textTem = text.split("=");
				if (subNetwork.trim().equals(textTem[0])) {
					if (textTem[0].contains("ROOT")) {
						continue;
					}
					subNetworkValue = textTem[1];
				}
				if (meContext.trim().equals(textTem[0])) {
					meContextValue = textTem[1];
				}

			}
			this.rncName = subNetworkValue;
			// if ( subNetworkValue != null && meContextValue != null )
			// {
			// if ( !subNetworkValue.equals(meContextValue) )
			// {
			// nodebCell = meContextValue;
			// }
			// }

			for (int i = 0; i < mdElements.size(); i++) {
				String tableName = null;
				Element mdEl = mdElements.get(i);
				List<Element> mts = mdEl.selectNodes("mi/mt");
				List<Element> mvs = mdEl.selectNodes("mi/mv");
				for (Element mv : mvs) {
					String strMoid = ((Element) mv.selectSingleNode("moid")).getTextTrim();
					List<Element> rs = mv.selectNodes("r");
					MOID moid = new MOID(strMoid);
					if (tableName == null) {
						tableName = TABLE_PREFIX + moid.getLastMoName().toUpperCase();
						if (tableName.length() > 30) {
							tableName = tableName.substring(0, 30);
						}
					}

					if (!FIRST_MT_NAME.containsKey(tableName)) {
						FIRST_MT_NAME.put(tableName, mts.get(0).getTextTrim());
					}

					if (!MOID_MAPS.containsKey(moid)) {
						Record record = new Record(tableName);
						for (int k = 0; k < mts.size(); k++) {
							String name = mts.get(k).getTextTrim();
							String colName = getColName(name, tableName);
							String value = rs.get(k).getTextTrim();
							record.addPair(new Pair(name, colName, value, tableName));
						}
						for (int k = 0; k < moid.pairs.size(); k++) {
							String name = moid.pairs.get(k).name;
							String colName = getColName(name, tableName);
							String value = moid.pairs.get(k).value;
							record.addPair(new Pair(name, colName, value, tableName));
						}
						MOID_MAPS.put(moid, record);
					} else {
						Record record = MOID_MAPS.get(moid);
						if (mts.get(0).getTextTrim().equals(FIRST_MT_NAME.get(tableName))) {
							MOID_MAPS.remove(moid);
						} else {
							for (int k = 0; k < mts.size(); k++) {
								String name = mts.get(k).getTextTrim();
								String colName = getColName(name, tableName);
								String value = rs.get(k).getTextTrim();
								record.addPair(new Pair(name, colName, value, tableName));
							}
						}
					}
				}
			}

			Iterator<MOID> keys = MOID_MAPS.keySet().iterator();
			List<Record> records = new ArrayList<Record>();
			while (keys.hasNext()) {
				MOID moid = keys.next();
				Record record = MOID_MAPS.get(moid);
				createTable(record);
				records.add(record);
				executeInsert(records, false);
			}
			executeInsert(records, true);
			Iterator<String> tns = COUNT.keySet().iterator();
			while (tns.hasNext()) {
				String tn = tns.next();
				int count = COUNT.get(tn);
				dbLogger.log(omcId, tn, this.stamptime, count, taskID);
			}
		} catch (Exception e) {
			logger.error("解析联通二期爱立信性能时异常", e);
			return false;
		}
		return true;
	}

	private void addCol(String tableName, String colName) {
		String sql = "alter table " + tableName + " add " + colName + " varchar2(300)";
		try {
			CommonDB.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error("增加列时异常:" + sql, e);
		}
	}

	private void executeInsert(List<Record> records, boolean b) {
		String sql = null;
		int count = 0;
		if (b || records.size() % INTERVAL == 0) {
			Connection con = DbPool.getConn();
			Statement st = null;
			try {
				con.setAutoCommit(false);
				st = con.createStatement();
				for (Record r : records) {
					handleCol(r);
					sql = r.toInsert();
					st.addBatch(sql);
					if (COUNT.containsKey(r.tableName)) {
						COUNT.put(r.tableName, COUNT.get(r.tableName) + 1);
					} else {
						COUNT.put(r.tableName, 1);
					}
					count++;
					if (count % INTERVAL == 0) {
						st.executeBatch();
					}
				}
				st.executeBatch();
			} catch (Exception e) {
				logger.error("" + sql);
				logger.error("插入数据时异常", e);
			} finally {
				try {
					records.clear();
					if (con != null) {
						con.commit();
					}
					if (st != null) {
						st.close();
					}
					if (con != null) {
						con.close();
					}
				} catch (Exception e) {
				}
			}
		}
	}

	private void handleCol(Record record) {
		String tn = record.tableName;
		if (!COLS.containsKey(tn)) {
			loadCols(tn);
		}

		List<String> cols = COLS.get(tn);
		for (Pair p : record.pairs) {
			if (!cols.contains(p.colName)) {
				addCol(tn, p.colName);
				cols.add(p.colName);
			}
		}

	}

	private boolean createTable(Record record) {
		if (TABLES.contains(record.tableName)) {
			return true;
		}
		StringBuilder sql = new StringBuilder();
		sql.append("create table ").append(record.tableName).append(" (");
		sql.append("OMCID NUMBER,COLLECTTIME DATE,STAMPTIME DATE,RNC_NAME VARCHAR2(100),SUBNETWORKROOT VARCHAR2(100),SUBNETWORK VARCHAR2(100),MECONTEXT VARCHAR2(100),");
		for (Pair p : record.pairs) {
			sql.append(p.colName).append(" VARCHAR(300),");
		}
		if (sql.charAt(sql.length() - 1) == ',') {
			sql.deleteCharAt(sql.length() - 1);
		}
		sql.append(")");
		try {
			CommonDB.executeUpdate(sql.toString());
			TABLES.add(record.tableName);
			return true;
		} catch (SQLException e) {
			if (e.getErrorCode() == 955) {
				TABLES.add(record.tableName);
				return true;
			}
			logger.error("建表时异常：" + sql, e);
			return false;
		}

	}

	private boolean createSeq() {
		String sql = "create sequence SEQ_CLT_PM_W_ERIC " + "minvalue 1 maxvalue 999999999999999999999999999" + " start with 1 increment by 1";
		try {
			CommonDB.executeUpdate(sql);
			return true;
		} catch (SQLException e) {
			if (e.getErrorCode() == 955) {
				return true;
			} else {
				logger.error("创建序列时异常，sql:" + sql, e);
				return false;
			}
		}
	}

	private boolean createMapTable() {
		String sql = "CREATE TABLE CLT_PM_W_ERIC_MAP " + " (	 COL_NAME VARCHAR2(100)," + " SHORT_COL_NAME VARCHAR2(30), "
				+ " TAB_NAME VARCHAR2(30),STAMPTIME DATE,CONSTRAINT pk_CLT_PM_W_ERIC_MAP primary key (COL_NAME, TAB_NAME) ) ";
		// + " TAB_NAME VARCHAR2(30),STAMPTIME DATE,unique (SHORT_COL_NAME,
		// TAB_NAME) ) ";
		try {
			CommonDB.executeUpdate(sql);
			return true;
		} catch (SQLException e) {
			if (e.getErrorCode() == 955) {
				return true;
			} else {
				logger.error("创建映射表时异常，sql:" + sql, e);
				return false;
			}
		}
	}

	private String getColName(String raw, String tableName) {
		if (BUFFERS.get(raw) != null) {
			return BUFFERS.get(raw);
		}

		String sql = "SELECT T.SHORT_COL_NAME FROM CLT_PM_W_ERIC_MAP T WHERE " + "UPPER(T.COL_NAME)=UPPER('%s') AND UPPER(T.TAB_NAME)=UPPER('%s')";
		sql = String.format(sql, raw, tableName);
		String sn = "";
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				sn = rs.getString(1);
			} else {
				sn = subCol(raw);
				addCol(raw, sn, tableName);
			}
			rs.close();
			BUFFERS.put(raw, sn);
		} catch (Exception e) {
			logger.error("查找列时异常:" + sql, e);
		} finally {
			if (con != null) {
				try {
					if (ps != null) {
						ps.close();
					}
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return sn;
	}

	private String subCol(String col) {
		if (col.length() <= 30) {
			return col.toUpperCase();
		}

		return "COL_" + getSeqNextval();
	}

	private void addCol(String col, String sn, String tn) {
		String sql = "INSERT INTO CLT_PM_W_ERIC_MAP " + "(COL_NAME,SHORT_COL_NAME,TAB_NAME,STAMPTIME) VALUES " + "('%s','%s','%s',sysdate)";
		sql = String.format(sql, col, sn.toUpperCase(), tn);

		try {
			CommonDB.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error("向映射表添加记录时异常", e);
		}
	}

	private void loadCols(String tableName) {
		String sql = "select * from " + tableName;
		List<String> cols = new ArrayList<String>();
		Connection con = DbPool.getConn();
		PreparedStatement st = null;
		ResultSet rs = null;
		ResultSetMetaData meta = null;
		try {
			st = con.prepareStatement(sql);
			rs = st.executeQuery();
			meta = rs.getMetaData();
			int count = meta.getColumnCount();
			for (int i = 0; i < count; i++) {
				cols.add(meta.getColumnName(i + 1));
			}
			COLS.put(tableName, cols);
		} catch (Exception e) {
			logger.error("读取列的元数据时异常：" + sql, e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
			}
		}
	}

	private int getSeqNextval() {
		String sql = "select SEQ_CLT_PM_W_ERIC.nextval from dual";

		int val = 0;

		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			rs = ps.executeQuery();
			if (rs.next()) {
				val = rs.getInt(1);
			}
		} catch (SQLException e) {
			logger.error("获取序列号时异常:" + sql, e);
			return 0;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
			}
		}

		return val;
	}

	private class MOID {

		List<Pair> pairs = new ArrayList<Pair>();

		MOID(String strMoid) {
			build(strMoid);
		}

		String getLastMoName() {
			if (pairs.size() > 0) {
				return pairs.get(pairs.size() - 1).name;
			}
			return "";
		}

		void build(String str) {
			pairs.clear();
			String[] items = str.split(",");
			for (String s : items) {
				String[] ss = s.split("=");
				pairs.add(new Pair(ss[0], ss[1]));
			}
		}

		@Override
		public String toString() {
			StringBuilder buffer = new StringBuilder();
			for (Pair p : pairs) {
				buffer.append(p.name).append("=").append(p.value).append(";");
			}
			return buffer.toString();
		}

		@Override
		public int hashCode() {
			String s = toString();
			return s.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			MOID m = (MOID) obj;
			for (int i = 0; i < m.pairs.size(); i++) {
				if (m.pairs.get(i).name.equals(pairs.get(i).name) && m.pairs.get(i).value.equals(pairs.get(i).value)) {
					continue;
				} else {
					return false;
				}
			}
			return true;
		}
	}

	private class Record {

		String tableName;

		Record(String tableName) {
			this.tableName = tableName;
		}

		List<Pair> pairs = new ArrayList<Pair>();

		void addPair(Pair p) {
			if (!pairs.contains(p)) {
				pairs.add(p);
			}
		}

		String toInsert() {
			StringBuilder sql = new StringBuilder();
			sql.append("insert into ").append(tableName).append(" (OMCID,COLLECTTIME,STAMPTIME,RNC_NAME,SUBNETWORKROOT ,SUBNETWORK ,MECONTEXT ,");

			for (Pair p : pairs) {
				sql.append(p.colName).append(",");
			}
			if (sql.charAt(sql.length() - 1) == ',') {
				sql.deleteCharAt(sql.length() - 1);
			}
			sql.append(") values (").append(omcid).append(",sysdate,to_date('").append(stamptime).append("','YYYY-MM-DD HH24:MI:SS'),'")
					.append(rncName).append("','").append(subNetworkRoot).append("','").append(subNetworkValue).append("','").append(meContextValue)
					.append("',");
			for (Pair p : pairs) {
				sql.append("'").append(p.value).append("',");
			}

			if (sql.charAt(sql.length() - 1) == ',') {
				sql.deleteCharAt(sql.length() - 1);
			}
			sql.append(")");
			return sql.toString();
		}

		@Override
		public String toString() {
			return toInsert();
		}

	}

	private class Pair {

		String name = "";

		String colName = "";

		String value;

		String tableName = "";

		Pair(String name, String value) {
			this.name = name;
			this.value = value;
		}

		Pair(String name, String colName, String value, String tableName) {
			this.name = name;
			this.colName = colName;
			this.value = value;
			this.tableName = tableName;
		}

		@Override
		public boolean equals(Object obj) {
			Pair p = (Pair) obj;
			return p.name.equalsIgnoreCase(name) && p.tableName.equalsIgnoreCase(tableName) && p.colName.equals(colName);
		}
	}

	public static void main(String[] args) {

		EricssonWPmXMLParser parser = new EricssonWPmXMLParser();
		try {
			long curr = System.currentTimeMillis();
			parser.parse(
					"C:\\Users\\ChenSijiang\\Desktop\\A20100519.1000+0800-1015+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=ECR27,MeContext=ECR27_statsfile.xml",
					111, new Timestamp(Util.getDate1("2010-04-04 12:00:00").getTime()), 989);
			// parser.parse("F:\\ftproot\\erc\\pm_nodeb\\A20100525.1930+0800-1945+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=ECR28,MeContext=WBJ94584_statsfile.xml",
			// 111, new Timestamp(Util.getDate1("2010-04-04
			// 12:00:00").getTime()));

			System.out.println((System.currentTimeMillis() - curr) / 1000);
		}

		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

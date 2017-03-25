package parser.eric.pm;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
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

/**
 * 爱立信性能解析入库。新方式，分成11张表，不自动建表建字段。
 * 
 * @author ChenSijiang 20100417
 */
public class EricssonPmParserNewImp implements EricssonPmParser {

	private String omcId;

	private String stampTime;

	private String rncName;

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	private DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private final List<String> INSERTS = new ArrayList<String>();

	private final Map<String, Pair> MOID_MAPS = new HashMap<String, Pair>();

	private final Map<String, List<String>> COLS = new HashMap<String, List<String>>();

	private final Map<String, String> BUFFERS = new HashMap<String, String>();

	private final Map<String, Integer> COUNT = new HashMap<String, Integer>();

	private static final int INTERVAL = 500;

	private static final int ATMPORT = 0;

	private static final int EUL = 1;

	private static final int GSMRELATION = 2;

	private static final int HSDSCH = 3;

	private static final int IUBLINK = 4;

	private static final int IURLINK = 5;

	private static final int LOADCONTROL = 6;

	private static final int RNCFUNCTION = 7;

	private static final int UTRANCELL = 8;

	private static final int UTRANRELATION = 9;

	private static final int VCLTP = 10;

	@SuppressWarnings("unchecked")
	@Override
	public void parse(String file, int omcId, Timestamp stampTime, int taskID) throws EricssonPmParserException {
		this.omcId = String.valueOf(omcId);
		this.stampTime = Util.getDateString(stampTime);

		SAXReader reader = new SAXReader();
		reader.setEntityResolver(new EntityResolver() {

			@Override
			public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
				return new InputSource(new ByteArrayInputStream("<?xml version='1.0' encoding='UTF-8'?>".getBytes()));
			}
		});

		Document doc = null;
		try {
			doc = reader.read(new File(file));
			List<Element> mdElements = doc.selectNodes("/mdc/md");
			Element snEl = (Element) doc.selectSingleNode("/mdc/mfh/sn");
			String[] ss = snEl.getTextTrim().split("=");
			this.rncName = ss[ss.length - 1];
			loadCols();
			for (int i = 0; i < mdElements.size(); i++) {
				Element mdEl = mdElements.get(i);
				List<Element> mtEls = mdEl.selectNodes("mi/mt");
				List<Element> mvEls = mdEl.selectNodes("mi/mv");
				if (mvEls.size() == 0) {
					continue;
				}

				List<String> mtList = new ArrayList<String>();
				List<String> rList = new ArrayList<String>();
				for (Element e : mtEls) {
					mtList.add(e.getTextTrim().toUpperCase());
				}
				for (Element e : mvEls) {
					List<Element> rEles = e.selectNodes("r");
					Element moidEl = (Element) e.selectSingleNode("moid");
					String strMoid = moidEl.getTextTrim();
					if (getNeedMoid(strMoid) < 0) {
						continue;
					}
					List<String> moidNames = new ArrayList<String>();
					List<String> moidVals = new ArrayList<String>();
					String[] items = strMoid.split(",");
					for (String s : items) {
						String name = s.split("=")[0];
						String val = s.split("=")[1];
						moidNames.add(name.trim().toUpperCase());
						moidVals.add(val.trim());
					}
					for (Element re : rEles) {
						rList.add(re.getTextTrim());
					}
					// if (!mtList.contains(moidNames.get(0))) {
					// mtList.addAll(moidNames);
					// }
					// rList.addAll(moidVals);
					if (MOID_MAPS.containsKey(strMoid)) {
						Pair p = MOID_MAPS.get(strMoid);
						if (p.moidNames.size() == 0) {
							p.moidNames.addAll(moidNames);
							p.moidValues.addAll(moidVals);
						}
					} else {
						Pair p = new Pair();
						if (p.moidNames.size() == 0) {
							p.moidNames.addAll(moidNames);
							p.moidValues.addAll(moidVals);
						}
						MOID_MAPS.put(strMoid, p);
					}
					MOID_MAPS.get(strMoid).names.addAll(mtList);
					MOID_MAPS.get(strMoid).values.addAll(rList);

					// String insert = createInsert(mtList, rList, tableName);
					// if (insert != null) {
					// INSERTS.add(insert);
					// }
					// try {
					// insert(INSERTS, false);
					// } catch (Exception ex) {
					// logger.error("插入数据出现异常", ex);
					// }

					rList.clear();
				}
			}

			String tableName = null;
			Iterator<String> moidKeys = MOID_MAPS.keySet().iterator();
			while (moidKeys.hasNext()) {
				String moidKey = moidKeys.next();
				Pair pair = MOID_MAPS.get(moidKey);
				int moidType = getNeedMoid(moidKey);
				switch (moidType) {
					case ATMPORT :
						tableName = "CLT_PM_W_ERIC_ATMPORT";
						break;
					case EUL :
						tableName = "CLT_PM_W_ERIC_EUL";
						break;
					case GSMRELATION :
						tableName = "CLT_PM_W_ERIC_GSMRELATION";
						break;
					case HSDSCH :
						tableName = "CLT_PM_W_ERIC_HSDSCH";
						break;
					case IUBLINK :
						tableName = "CLT_PM_W_ERIC_IUBLINK";
						break;
					case IURLINK :
						tableName = "CLT_PM_W_ERIC_IURLINK";
						break;
					case LOADCONTROL :
						tableName = "CLT_PM_W_ERIC_LOADCONTROL";
						break;
					case RNCFUNCTION :
						tableName = "CLT_PM_W_ERIC_RNCFUNCTION";
						break;
					case UTRANCELL :
						tableName = "CLT_PM_W_ERIC_UTRANCELL";
						break;
					case UTRANRELATION :
						tableName = "CLT_PM_W_ERIC_UTRANRELATION";
						break;
					case VCLTP :
						tableName = "CLT_PM_W_ERIC_VCLTP";
						break;
					default :
						break;
				}

				if (pair.names.size() > 0 && Util.isNotNull(pair.moidValues.get(pair.moidValues.size() - 1))) {
					pair.names.addAll(pair.moidNames);
					pair.values.addAll(pair.moidValues);
					String insert = createInsert(pair.names, pair.values, tableName);
					if (insert != null) {
						INSERTS.add(insert);
						try {
							insert(INSERTS, false);
						} catch (Exception ex) {
							logger.error("插入数据出现异常", ex);
						}
					}
				}
			}
			try {
				insert(INSERTS, false);
			} catch (Exception ex) {
				logger.error("插入数据出现异常", ex);
			}
			// try {
			// insert(INSERTS, true);
			// } catch (Exception ex) {
			// logger.error("插入数据出现异常", ex);
			// }
		} catch (Exception e) {
			throw new EricssonPmParserException("解析联通二期爱立信性能时异常", e);
		} finally {
			try {
				Iterator<String> keys = COUNT.keySet().iterator();
				while (keys.hasNext()) {
					String key = keys.next();
					dbLogger.log(omcId, key, this.stampTime, COUNT.get(key), taskID);
				}
				COUNT.clear();
			} catch (Exception e) {
			}
		}
	}

	private int getNeedMoid(String moid) {
		String[] items = moid.split(",");
		String lastMo = items[items.length - 1].split("=")[0].trim().toLowerCase();

		if (lastMo.equals("utrancell")) {
			return UTRANCELL;
		}
		if (lastMo.equals("gsmrelation")) {
			return GSMRELATION;
		}
		if (lastMo.equals("utranrelation")) {
			return UTRANRELATION;
		}
		if (lastMo.equals("eul")) {
			return EUL;
		}
		if (lastMo.equals("hsdsch")) {
			return HSDSCH;
		}
		if (lastMo.equals("vcltp")) {
			return VCLTP;
		}
		if (lastMo.equals("iurlink")) {
			return IURLINK;
		}
		if (lastMo.equals("iublink")) {
			return IUBLINK;
		}
		if (lastMo.equals("rncfunction")) {
			return RNCFUNCTION;
		}
		if (lastMo.equals("atmport")) {
			return ATMPORT;
		}
		if (lastMo.equals("loadcontrol")) {
			return LOADCONTROL;
		}
		return -1;
	}

	private String createInsert(List<String> mtList, List<String> rList, String tn) {
		if (mtList.size() != rList.size()) {
			return null;
		}
		StringBuilder insert = new StringBuilder();
		insert.append("INSERT INTO ").append(tn).append(" (OMCID,COLLECTTIME,STAMPTIME,RNCNAME,");
		List<Integer> delList = new ArrayList<Integer>();
		for (int i = 0; i < mtList.size(); i++) {
			String col = getColName(mtList.get(i));
			if (!COLS.get(tn).contains(col) || insert.indexOf("," + col + ",") > 0) {
				delList.add(i);
			} else {
				insert.append(col).append(",");
			}
		}
		if (insert.charAt(insert.length() - 1) == ',') {
			insert.deleteCharAt(insert.length() - 1);
		}
		insert.append(") VALUES ('").append(omcId).append("',SYSDATE,TO_DATE('").append(stampTime);
		insert.append("','YYYY-MM-DD HH24:MI:SS'),'").append(this.rncName).append("',");

		for (int i = 0; i < rList.size(); i++) {
			if (!delList.contains(i)) {
				String val = rList.get(i);
				insert.append("'").append(val).append("',");
			}
		}
		if (insert.charAt(insert.length() - 1) == ',') {
			insert.deleteCharAt(insert.length() - 1);
		}
		insert.append(")");
		if (insert.indexOf(",MANAGEDELEMENT)") > 0 || (insert.indexOf(",RNCFUNCTION)") > 0 && !tn.equals("CLT_PM_W_ERIC_RNCFUNCTION"))
				|| insert.indexOf(",GENERALPROCESSORUNIT)") > 0) {
			return null;
		}
		return insert.toString();
	}

	private String getColName(String raw) {
		if (raw.length() <= 30) {
			return raw;
		}

		if (BUFFERS.get(raw) != null) {
			return BUFFERS.get(raw);
		}

		String sql = "SELECT T.SHORT_COL_NAME FROM CLT_PM_W_ERIC_MAP T WHERE " + "T.COL_NAME='%s'";
		sql = String.format(sql, raw);
		String sn = "";
		Connection con = null;
		try {
			con = DbPool.getConn();
			ResultSet rs = con.prepareStatement(sql).executeQuery();
			if (rs.next()) {
				sn = rs.getString(1);
			}
			rs.close();
			BUFFERS.put(raw, sn);
		} catch (Exception e) {
			logger.error("查找列时异常:" + sql, e);
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return sn;
	}

	private void loadCols() {
		String strTbs = "clt_pm_w_eric_atmport, clt_pm_w_eric_eul, clt_pm_w_eric_gsmrelation,"
				+ " clt_pm_w_eric_hsdsch, clt_pm_w_eric_iublink, clt_pm_w_eric_iurlink,"
				+ " clt_pm_w_eric_loadcontrol, clt_pm_w_eric_rncfunction, clt_pm_w_eric_utran"
				+ "cell, clt_pm_w_eric_utranrelation, clt_pm_w_eric_vcltp".toUpperCase();
		String[] tables = strTbs.split(",");
		for (String s : tables) {
			String tn = s.trim().toUpperCase();
			Connection con = null;
			try {
				con = DbPool.getConn();
				ResultSetMetaData meta = con.prepareStatement("select * from " + tn).executeQuery().getMetaData();
				int count = meta.getColumnCount();
				for (int i = 0; i < count; i++) {
					String cn = meta.getColumnName(i + 1);
					if (COLS.containsKey(tn)) {
						List<String> list = COLS.get(tn);
						list.add(cn);
						COLS.put(tn, list);
					} else {
						List<String> list = new ArrayList<String>();
						list.add(cn);
						COLS.put(tn, list);
					}
				}
			} catch (Exception e) {
				logger.error("读取列名时异常", e);
			} finally {
				if (con != null) {
					try {
						con.close();
					} catch (SQLException e) {
					}
				}
			}
		}
	}

	private void insert(List<String> inserts, boolean insertNow) throws Exception {
		String tbname = "";
		if (insertNow || inserts.size() % INTERVAL == 0) {
			Connection con = null;
			Statement st = null;
			try {
				con = DbPool.getConn();
				con.setAutoCommit(false);

				st = con.createStatement();

				for (String sql : inserts) {
					tbname = getTableName(sql);
					st.addBatch(sql);
					// logger.debug(sql);

					if (COUNT.containsKey(tbname)) {
						COUNT.put(tbname, COUNT.get(tbname) + 1);
					} else {
						COUNT.put(tbname, 1);
					}
				}

				st.executeBatch();
			} catch (Exception e) {
				for (String s : inserts) {
					logger.debug(s);
				}
				throw e;
			} finally {
				inserts.clear();
				if (con != null) {
					con.commit();
				}
				if (st != null) {
					st.close();
				}
				if (con != null) {
					con.close();
				}
			}
		}
	}

	private String getTableName(String insert) {
		String tableName = insert.substring(insert.indexOf("INTO ") + 4, insert.indexOf(" (OMCID"));

		return tableName.trim();
	}

	private String subCol(String col) {
		if (col.length() <= 30) {
			return col;
		}

		return "COL_" + getSeqNextval();
	}

	private int getSeqNextval() {
		String sql = "select SEQ_CLT_PM_W_ERIC.nextval from dual";

		int val = 0;

		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			con = DbPool.getConn();;
			ps = con.prepareStatement(sql);
			rs = ps.executeQuery();
			if (rs.next()) {
				val = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
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

	private void addCol(String col, String sn, String tn) {
		String sql = "INSERT INTO CLT_PM_W_ERIC_MAP " + "(COL_NAME,SHORT_COL_NAME,TAB_NAME) VALUES " + "('%s','%s','%s')";
		sql = String.format(sql, col, sn, tn);

		try {
			CommonDB.executeUpdate(sql);
		} catch (SQLException e) {
			if (e.getErrorCode() != 1) {
				e.printStackTrace();
			}
		}
	}

	private void createTab(List<String> cols, String tn) {
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE ").append(tn).append(" (OMCID NUMBER,");
		sql.append("COLLECTTIME DATE,STAMPTIME DATE,");
		for (int i = 0; i < cols.size(); i++) {
			sql.append(cols.get(i)).append(" NUMBER(13,2)");
			if (i != cols.size() - 1) {
				sql.append(",");
			}
		}
		sql.append(")");

		try {
			System.out.println(sql);
			CommonDB.executeUpdate(sql.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unused")
	// 建表，测试用。
	private void makeTable() throws Exception {
		final String PREFIX = "CLT_PM_W_ERIC_";
		File file = new File("d:\\chensijiang\\要入库的moid.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		String line = null;
		String shuffix = null;
		String tn = null;
		List<String> cols = new ArrayList<String>();
		while ((line = reader.readLine()) != null) {
			line = line.trim();
			if (line.equals("")) {
				continue;
			}

			if (line.startsWith("[")) {
				if (cols.size() > 0 && tn != null) {
					createTab(cols, tn);
					cols.clear();
					tn = null;
				}
				String[] items = line.split("  ");
				shuffix = items[items.length - 1];
				shuffix = shuffix.replaceAll("]", "").trim().toUpperCase();
				tn = PREFIX + shuffix;
			} else {
				String col = line.toUpperCase().trim();
				String sn = subCol(col);
				addCol(col, sn, tn);
				cols.add(sn);
			}
		}
		if (cols.size() > 0) {
			createTab(cols, tn);
			cols.clear();
			tn = null;
		}
		reader.close();
	}

	class Pair {

		List<String> names = new ArrayList<String>();

		List<String> values = new ArrayList<String>();

		List<String> moidNames = new ArrayList<String>();

		List<String> moidValues = new ArrayList<String>();
	}

	public static void main(String[] args) {
		EricssonPmParserNewImp parser = new EricssonPmParserNewImp();
		try {
			long curr = System.currentTimeMillis();
			parser.parse("d:\\chensijiang\\A20100417.1600+0800-1615+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=DGRNC06,MeContext=DGRNC06_statsfile.xml",
					777, new Timestamp(Util.getDate1("2010-04-04 12:00:00").getTime()), 989);
			System.out.println((System.currentTimeMillis() - curr) / 1000);
		} catch (EricssonPmParserException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}

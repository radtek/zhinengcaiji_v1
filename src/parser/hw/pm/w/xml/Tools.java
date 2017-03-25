package parser.hw.pm.w.xml;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;
import framework.SystemConfig;

final class Tools {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	// 中文字段处理。
	public static final Map<String, String> ALIAS = new HashMap<String, String>();
	static {// 中文字段处理。
		ALIAS.put("邻节点标识", "AJD_NODE_ID");
		ALIAS.put("IP_PATH标识", "IP_PATH_ID");
		ALIAS.put("信令链路集索引", "SIGN_LINK_SET_INDEX");
		ALIAS.put("信令链路标识", "SIGN_LINK_ID");
		ALIAS.put("源信令点索引", "SRC_SIGN_POINT_INDEX");
		ALIAS.put("DSP编号", "DSP_ID");
		ALIAS.put("槽位号", "SLOT_ID");
		ALIAS.put("插框号", "FRAME_ID");
		ALIAS.put("INDEX", "INDEX_A");
		ALIAS.put("本地小区标识", "LOCELL");
	}

	@SuppressWarnings("unchecked")
	public static void makeTables(String source, String target) {
		try {
			SAXReader reader = new SAXReader();
			Document doc = reader.read(new File(source));
			List<Element> measInfos = doc.getRootElement().element("measData").elements("measInfo");
			PrintWriter pw = new PrintWriter(new FileOutputStream(target), true);
			for (Element e : measInfos) {
				String measInfoId = e.attributeValue("measInfoId");
				Element firstMeasValue = (Element) e.elements("measValue").get(0);
				List<MeasObjLdn> measObjLdn = parseMeasObjLdn(firstMeasValue.attributeValue("measObjLdn"));
				List<String> measTypes = parseMeasTypes(e.elementText("measTypes"));
				pw.println("CREATE TABLE \"CLT_PM_W_HW_X_" + measInfoId + "\"");
				pw.println("(");
				pw.println("\t\"OMCID\" NUMBER,");
				pw.println("\t\"COLLECTTIME\" DATE,");
				pw.println("\t\"STAMPTIME\" DATE,");
				pw.println("\t\"RNC_NAME\" VARCHAR2(100),");
				if (measObjLdn != null) {
					for (MeasObjLdn moj : measObjLdn) {
						pw.println("\t\"" + moj.name + "\" " + (Util.isOracleNumberString(moj.value) != null ? "NUMBER" : "VARCHAR2(100)") + ",");
					}
				}
				for (int i = 0; i < measTypes.size(); i++) {
					pw.print("\t\"C_" + measTypes.get(i) + "\" NUMBER");
					if (i < measTypes.size() - 1)
						pw.print(",");
					pw.println();
				}
				pw.println(");");
				pw.println();
			}
		} catch (Exception e) {
		}
	}

	public static List<String> parseMeasTypes(String measTypes) {
		if (Util.isNull(measTypes))
			return null;

		try {
			String[] splited = measTypes.split(" ");
			List<String> result = new ArrayList<String>();
			for (String s : splited) {
				if (Util.isNotNull(s))
					result.add(s.trim());
			}
			return result;
		} catch (Exception e) {
			return null;
		}

	}

	static class MeasObjLdn {

		String name;

		String value;

		public MeasObjLdn(String name, String value) {
			super();
			this.name = name;
			this.value = value;
		}

		@Override
		public String toString() {
			return "MeasObjLdn [name=" + name + ", value=" + value + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			result = prime * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MeasObjLdn other = (MeasObjLdn) obj;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			if (value == null) {
				if (other.value != null)
					return false;
			} else if (!value.equals(other.value))
				return false;
			return true;
		}

	}

	public static List<MeasObjLdn> parseMeasObjLdn(String measObjLdn) {
		if (Util.isNull(measObjLdn))
			return null;

		List<MeasObjLdn> result = null;

		int colonIndex = measObjLdn.indexOf(":");
		if (colonIndex < 0)
			return null;

		String mainContent = measObjLdn.substring(colonIndex + 1, measObjLdn.length());
		try {
			String[] splited = mainContent.split(",");
			result = new ArrayList<MeasObjLdn>();
			for (String s : splited) {
				String[] items = s.split("=");
				String name = items[0].trim().replace(" ", "_").toUpperCase();
				String value = items.length == 1 || items[1] == null ? "" : items[1].trim();
				if (name.equals("CELLID")) {
					if (value.toUpperCase().contains("MCC:") && value.toUpperCase().contains("MNC:")) {
						String[] ss = value.split("/");
						for (String x : ss) {
							if (x.split(":").length == 2 && x.split(":")[0].equals("MCC")) {
								MeasObjLdn mol = new MeasObjLdn("MCC", x.split(":")[1]);
								if (!result.contains(mol))
									result.add(mol);
							} else if (x.split(":").length == 4 && x.split(":")[0].equals("MNC")) {
								MeasObjLdn mol = new MeasObjLdn("MNC", x.split(":")[1]);
								if (!result.contains(mol))
									result.add(mol);
								mol = new MeasObjLdn("GSM_LAC", x.split(":")[2]);
								if (!result.contains(mol))
									result.add(mol);
								mol = new MeasObjLdn("GSM_CI", x.split(":")[3]);
								if (!result.contains(mol))
									result.add(mol);
							}
						}

					}
					// 3071/BSC6810(R11+):3009/DEST Cell ID:1002
					if (value.contains("DEST Cell ID")) {
						try {
							String dcid = value.substring(value.lastIndexOf(":") + 1);
							result.add(new MeasObjLdn("DEST_CELL_ID", dcid));
							String drncid = value.substring(value.indexOf(":") + 1, value.indexOf("/DEST Cell ID"));
							result.add(new MeasObjLdn("DEST_RNC_ID", drncid));
						} catch (Exception e) {
						}
					}

					int i = value.indexOf("/");
					if (i > -1)
						value = value.substring(0, i);

				}
				result.add(new MeasObjLdn(name, value));
			}
		} catch (Exception e) {
			logger.error("分拆MeasObjLdn出错：", e);
			return null;
		}

		return result;
	}

	public static List<String> parseMeasResults(String measResults) {
		if (Util.isNull(measResults))
			return null;

		try {
			List<String> result = new ArrayList<String>();
			String[] splited = measResults.split(" ");
			for (String s : splited) {
				if (Util.isNotNull(s))
					result.add(s.trim());
				else
					result.add("");
			}
			return result;
		} catch (Exception e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static Map<String, String> readTemplet(int parseTempletId) {
		Connection con = DbPool.getConn();
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			String sql = "select tempfilename  from igp_conf_templet where tmpid = ?";
			st = con.prepareStatement(sql);
			st.setInt(1, parseTempletId);
			rs = st.executeQuery();
			if (rs.next()) {
				String templetFilename = rs.getString("tempfilename");
				if (Util.isNull(templetFilename))
					throw new Exception("模板表中的tempfilename为空,tmpid=" + parseTempletId);
				templetFilename = templetFilename.trim();
				File f = new File(SystemConfig.getInstance().getTempletPath(), templetFilename);
				SAXReader reader = new SAXReader();
				Document doc = reader.read(f);
				List<Element> collects = doc.getRootElement().elements("collect");
				Map<String, String> map = new HashMap<String, String>();
				for (Element e : collects) {
					String cn = e.attributeValue("measInfoId");
					String tn = e.attributeValue("tableName");
					if (Util.isNotNull(cn) && Util.isNotNull(tn))
						map.put(cn.trim().toUpperCase(), tn.trim().toUpperCase());
				}
				return map;
			} else {
				throw new Exception("找不到模板,tmpid=" + parseTempletId);
			}
		} catch (Exception e) {
			logger.error("读取解析模板时出错,tmpid=" + parseTempletId, e);
		} finally {
			CommonDB.closeDBConnection(con, st, rs);
		}
		return null;
	}

	private static final Map<String, List<String>> CACHED_COLS = new HashMap<String, List<String>>();

	public synchronized static void loadTableCols(Collection<String> tables, Map<String, List<String>> tableCols) throws Exception {
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		ResultSetMetaData meta = null;
		try {
			for (String tb : tables) {
				if (CACHED_COLS.containsKey(tb))
					tableCols.put(tb, CACHED_COLS.get(tb));
				else {
					if (con == null)
						con = DbPool.getConn();
					String sql = "select * from " + tb + " where 1=2";
					st = con.prepareStatement(sql);
					rs = st.executeQuery();
					meta = rs.getMetaData();
					int count = meta.getColumnCount();
					List<String> cols = new ArrayList<String>();
					for (int i = 0; i < count; i++) {
						String col = meta.getColumnName(i + 1).toUpperCase();
						cols.add(col);
					}
					CACHED_COLS.put(tb, cols);
					tableCols.put(tb, cols);
				}
			}
		} finally {
			CommonDB.closeDBConnection(con, st, rs);
		}
	}

	private Tools() {
	}

	public static void main(String[] args) {
		System.out.println(parseMeasObjLdn("NCRNC1/UCELL_NCELL:Label=WDHCX锦江之星(南京西路店)_1, CellID=3071/BSC6810(R11+):3009/DEST Cell ID:1002"));

	}
}

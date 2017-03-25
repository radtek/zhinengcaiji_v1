package parser.hw.cm.w;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;
import framework.SystemConfig;

/**
 * 工具类，针对华为wcdma参数文件，XML方式。
 * 
 * @author ChenSijiang 2011-1-4 下午04:51:01
 */
public class Tools {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	/**
	 * 创建建表语句。
	 * 
	 * @param source
	 *            原始文件路径
	 * @param target
	 *            生成的建表语句文件路径
	 * @throws Exception
	 *             发生任何异常时
	 */
	@SuppressWarnings("unchecked")
	public static void makeTables(String source, String target) throws Exception {
		InputStream in = new FileInputStream(source);
		PrintWriter pw = new PrintWriter(new FileOutputStream(target), false);

		Set<String> tables = new HashSet<String>();

		SAXReader reader = new SAXReader();

		Document doc = reader.read(in);
		List<Element> mos = doc.getRootElement().element("MO").elements("MO");
		for (Element mo : mos) {
			String classname = mo.attributeValue("className");
			if (tables.contains(classname)) {
				continue;
			} else {
				System.out.println(classname);
				tables.add(classname);
				pw.println("CREATE TABLE " + classname.toUpperCase());
				pw.println("(");
				pw.println("\tOMCID NUMBER,");
				pw.println("\tCOLLECTTIME DATE,");
				pw.println("\tSTAMPTIME DATE,");
				pw.println("\tRNC_NAME VARCHAR2(100),");
				List<Element> attrs = mo.elements("attr");
				for (int i = 0; i < attrs.size(); i++) {
					Element attr = attrs.get(i);
					pw.print("\t" + attr.attributeValue("name").toUpperCase() + " ");
					String v = attr.getTextTrim();
					if (Util.isOracleNumberString(v) != null)
						pw.print("NUMBER");
					else
						pw.print("VARCHAR2(100)");
					if (i < attrs.size() - 1) {
						pw.print(",");
					}
					pw.println();
				}
				pw.println(");");
				pw.println();
				pw.flush();
			}
		}

		in.close();
		pw.close();
	}

	/**
	 * 从className="BSC6810R11AAL2PATH"这样的XML属性中提取出表名。但需要className="BSC6810R11NE" 这样的NE级别className作为参造。 "BSC6810R11"是设备版本号，之后的字符，是表名。
	 * 
	 * @param moClassName
	 *            MO级别的className
	 * @param neClassName
	 *            NE级别的className
	 * @return 表名，如果提取失败，则返回<code>null</code>.
	 */
	public static String getTableNameFromMOClassName(String moClassName, String neClassName) {
		if (Util.isNull(moClassName) || Util.isNull(neClassName))
			return null;

		try {
			int neIndex = neClassName.lastIndexOf("NE");
			String version = neClassName.substring(0, neIndex);
			return moClassName.replace(version, "");
		} catch (Exception e) {
			logger.error(String.format("提取表名时异常,moClassName=%s,neClassName=%s", moClassName, neClassName), e);
		}
		return null;
	}

	private static final Map<String, List<String>> CACHED_COLS = new HashMap<String, List<String>>();

	/**
	 * 加载采集表的列名。
	 * 
	 * @param tables
	 *            采集表集合。
	 * @param tableCols
	 *            表名-列映射。
	 * @throws Exception
	 *             发生任何异常时。
	 */
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
					st = con.prepareStatement("select * from " + tb + " where 1=2");
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
					cols.remove("OMCID");
					cols.remove("STAMPTIME");
					cols.remove("COLLECTTIME");
					cols.add(0, "OMCID");
					cols.add(1, "COLLECTTIME");
					cols.add(2, "STAMPTIME");
				}
			}
		} finally {
			CommonDB.closeDBConnection(con, st, rs);
		}
	}

	/**
	 * 读取模板中的className-表名对应关系，如果读取失败，则返回<code>null</code>.
	 * 
	 * @param parseTempletId
	 *            模板ID.
	 * @return className-表名对应关系。
	 */
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
					String cn = e.attributeValue("className");
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

	public static void main(String[] args) throws Exception {
		// Map<String, String> map = readTemplet(20110104);
		// System.out.println(map);

		makeTables("D:\\chensj_20110107\\河南二期网优平台接口-华为\\河南华为参数\\RNC\\CMExport_ZZR33(RNC10)_172.21.33.22_2011010323.xml", "c:\\rnc.txt");
		makeTables("D:\\chensj_20110107\\河南二期网优平台接口-华为\\河南华为参数\\基站\\CMExport_ZZWH1403_192.168.60.188_2011010323.xml", "c:\\nodeb.txt");

		// makeTables("E:\\资料\\解析\\hw\\河南二期网优平台接口-华为\\河南华为参数\\基站\\CMExport_ZZWH1403_192.168.60.188_2011010323.xml",
		// "E:\\资料\\解析\\hw\\河南二期网优平台接口-华为\\河南华为参数\\tables_nodeb.txt");
		// System.out.println(getTableNameFromMOClassName("NodeBAAL2Path",
		// "NodeBNE"));
	}
}

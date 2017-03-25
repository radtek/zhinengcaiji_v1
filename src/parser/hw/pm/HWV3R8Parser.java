package parser.hw.pm;

import java.io.File;
import java.io.FileInputStream;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import task.CollectObjInfo;
import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.LogMgr;
import util.Util;
import util.dbf.DBFField;
import util.dbf.DBFReader;
import framework.SystemConfig;

/**
 * 华为V3R8解析器 HW_V3R8_Parser
 * 
 * @author ChenSijiang 2010-4-12
 */
public class HWV3R8Parser {

	private CollectObjInfo info;

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private int count;

	public static final int INTERVAL = 50;

	public HWV3R8Parser(CollectObjInfo info) {
		this.info = info;
	}

	public HWV3R8Parser() {

	}

	public void setInfo(CollectObjInfo info) {
		this.info = info;
	}

	/**
	 * 解析并入库一个DBF文件
	 * 
	 * @param file
	 *            要解析的文件
	 * @return 是否成功
	 */
	public boolean parser(File file) {
		TempInfo tempInfo = getTempInfo();

		if (tempInfo == null) {
			return false;
		}

		List<String> inserts = new ArrayList<String>();
		DBFReader reader = null;
		FileInputStream fis = null;
		try {
			Table t = tempInfo.findTable(file.getName());
			if (t == null) {
				logger.error("未能在模板中找到此文件的配置信息:" + file.getAbsolutePath());
				return false;
			}
			if (!file.exists()) {
				logger.error(info.getTaskID() + "-文件不存在:" + file.getAbsolutePath());
				String tn = t.name.toUpperCase();
				dbLogger.log(info.getDevInfo().getOmcID(), tn, info.getLastCollectTime(), -1, info.getTaskID());
				return false;
			}
			fis = new FileInputStream(file);
			reader = new DBFReader(fis);
			Object[] rowObjects = null;
			String tableName = t.name;
			while ((rowObjects = reader.nextRecord()) != null) {
				String insert = createInsert(t, reader, rowObjects);
				if (Util.isNotNull(insert)) {
					inserts.add(insert);
					try {
						insert(inserts, false, tableName);
						// count++;
					} catch (Exception e) {
						logger.error("批量提交时异常", e);
					}
				}
			}
			insert(inserts, true, tableName);
			dbLogger.log(info.getDevInfo().getOmcID(), tableName, info.getLastCollectTime(), count, info.getTaskID());
		} catch (Exception e) {
			logger.error("解析华为V3R8文件时异常:" + file.getAbsolutePath(), e);
			return false;
		} finally {
			IOUtils.closeQuietly(fis);
		}
		return true;
	}

	private TempInfo getTempInfo() {
		TempInfo t = null;

		String sql = "SELECT TEMPFILENAME FROM IGP_CONF_TEMPLET WHERE TMPID = ?";
		ResultSet rs = null;
		PreparedStatement st = null;
		Connection con = DbPool.getConn();
		try {
			st = con.prepareStatement(sql);
			st.setInt(1, info.getDisTmpID());
			rs = st.executeQuery();
			if (rs.next()) {
				String fileName = rs.getString("TEMPFILENAME");
				if (Util.isNull(fileName)) {
					throw new Exception("模板文件名为空，SQL:" + sql);
				}
				File f = new File(SystemConfig.getInstance().getTempletPath(), fileName);
				if (!f.exists()) {
					throw new Exception("模板文件不存在，模板目录：" + SystemConfig.getInstance().getTempletPath() + "，模板文件名：" + fileName);
				}
				t = TempInfo.getInstance(f.getAbsolutePath());
			} else {
				throw new Exception("模板记录未找到，TMPID=" + info.getDisTmpID());
			}
		} catch (Exception e) {
			logger.error("查找模板时异常:" + sql, e);
		} finally {
			CommonDB.close(rs, st, con);
		}
		return t;
	}

	private void insert(List<String> inserts, boolean insertNow, String tbname) throws Exception {
		if (insertNow || inserts.size() % INTERVAL == 0) {
			Connection con = null;
			Statement st = null;
			try {
				con = DbPool.getConn();
				con.setAutoCommit(false);

				st = con.createStatement();

				for (String sql : inserts) {
					st.addBatch(sql);
					// logger.debug(sql);
					count++;
				}

				st.executeBatch();
				con.commit();
			} catch (Exception e) {
				if (e instanceof BatchUpdateException) {
					BatchUpdateException bue = (BatchUpdateException) e;
					if (bue.getErrorCode() == 17081) {
						return;
					}
				}
				logger.error("插入数据出现异常，表：" + tbname);
				throw e;
			} finally {
				CommonDB.close(null, st, con);
				inserts.clear();
			}
		}
	}

	private String createInsert(Table t, DBFReader reader, Object[] records) {

		String tableName = t.name.toUpperCase();

		StringBuilder insert = new StringBuilder();

		try {
			insert.append("INSERT INTO ").append(tableName).append(" (OMCID,COLLECTTIME,STAMPTIME,");

			List<Field> fields = t.fields;
			for (int i = 0; i < reader.getFieldCount(); i++) {
				DBFField dbfField = reader.getField(i);
				if (fields.contains(new Field(0, dbfField.getName(), 0, null))) {
					insert.append(dbfField.getName()).append(",");
				}
			}
			insert.deleteCharAt(insert.length() - 1);
			insert.append(") VALUES (").append(info.getDevInfo().getOmcID());
			insert.append(",SYSDATE,TO_DATE('").append(Util.getDateString(info.getLastCollectTime())).append("'");
			insert.append(",'YYYY-MM-DD HH24:MI:SS'),");
			for (int i = 0; i < reader.getFieldCount(); i++) {
				DBFField dbfField = reader.getField(i);
				if (fields.contains(new Field(0, dbfField.getName(), 0, null))) {
					String val = records[i] == null ? "" : records[i].toString().trim();
					val = val.equalsIgnoreCase("FFFB") ? "" : val;
					val = new String(val.getBytes("iso_8859-1"), "gbk");
					insert.append("'").append(val).append("'").append(",");
				}
			}
			insert.deleteCharAt(insert.length() - 1);
			insert.append(")");
		} catch (Exception e) {
			logger.error("生成SQL语句时发生异常", e);
			return null;
		}

		return insert.toString();
	}

	public static void main(String[] args) {
		HWV3R8Parser p = new HWV3R8Parser();
		p.parser(new File("E:\\资料\\解析\\huawei\\华为v3r8\\2-23华为v3r8原始数据\\BSCCELL.DBF"));
	}
}

class TempInfo {

	List<Table> tables = new ArrayList<Table>();

	static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private TempInfo() {
	}

	public Table findTable(String index) {
		for (Table t : tables) {
			if (t.index.trim().equalsIgnoreCase(index.trim())) {
				return t;
			}
		}
		return null;
	}

	static synchronized TempInfo getInstance(String file) {
		Document doc = null;
		TempInfo tempInfo = null;
		try {
			doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new File(file));
			tempInfo = new TempInfo();

			Element root = doc.getDocumentElement();
			NodeList datatablesList = root.getElementsByTagName("DATATABLE");
			for (int i = 0; i < datatablesList.getLength(); i++) {
				Element te = (Element) datatablesList.item(i);
				Table tab = new Table();
				tab.index = getStrVal(te, "TABLEINDEX");
				tab.name = getStrVal(te, "TABLENAME");
				Element fieldsElement = (Element) te.getElementsByTagName("FIELDS").item(0);
				NodeList fieldItemsList = fieldsElement.getElementsByTagName("FIELDITEM");
				for (int j = 0; j < fieldItemsList.getLength(); j++) {
					Element fe = (Element) fieldItemsList.item(j);

					tab.fields.add(new Field(getIntVal(fe, "FIELDINDEX"), getStrVal(fe, "FIELDNAME"), getIntVal(fe, "DATATYPE"), getStrVal(fe,
							"DATATIMEFORMAT")));
				}
				tempInfo.tables.add(tab);
			}

		} catch (Exception e) {
			logger.error("解析模板时异常:" + file, e);
			return null;
		}
		return tempInfo;
	}

	private static int getIntVal(Element e, String tagName) throws Exception {
		return Integer.parseInt(getStrVal(e, tagName));
	}

	private static String getStrVal(Element e, String tagName) throws Exception {
		return ((Element) e.getElementsByTagName(tagName).item(0)).getTextContent().trim();
	}

}

class Table {

	String index;

	String name;

	List<Field> fields = new ArrayList<Field>();

	@Override
	public boolean equals(Object obj) {
		Table t = (Table) obj;
		return t.index.equalsIgnoreCase(index);
	}
}

class Field {

	int index;

	String name;

	int dataType;

	String formart;

	public Field(int index, String name, int dataType, String formart) {
		this.index = index;
		this.name = name;
		this.dataType = dataType;
		this.formart = formart;
	}

	@Override
	public boolean equals(Object obj) {
		Field f = (Field) obj;
		return f.name.equalsIgnoreCase(name);
	}
}

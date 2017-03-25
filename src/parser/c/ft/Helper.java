package parser.c.ft;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;
import framework.SystemConfig;

final class Helper {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	public static Map<String/* 文件匹配字符串 */, FTTemplet> readTemplet(int tmpId) throws Exception {
		Map<String, FTTemplet> tmp = new HashMap<String, FTTemplet>();

		String sql = "select tempfilename from igp_conf_templet where tmpid=?";

		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String tempfilename = null;
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			ps.setInt(1, tmpId);
			rs = ps.executeQuery();

			if (rs.next()) {
				tempfilename = rs.getString("tempfilename");
			} else
				throw new Exception("模板记录不存在，tmpid=" + tmpId);
		} finally {
			CommonDB.close(rs, ps, con);
		}

		if (Util.isNull(tempfilename))
			throw new Exception("模板文件名为空，tmpid=" + tmpId + "， tempfilename=" + tempfilename);

		File file = new File(SystemConfig.getInstance().getTempletPath(), tempfilename);
		if (!file.exists() || !file.isFile())
			throw new Exception("模板文件不存在，或不是文件，tmpid=" + tmpId + ", 文件=" + file.getAbsolutePath());

		SAXReader r = new SAXReader();
		Document doc = r.read(file);

		List<Element> ts = doc.getRootElement().elements("templet");

		for (Element t : ts) {
			SortedMap<Integer, FTField> fields = new TreeMap<Integer, FTField>();
			FTTemplet ft = new FTTemplet(t.attributeValue("file"), t.attributeValue("table").toUpperCase(), fields);
			ft.setRowSep(t.attributeValue("rowSep"));
			ft.setFieldSep(t.attributeValue("fieldSep"));
			List<Element> fs = t.elements("field");
			for (Element f : fs)
				fields.put(Integer.parseInt(f.attributeValue("index")),
						new FTField(f.attributeValue("name").toUpperCase(), Util.nvl(f.attributeValue("type"), "").trim().toUpperCase()));
			tmp.put(ft.getFilePattern(), ft);
		}

		return tmp;
	}

	private Helper() {
		super();
	}

	public static void main(String[] args) {
		try {
			System.out.println(readTemplet(11040801));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

class FTTemplet {

	String filePattern;

	String table;

	SortedMap<Integer/* 字段在原始文件中的索引位置 */, FTField/* 对应的列 */> fields;

	String rowSep;

	String fieldSep;

	public FTTemplet(String filePattern, String table, SortedMap<Integer, FTField> fields) {
		super();
		this.filePattern = filePattern;
		this.table = table;
		this.fields = fields;
	}

	public String getFilePattern() {
		return filePattern;
	}

	public String getTable() {
		return table;
	}

	public SortedMap<Integer, FTField> getFields() {
		return fields;
	}

	@Override
	public String toString() {
		return "Templet [fields=" + fields + ", filePattern=" + filePattern + ", table=" + table + "]";
	}

	public String getRowSep() {
		return rowSep;
	}

	public void setRowSep(String rowSep) {
		this.rowSep = rowSep;
	}

	public String getFieldSep() {
		return fieldSep;
	}

	public void setFieldSep(String fieldSep) {
		this.fieldSep = fieldSep;
	}

}

class FTField {

	String name;

	String type;

	public FTField(String name, String type) {
		super();
		this.name = name;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	@Override
	public String toString() {
		return getName();
	}

}

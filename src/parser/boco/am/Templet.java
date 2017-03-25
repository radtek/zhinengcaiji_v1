package parser.boco.am;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import util.CommonDB;
import util.DbPool;
import util.Util;
import framework.SystemConfig;

public class Templet {

	public List<String> gsmOmcIdIncludeList = new ArrayList<String>();

	public List<String> gsmOmcIdExcludeList = new ArrayList<String>();

	public List<String> wcdmaOmcIdIncludeList = new ArrayList<String>();

	public List<String> wcdmaOmcIdExcludeList = new ArrayList<String>();

	public String gsmTable;

	public String wcdmaTable;

	public String gsmDbUser;

	public String wcdmaDbUser;

	public String gsmDbPwd;

	public String wcdmaDbPwd;

	public String gsmDbUrl;

	public String wcdmaDbUrl;

	public List<TField> gsmFields = new ArrayList<TField>();

	public List<TField> wcdmaFields = new ArrayList<TField>();

	public String gsmSQL;

	public String wcdmaSQL;

	private int omcid;

	public void parse(int tmpid, int omcid) throws Exception {
		this.omcid = omcid;
		Connection con = null;
		ResultSet rs = null;
		Statement st = null;
		try {
			con = DbPool.getConn();
			st = con.createStatement();
			rs = st.executeQuery("select tempfilename from igp_conf_templet where tmpid=" + tmpid);
			if (rs.next()) {
				String filename = rs.getString("tempfilename");
				File file = new File(SystemConfig.getInstance().getTempletPath() + File.separator + filename);
				Document doc = new SAXReader().read(file);
				Element root = doc.getRootElement();
				Element gsm = root.element("gsm");
				Element wcdma = root.element("wcdma");
				String tmpExclude = find(gsm, "omc_id", "exclude");
				String tmpInclude = null;
				if (Util.isNull(tmpExclude)) {
					tmpInclude = find(gsm, "omc_id", "include");
					toList(this.gsmOmcIdIncludeList, tmpInclude);
				} else {
					toList(this.gsmOmcIdExcludeList, tmpExclude);
				}
				tmpExclude = find(wcdma, "omc_id", "exclude");
				tmpInclude = null;
				if (Util.isNull(tmpExclude)) {
					tmpInclude = find(wcdma, "omc_id", "include");
					toList(this.wcdmaOmcIdIncludeList, tmpInclude);
				} else {
					toList(this.wcdmaOmcIdExcludeList, tmpExclude);
				}

				this.gsmDbUrl = find(gsm, "dburl");
				this.wcdmaDbUrl = find(wcdma, "dburl");

				this.gsmDbUser = find(gsm, "dbuser");
				this.wcdmaDbUser = find(wcdma, "dbuser");

				this.gsmDbPwd = find(gsm, "dbpwd");
				this.wcdmaDbPwd = find(wcdma, "dbpwd");

				this.gsmTable = find(gsm, "table");
				this.wcdmaTable = find(wcdma, "table");

				loadTList(this.gsmFields, gsm.element("fields"));
				loadTList(this.wcdmaFields, wcdma.element("fields"));

				this.gsmSQL = createSQL(this.gsmFields, this.gsmTable);
				this.wcdmaSQL = createSQL(this.wcdmaFields, this.wcdmaTable);
			} else {
				throw new Exception("igp_conf_templet中没有tmpid为" + tmpid + "的记录。");
			}
		} finally {
			CommonDB.close(rs, st, con);
		}
	}

	String createSQL(List<TField> fields, String table) {
		if (fields == null || fields.isEmpty() || Util.isNull(table))
			return "";

		StringBuilder buff = new StringBuilder();
		buff.append("insert into ").append(table).append(" (omcid,collecttime,stamptime,");
		for (int i = 0; i < fields.size(); i++) {
			buff.append(fields.get(i).name);
			if (i < fields.size() - 1)
				buff.append(",");
		}
		buff.append(") values (").append(this.omcid).append(",sysdate,trunc(sysdate,'hh24'),");
		for (int i = 0; i < fields.size(); i++) {
			buff.append("?");
			if (i < fields.size() - 1)
				buff.append(",");
		}
		buff.append(")");
		return buff.toString();
	}

	String find(Element el, String subEl, String attName) {
		if (attName != null) {
			return el.element(subEl).attributeValue(attName);
		} else {
			return el.element(subEl).getText();
		}
	}

	String find(Element el, String subEl) {
		return this.find(el, subEl, null);
	}

	void toList(List<String> outList, String content) {
		if (outList == null || Util.isNull(content))
			return;
		String[] sp = content.split(",");
		for (String s : sp) {
			if (Util.isNotNull(s))
				outList.add(s.trim());
		}
	}

	void loadTList(List<TField> outlist, Element fieldsEL) {
		if (outlist == null || fieldsEL == null)
			return;

		List<Element> fieldList = fieldsEL.elements("field");
		for (Element el : fieldList) {
			outlist.add(new TField(Integer.parseInt(el.attributeValue("index")), el.attributeValue("name").toLowerCase(), el
					.attributeValue("dateFormat")));
		}
	}

	@Override
	public String toString() {
		return "Templet [gsmOmcIdIncludeList=" + gsmOmcIdIncludeList + ", gsmOmcIdExcludeList=" + gsmOmcIdExcludeList + ", wcdmaOmcIdIncludeList="
				+ wcdmaOmcIdIncludeList + ", wcdmaOmcIdExcludeList=" + wcdmaOmcIdExcludeList + ", gsmTable=" + gsmTable + ", wcdmaTable="
				+ wcdmaTable + ", gsmDbUser=" + gsmDbUser + ", wcdmaDbUser=" + wcdmaDbUser + ", gsmDbPwd=" + gsmDbPwd + ", wcdmaDbPwd=" + wcdmaDbPwd
				+ ", gsmDbUrl=" + gsmDbUrl + ", wcdmaDbUrl=" + wcdmaDbUrl + ", gsmSQL=" + gsmSQL + ", wcdmaSQL=" + wcdmaSQL + "]";
	}

	public static void main(String[] args) throws Exception {
		Templet t = new Templet();
		t.parse(20120912, 808);
		System.out.println(t);
	}
}

package parser.dt.dingli201;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;
import framework.SystemConfig;

public final class Tools {

	// static int timeModif = 0;

	static int timezone = 0;

	public static Map<String, String> readTemplet(int tmpId) throws Exception {
		Map<String, String> templet = new HashMap<String, String>();

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

		List<Element> collects = doc.getRootElement().elements("collect");
		try {
			// timeModif = Integer.parseInt(doc.getRootElement().attributeValue("timeModif"));
			timezone = Integer.parseInt(doc.getRootElement().attributeValue("timezone"));
		} catch (NumberFormatException e) {
		}
		for (Element e : collects)
			templet.put(e.attributeValue("key").toUpperCase().trim(), e.attributeValue("table").toUpperCase().trim());

		return templet;
	}

	public static void insertCalLog(String rcuId, Timestamp stamp, int state) {
		Connection con = null;
		PreparedStatement ps = null;
		String sql = "insert into mod_dt_collect_state (rcu_id,stamp,state) values (?,?,?)";
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			ps.setString(1, rcuId);
			ps.setTimestamp(2, stamp);
			ps.setInt(3, state);
			ps.executeUpdate();
		} catch (Exception e) {
			LogMgr.getInstance().getSystemLogger().error("记录MOD_DT_COLLECT_STATE日志时异常（sql - " + sql + "）", e);
		} finally {
			CommonDB.close(null, ps, con);
		}
	}

	private Tools() {
	}
}

package parser.al.w.pm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.DbPool;
import util.LogMgr;

final class Helper {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	/**
	 * 解析sn节点中的内容，返回结果为String数据，长度为3，第0个内容为rnc_name，第1个内容为subnetwork， 第2个内容为managedelement.
	 */
	public static String[] parseSN(String sn) {
		String[] ret = new String[]{"", "", ""};
		String[] sp0 = sn.split(",");
		for (String s0 : sp0) {
			String[] sp1 = s0.split("=");
			if (sp1[0].equals("subNetwork") || sp1[0].equals("SubNetwork")) {
				/* sn中有两个subNetwork，第一个是rnc_name，第二个是subnetwork */
				if (ret[0].isEmpty())
					ret[0] = sp1[1];
				else
					ret[1] = sp1[1];
			} else if (sp1[0].equals("ManagedElement")) {
				ret[2] = sp1[1];
			}
		}
		return ret;
	}

	public static void loadRealCols(Map<String, Map<String, String>> map, Map<String, List<String>> realCols) throws Exception {
		for (String tn : map.keySet()) {
			if (!realCols.containsKey(tn))
				realCols.put(tn, CommonDB.loadCols(tn));
		}
	}

	/** 加载clt_pm_w_al_map表。 */
	public static void loadMap(Map<String, Map<String, String>> map) throws Exception {
		/* 此语句关联到了user_tab_columns系统表，确保查出来的表名与列名是确实在库中存在的。 */
		String sql = "SELECT m.col_name       AS counter_name,\n" + "       m.short_col_name AS col_name,\n"
				+ "       m.tab_name       AS table_name\n" + "  FROM user_tab_columns c\n" + "  left join clt_pm_w_al_map m\n"
				+ "    ON c.table_name = m.tab_name\n" + "   AND c.column_name = Upper(m.short_col_name)\n" + " WHERE m.col_name IS NOT NULL\n"
				+ "   AND m.short_col_name IS NOT NULL\n" + "   AND m.tab_name IS NOT NULL";

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			st = con.prepareStatement(sql);
			log.debug("正在查询W网阿朗性能采集表列信息，SQL语句：\n" + sql);
			rs = st.executeQuery();
			while (rs.next()) {
				String colName = rs.getString("counter_name");
				String shortColName = rs.getString("col_name").toUpperCase();
				String tabName = rs.getString("table_name").toUpperCase();
				Map<String, String> subMap = null;
				if (map.containsKey(tabName)) {
					subMap = map.get(tabName);
				} else {
					subMap = new HashMap<String, String>();
					map.put(tabName, subMap);
				}
				subMap.put(colName, shortColName);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			log.debug("查询W网阿朗性能采集表列信息完毕。");
			CommonDB.close(rs, st, con);
		}
	}

	public static List<String[]> parseMOID(String moid) {
		String[] sp0 = moid.split(",");
		List<String[]> list = new ArrayList<String[]>();
		for (String s : sp0) {
			list.add(s.split("="));
		}
		return list;
	}

	public static String findByName(List<String[]> list, String name) {
		for (String[] arr : list) {
			if (arr[0].equalsIgnoreCase(name))
				return arr[1];
		}
		return "";
	}

	public static void main(String[] args) {
		System.out.println(parseMOID("NodeBEquipment=0,Board=723457,RRH=1"));
	}
}

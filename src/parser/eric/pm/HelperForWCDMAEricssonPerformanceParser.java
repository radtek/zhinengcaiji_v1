package parser.eric.pm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import parser.eric.pm.WCDMAEricssonPerformanceParser.Record;
import util.CommonDB;
import util.DbPool;
import util.Util;

public final class HelperForWCDMAEricssonPerformanceParser {

	/* 加载clt_pm_w_eric_map表 */
	public static void loadMappingTable(Map<String, Map<String, String>> map) throws Exception {

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			String sql = "select col_name,short_col_name,tab_name from clt_pm_w_eric_map";
			st = con.prepareStatement(sql);
			rs = st.executeQuery();
			while (rs.next()) {
				String raw = rs.getString("col_name");
				String shortName = rs.getString("short_col_name").toUpperCase();
				if (Util.isNull(shortName) || shortName.equalsIgnoreCase("OMCID") || shortName.equalsIgnoreCase("COLLECTTIME")
						|| shortName.equalsIgnoreCase("STAMPTIME") || shortName.equalsIgnoreCase("RNC_NAME")
						|| shortName.equalsIgnoreCase("SUBNETWORKROOT") || shortName.equalsIgnoreCase("SUBNETWORK")
						|| shortName.equalsIgnoreCase("MECONTEXT")) {
					continue;
				}
				String tbName = rs.getString("tab_name").toUpperCase();
				if (map.containsKey(tbName)) {
					map.get(tbName).put(raw, shortName);
				} else {
					Map<String, String> rawToShort = new HashMap<String, String>();
					rawToShort.put(raw, shortName);
					map.put(tbName, rawToShort);
				}
			}
		} finally {
			CommonDB.close(rs, st, con);
		}

	}

	/* 合并MOID相同的记录。 */
	public static List<Record> mergeRecords(Map<List<String[]>, List<Record>> map) {
		List<Record> list = new ArrayList<WCDMAEricssonPerformanceParser.Record>();

		Iterator<List<Record>> recIt = map.values().iterator();
		while (recIt.hasNext()) {
			List<Record> recs = recIt.next();
			if (recs.size() == 1) {
				list.add(recs.get(0));
			} else {
				Record tmpRec = null;
				for (Record rec : recs) {
					if (tmpRec == null) {
						tmpRec = rec;
					} else {
						tmpRec.appendRecord(rec);
					}
				}
				list.add(tmpRec);
			}
		}

		return list;
	}

	/* 比较两个moid是否是一样的 */
	public static boolean compareMOID(List<String[]> a, List<String[]> b) {
		if (a == b)
			return true;
		if (a.size() != b.size())
			return false;
		for (int i = 0; i < a.size(); i++) {
			if (!a.get(i)[0].equals(b.get(i)[0]) || !a.get(i)[1].equals(b.get(i)[1])) {
				return false;
			}
		}
		return true;
	}

	/* 提取moid的最后一项 */
	public static String[] lastMOIDEntry(List<String[]> moid) {
		return moid.get(moid.size() - 1);
	}

	/* 在List<String[]>中按key名查找value. */
	public static String findByName(List<String[]> list, String name) {
		if (list == null || name == null)
			return "";
		for (String[] arr : list) {
			if (arr[0].equalsIgnoreCase(name))
				return arr[1];
		}
		return "";
	}

	/* 解moid标签内容 */
	public static List<String[]> parseMOID(String moid) {
		return parseKeyValue(moid, false);
	}

	/* 解sn标签内容 */
	public static List<String[]> parseSN(String moid) {
		return parseKeyValue(moid, true);
	}

	/*
	 * 解析键值对字符串，即sn和moid标签中的内容，并存入有序列表。列表中的对象，是String数组， [0]为key,[1]为value. 参数str为要解析的字符串，isSN表示是否解析的是sn标签，sn标签中有同名的key，要特别处理。
	 */
	private static List<String[]> parseKeyValue(String str, boolean isSN) {
		if (Util.isNull(str))
			return null;
		List<String[]> list = new ArrayList<String[]>();
		String[] sp = selfSplit(str, ',');
		for (int i = 0; i < sp.length; i++) {
			if (!isSN) {
				list.add(selfSplit(sp[i], '='));
			} else {
				String[] entry = selfSplit(sp[i], '=');
				/*
				 * 处理sn标签内容 ，格式是 "SubNetwork=ONRM_ROOT_MO_R,SubNetwork=DGRNC01,MeContext=FG_BenCaoDaSha-_1502" 这样的，第一和第二个都是SubNetwork，第三个是MeContext
				 */
				switch (i) {
					case 0 :
						// 第一个SubNetwork改名为SubNetworkRoot
						entry[0] = "SubNetworkRoot";
						list.add(entry);
						break;
					case 1 :
						// 第二个SubNetwork，正常添加。
						list.add(entry);
						break;
					case 2 :
						// 第三个key，是MeContext，也是最后一个，这时，要添加一个RNC_NAME，就用第二个SubNetWork的值.
						list.add(entry);
						list.add(new String[]{"RNC_NAME", list.get(1)[1]});
						break;
					default :
						break;
				}

			}
		}
		return list;
	}

	/* 考虑到效率，这里使用自己写的字符串分隔方法，代替String.split(). */
	private static String[] selfSplit(String s, char de) {
		String[] tmp = new String[s.length()];
		int count = 0;
		char[] cs = s.toCharArray();
		char sep = de;
		StringBuilder sb = new StringBuilder();
		for (char c : cs) {
			if (c == sep) {
				tmp[count++] = sb.toString();
				sb.setLength(0);
			} else {
				sb.append(c);
			}
		}
		tmp[count++] = sb.toString();
		sb.setLength(0);
		sb = null;

		String[] result = new String[count];
		System.arraycopy(tmp, 0, result, 0, count);
		tmp = null;
		cs = null;
		return result;
	}

	private HelperForWCDMAEricssonPerformanceParser() {
		super();
	}

	public static void main(String[] args) throws Exception {
		List<String[]> aaa = parseKeyValue("SubNetwork=ONRM_ROOT_MO_R,SubNetwork=DGRNC01,MeContext=FG_BenCaoDaSha-_1502", true);
		System.out.println(aaa);
	}
}

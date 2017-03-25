package parser.lucent.w.pm;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import javax.servlet.jsp.jstl.sql.Result;

import util.CommonDB;
import util.Util;

/**
 * counter管理，原始文件到CLT表的映射
 * 
 * @author ChenSijiang 2010-9-9
 */
final class CounterMgr {

	// clt_pm_w_al_map表中所有信息 , <原始counter名与表名相加的crc值,Counter信息>
	private Map<Long, Counter> allCounters = new HashMap<Long, Counter>();

	private static final CounterMgr INSTANCE = new CounterMgr();

	@SuppressWarnings({"unchecked"})
	private void initCounters() {
		Result rs = null;
		try {
			rs = CommonDB.queryForResult("SELECT COL_NAME,SHORT_COL_NAME,TAB_NAME  FROM CLT_PM_W_AL_MAP");
			if (rs != null) {
				SortedMap[] rows = rs.getRows();
				for (int i = 0; i < rows.length; i++) {
					SortedMap<String, Object> row = rows[i];
					if (row.get("COL_NAME") == null || row.get("SHORT_COL_NAME") == null || row.get("TAB_NAME") == null) {
						continue;
					}
					String colName = row.get("COL_NAME").toString();
					String scolName = row.get("SHORT_COL_NAME").toString();
					String tbName = row.get("TAB_NAME").toString();
					// boolean isused = true;
					// if ( row.get("ISUSED") != null )
					// {
					// isused = (Util.isNull(row.get("ISUSED").toString()) ?
					// true : (Integer.parseInt(row.get("ISUSED").toString()) >
					// 0));
					// }

					allCounters.put(Util.crc32(colName + tbName), new Counter(colName, scolName, "", tbName/*
																											 * , isused
																											 */));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		allCounters.put(Util.crc32("RNCFUNCTIONCLT_PM_W_AL_RNCFUNCTION2"), new Counter("RNCFUNCTION", "RNCFUNCTION", "", "CLT_PM_W_AL_RNCFUNCTION2"));
		allCounters.put(Util.crc32("UTRANCELLCLT_PM_W_AL_UTRANCELL1"), new Counter("UTRANCELL", "UTRANCELL", "", "CLT_PM_W_AL_UTRANCELL1"));
		allCounters.put(Util.crc32("UTRANCELLCLT_PM_W_AL_UTRANCELL2"), new Counter("UTRANCELL", "UTRANCELL", "", "CLT_PM_W_AL_UTRANCELL2"));
		allCounters.put(Util.crc32("UTRANCELLCLT_PM_W_AL_UTRANCELL3"), new Counter("UTRANCELL", "UTRANCELL", "", "CLT_PM_W_AL_UTRANCELL3"));
		allCounters.put(Util.crc32("UTRANCELLCLT_PM_W_AL_UTRANCELL4"), new Counter("UTRANCELL", "UTRANCELL", "", "CLT_PM_W_AL_UTRANCELL4"));
		allCounters.put(Util.crc32("UTRANCELLCLT_PM_W_AL_UTRANCELL5"), new Counter("UTRANCELL", "UTRANCELL", "", "CLT_PM_W_AL_UTRANCELL5"));
	}

	public static CounterMgr getInstance() {
		return INSTANCE;
	}

	public Counter getBySourceName(String sourceName, String tableName) {
		long key = Util.crc32(sourceName + tableName);
		Counter c = allCounters.get(key);
		return c;
	}

	private CounterMgr() {
		initCounters();
	}

	public static void main(String[] args) {
		CounterMgr m = CounterMgr.getInstance();
		System.out.println(m.getBySourceName("VS.FailedAdmissionDueToULload.NbEvt", "CLT_PM_W_AL_BTSCELL"));
	}
}

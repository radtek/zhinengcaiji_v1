package task;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Pattern;

import javax.servlet.jsp.jstl.sql.Result;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.LogMgr;
import util.Util;
import framework.ConstDef;

/**
 * <p>
 * 管理igp_conf_ignores表，即用于控制每个任务采集路径，是否要忽略。
 * </p>
 * <p>
 * 策略：igp_conf_ignores表中指定的路径，如果采集时不存在，则不补采，如果一旦发现存在了，那么， igp_conf_ignores表的isused字段设为0，以后不再忽略。
 * </p>
 * <p>
 * 关于采集路径的判断，为了方便配置，使用如下方式进行判定：
 * <ul>
 * 例如task_id为713的任务，有一条采集路径为"/w/sie/cm/c_fmci.csv"，igp_conf_ignores表中有一条记录为{ taskid:713,path:c_fmci,isused:1}，那么，"/w/sie/cm/c_fmci.csv"
 * 会被应用到igp_conf_ignores表配置的这条规则 ，因为"/w/sie/cm/c_fmci.csv"包含了igp_conf_ignores表中path字段所配置的内容 "c_fmci"。注意，匹配不区别大写小。
 * </ul>
 * </p>
 * 
 * @author ChenSijiang 2010-10-27
 * @since 1.1
 */
public final class IgnoresMgr {

	private static IgnoresMgr instance;

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private Map<Long, List<IgnoresInfo>> ignoreses = new HashMap<Long, List<IgnoresInfo>>();

	public synchronized static IgnoresMgr getInstance() {
		if (instance != null)
			return instance;
		instance = new IgnoresMgr();
		return instance;
	}

	/**
	 * 根据指定的任务ID，获取到此任务的所有要忽略的采集路径，如果没有找到，那么，将返回一个长度为0的List，永远不会返回null
	 * 
	 * @param taskId
	 *            指定的任务ID
	 * @return 指定任务的所有要忽略的采集路径
	 */
	public synchronized List<IgnoresInfo> getIgnoresesByTaskId(long taskId) {
		return ignoreses.containsKey(taskId) ? ignoreses.get(taskId) : new ArrayList<IgnoresInfo>();
	}

	/**
	 * 根据指定的任务ID与路径，判断此条路径是不是要被忽略的。
	 * 
	 * @param taskId
	 *            任务ID
	 * @param path
	 *            一条采集路径
	 * @param time
	 *            数据时间
	 * @return 路径是不是要被忽略的，如果是，返回相应的IgnoresInfo对象，否则返回null
	 */
	public synchronized IgnoresInfo checkIgnore(long taskId, String path, Timestamp time) {
		String aPath = path.replace('\\', '/');
		List<IgnoresInfo> list = getIgnoresesByTaskId(taskId);
		IgnoresInfo ignoresInfo = null;
		for (IgnoresInfo ig : list) {
			if (logicEquals(aPath.toLowerCase(), ConstDef.ParseFilePath(ig.getPath(), time).toLowerCase()) && ig.isUsed()) {
				ignoresInfo = ig;
				break;
			}
		}
		return ignoresInfo;
	}

	private class IgnoresInfoImpl implements IgnoresInfo {

		String path = "";

		long taskId;

		boolean isUsed;

		int intIsUsed;

		public IgnoresInfoImpl(String path, long taskId, boolean isUsed, int intIsUsed) {
			super();
			this.path = path;
			this.taskId = taskId;
			this.isUsed = isUsed;
			this.intIsUsed = intIsUsed;
		}

		@Override
		public String getPath() {
			return path;
		}

		@Override
		public long getTaskId() {
			return taskId;
		}

		@Override
		public boolean isUsed() {
			return isUsed;
		}

		@Override
		public void setNotUsed() {
			String sql = "update igp_conf_ignores set isused=0,modif_time=sysdate where taskid=" + taskId + " and path='" + path + "'";
			try {
				CommonDB.executeUpdate(sql);
				isUsed = false;
			} catch (SQLException e) {
				// logger.error("更改igp_conf_ignores记录时异常,语句:" + sql, e);
			}
		}

		@Override
		public boolean equals(Object obj) {
			if (obj != null) {
				if (obj == this) {
					return true;
				}
				if (obj instanceof IgnoresInfoImpl) {
					IgnoresInfoImpl instance = (IgnoresInfoImpl) obj;
					return instance.getTaskId() == this.getTaskId() && instance.getPath().equals(this.getPath());
				}
			}
			return false;
		}

		@Override
		public String toString() {
			return "{tastid:" + taskId + ", path:" + path + ", isused:" + intIsUsed + "}";
		}
	}

	private IgnoresMgr() {
		load();
	}

	/**
	 * 加载igp_conf_ignores表所有isused=1的记录
	 */
	@SuppressWarnings("unchecked")
	private void load() {
		String sql = "select * from igp_conf_ignores where isused=1";
		Result result = null;
		try {
			result = CommonDB.queryForResult(sql);
			if (result != null) {
				SortedMap[] maps = result.getRows();
				int size = maps.length;
				for (int i = 0; i < size; i++) {
					SortedMap m = maps[i];
					String path = m.get("path").toString();
					if (Util.isNull(path)) {
						continue;
					}
					long taskId = Long.parseLong(m.get("taskid").toString());
					int intIsused = Integer.parseInt(m.get("isused").toString());
					if (ignoreses.containsKey(taskId)) {
						ignoreses.get(taskId).add(new IgnoresInfoImpl(path, taskId, intIsused == 1, intIsused));
					} else {
						List<IgnoresInfo> list = new ArrayList<IgnoresInfo>();
						list.add(new IgnoresInfoImpl(path, taskId, intIsused == 1, intIsused));
						ignoreses.put(taskId, list);
					}
				}
			}
		} catch (Exception e) {
			// logger.error("读取igp_conf_ignores表时异常,语句:" + sql, e);
		}
	}

	/**
	 * 判断两个字符串是否是逻辑意义上的相等。一个字符串是有通配符的，另一个是没有通配符的。 例如，"*_Adjacent_cell_handover_01Jul2010_*.csv"
	 * 可以匹配到"4_Adjacent_cell_handover_01Jul2010_0433.csv"，于是它们相等。 如果没有通配符，就按String.equals()方法来判断。
	 * 
	 * @param shortFileName
	 *            实际的文件名
	 * @param fileName
	 *            解析模板中，<FILENAME>中配的文件名
	 * @return 是否相等
	 */
	private boolean logicEquals(final String shortFileName, final String fileName) {
		// 不包含通配符的情况下，当作普通的String.equals()方法处理
		if (!fileName.contains("*") && !fileName.contains("?")) {
			return shortFileName.contains(fileName);
		}

		String s1 = shortFileName.replaceAll("\\.", ""); // 把.号去掉，因为它在正则表达式中有意义。
		String s2 = fileName.replaceAll("\\.", ""); // 把.号去掉，因为它在正则表达式中有意义。
		s1 = s1.replaceAll("\\+", "");
		s2 = s2.replaceAll("\\+", "");
		s2 = s2.replaceAll("\\*", ".*"); // *换成.*，表示多匹配多个字符
		s2 = s2.replaceAll("\\?", "."); // ?换成.，表示匹配单个字符
		s2 = ".*" + s2 + ".*";
		return Pattern.matches(s2, s1); // 通过正则表达式方式判断
	}

	public static void main(String[] args) throws ParseException {
		// IgnoresMgr mgr = IgnoresMgr.getInstance();
		// IgnoresInfo info = mgr.checkIgnore(713,
		// "/w/sie/cm/c_adjg_1.CSV_test_20101028", new
		// Timestamp(Util.getDate1("2010-10-28 00:00:00").getTime()));
		// System.out.println(info);
		IgnoresMgr mgr = IgnoresMgr.getInstance();
		IgnoresInfo info = mgr.checkIgnore(1027, "aac:\\usr\\cfg_perf_luaac_2010", new Timestamp(Util.getDate1("2010-10-28 00:00:00").getTime()));
		System.out.println(info);
		// info.setNotUsed();
	}
}

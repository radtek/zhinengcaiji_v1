package task;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import util.LogMgr;
import util.Util;
import access.AbstractDBAccessor;

/**
 * 补采情况统计 类
 * 
 * @author liuwx 2010-4-21
 * @since 3.0
 */
public class RegatherStatistics {

	/** 统计情况映射表 <crc32(补采任务的TASKID+FILEPATH+COLLECTTIME),补采任务补采情况统计对象> */
	private Map<Long, RegatherStatisticsObj> statisticsMap;

	public int MAX_REGATHER_TIMES = 10; // 最大补采次数

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	private static RegatherStatistics instance = null;

	private RegatherStatistics() {
		super();
		statisticsMap = new HashMap<Long, RegatherStatisticsObj>();
	}

	public static synchronized RegatherStatistics getInstance() {
		if (instance == null) {
			instance = new RegatherStatistics();
		}
		return instance;
	}

	/**
	 * 获取一个补采任务的执行次数，即第几次补采。
	 * 
	 * @param obj
	 *            补采任务
	 * @return 补采的执行次数
	 */
	public synchronized int getRecltTimes(CollectObjInfo obj) {
		String str = String.valueOf(obj.getTaskID()) + Util.getDateString(obj.getLastCollectTime()) + obj.getCollectPath();
		long key = Util.crc32(str);
		if (statisticsMap.containsKey(key)) {
			return statisticsMap.get(key).getTimes() + 1;
		} else {
			return 1;
		}
	}

	/**
	 * 检查指定的补采任务是否已经合法
	 * 
	 * @param obj
	 *            补采任务对象
	 * @return true为合法，false为非法
	 */
	public synchronized boolean check(RegatherObjInfo obj) {
		boolean b = true;

		MAX_REGATHER_TIMES = obj.getMaxReCollectTime();
		// 如果是数据库采集方式并且最大补采次数配置的值小于等于-2，表示此时需要补采的次数为Math.abs(obj.getMaxReCollectTime()
		// + 2)，比如，-3时表示补采1次；
		if (obj.getCollectThread() instanceof AbstractDBAccessor) {
			if (obj.getMaxReCollectTime() <= -2) {
				MAX_REGATHER_TIMES = Math.abs(obj.getMaxReCollectTime() + 2);
			}
		}

		long taskID = obj.getTaskID();
		String filePath = obj.getCollectPath();
		String strCollectTime = Util.getDateString(obj.getLastCollectTime());
		String str = String.valueOf(taskID) + strCollectTime + filePath;

		long key = Util.crc32(str);
		if (statisticsMap.containsKey(key)) {
			RegatherStatisticsObj sObj = statisticsMap.get(key);
			if (sObj == null)
				statisticsMap.remove(key);

			int currentTimes = sObj.getTimes();
			if (currentTimes >= MAX_REGATHER_TIMES) {
				b = false;
				statisticsMap.remove(key);
				log.info("补采任务" + obj + ": 已达到最大补采次数.(" + filePath + " " + strCollectTime + " 此时补采的ID为:" + (obj.getKeyID() - 10000000) + ")");
			} else {
				sObj.setTimes(currentTimes + 1);
				log.info("补采任务" + obj + ": 当前补采次数.(" + sObj.getTimes() + " " + filePath + " " + strCollectTime + " 此时补采的ID为:"
						+ (obj.getKeyID() - 10000000) + ")");
			}
		} else
			statisticsMap.put(key, new RegatherStatisticsObj(key, 1));

		return b;

	}
}

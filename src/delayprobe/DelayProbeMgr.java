package delayprobe;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import util.LogMgr;
import util.Util;
import framework.ConstDef;
import framework.SystemConfig;

public class DelayProbeMgr {

	/**
	 * <任务号+时间点,是不是第一次探测>
	 */
	private static Map<String, Boolean> firsts = new HashMap<String, Boolean>();

	/**
	 * 放入"任务号+时间"，以标识是否出现过异常
	 */
	private static Set<String> errors = new HashSet<String>();

	/**
	 * 一个任务号所对应的TaskDataEntry对象
	 */
	private static Map<Long, TaskDataEntry> taskEntrys = new HashMap<Long, TaskDataEntry>();

	/**
	 * 运行的分钟数，每扫描一次任务，增加1，以此作为计算间隔的依据
	 */
	public static int time = -1;

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	public static List<CollectObjInfo> probe(List<CollectObjInfo> tempTasks) {
		if (!SystemConfig.getInstance().isEnableDelayProbe()) {
			return null;
		}
		final List<CollectObjInfo> results = new ArrayList<CollectObjInfo>();
		List<Thread> threads = new ArrayList<Thread>();
		for (final CollectObjInfo c : tempTasks) {
			if (!validate(c)) {
				continue;
			}

			final String key = c.getTaskID() + "[" + Util.getDateString(c.getLastCollectTime()) + "]";
			threads.add(new Thread("  ProbeThread - " + key + "  ") {

				@Override
				public void run() {
					TaskDataEntry newTde = new TaskDataEntry(c);
					int probeTime = c.getProbeTime();
					if (!newTde.isNoError()) {
						String log = key + ":执行探针时异常，此时间点不再使用探针";
						logger.error(log);
						newTde.setProbeLogger(new ProbeLogger(c.getTaskID()));
						newTde.getProbeLogger().println(log);
						newTde.getProbeLogger().dispose();
						errors.add(key);
						return;
					}
					if (taskEntrys.containsKey(c.getTaskID())) {
						TaskDataEntry tde = taskEntrys.get(c.getTaskID());
						newTde.setProbeCount(tde.getProbeCount() + 1);
						newTde.setPre(tde);
						newTde.setEqCount(tde.getEqCount());
						newTde.setProbeLogger(tde.getProbeLogger());
					} else {
						newTde.setProbeLogger(new ProbeLogger(c.getTaskID()));
						newTde.setProbeCount(1);
					}
					boolean bEq = newTde.compare();
					String result = bEq ? "相等" : "不相等";
					String log = "任务号: " + c.getTaskID() + ", 时间点: " + Util.getDateString(c.getLastCollectTime()) + ", 任务设置的延时(分钟):"
							+ c.getCollectTimePos() + ", 探测间隔(分钟):" + SystemConfig.getInstance().getProbeInterval() + ", 探测开始时间：" + probeTime + "分"
							+ ", 探测次数: " + newTde.getProbeCount() + (newTde.getPre() == null ? "(首次探测，尚无上次数据，不能比较)" : ", 比较结果: " + result);
					newTde.getProbeLogger().println(log);
					logger.debug(log);
					newTde.getProbeLogger().println("");
					taskEntrys.put(c.getTaskID(), newTde);
					boolean b = newTde.isNoError() && newTde.getEqCount() >= SystemConfig.getInstance().getDelayProbeTimes();
					if (b) {
						log = key + "经过" + SystemConfig.getInstance().getDelayProbeTimes() + "次比较，数据量未变化，确认可以开始采集";
						newTde.getProbeLogger().println(log);
						logger.debug(log);
						newTde.getProbeLogger().println("");
						newTde.getProbeLogger().dispose();
						taskEntrys.remove(c.getTaskID());
						results.add(c);
					}
				}
			});
		}
		for (Thread t : threads) {
			t.start();
		}

		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return results;
	}

	public static Map<Long, TaskDataEntry> getTaskEntrys() {
		return taskEntrys;
	}

	private static boolean validate(CollectObjInfo c) {
		if (c == null || (c instanceof RegatherObjInfo) || c.getPeriod() != ConstDef.COLLECT_PERIOD_HOUR) {
			return false;
		}

		Calendar calendar = Calendar.getInstance();
		long currTime = calendar.getTimeInMillis();
		int currMinute = calendar.get(Calendar.MINUTE);
		boolean isRightTime = ((currTime >= c.getLastCollectTime().getTime() + c.getPeriodTime()
				&& (currMinute >= c.getProbeTime() || currTime >= c.getLastCollectTime().getTime() + c.getPeriodTime() * 2) && currTime < c
				.getLastCollectTime().getTime() + c.getCollectTimePos() * 60 * 1000));
		if (!isRightTime) {
			return false;
		}

		int probeTime = c.getProbeTime();
		if (probeTime < 0) {
			return false;
		}
		int type = c.getCollectType();
		if (type != 6 && type != 60 && type != 3 && type != 9) {
			return false;
		}
		if ((type == 3 || type == 9) && !SystemConfig.getInstance().isProbeFTP()) {
			return false;
		}
		String key = c.getTaskID() + "[" + Util.getDateString(c.getLastCollectTime()) + "]";
		boolean isFirstProbe = firsts.containsKey(key) ? firsts.get(key) : true;
		if (time % SystemConfig.getInstance().getProbeInterval() != 0 && !isFirstProbe) {
			return false;
		}
		firsts.put(key, false);
		if (errors.contains(key)) {
			return false;
		}
		return true;
	}
}

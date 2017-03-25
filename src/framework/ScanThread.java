package framework;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.Logger;

import task.TaskMgr;
import util.CommonDB;
import util.LogMgr;
import util.Util;
import util.mr.CMRCalculate;
import delayprobe.DelayProbeMgr;

/**
 * 扫描线程
 * 
 * @author ChenSijiang
 * @since 3.1
 */
public class ScanThread implements Runnable {

	// 停止扫描线程的标志位
	private boolean stopFlag = false;

	// 扫描任务队列，存储不丢失的分钟序列
	private ArrayList<ScanInfo> scanInfoQueue = new ArrayList<ScanInfo>();

	private Thread thread = new Thread(this, toString());

	private ScanEndAction endAction;

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	private static TaskMgr taskMgr = TaskMgr.getInstance();

	private static ScanThread instance;

	private ScanThread() {
		super();
	}

	public synchronized static ScanThread getInstance() {
		if (instance == null) {
			instance = new ScanThread();
		}
		return instance;
	}

	/**
	 * 是否已停止扫描
	 * 
	 * @return
	 */
	public synchronized boolean isStop() {
		return stopFlag;
	}

	/**
	 * 停止扫描
	 * 
	 * @return
	 */
	public void stopScan() {
		stopFlag = true;
		this.thread.interrupt();
		scanInfoQueue.clear();
	}

	/**
	 * 开始扫描
	 */
	public void startScan() {
		logger.info("开始扫描");
		loadRegatherInfos();
		thread.start();

		// 根据配置信息启动所有采集线程

		Date beginTime = new Date();
		Date lastScanTime = beginTime;
		Date lastReAdoptTime = beginTime;

		while (!isStop()) {
			try {
				// 生成扫描任务
				Date now = new Date();

				// 1分钟启动一次扫描
				if (now.getTime() - lastScanTime.getTime() > 60 * 1000) {
					DelayProbeMgr.time++;
					// 添加一次扫描任务
					ScanInfo info = new ScanInfo();
					info.now = new Date(lastScanTime.getTime() + 60 * 1000);
					info.bReAdopt = false;

					synchronized (scanInfoQueue) {
						scanInfoQueue.add(info);
					}

					lastScanTime = info.now;
				}

				// 5分钟启动一次补采线程
				if (now.getTime() - lastReAdoptTime.getTime() > 5 * 60 * 1000) {
					logger.debug("System.gc()...");
					System.gc();
					logger.debug("System.gc()...done");

					// 添加一次扫描任务
					ScanInfo info = new ScanInfo();
					info.now = new Date(lastReAdoptTime.getTime() + 5 * 60 * 1000);
					info.bReAdopt = true;

					synchronized (scanInfoQueue) {
						scanInfoQueue.add(info);
					}

					lastReAdoptTime = info.now;
				}

				// 休息1秒
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					stopScan();
				}
			} catch (Exception e) {
				logger.error(this + "-startSystem: 扫描任务出现异常.", e);
			}
		} // while

		// 退出时关闭连接
		CommonDB.closeDbConnection();
	}

	public void setEndAction(ScanEndAction endAction) {
		this.endAction = endAction;
	}

	@Override
	public void run() {
		// 扫描任务执行线程
		while (!isStop()) {
			try {
				if (scanInfoQueue.size() > 0) {
					ScanInfo info = null;
					synchronized (scanInfoQueue) {
						info = scanInfoQueue.remove(0);
					}

					if (info.bReAdopt) {
						loadRegatherInfos();
					} else {
						// 当前活跃的线程个数
						logger.info(this + ": Current Active Thread Count:" + Thread.activeCount());

						Util.showOSState();

						// 扫描任务表
						loadGatherInfos(info.now);

						// 检查MR快速定位标志
						CMRCalculate.bFast = SystemConfig.getInstance().isMRFast();
					}
				}

				// 防止锁住。2010 05 06
				Thread.sleep(10);
			} catch (InterruptedException ie) {
				logger.warn("任务表扫描线程被外界强行中断.");
				stopFlag = true;
				scanInfoQueue.clear();
				break;
			} catch (Exception e) {
				logger.error(this + ": 扫描异常.原因:", e);
				break;
			}
		} // while
		if (endAction != null) {
			endAction.actionPerformed(taskMgr);
		}
		logger.info("扫描线束");
	}

	// 从任务表中加载任务
	private boolean loadGatherInfos(Date now) {
		boolean bReturn = taskMgr.loadNormalTasksFromDB(now);

		int activeTaskCount = taskMgr.size();
		logger.info(this + ": 当前有效任务数为:" + activeTaskCount);

		// 此处代码给仪表使用，end.txt的出现，表明需要处理的任务已经全部完成
		if (activeTaskCount == 0) {
			try {
				File f = new File("end.txt");
				f.createNewFile();
			} catch (Exception e) {
				logger.error(this + ": 创建end.txt文件异常.原因:", e);
			}
		}

		return bReturn;
	}

	// 加载补采表信息
	private void loadRegatherInfos() {
		taskMgr.loadReGatherTasksFromDB();
	}

	@Override
	public String toString() {
		return "Scan-Thread";
	}

	public static interface ScanEndAction {

		/**
		 * 扫描线程线束之后的行为
		 * 
		 * @param taskMgr
		 */
		public void actionPerformed(TaskMgr taskMgr);
	}
}

class ScanInfo {

	Date now;

	boolean bReAdopt;
}

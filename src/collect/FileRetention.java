package collect;

import java.io.File;
import java.util.Collection;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import util.LogMgr;

/**
 * 管理文件保留周期。
 * 
 * @author ChenSijiang
 * @see FTPConfig
 */
public class FileRetention extends Thread {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	/* ftpConfig.xml中的所有配置。 */
	private static final Collection<FTPConfig> CONFIGS = FTPConfig.getAllConfigs();

	/* 扫描间隔时间，毫秒。 */
	private static final int SCAN_PERIOD = 1000 * 60 * 5;

	private Timer timer;

	private Object scanLock = new Object();

	@Override
	public void run() {
		synchronized (scanLock) {
			if (CONFIGS == null || CONFIGS.size() == 0)
				return;
			if (timer == null) {
				timer = new Timer("retention_scanner");
				timer.schedule(new ScanTask(), 0, SCAN_PERIOD);
				logger.info("文件管理线程已启动，扫描周期为" + (SCAN_PERIOD / 1000 / 60) + "分钟。");
				try {
					scanLock.wait();
				} catch (InterruptedException e) {
					logger.warn("文件管理线程被中断。");
				}
			}
		}
	}

	private class ScanTask extends TimerTask {

		@Override
		public void run() {
			for (FTPConfig cfg : CONFIGS) {
				Thread th = new Thread(new DirHandler(cfg));
				th.start();
			}
		}

	}

	private class DirHandler implements Runnable {

		FTPConfig cfg;

		public DirHandler(FTPConfig cfg) {
			super();
			this.cfg = cfg;
		}

		@Override
		public void run() {
			if (cfg == null)
				return;
			synchronized (cfg) {
				File dir = cfg.getLocalPath();
				logger.info(cfg.getTaskId() + " - 开始处理目录'" + dir.getAbsolutePath() + "'");
				long currDate = System.currentTimeMillis();
				long renTime = ((long) cfg.getRetentionTime()) * 60 * 60 * 1000;// 小时转毫秒。

				int countTotal = 0;
				int countNeedDel = 0;
				int countDelSuc = 0;
				int countDelFail = 0;
				logger.debug(cfg.getTaskId() + " - renTime=" + renTime + ", currDate=" + new Date(currDate));
				File[] files = dir.listFiles();
				for (File f : files) {
					countTotal++;
					long fDate = f.lastModified();

					if (currDate - fDate > renTime) {
						countNeedDel++;
						if (f.delete())
							countDelSuc++;
						else
							countDelFail++;
					}
				}
				logger.info(cfg.getTaskId() + " - 处理目录'" + dir.getAbsolutePath() + "'完毕，文件总数=" + countTotal + "，需删除数=" + countNeedDel + "，成功删除数="
						+ countDelSuc + "，删除失败数=" + countDelFail);
			}

		}

	}

	public static void main(String[] args) {
		long renTime = ((long) 720 * 60 * 60 * 1000);
		System.out.println(renTime);

	}
}

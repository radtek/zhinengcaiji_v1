package framework;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import util.LogMgr;

/**
 * 文件清理功能，定时清理currentPath下的文件。
 * 
 * @author ChenSijiang 2012-5-28
 */
public class TempFileCleaner extends Thread {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	public TempFileCleaner() {
		super("TempFileCleaner");
		super.setDaemon(true);
	}

	@Override
	public void run() {
		if (!SystemConfig.getInstance().isEnableTempFileCleaner())
			return;

		int timeMinutes = SystemConfig.getInstance()
				.getTempFileCleanerTimeMinutes();
		long timeMill = timeMinutes * 60 * 1000;
		String[] exts = SystemConfig.getInstance()
				.getTempFileCleanerExtensions();
		int interMinutes = SystemConfig.getInstance()
				.getTempFileCleanerIntervalMinutes();
		int interMill = interMinutes * 60 * 1000;
		File path = new File(SystemConfig.getInstance().getCurrentPath());
		if (!path.exists())
			path.mkdirs();

		log.info("文件清理：文件清理功能已开启，文件保留时间：" + timeMinutes + "分钟，清理扩展名："
				+ Arrays.asList(exts).toString() + "，清理间隔：" + interMinutes
				+ "分钟，清理起始目录：" + path);

		while (true) {
			log.debug("文件清理：开始检查目录。");

			/* 查找到的文件总数（不包括非文件对象个数）。 */
			int fileCount = 0;

			/* 经判断满足删除条件的文件数。 */
			int needDelCount = 0;

			/* 删除成功数。 */
			int delSucCount = 0;

			/* 删除失败数。 */
			int delFailCount = 0;

			/* 非文件对象个数。 */
			int errFileCount = 0;

			long currTime = System.currentTimeMillis();

			@SuppressWarnings("unchecked")
			Iterator<File> itFiles = FileUtils.iterateFiles(path,// 查找的根路径。
					exts,// 指定搜索的文件扩展名。
					true // 以递归的方式搜索。
					);
			while (itFiles.hasNext()) {
				File f = itFiles.next();
				if (f.isFile()) {
					fileCount++;
					if (currTime - f.lastModified() >= timeMill) {
						needDelCount++;
						if (f.delete()) {
							delSucCount++;
						} else {
							delFailCount++;
						}
					}
				} else {
					errFileCount++;
				}
			}
			log.debug("文件清理：本次操作完成，查找到的文件总数（不包括非文件对象个数）：" + fileCount
					+ "，经判断满足删除条件的文件数：" + needDelCount + "，删除成功数："
					+ delSucCount + "，删除失败数：" + delFailCount + "，非文件对象个数："
					+ errFileCount);

			try {
				Thread.sleep(interMill);
			} catch (InterruptedException e) {
				break;
			}
		}
	}
}

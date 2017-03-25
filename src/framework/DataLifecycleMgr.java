package framework;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import util.LogMgr;
import framework.SystemConfig;
import util.Util;

/**
 * 数据文件存活时间管理模块
 * 
 * @author Yuanxf
 * @since 3.0
 */
public class DataLifecycleMgr {

	private boolean enable = false; // 模块是否开启标识

	private boolean working = false; // 工作状态标识

	private boolean DeleteWhenOff = true; // 模块为关闭时，数据文件是否删除

	// 文件根目录
	private String rootPath = "";

	// 文件生命周期（单位：分钟）
	private int fileLifecycle;

	// 时间戳文件后缀名
	private String fileExtName;

	private Work workHandler;

	private static DataLifecycleMgr mgr = null;

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final int DEFAULT_FILE_LIFECYCLE = 7 * 24 * 60; // 默认文件存活时间为7天,单位:分钟

	private static final String DEFAULT_FILE_EXT = ".uway_ts"; // 默认时间戳文件后缀名

	private DataLifecycleMgr() {
		super();
		try {
			init();// 初始化信息
		} catch (Exception e) {
			logger.error("数据文件存活时间管理模块加载失败,原因:加载配置文件失败. 堆栈:", e);
		}

	}

	public static synchronized DataLifecycleMgr getInstance() {
		if (mgr == null) {
			mgr = new DataLifecycleMgr();
		}
		return mgr;
	}

	public void start() {
		if (enable && !isWorking()) {
			// 调用扫描时间戳文件线程
			logger.info("开启扫描时间戳文件线程");
			workHandler = new Work("DataLifecycleMgr-work-thrd");
			workHandler.start();

		}
	}

	public void stop() {
		if (workHandler != null) {
			workHandler.shutdown();
			workHandler = null;
		}
	}

	private void init() {
		// 文件根目录
		rootPath = SystemConfig.getInstance().getCurrentPath().trim();
		if (Util.isNull(rootPath)) {
			logger.debug("文件根目录不存在异常.");
			return;
		}

		// 文件生命周期（单位：分钟）
		String filePeriod = SystemConfig.getInstance().getFilecycle() + "";
		try {
			fileLifecycle = Integer.parseInt(filePeriod);
		} catch (NumberFormatException e) {
			fileLifecycle = DEFAULT_FILE_LIFECYCLE;
		}
		if (fileLifecycle < 0)
			fileLifecycle = DEFAULT_FILE_LIFECYCLE;

		// 时间戳文件后缀标示符
		fileExtName = SystemConfig.getInstance().getLifecycleFileExt();
		if (Util.isNull(fileExtName)) {
			fileExtName = DEFAULT_FILE_EXT;
		}

		// 控制开关
		if (SystemConfig.getInstance().isEnableDataFileLifecycle())
			enable = true;

		// 模块为关闭时，数据文件是否删除
		if (!SystemConfig.getInstance().isDeleteWhenOff())
			DeleteWhenOff = false;
	}

	public synchronized boolean isEnable() {
		return this.enable;
	}

	/**
	 * 给文件打时间戳 <br>
	 * 生成时间戳文件的格式：被打时间戳的文件名###数据时间###采集时间.uway_ts</br> <br>
	 * 例如: 0_99111_20100310033000_2.txt###20100310033000###20100416174236.uway_ts </br>
	 * 
	 * @param filePath
	 *            原始数据文件名
	 * @param dataTime
	 *            数据时间
	 */
	public void doFileTimestamp(String filePath, Date dataTime) {
		if (!isEnable()) {
			return;
		} else {
			if (!isWorking()) {
				start();
			}
		}

		if (dataTime == null)
			return;

		// 数据时间
		String collectDataTime = Util.getDateString_yyyyMMddHHmmss(dataTime);

		// 下载文件时间
		String nowTime = Util.getDateString_yyyyMMddHHmmss(new Date());

		String fileTimePostfix = filePath + "###" + collectDataTime + "###" + nowTime + fileExtName;
		File fileTime = new File(fileTimePostfix);
		if (!fileTime.exists()) {
			try {
				fileTime.createNewFile();
			} catch (IOException e) {
				logger.error("创建时间戳文件失败.原因:", e);
			}
		}
	}

	class Work extends Thread {

		boolean flag = true;

		public Work() {
			super();
		}

		public Work(String thrdName) {
			super(thrdName);
		}

		synchronized boolean isRunning() {
			return this.flag;
		}

		synchronized void shutdown() {
			this.flag = false;
		}

		/**
		 * 递归获得该路径下所有时间戳文件
		 * 
		 * @param file
		 * @return
		 */
		private List<File> loadFilename(File file) {
			if (file == null)
				return null;

			List<File> filenameList = new ArrayList<File>();
			if (file.isFile()) {
				String path = file.getPath();
				if (path.endsWith(fileExtName)) {
					filenameList.add(file);
				}
			}
			if (file.isDirectory()) {
				for (File f : file.listFiles()) {
					if (f == null)
						continue;
					filenameList.addAll(loadFilename(f));
				}
			}
			return filenameList;
		}

		/**
		 * 获取不包含扩展名的文件名
		 * 
		 * @param path
		 * @return
		 */
		private String getFileNameExcludeExt(String path) {
			if (path == null)
				return null;

			String fileName = null;
			fileName = path.substring(path.lastIndexOf(File.separator) + 1, path.lastIndexOf("."));
			return fileName;
		}

		private void compareDate() {
			File file = new File(rootPath);
			List<File> lst = loadFilename(file);

			for (File f : lst) {
				String tsFilepath = f.getPath();
				String fileName = getFileNameExcludeExt(tsFilepath);
				String folderPath = tsFilepath.substring(0, tsFilepath.lastIndexOf(File.separator));

				String strPath[] = fileName.split("###");
				if (strPath.length != 3)
					continue;

				String rawFilePath = folderPath + File.separator + strPath[0];
				// String collectFileTime = strPath[1];
				String downFileTime = strPath[2];

				// 下载文件时间
				Date colleDate = null;
				try {
					colleDate = Util.getDate2(downFileTime);
				} catch (ParseException e) {
					continue;
				}
				if (colleDate == null)
					continue;

				// 文件存活时间
				long times = colleDate.getTime() + fileLifecycle * 60 * 1000;
				// 现在时间
				long now = new Date().getTime();
				if (times <= now) {
					File oldfile = new File(rawFilePath);
					String strFlag = oldfile.delete() ? "成功" : "失败";
					logger.debug("文件存活周期已到,删除原始文件：" + rawFilePath + " --" + strFlag);

					File flagfile = new File(tsFilepath);
					strFlag = flagfile.delete() ? "成功" : "失败";
					logger.debug("文件存活周期已到,删除时间戳文件：" + tsFilepath + " --" + strFlag);

					// 删除.CTL
					File ctlfile = null;
					// 判断数据文件名是否带有后缀
					String dataFileName = rawFilePath.substring(rawFilePath.lastIndexOf("\\") + 1, rawFilePath.length());
					if (dataFileName.contains(".")) {
						ctlfile = new File(rawFilePath.substring(0, rawFilePath.lastIndexOf(".")) + ".ctl");
					} else {
						ctlfile = new File(rawFilePath + ".ctl");
					}

					if (ctlfile.exists()) {
						strFlag = ctlfile.delete() ? "成功" : "失败";
						logger.debug(ctlfile.getPath() + " 删除" + strFlag);
					}

				}

			}
		}

		@Override
		public void run() {
			setWorking(true);

			while (isRunning()) {
				try {
					compareDate();
				} catch (Exception e) {
					logger.error("删除时间戳文件时错误", e);
				}

				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
				}
			}

			setWorking(false);
		}

	}

	public boolean isDeleteWhenOff() {
		return DeleteWhenOff;
	}

	public synchronized boolean isWorking() {
		return working;
	}

	public synchronized void setWorking(boolean working) {
		this.working = working;
	}

	// 单元测试
	public static void main(String[] args) {
		// DataLifecycleMgr.getInstance().doFileTimestamp("E:\\datacollector_path\\20080205\\CFGMML-RNC320-20100328060007.txt",
		// new Date());
		//
		// try
		// {
		// DataLifecycleMgr.getInstance().start();
		// }
		// catch (Exception e)
		// {
		// e.printStackTrace();
		// }
	}

}

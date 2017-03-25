package collect;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import util.LogMgr;
import util.Util;

/**
 * 配置每个FTP方式采集任务的FTP策略。
 * 
 * @author ChenSijiang 2011-07-09
 */
public final class FTPConfig {

	public static final String TRANS_MODE_PORT = "port";

	public static final String TRANS_MODE_PASV = "pasv";

	private static final Map<Long, FTPConfig> CONFIGS = new HashMap<Long, FTPConfig>();

	private static final String CONFIG_PATH = "." + File.separator + "conf" + File.separator + "ftpConfig.xml";

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private long taskId;

	private String transMode;

	private int bufferSize;

	private File localPath;

	private int dataTimeout;

	private int loginTryTimes;

	private int loginTryDelay;

	private int listTryTimes;

	private int listTryDelay;

	private int downloadTryTimes;

	private int downloadTryDelay;

	private boolean isDelete;

	private int retentionTime;

	/* 是否是按文件创建时间扫描。 */
	private boolean isByCreateTime;

	/* 如果是按文件创建时间扫描，每次取时间最新的多少个。 */
	private int downCount;

	static {
		SAXReader r = null;
		try {
			r = new SAXReader();
			Document doc = r.read(new File(CONFIG_PATH));
			Element root = doc.getRootElement();
			List<?> taskElements = root.elements("task");
			for (Object taskElement : taskElements) {
				if (taskElement != null && (taskElement instanceof Element)) {
					Element e = (Element) taskElement;
					long taskId = Long.parseLong(e.attributeValue("id"));
					String transMode = e.elementText("transMode").trim().toLowerCase();
					int bufferSize = Integer.parseInt(e.elementText("bufferSize"));
					File localPath = new File(e.elementText("localPath"));
					int dataTimeout = Integer.parseInt(e.elementText("dataTimeout"));
					int loginTryTimes = Integer.parseInt(e.elementText("loginTryTimes"));
					int loginTryDelay = Integer.parseInt(e.elementText("loginTryDelay"));
					int listTryTimes = Integer.parseInt(e.elementText("listTryTimes"));
					int listTryDelay = Integer.parseInt(e.elementText("listTryDelay"));
					int downloadTryTimes = Integer.parseInt(e.elementText("downloadTryTimes"));
					int downloadTryDelay = Integer.parseInt(e.elementText("downloadTryDelay"));
					int renTime = 36;
					boolean byCreateTime = Util.isNotNull(e.elementText("isByCreateTime"))
							&& e.elementText("isByCreateTime").equalsIgnoreCase("true");
					int downCount = 10;
					try {
						downCount = Integer.parseInt(e.elementText("downCount"));
					} catch (Exception exIgnore) {
					}
					try {
						renTime = Integer.parseInt(e.elementText("retentionTime"));
					} catch (Exception exIgnore) {
					}
					boolean isDelete = Boolean.parseBoolean(e.elementText("isDelete").trim().toLowerCase());
					FTPConfig instance = new FTPConfig(taskId, transMode, bufferSize, localPath, dataTimeout, loginTryTimes, loginTryDelay,
							listTryTimes, listTryDelay, downloadTryTimes, downloadTryDelay, isDelete, renTime);
					instance.setByCreateTime(byCreateTime);
					instance.setDownCount(downCount);
					if (checkConfig(instance))
						CONFIGS.put(taskId, instance);
				}
			}
		} catch (Throwable ex) {
			logger.error("加载FTP配置失败，将使用默认配置。错误信息：" + ex.getMessage());
		} finally {
		}
	}

	private static boolean checkConfig(FTPConfig instance) {
		if (instance == null)
			return false;
		if (instance.getBufferSize() <= 0) {
			logger.error(instance.getTaskId() + " - buffSize不正确，必须大于0.");
			return false;
		}
		if (Util.isNull(instance.getTransMode())) {
			logger.error(instance.getTaskId() + " - transMode为空。");
			return false;
		}
		if (!instance.getTransMode().equals(TRANS_MODE_PASV) && !instance.getTransMode().equals(TRANS_MODE_PORT)) {
			logger.error(instance.getTaskId() + " - transMode只能是" + TRANS_MODE_PASV + "或" + TRANS_MODE_PORT + ".");
			return false;
		}
		if (instance.getLoginTryTimes() <= 0) {
			logger.error(instance.getTaskId() + " - loginTryTimes必须大于0.");
			return false;
		}
		if (instance.getLoginTryDelay() <= 0) {
			logger.error(instance.getTaskId() + " - loginTryDelay必须大于0.");
			return false;
		}
		if (instance.getListTryTimes() <= 0) {
			logger.error(instance.getTaskId() + " - listTryTimes必须大于0.");
			return false;
		}
		if (instance.getListTryDelay() <= 0) {
			logger.error(instance.getTaskId() + " - listTryDelay必须大于0.");
			return false;
		}
		if (instance.getDownloadTryTimes() <= 0) {
			logger.error(instance.getTaskId() + " - downloadTryTimes必须大于0.");
			return false;
		}
		if (instance.getDownloadTryDelay() <= 0) {
			logger.error(instance.getTaskId() + " - downloadTryDelay必须大于0.");
			return false;
		}
		if (instance.getLocalPath() == null) {
			logger.error(instance.getTaskId() + " - localPath为空或不合法。");
			return false;
		}
		if (instance.getRetentionTime() <= 1) {
			logger.error(instance.getRetentionTime() + " - retentionTime必须大于等于1.");
			return false;
		}
		if (!instance.getLocalPath().isDirectory() || !instance.getLocalPath().exists()) {
			boolean b = instance.getLocalPath().mkdirs();
			if (!b) {
				logger.error(instance.getTaskId() + " - 目录\"" + instance.getLocalPath().getAbsolutePath() + "\"不存在，并且在尝试自动创建时失败。");
				return false;
			}
		}
		return true;
	}

	/**
	 * 根据任务ID获取FTP配置实例，如果没有，将返回<code>null</code>.
	 * 
	 * @param taskId
	 *            任务ID.
	 * @return FTP配置实例。
	 */
	public static FTPConfig getFTPConfig(long taskId) {
		return CONFIGS.get(taskId);
	}

	/**
	 * 获取所有配置。
	 * 
	 * @return
	 */
	public static Collection<FTPConfig> getAllConfigs() {
		return CONFIGS.values();
	}

	/**
	 * 任务号。
	 * 
	 * @return
	 */
	public long getTaskId() {
		return taskId;
	}

	/**
	 * 传输模式，port或pasv.
	 * 
	 * @return
	 */
	public String getTransMode() {
		return transMode;
	}

	/**
	 * 传输数据时的缓冲区大小（节字）。
	 * 
	 * @return
	 */
	public int getBufferSize() {
		return bufferSize;
	}

	/**
	 * 本地下载目录。
	 * 
	 * @return
	 */
	public File getLocalPath() {
		return localPath;
	}

	/**
	 * 数据传输超时时间（秒）。
	 * 
	 * @return
	 */
	public int getDataTimeout() {
		return dataTimeout;
	}

	/**
	 * 登录重试次数
	 * 
	 * @return
	 */
	public int getLoginTryTimes() {
		return loginTryTimes;
	}

	/**
	 * 登录重试间隔时间（秒）
	 * 
	 * @return
	 */
	public int getLoginTryDelay() {
		return loginTryDelay;
	}

	/**
	 * 执行LIST命令的重试次数
	 * 
	 * @return
	 */
	public int getListTryTimes() {
		return listTryTimes;
	}

	/**
	 * 执行LIST命令的重试间隔时间（秒)
	 * 
	 * @return
	 */
	public int getListTryDelay() {
		return listTryDelay;
	}

	/**
	 * 下载重试次数
	 * 
	 * @return
	 */
	public int getDownloadTryTimes() {
		return downloadTryTimes;
	}

	/**
	 * 下载重试间隔时间（秒）
	 * 
	 * @return
	 */
	public int getDownloadTryDelay() {
		return downloadTryDelay;
	}

	/**
	 * 是否删除原始文件
	 * 
	 * @return
	 */
	public boolean isDelete() {
		return isDelete;
	}

	/**
	 * 保留多少个小时的数据。
	 * 
	 * @return
	 */
	public int getRetentionTime() {
		return retentionTime;
	}

	public boolean isByCreateTime() {
		return isByCreateTime;
	}

	public void setByCreateTime(boolean isByCreateTime) {
		this.isByCreateTime = isByCreateTime;
	}

	public int getDownCount() {
		return downCount;
	}

	public void setDownCount(int downCount) {
		this.downCount = downCount;
	}

	@Override
	public String toString() {
		return "FTPConfig [taskId=" + taskId + ", transMode=" + transMode + ", bufferSize=" + bufferSize + ", localPath=" + localPath
				+ ", dataTimeout=" + dataTimeout + ", loginTryTimes=" + loginTryTimes + ", loginTryDelay=" + loginTryDelay + ", listTryTimes="
				+ listTryTimes + ", listTryDelay=" + listTryDelay + ", downloadTryTimes=" + downloadTryTimes + ", downloadTryDelay="
				+ downloadTryDelay + ", isDelete=" + isDelete + "]";
	}

	private FTPConfig(long taskId, String transMode, int bufferSize, File localPath, int dataTimeout, int loginTryTimes, int loginTryDelay,
			int listTryTimes, int listTryDelay, int downloadTryTimes, int downloadTryDelay, boolean isDelete, int retentionTime) {
		super();
		this.taskId = taskId;
		this.transMode = transMode;
		this.bufferSize = bufferSize;
		this.localPath = localPath;
		this.dataTimeout = dataTimeout;
		this.loginTryTimes = loginTryTimes;
		this.loginTryDelay = loginTryDelay;
		this.listTryTimes = listTryTimes;
		this.listTryDelay = listTryDelay;
		this.downloadTryTimes = downloadTryTimes;
		this.downloadTryDelay = downloadTryDelay;
		this.isDelete = isDelete;
		this.retentionTime = retentionTime;
	}

	public static void main(String[] args) {
		System.out.println(FTPConfig.getFTPConfig(333));

	}
}

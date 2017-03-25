package parser.corba.nms;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import task.CollectObjInfo;
import util.Util;
import collect.DownStructer;
import collect.FTPTool;
import framework.SystemConfig;

/**
 * 用于获取EPIRP对象的ior字符串。
 * 
 * @author ChenSijiang 2012-6-8
 */
public class IorFinder {

	/**
	 * 缓存每个任务获取到的ior字符串，目的是当一次没能获取到ior文件时，任务还可以使用上次获取到的ior文件。 Long表示task_id，String表示ior字符串。
	 */
	private static final ConcurrentHashMap<Long, String> CACHED_IOR = new ConcurrentHashMap<Long, String>();

	public String find(CollectObjInfo taskInfo) throws IorNotFoundException {
		check(taskInfo);
		String ior = null;

		String iorCollectPath = taskInfo.getCollectPath().split(";")[0].trim();
		File downPath = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + taskInfo.getTaskID() + File.separator + "ior_file"
				+ File.separator);
		if (!downPath.exists() || !downPath.isDirectory()) {
			if (!downPath.mkdirs())
				throw new IorNotFoundException("无法创建临时目录：" + downPath);
		}

		InputStream in = null;
		File iorFile = null;
		LineIterator lineItr = null;
		FTPTool ftp = new FTPTool(taskInfo);
		try {
			ftp.login(1000, 3);
			DownStructer downResult = ftp.downFile(iorCollectPath, downPath.getAbsolutePath());
			if (downResult != null && downResult.getSuc() != null && !downResult.getSuc().isEmpty()) {
				String iorFilepath = downResult.getSuc().get(0);
				iorFile = new File(iorFilepath);
				in = new FileInputStream(iorFilepath);
				lineItr = IOUtils.lineIterator(in, null);
				ior = lineItr.nextLine();
			}
		} catch (Exception e) {
			if (e instanceof IorNotFoundException)
				throw (IorNotFoundException) e;
			throw new IorNotFoundException("获取ior文件过程中异常。", e);
		} finally {
			ftp.disconnect();
			if (lineItr != null)
				lineItr.close();
			IOUtils.closeQuietly(in);
			if (iorFile != null)
				iorFile.delete();

			if (Util.isNotNull(ior)) {
				CACHED_IOR.put(taskInfo.getTaskID(), ior);
			} else {
				ior = CACHED_IOR.get(taskInfo.getTaskID());
				if (ior == null)
					throw new IorNotFoundException("ior文件未能获取成功，并且在历史缓存ior列表中也未找到。");
			}
		}
		return ior;
	}

	private static void check(CollectObjInfo taskInfo) throws IorNotFoundException {
		if (taskInfo == null)
			throw new IorNotFoundException("CollectObjInfo为null.");
		if (taskInfo.getDevInfo() == null || Util.isNull(taskInfo.getDevInfo().getIP()))
			throw new IorNotFoundException("igp_conf_device表的host_ip未配置。");
		if (Util.isNull(taskInfo.getCollectPath()))
			throw new IorNotFoundException("igp_conf_task表的collect_path未配置，必须指定ior文件的获取路径。");
	}
}

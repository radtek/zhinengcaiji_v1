package util;

import java.io.File;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import templet.TempletBase;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 解压工具类 DeCompression
 * 
 * @version 1.0 1.0.1 liangww 2012-06-04 修改descompress函数参数，增加是否删除原始文件bDelStrFile参数<br>
 * @author
 */
public class DeCompression {

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	/**
	 * 解压文件
	 * 
	 * @param nTaskID
	 * @param base
	 * @param strFile
	 * @param timestamp
	 * @param nPeriod
	 * @param bDelStrFile
	 *            是否要删除原文件，true删除，false不删除
	 * @return
	 * @throws Exception
	 */
	public static ArrayList<String> decompress(long nTaskID, TempletBase base, String strFile, Timestamp timestamp, int nPeriod, boolean bDelStrFile)
			throws Exception {
		if (Util.isWindows()) {
			return decompressWin(nTaskID, base, strFile, timestamp, nPeriod, bDelStrFile);
		} else {
			return decompressUnix(nTaskID, base, strFile, timestamp, nPeriod, bDelStrFile);
		}
	}

	private static ArrayList<String> decompressWin(long nTaskID, TempletBase base, String strFile, Timestamp timestamp, int nPeriod,
			boolean bDelStrFile) throws Exception {
		ArrayList<String> filelist = new ArrayList<String>();
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath() + File.separatorChar + String.valueOf(nTaskID);

		/* winrar执行文件的绝对路径。 */
		File winrarFullPath = new File(SystemConfig.getInstance().getWinrarPath() + File.separator + "winrar.exe");
		if (!winrarFullPath.exists())
			throw new Exception("winrar不存在，位置：" + winrarFullPath);

		/* 待解压的压缩文件。 */
		File zipFile = new File(strFile);

		log.debug("Decompress start:" + new java.util.Date() + " file=" + zipFile);

		String strFolderName = "";
		SimpleDateFormat formatter = null;
		switch (nPeriod) {
			case ConstDef.COLLECT_PERIOD_FOREVER :
				// formatter = new SimpleDateFormat ("yyyyMMddHHmmss");
				formatter = new SimpleDateFormat("yyyyMMdd");
				break;
			case ConstDef.COLLECT_PERIOD_DAY :
				formatter = new SimpleDateFormat("yyyyMMdd");
				break;
			case ConstDef.COLLECT_PERIOD_HOUR :
				formatter = new SimpleDateFormat("yyyyMMddHH");
				break;
			case ConstDef.COLLECT_PERIOD_MINUTE_HALFHOUR :
				formatter = new SimpleDateFormat("yyyyMMddHHmm");
				break;
			// case ConstDef.COLLECT_PERIOD_MINUTE_QUARTER:
			default :
				formatter = new SimpleDateFormat("yyyyMMddHHmm");
				break;
		}

		strFolderName = formatter.format(timestamp);
		String fold = strCurrentPath + File.separatorChar + strFolderName + "_" + getSeqNum();
		/* 解压后文件的存放目录。 */
		File dir = new File(fold);
		if (!dir.exists() || !dir.isDirectory()) {
			if (!dir.mkdirs())
				throw new Exception("目录" + dir + "不存在，并且在尝试创建时失败。");
		}

		String cmd = winrarFullPath.getAbsolutePath() + " e " + zipFile.getAbsolutePath() + " " + dir.getAbsolutePath() + " -y -ibck";

		int nSucceed = -1;
		try {
			log.debug(nTaskID + " 解压缩命令为：" + cmd);
			nSucceed = new ExternalCmd().execute(cmd);
		} catch (Exception e) {
			throw e;
		}

		log.debug(nTaskID + " 解压缩命令执行完毕：" + cmd + "，exitValue=" + nSucceed);
		// 0表示返回无错误。成功的标记
		if (nSucceed == 0) {
			// 遍历文件夹
			File[] files = dir.listFiles();

			for (int i = 0; i < files.length; i++) {
				String FilePath = files[i].getAbsolutePath();

				filelist.add(FilePath);

			}

			// liangww modify 2012-06-04 增加是否删除原始文件判断
			if (bDelStrFile) {
				// 删除压缩文件
				boolean bDel = zipFile.delete();
				log.debug("taskid-" + nTaskID + ": 删除压缩文件 " + strFile + "，结果=" + bDel);
			}

		} else {
			throw new Exception("decompress file error. file:" + strFile);
		}

		log.debug("Decompress end:" + new java.util.Date() + " file=" + strFile);

		return filelist;
	}

	/** 使用gzip解压缩文件 */
	private static ArrayList<String> decompressUnix(long nTaskID, TempletBase base, String strFile, Timestamp timestamp, int nPeriod,
			boolean bDelStrFile) throws Exception {
		ArrayList<String> fileList = null;
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath() + File.separatorChar + nTaskID;

		String strFolderName = "";
		SimpleDateFormat sdf = null;
		switch (nPeriod) {
			case ConstDef.COLLECT_PERIOD_FOREVER :
				// formatter = new SimpleDateFormat ("yyyyMMddHHmmss");
				sdf = new SimpleDateFormat("yyyyMMdd");
				break;
			case ConstDef.COLLECT_PERIOD_DAY :
				sdf = new SimpleDateFormat("yyyyMMdd");
				break;
			case ConstDef.COLLECT_PERIOD_HOUR :
				sdf = new SimpleDateFormat("yyyyMMddHH");
				break;
			case ConstDef.COLLECT_PERIOD_MINUTE_HALFHOUR :
				sdf = new SimpleDateFormat("yyyyMMddHHmm");
				break;
			// case ConstDef.COLLECT_PERIOD_MINUTE_QUARTER:
			default :
				sdf = new SimpleDateFormat("yyyyMMddHHmm");
				break;
		}
		strFolderName = sdf.format(timestamp);

		String strLogFile = strFile.substring(0, strFile.lastIndexOf('.'));

		File fFolder = new File(strCurrentPath + File.separatorChar + strFolderName);
		if (!fFolder.exists()) {
			if (!fFolder.mkdir()) {
				throw new Exception("mkdir error");
			}
		}

		String cmd = (strFile.endsWith(".zip") ? "unzip -o " : "gzip -d -f ") + strFile
				+ (strFile.endsWith(".zip") ? (" -d " + fFolder.getAbsolutePath()) : "");
		log.debug("cmd - " + cmd);
		int retCode = Util.execExternalCmd(cmd);

		if (retCode != 0) {
			log.error(cmd + " retCode=" + retCode);
			return new ArrayList<String>();
		}

		log.debug(cmd + " retCode=" + retCode);

		cmd = "mv " + strLogFile + " " + strCurrentPath + File.separatorChar + strFolderName;
		if (strFile.endsWith(".zip"))
			retCode = 0;
		else
			retCode = Util.execExternalCmd(cmd);

		if (retCode == 0) {
			fileList = new ArrayList<String>();

			// 遍历文件夹
			File dir = new File(fFolder.getAbsolutePath());
			log.debug("dir - " + dir.getAbsolutePath());
			File[] files = dir.listFiles();

			for (int i = 0; i < files.length; i++) {
				String FilePath = files[i].getAbsolutePath();
				fileList.add(FilePath);
			}

			// liangww modify 2012-06-04 增加是否删除原始文件判断
			if (bDelStrFile) {
				// 删除压缩文件
				File fTar = new File(strFile);
				fTar.delete();
				log.debug("taskid-" + nTaskID + ": 删除压缩文件 " + strFile);
			}
		}
		if (retCode != 0)
			log.debug(cmd + " retCode=" + retCode);

		return fileList;
	}

	private static int _seq = 0;

	/* 生成一个唯一序号，用于创建解压目录。当此数字太大时，置为0，以防万一。 */
	private synchronized static int getSeqNum() {
		int ret = (++_seq);

		if (_seq == Integer.MAX_VALUE - 1)
			_seq = 0;

		return ret;
	}
}

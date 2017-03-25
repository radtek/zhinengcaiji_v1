package parser.others.gpslog;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import task.TaskMgr;
import util.DeCompression;
import util.LogMgr;
import util.Util;
import collect.DownStructer;
import collect.FTPTool;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 定位日志分析文件解析. 注意：这个没有实现自动补采功能的。
 * 
 * @author liuwx 2010-3-19
 * @version 1.0 1.0.1 liangww 2012-06-04 增加文件下载时支持压缩文件功能<br>
 */
public class CV1BinMgr {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final SystemConfig cfg = SystemConfig.getInstance();

	public static final int RESULT_OK = 0;

	public static final int RESULT_FAIL = -1;

	private CollectObjInfo task;

	private String logKey;

	private FTPTool ftpTool;

	public CV1BinMgr(CollectObjInfo task) {
		this.task = task;
		this.ftpTool = new FTPTool(task);
		long id = task.getTaskID();
		if (task instanceof RegatherObjInfo)
			id = task.getKeyID() - 10000000;
		this.logKey = task.getTaskID() + "-" + id;
		this.logKey = String.format("[%s][%s]", this.logKey, Util.getDateString(task.getLastCollectTime()));
	}

	public boolean parse() {
		String collectPath = task.getCollectPath();
		if (Util.isNull(collectPath)) {
			error("collect_path为空");
			return false;
		}

		if (!loginForFTP())
			return false;

		String[] splited = collectPath.split(";");
		List<String> paths = new ArrayList<String>();
		for (String s : splited) {
			if (Util.isNotNull(s))
				paths.add(ConstDef.ParseFilePath(s.trim(), task.getLastCollectTime()));
		}

		for (String path : paths) {
			// 这里认为collect_path中的每条路径都是“/{02323-11-22-09-19989}/*.loc”这样的，即匹配每个设备目录下的所有.log文件。
			FTPFile[] fs = listFTPFiles(path);
			if (fs != null) {
				// ftpfiles:所有符合时间条件，并且在FTP存在的文件。
				List<FTPFile> ftpfiles = new ArrayList<FTPFile>();
				for (FTPFile x : fs) {
					if (x == null)
						continue;
					ftpfiles.add(x);
				}
				if (ftpfiles.isEmpty()) {
					continue;
				}

				int result = RESULT_OK;
				for (FTPFile ensuredFTPFile : ftpfiles) {
					String fname = ensuredFTPFile.getName();
					DownStructer struct = ftpTool.downFile(path, fname);
					List<String> files = struct.getSuc();

					for (String tmp : files) {
						File tmpFile = new File(tmp);

						if (!tmpFile.exists()) {
							String remoteDir = FilenameUtils.getFullPath(ensuredFTPFile.getName().contains("/") ? ensuredFTPFile.getName() : path);
							String fileName = FilenameUtils.getName(ensuredFTPFile.getName());
							String downpath = remoteDir + fileName;
							for (int times = 0; times < 3; times++) {
								debug(" 尝试重试下载 - " + (times + 1) + " - " + downpath);
								try {
									Thread.sleep(1000);
								} catch (InterruptedException e) {
								}
								ftpTool.disconnect();
								ftpTool = new FTPTool(task);
								ftpTool.login(2000, 2);
								tmpFile = download(path, ensuredFTPFile);
								if (tmpFile != null) {
									debug("重试下载成功 - " + (times + 1) + " - " + downpath);
									break;
								}
								if (times == 2 && tmpFile == null)
									debug("重试下载失败 - " + (times + 1) + " - " + downpath);
							}

						} else
							debug("下载成功  - " + tmpFile.getAbsolutePath());

						if (tmpFile != null) {
							CV1Bin w = new CV1Bin(task);
							w.setCollectObjInfo(task);
							List<String> compressPaths = null;

							try {
								compressPaths = getCompessPaths(this.task, tmpFile.getAbsolutePath());
								for (int i = 0; i < compressPaths.size(); i++) {
									w.setFileName(compressPaths.get(i));
									w.parseData(compressPaths.get(i));
								}
							} catch (Exception e) {
								error("解析/入库时异常 - " + tmpFile.getAbsolutePath(), e);
								result = RESULT_FAIL;
							} finally {
								// 如果是压缩文件，处理完后，要清除
								if (Util.isZipFile(tmpFile.getAbsolutePath()) && compressPaths != null && compressPaths.size() > 0) {
									// liangww modify 2012-07-27
									File[] subFile = new File(compressPaths.get(0)).getParentFile().listFiles();
									for (int i = 0; i < subFile.length; i++) {
										subFile[i].delete();
									}
									// 删除目录
									new File(compressPaths.get(0)).getParentFile().delete();
								}// if(Util.isZipFile(tmpFile.getAbsolutePath()) && compressPaths != null)
							}// finally
						}// if ( tmpFile != null )
						else {
							result = RESULT_FAIL;
						}
					}
				}
				if (!ftpfiles.isEmpty()) {
					info("任务(" + task.getTaskID() + ")，采集完成，时间=" + task.getLastCollectTime() + "，状态=" + result);
				}

			}
		}
		return true;
	}

	/**
	 * 记录INFO级别日志。
	 * 
	 * @param msg
	 *            日志信息。
	 */
	protected void info(Object msg) {
		logger.info(logKey + msg);
	}

	/**
	 * 记录DEBUG级别日志。
	 * 
	 * @param msg
	 *            日志信息。
	 */
	protected void debug(Object msg) {
		logger.debug(logKey + msg);
	}

	/**
	 * 记录ERROR级别日志。
	 * 
	 * @param msg
	 *            日志信息。
	 * @param ex
	 *            异常信息。
	 */
	protected void error(Object msg, Throwable ex) {
		if (ex == null)
			logger.error(logKey + msg);
		else
			logger.error(logKey + msg, ex);
	}

	/**
	 * 记录ERROR级别日志。
	 * 
	 * @param msg
	 *            日志信息。
	 */
	protected void error(Object msg) {
		error(msg, null);
	}

	protected void warn(Object msg) {
		logger.warn(logKey + msg);
	}

	/**
	 * 在FTP上列出指定路径的文件。如果失败，将返回<code>null</code>.
	 * 
	 * @param path
	 *            FTP文件路径。
	 * @return FTP文件路径。
	 */
	protected FTPFile[] listFTPFiles(String path) {
		FTPFile[] ftpFiles = null;
		boolean isException = false;
		try {
			ftpFiles = ftpTool.getFtpClient().listFiles(path);
		} catch (Exception e) {
			error("listFiles异常 - " + path, e);
			isException = true;
		}

		if (!isFilesNotNull(ftpFiles)) {
			debug("重试listFiles - " + path);
			if (isException)
				ftpTool.login(2000, 2);
			try {
				ftpFiles = ftpTool.getFtpClient().listFiles(path);
			} catch (Exception e) {
				error("重试listFiles异常 - " + path, e);
			}

			if (isFilesNotNull(ftpFiles)) {
				debug("重试listFiles成功 - " + path + " 文件个数 - " + ftpFiles.length);
			} else {
				error("重试listFiles失败 - " + path);
				TaskMgr.getInstance().newRegather(task, path, String.format("listFiles失败(ftpFiles%s)", ftpFiles == null ? "为null" : "长度为0"));
				return null;
			}
		} else {
			debug("listFiles成功 - " + path + " 文件个数 - " + ftpFiles.length);
		}

		return ftpFiles;
	}

	/**
	 * 下载文件到本地磁盘。如果下载失败，则返回<code>null</code>.
	 * 
	 * @param path
	 *            FTP原始路径。
	 * @param ftpFile
	 *            list出来的FTP文件信息。
	 * @return 本地文件路径。
	 */
	protected File download(String path, FTPFile ftpFile) {

		String remoteDir = FilenameUtils.getFullPath(ftpFile.getName().contains("/") ? ftpFile.getName() : path);
		String fileName = FilenameUtils.getName(ftpFile.getName());
		String downpath = remoteDir + fileName;

		File localDir = new File(cfg.getCurrentPath() + File.separator + task.getTaskID() + File.separator + remoteDir.replace("/", File.separator));
		localDir.mkdirs();
		File localFile = new File(localDir, fileName);
		FileOutputStream out = null;
		boolean isDownloaded = false;
		try {
			out = new FileOutputStream(localFile);
		} catch (Exception e) {
		}
		try {
			if (!ftpTool.getFtpClient().retrieveFile(downpath, out))
				error("文件下载失败 - " + downpath);
			else
				isDownloaded = true;
		} catch (Exception e) {
			error("下载文件失败 - " + downpath, e);
			return null;
		} finally {
			try {
				if (out != null)
					out.flush();
			} catch (Exception e) {
			}
			IOUtils.closeQuietly(out);
		}
		if (!isDownloaded)
			TaskMgr.getInstance().newRegather(task, downpath, "下载失败");
		return localFile;
	}

	/**
	 * 登录到FTP.
	 * 
	 * @return 是否登录成功。
	 */
	protected boolean loginForFTP() {
		if (ftpTool.login(2000, 3)) {
			info("FTP登录成功");
			return true;
		} else {
			error("FTP登录失败");
			TaskMgr.getInstance().newRegather(task, "", "多次登陆失败，全部补采");
			return false;
		}
	}

	/**
	 * 判断listFiles出来的FTP文件，是否不是空的。
	 * 
	 * @param fs
	 *            listFiles出来的FTP文件。
	 * @return listFiles出来的FTP文件，是否不是空的。
	 */
	private boolean isFilesNotNull(FTPFile[] fs) {
		if (fs == null) {
			return false;
		}
		if (fs.length == 0) {
			return false;
		}
		for (FTPFile f : fs) {
			if (f == null) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 获取解压后的路径
	 * 
	 * @param task
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static List<String> getCompessPaths(CollectObjInfo task, String path) throws Exception {
		List<String> paths = null;

		if (Util.isZipFile(path)) {
			paths = DeCompression.decompress(task.getTaskID(), task.getParseTemplet(), path, task.getLastCollectTime(), task.getPeriod(), false);
		} else {
			paths = new ArrayList<String>(1);
			paths.add(path);
		}

		return paths;
	}

}

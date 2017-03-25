package access.special;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;

import parser.dt.dingli201.CV201ASCII;
import parser.dt.dingli201.Tools;
import task.CollectObjInfo;
import task.RegatherObjInfo;
import task.TaskMgr;
import util.LogMgr;
import util.Util;
import collect.FTPTool;
import framework.ConstDef;
import framework.SystemConfig;

public class CDMDDingliDTParser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final SystemConfig cfg = SystemConfig.getInstance();

	public static final int RESULT_OK = 0;

	public static final int RESULT_FAIL = -1;

	// 时差：-8小时。
	private static final int TIME_OFFSET = -28800000;

	private CollectObjInfo task;

	private String logKey;

	private FTPTool ftpTool;

	public CDMDDingliDTParser(CollectObjInfo task) {
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

		// 真正的数据时间，即加了时差的。
		Timestamp dataTime = new Timestamp(task.getLastCollectTime().getTime() + TIME_OFFSET);
		// 开始
		Calendar begin = Calendar.getInstance();
		begin.setTime(dataTime);

		// 下面算出数据时间内，可能的文件名（不是完整的，只到小时）。
		List<String> filenames = new ArrayList<String>();
		for (int i = 0; i < 24; i++) {
			Calendar curr = Calendar.getInstance();
			curr.setTime(new Date(begin.getTime().getTime() + i * 60 * 60 * 1000));
			int year = curr.get(Calendar.YEAR);
			int month = curr.get(Calendar.MONTH) + 1;
			String strMonth = month < 10 ? "0" + month : String.valueOf(month);
			int day = curr.get(Calendar.DATE);
			String strDay = day < 10 ? "0" + day : String.valueOf(day);
			int hour = curr.get(Calendar.HOUR_OF_DAY);
			String strHour = hour < 10 ? "0" + hour : String.valueOf(hour);
			String filename = String.format("%s-%s-%s %s-", year, strMonth, strDay, strHour);
			filenames.add(filename);
		}

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
					for (String fn : filenames) {
						if (x.getName().startsWith(fn))
							ftpfiles.add(x);
					}
				}
				if (ftpfiles.isEmpty()) {
					warn("采集路径\"" + path + "\"中，未能找到符合\"" + task.getLastCollectTime() + "\"的文件，时差为" + (TIME_OFFSET / 1000 / 60 / 60) + "小时。");
					continue;
				}

				// 解一个设备的一个时间的所有文件。
				String rcuId = "";
				try {
					rcuId = path.substring(path.indexOf("{") + 1, path.lastIndexOf("}"));
				} catch (Exception e) {
				}
				int result = RESULT_OK;
				for (FTPFile ensuredFTPFile : ftpfiles) {
					File tmpFile = download(path, ensuredFTPFile);
					if (tmpFile != null)
						debug("下载成功 - " + tmpFile.getAbsolutePath());
					if (tmpFile == null) {
						String remoteDir = FilenameUtils.getFullPath(ensuredFTPFile.getName().contains("/") ? ensuredFTPFile.getName() : path);
						String fileName = FilenameUtils.getName(ensuredFTPFile.getName());
						String downpath = remoteDir + fileName;
						for (int times = 0; times < 3; times++) {
							debug("尝试重试下载 - " + (times + 1) + " - " + downpath);
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

					}

					if (tmpFile != null) {
						CV201ASCII w = new CV201ASCII();
						w.setFileName(tmpFile.getAbsolutePath());
						w.setCollectObjInfo(task);
						try {
							if (!w.parseData())
								result = RESULT_FAIL;
						} catch (Exception e) {
							error("解析/入库时异常 - " + tmpFile.getAbsolutePath(), e);
							result = RESULT_FAIL;
						} finally {
							tmpFile.delete();
						}
					} else {
						result = RESULT_FAIL;
					}
				}
				if (!ftpfiles.isEmpty()) {
					Tools.insertCalLog(rcuId, task.getLastCollectTime(), result);
					info("路测设备(" + rcuId + ")，采集完成，时间=" + task.getLastCollectTime() + "，状态=" + result);
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

}

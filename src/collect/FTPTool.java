package collect;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.parser.UnixFTPEntryParser;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import task.DevInfo;
import task.RegatherObjInfo;
import util.LogMgr;
import util.Util;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * FTP工具
 * 
 * @author ChenSijiang 2010-8-24
 */
public class FTPTool {

	protected String ip;

	protected int port;

	protected String user;

	protected String pwd;

	protected String encode;

	protected CollectObjInfo taskinInfo;

	protected String keyId;

	protected FTPClient ftp;

	protected FTPClientConfig ftpCfg;

	protected FTPConfig downCfg;

	protected static Logger logger = LogMgr.getInstance().getSystemLogger();

	/**
	 * 构造方法，关联任务信息
	 * 
	 * @param taskInfo
	 *            任务信息
	 */
	public FTPTool(CollectObjInfo taskInfo) {
		super();
		DevInfo dev = taskInfo.getDevInfo();
		ip = dev.getIP();
		port = taskInfo.getDevPort();
		user = dev.getHostUser();
		pwd = dev.getHostPwd();
		encode = dev.getEncode();
		this.taskinInfo = taskInfo;
		long id = taskinInfo.getTaskID();
		if (taskinInfo instanceof RegatherObjInfo)
			id = taskinInfo.getKeyID() - 10000000;
		keyId = taskinInfo.getTaskID() + "-" + id + " ";
		this.downCfg = FTPConfig.getFTPConfig(taskinInfo.getTaskID());
		if (downCfg != null) {
			logger.debug(keyId + "注意：此任务将使用ftpConfig.xml中的配置 - " + downCfg);
		}
	}

	/**
	 * 登录FTP
	 * 
	 * @param sleepTime
	 *            重试时的休眠时长，毫秒
	 * @param tryTimes
	 *            最大重试次数
	 * @return 是否登录成功
	 */
	public boolean login(int sleepTime, int tryTimes) {
		boolean b = false;
		if (login()) {
			return true;
		}
		int st = sleepTime;
		int tt = tryTimes;
		if (downCfg != null) {
			st = downCfg.getLoginTryDelay() * 1000;
			tt = downCfg.getLoginTryTimes();
		}
		if (tt > 0) {
			for (int i = 0; i < tt; i++) {
				if (st > 0) {
					logger.debug(keyId + "尝试重新登录，次数:" + (i + 1));
					try {
						Thread.sleep(st);
					} catch (InterruptedException e) {
						logger.error(keyId + "休眠时线程被中断");
					}
					b = login();
					if (b) {
						logger.debug(keyId + "重新登录成功");
						break;
					}
				}
			}
		}
		if (!b) {
			logger.debug(keyId + "重新登录失败");
		}
		return b;
	}

	/**
	 * 下载指定的文件
	 * 
	 * @param ftpPath
	 *            需要下载的文件绝对路径
	 * @param localPath
	 *            放置下载后文件的本地文件夹
	 * @return 下载到的所有文件的本地路径，如果返回null，则表示下载失败
	 */
	public DownStructer downFile(String ftpPath, String localPath) {
		String aFtpPath = ftpPath;
		String localDir = localPath;
		if (downCfg != null) {
			localDir = downCfg.getLocalPath().getAbsolutePath();
		}
		/*
		 * /w/zte/!20110209{888}!/00.txt
		 * !20110209{888}!中20110209是实际的FTP路径，888是本地的路径。
		 * 程序会把/w/zte/!20110209{888}
		 * !/00.txt转化为/w/zte/20110209/00.txt，然后下载到....\w\zte\888/00.txt
		 */
		if (Util.isNotNull(ftpPath)) {
			if (ftpPath.contains("!") && ftpPath.contains("{") && ftpPath.contains("}")) {
				int begin = ftpPath.indexOf("!");
				int end = ftpPath.lastIndexOf("!");
				if (begin > -1 && end > -1 && begin < end) {
					String content = ftpPath.substring(begin, end + 1);
					int cBegin = content.indexOf("!");
					int cEnd = content.indexOf("{");
					if (cBegin > -1 && cEnd > -1 && cBegin < cEnd) {
						String dir = content.substring(cBegin + 1, cEnd);
						aFtpPath = aFtpPath.replace(content, dir);
					}
				}
			}
		}

		FTPFile[] ftpFiles = null;
		DownStructer downStruct = new DownStructer();
		try {

			boolean isEx = false;
			try {
				ftpFiles = ftp.listFiles(encodeFTPPath(aFtpPath));
				logger.debug(keyId + "replyCode:" + ftp.getReplyCode() + ", replyString:" + ftp.getReplyString());
			} catch (Exception e) {
				logger.error(keyId + "listFiles失败:" + aFtpPath, e);
				isEx = true;
			}
			if (!isFilesNotNull(ftpFiles)) {
				int listTimes = 3;
				if (downCfg != null) {
					listTimes = downCfg.getListTryTimes();
				}
				if (taskinInfo.getPeriod() == ConstDef.COLLECT_PERIOD_ONE_MINUTE) {
					listTimes = 1;
					logger.debug(keyId + "该任务为1分钟粒度，list重试次数将固定为1次");
				}
				for (int i = 0; i < listTimes; i++) {
					int tryDelay = 2000;
					if (downCfg != null) {
						tryDelay = downCfg.getListTryDelay() * 1000;
					}
					int sleepTime = tryDelay * (i + 1);
					if (isEx) {
						logger.debug(keyId + "listFiles异常，断开重连");
						login();
					}
					// login();
					logger.debug(keyId + "重新尝试listFiles: " + aFtpPath + ",次数:" + (i + 1));
					Thread.sleep(sleepTime);
					try {
						ftpFiles = ftp.listFiles(encodeFTPPath(aFtpPath));
						logger.debug(keyId + "replyCode:" + ftp.getReplyCode() + ", replyString:" + ftp.getReplyString());
					} catch (Exception e) {
						logger.error("listFiles失败：" + aFtpPath, e);
					}
					if (isFilesNotNull(ftpFiles)) {
						logger.debug(keyId + "重试listFiles成功：" + aFtpPath);
						break;
					}
				}
				if (!isFilesNotNull(ftpFiles)) {
					logger.warn(keyId + "重试" + listTimes + "次listFiles失败，不再尝试：" + aFtpPath);
					if (!aFtpPath.contains("?") && !aFtpPath.contains("*")) {
						FTPFile tmpf = new FTPFile();
						tmpf.setName(FilenameUtils.getName(aFtpPath));
						tmpf.setType(FTPFile.FILE_TYPE);
						ftpFiles = new FTPFile[]{tmpf};
						logger.debug(keyId + "不包含通配符的路径，尝试直接下载 - " + aFtpPath);
					} else {
						return downStruct;
					}
				}
			}
			logger.debug(keyId + "listFiles成功,文件个数:" + ftpFiles.length + " (" + encodeFTPPath(aFtpPath) + ")");
			for (FTPFile f : ftpFiles) {
				if (f.isFile() || f.isSymbolicLink()) {
					String name = decodeFTPPath(f.isSymbolicLink() ? f.getLink() : f.getName());
					name = name.substring(name.lastIndexOf("/") + 1, name.length());
					String singlePath = aFtpPath.substring(0, aFtpPath.lastIndexOf("/") + 1) + name;
					String fpath = localDir + File.separator + name.replace(":", "");
					if (taskinInfo.getParserID() == 18 || taskinInfo.getParserID() == 19/* 广州爱立信 */
							|| taskinInfo.getParserID() == 4001 /* 阿朗 */) {
						if (f.getName().contains("/"))
							singlePath = decodeFTPPath(f.isSymbolicLink() ? f.getLink() : f.getName());
					}
					boolean b = downSingleFile(singlePath, localDir, name.replace(":", ""), downStruct, f.getSize());
					if (!b) {
						logger.error(keyId + "下载单个文件时失败:" + singlePath + ",开始重试");
						int downTimes = 3;
						if (downCfg != null) {
							downTimes = downCfg.getDownloadTryTimes();
						}
						if (taskinInfo.getPeriod() == ConstDef.COLLECT_PERIOD_ONE_MINUTE) {
							downTimes = 1;
							logger.debug(keyId + "此任务为1分钟粒度，下载重试次数将固定为1次。");
						}
						for (int i = 0; i < downTimes; i++) {
							int downDelay = 2000;
							if (downCfg != null) {
								downDelay = downCfg.getDownloadTryDelay() * 1000;
							}
							int sleepTime = downDelay * (i + 1);
							logger.debug(keyId + "重试下载:" + singlePath + ",次数:" + (i + 1));
							Thread.sleep(sleepTime);
							login();
							if (downSingleFile(singlePath, localDir, name.replace(":", ""), downStruct, f.getSize())) {
								b = true;
								logger.debug(keyId + "重试下载成功:" + singlePath);
								break;
							}
						}
						if (!b) {
							logger.debug(keyId + "重试" + downTimes + "次下载失败:" + singlePath);
						} else {
							if (!downStruct.getLocalFail().contains(fpath))
								downStruct.getSuc().add(fpath);
						}
					} else {
						if (!downStruct.getLocalFail().contains(fpath))
							downStruct.getSuc().add(fpath);
					}
				}
			}
		} catch (Exception e) {
			logger.error(keyId + "下载文件时异常", e);
		}

		return downStruct;
	}

	/**
	 * 获取此类使用的{@link FTPClient}对象。
	 * 
	 * @return 此类使用的{@link FTPClient}对象。
	 */
	public FTPClient getFtpClient() {
		return ftp;
	}

	/**
	 * 断开FTP连接
	 */
	public void disconnect() {
		if (ftp != null) {
			try {
				ftp.logout();
				// liangww add 2012-05-23 增加logout时打印ftp返回的信息
				// liangww add 2012-05-23 增加logout时打印ftp返回的信息
				logger.debug(keyId + "【FTP退出信息。】 ftp logout" + ftp.getReplyString() + "  thread:" + Thread.currentThread());
			} catch (Exception e) {
				logger.error(keyId + " 【FTP断开时报错，不影响采集】ftp error logout" + ftp.getReplyString() + "  thread:" + Thread.currentThread(), e);
			}
			try {
				ftp.disconnect();
			} catch (Exception e) {
				logger.error(keyId + "【FTP断开时报错，不影响采集】ftp error disconnect" + ftp.getReplyString() + "  thread:" + Thread.currentThread(), e);
			}
			ftp = null;
		}
	}

	/**
	 * 解析完要删掉服务器上的文件 add by lijiayu 2013-10-08
	 * 
	 * @param ftpPaths 一组路径
	 */
	public void deleteFiles(String[] ftpPaths, String strShellCmdFinish) {
		try {
			for (int i = 0; i < ftpPaths.length; i++) {
				String ftpPath = ftpPaths[i];
				logger.info("FTPTool begin to delete file the path is:" + ftpPath);
				deleteFile(strShellCmdFinish, i, ftpPath);
			}
		} catch (IOException e) {
			logger.error("FTPTool delete file failed", e);
		}
	}

	/**
	 * 解析完要删掉服务器上的文件 add by lijiayu 2013-10-08
	 * 
	 * @param ftpPath 文件路径
	 */
	private void deleteFile(String strShellCmdFinish, int i, String ftpPath) throws IOException {
		// 从采集路径的通配符里取日期
		String regex = getDateRegex(i);
		// 得到文件要保留的天数
		String[] signs = strShellCmdFinish.split(",");
		// 得到所有同类文件路径
		String filePath = ftpPath.substring(0, ftpPath.lastIndexOf("/") + 1) + "*" + ftpPath.substring(ftpPath.lastIndexOf("."));
		// 服务器要删除的文件
		FTPFile[] ftpFiles = ftp.listFiles(encodeFTPPath(filePath));
		for (FTPFile f : ftpFiles) {
			if (f.isFile() || f.isSymbolicLink()) {
				// 得到文件在服务器上的路径
				String name = decodeFTPPath(f.isSymbolicLink() ? f.getLink() : f.getName());
				name = name.substring(name.lastIndexOf("/") + 1, name.length());
				String singlePath = ftpPath.substring(0, ftpPath.lastIndexOf("/") + 1) + name;
				singlePath = encodeFTPPath(singlePath);
				// 如果没有配 保留日期则全删除
				if (signs.length < 2) {
					ftp.deleteFile(singlePath);
					logger.info("delete file success file is:" + singlePath);
					continue;
				}
				// 校验是否为保留期内的文件
				if (isvalidDay(singlePath, signs, regex, i))
					continue;
				else
					ftp.deleteFile(singlePath);
				logger.info("delete file success file is:" + singlePath);
			}
		}
	}

	/**
	 * 验证一个文件是否为最新文件，以当前日期减传入的天数 为基准
	 * 
	 * @param fileName
	 *            文件名称
	 * @param signs
	 *            标识符里面包含文件要保留的时间
	 * @return true 文件是属于最新文件 false 文件需要删除
	 */
	private boolean isvalidDay(String fileName, String[] signs, String regex, int pathNum) {
		// 如果文件不包含日期通配符，直接返回true
		String gatherPath = taskinInfo.getCollectPath().split(";")[pathNum];
		if (gatherPath.indexOf("%%") < 0)
			return true;

		String times = signs[1];
		// 文件要保留的时间单位
		int retentionTime = Integer.parseInt(times.substring(0, times.length() - 1));
		// 文件要保留的时间单位
		String dateUnitStr = times.substring(times.length() - 1);
		SimpleDateFormat simpleFormat = new SimpleDateFormat(getDateFormatStr(pathNum));
		// 把当前日期减去要保留的时间
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		if (dateUnitStr.equals("Y"))
			calendar.set(Calendar.YEAR, calendar.get(Calendar.YEAR) - retentionTime);
		if (dateUnitStr.equals("M"))
			calendar.set(Calendar.MONTH, calendar.get(Calendar.MONTH) - retentionTime);
		if (dateUnitStr.equals("D"))
			calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH) - retentionTime);
		if (dateUnitStr.equals("H"))
			calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) - retentionTime);
		if (dateUnitStr.equals("m"))
			calendar.set(Calendar.MINUTE, calendar.get(Calendar.MINUTE) - retentionTime);
		if (dateUnitStr.equals("W"))
			calendar.set(Calendar.WEEK_OF_YEAR, calendar.get(Calendar.WEEK_OF_YEAR) - retentionTime);

		// 从文件名称里面取出文件日期
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(fileName);
		while (matcher.find()) {
			String tempDateStr = matcher.group();
			try {
				Date tempDate = simpleFormat.parse(tempDateStr);
				// 如果文件日期大于等于（当前日期-保留的天数）
				if (tempDate.compareTo(calendar.getTime()) >= 0)
					return true;
			} catch (ParseException e) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 根据采集路径里面的通配符，得到正则表达式
	 * 
	 * @param num
	 *            如果采集是多少路径，要标明是哪个路径
	 * @return
	 */
	public String getDateRegex(int pathNum) {
		// 如果文件不包含日期通配符，直接返回true
		String gatherPath = taskinInfo.getCollectPath().split(";")[pathNum];
		if (gatherPath.indexOf("%%") < 0)
			return "";
		int lastLength = getLastLength(gatherPath);
		String regex = gatherPath.substring(gatherPath.indexOf("%%"), gatherPath.lastIndexOf("%%") + lastLength);
		logger.info("gather path begin to convert regex:" + regex);
		for (String sign : regex.split("%%")) {
			if (sign.startsWith("Y"))
				regex = regex.replaceAll("%%Y", "(?!0000)[0-9]{4}");
			if (sign.startsWith("y"))
				regex = regex.replaceAll("%%y", "(?!0000)[0-9]{4}");
			if (sign.startsWith("WEEK"))
				regex = regex.replaceAll("%%WEEK", "\\d{1}");
			if (sign.startsWith("DayOfYear"))
				regex = regex.replaceAll("%%DayOfYear", "\\d{3}");
			if (sign.startsWith("EM"))
				regex = regex.replaceAll("%%EM", "[A-Za-z]{3}");
			if (sign.startsWith("M"))
				regex = regex.replaceAll("%%M", "(0[1-9]|1[0-2])");
			if (sign.startsWith("d"))
				regex = regex.replaceAll("%%d", "(0[1-9]|1[0-9]|2[0-9]|3[01])");
			if (sign.startsWith("D"))
				regex = regex.replaceAll("%%D", "(0[1-9]|1[0-9]|2[0-9]|3[01])");
			if (sign.startsWith("fd"))
				regex = regex.replaceAll("%%fd", "\\d{2}");
			if (sign.startsWith("FD"))
				regex = regex.replaceAll("%%FD", "\\d{2}");
			if (sign.startsWith("FH"))
				regex = regex.replaceAll("%%FH", "\\d{1-3}");
			if (sign.startsWith("BH"))
				regex = regex.replaceAll("%%BH", "\\d{2}");
			if (sign.startsWith("H"))
				regex = regex.replaceAll("%%H", "([01][0-9]|2[0-3])");
			if (sign.startsWith("h"))
				regex = regex.replaceAll("%%h", "([01][0-9]|2[0-3])");
			if (sign.startsWith("m"))
				regex = regex.replaceAll("%%m", "([0-5][0-9]|60)");
			if (sign.startsWith("s"))
				regex = regex.replaceAll("%%s", "([0-5][0-9]|60)");
			if (sign.startsWith("S"))
				regex = regex.replaceAll("%%S", "([0-5][0-9]|60)");
			if (sign.startsWith("NWEEK"))
				regex = regex.replaceAll("%%NWEEK", "\\d{1}");
			if (sign.startsWith("NY"))
				regex = regex.replaceAll("%%NY", "\\d{4}");
			if (sign.startsWith("Ny"))
				regex = regex.replaceAll("%%Ny", "\\d{2}");
			if (sign.startsWith("NEM"))
				regex = regex.replaceAll("%%NEM", "[A-Za-z]{3}");
			if (sign.startsWith("NM"))
				regex = regex.replaceAll("%%NM", "\\d{2}");
			if (sign.startsWith("Nd"))
				regex = regex.replaceAll("%%Nd", "\\d{2}");
			if (sign.startsWith("ND"))
				regex = regex.replaceAll("%%ND", "\\d{2}");
			if (sign.startsWith("NH"))
				regex = regex.replaceAll("%%NH", "\\d{2}");
			if (sign.startsWith("NV4"))
				regex = regex.replaceAll("%%NV4", "\\d{2}");
			if (sign.startsWith("Nh"))
				regex = regex.replaceAll("%%Nh", "\\d{2}");
			if (sign.startsWith("Nm"))
				regex = regex.replaceAll("%%Nm", "\\d{2}");
			if (sign.startsWith("Ns"))
				regex = regex.replaceAll("%%Ns", "\\d{2}");
			if (sign.startsWith("NS"))
				regex = regex.replaceAll("%%NS", "\\d{2}");
		}
		logger.info("gather path convert to regex:" + regex);
		return regex;
	}

	public String getDateFormatStr(int pathNum) {
		// 如果文件不包含日期通配符，直接返回true
		String gatherPath = taskinInfo.getCollectPath().split(";")[pathNum];
		if (gatherPath.indexOf("%%") < 0)
			return "";
		int lastLength = getLastLength(gatherPath);
		String regex = gatherPath.substring(gatherPath.indexOf("%%"), gatherPath.lastIndexOf("%%") + lastLength);

		for (String sign : regex.split("%%")) {
			if (sign.startsWith("Y"))
				regex = regex.replaceAll("%%Y", "yyyy");
			if (sign.startsWith("y"))
				regex = regex.replaceAll("%%y", "yyyy");
			if (sign.startsWith("WEEK"))
				regex = regex.replaceAll("%%WEEK", "EEE");
			if (sign.startsWith("DayOfYear"))
				regex = regex.replaceAll("%%DayOfYear", "yyy");
			if (sign.startsWith("EM"))
				regex = regex.replaceAll("%%EM", "MMM");
			if (sign.startsWith("M"))
				regex = regex.replaceAll("%%M", "MM");
			if (sign.startsWith("d"))
				regex = regex.replaceAll("%%d", "dd");
			if (sign.startsWith("D"))
				regex = regex.replaceAll("%%D", "dd");
			if (sign.startsWith("fd"))
				regex = regex.replaceAll("%%fd", "dd");
			if (sign.startsWith("FD"))
				regex = regex.replaceAll("%%FD", "dd");
			if (sign.startsWith("FH"))
				regex = regex.replaceAll("%%FH", "HH");
			if (sign.startsWith("BH"))
				regex = regex.replaceAll("%%BH", "HH");
			if (sign.startsWith("H"))
				regex = regex.replaceAll("%%H", "HH");
			if (sign.startsWith("h"))
				regex = regex.replaceAll("%%h", "HH");
			if (sign.startsWith("m"))
				regex = regex.replaceAll("%%m", "mm");
			if (sign.startsWith("s"))
				regex = regex.replaceAll("%%s", "ss");
			if (sign.startsWith("S"))
				regex = regex.replaceAll("%%S", "ss");
			if (sign.startsWith("NWEEK"))
				regex = regex.replaceAll("%%NWEEK", "EEE");
			if (sign.startsWith("NY"))
				regex = regex.replaceAll("%%NY", "yyyy");
			if (sign.startsWith("Ny"))
				regex = regex.replaceAll("%%Ny", "yyy");
			if (sign.startsWith("NEM"))
				regex = regex.replaceAll("%%NEM", "EEE");
			if (sign.startsWith("NM"))
				regex = regex.replaceAll("%%NM", "MM");
			if (sign.startsWith("Nd"))
				regex = regex.replaceAll("%%Nd", "dd");
			if (sign.startsWith("ND"))
				regex = regex.replaceAll("%%ND", "dd");
			if (sign.startsWith("NH"))
				regex = regex.replaceAll("%%NH", "HH");
			if (sign.startsWith("NV4"))
				regex = regex.replaceAll("%%NV4", "HH");
			if (sign.startsWith("Nh"))
				regex = regex.replaceAll("%%Nh", "HH");
			if (sign.startsWith("Nm"))
				regex = regex.replaceAll("%%Nm", "mm");
			if (sign.startsWith("Ns"))
				regex = regex.replaceAll("%%Ns", "ss");
			if (sign.startsWith("NS"))
				regex = regex.replaceAll("%%NS", "ss");
		}
		return regex;
	}

	public int getLastLength(String gatherPath) {
		String[] signs = gatherPath.split("%%");
		String sign = signs[signs.length - 1];
		int lenght = 3;
		if (sign.toUpperCase().startsWith("WEEK"))
			lenght = 6;
		if (sign.toUpperCase().startsWith("EM"))
			lenght = 4;
		if (sign.toUpperCase().startsWith("FD"))
			lenght = 4;
		if (sign.toUpperCase().startsWith("FH"))
			lenght = 4;
		if (sign.toUpperCase().startsWith("BH"))
			lenght = 4;
		if (sign.toUpperCase().startsWith("NWEEK"))
			lenght = 7;
		if (sign.toUpperCase().startsWith("NY"))
			lenght = 4;
		if (sign.toUpperCase().startsWith("NEM"))
			lenght = 5;
		if (sign.toUpperCase().startsWith("NM"))
			lenght = 4;
		if (sign.toUpperCase().startsWith("ND"))
			lenght = 4;
		if (sign.toUpperCase().startsWith("NH"))
			lenght = 4;
		if (sign.toUpperCase().startsWith("NV4"))
			lenght = 5;
		if (sign.toUpperCase().startsWith("NS"))
			lenght = 4;
		return lenght;
	}

	/**
	 * 登录到FTP服务器
	 * 
	 * @return 是否成功
	 */
	private boolean login() {
		disconnect();
		ftp = new UwayFTPClient();
		if (downCfg != null)
			ftp.setBufferSize(downCfg.getBufferSize());

		ftp.setRemoteVerificationEnabled(false);
		int timeout = 0;

		if (downCfg != null) {
			timeout = downCfg.getDataTimeout();
			ftp.setDataTimeout(timeout * 1000);
			ftp.setDefaultTimeout(timeout * 1000);
		} else {
			timeout = taskinInfo.getCollectTimeOut() < 1 ? 5 : taskinInfo.getCollectTimeOut(); // 超时(分钟)
			ftp.setDataTimeout(timeout * 60 * 1000);
			ftp.setDefaultTimeout(timeout * 60 * 1000);
		}

		boolean b = false;
		try {
			logger.debug(keyId + "正在连接到 - " + ip + ":" + port);
			ftp.connect(ip, port);
			logger.debug(keyId + "ftp connected");
			logger.debug(keyId + "正在进行安全验证 - " + user + " " + pwd);
			b = ftp.login(user, pwd);
			logger.debug(keyId + "ftp logged in" + "   thread:" + Thread.currentThread());
		} catch (Exception e) {
			logger.error(keyId + "登录FTP服务器时异常", e);
			return false;
		}
		if (b) {
			/* ftpConfig.xml中配置了此任务使用PASV模式 */
			if (downCfg != null && downCfg.getTransMode().equalsIgnoreCase(FTPConfig.TRANS_MODE_PASV)) {
				ftp.enterLocalPassiveMode();
				logger.debug(keyId + "进入FTP被动模式。ftp entering passive mode");
			}
			/* ftpConfig.xml中未配置，并且config.xml中没有指定使用port模式。 */
			else if (downCfg == null && !SystemConfig.getInstance().isFtpPortMode()) {
				ftp.enterLocalPassiveMode();
				logger.debug(keyId + "进入FTP被动模式。ftp entering passive mode");
			} else {
				logger.debug(keyId + "进入FTP主动模式。ftp entering local mode");
			}
			if (this.ftpCfg == null) {
				this.ftpCfg = setFTPClientConfig();
			} else {
				ftp.configure(this.ftpCfg);
			}
			try {
				ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
			} catch (IOException e) {
				logger.error(keyId + "设置FileType时异常", e);
			}
		} else {
			String rep = "";
			try {
				rep = ftp.getReplyString().trim();
			} catch (Exception e) {
			}
			logger.warn(keyId + "注意：用户名/密码可能错误，服务器返回信息：" + rep);
		}
		return b;
	}

	/**
	 * 判断FTP服务器返回码是否是2XX，即成功
	 * 
	 * @return FTP服务器返回码是否是2XX，即成功
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private boolean isPositiveCompletion() throws IOException {
		if (ftp == null) {
			return false;
		}
		return ftp.completePendingCommand();
	}

	/**
	 * 判断listFiles出来的FTP文件，是否不是空的
	 * 
	 * @param fs
	 *            listFiles出来的FTP文件
	 * @return listFiles出来的FTP文件，是否不是空的
	 */
	private boolean isFilesNotNull(FTPFile[] fs) {
		return Util.isFileNotNull(fs, this.taskinInfo.getTaskID() + "-" + this.taskinInfo.getDevInfo().getIP());
	}

	/**
	 * 编码一条FTP路径
	 * 
	 * @param ftpPath
	 *            FTP路径
	 * @return 编码后的路径
	 */
	public String encodeFTPPath(String ftpPath) {
		try {
			String str = Util.isNotNull(encode) ? new String(ftpPath.getBytes(encode), "iso_8859_1") : ftpPath;
			return str;
		} catch (UnsupportedEncodingException e) {
			logger.error(keyId + "设置的编码不正确:" + encode, e);
		}
		return ftpPath;
	}

	/**
	 * 解码一条FTP路径
	 * 
	 * @param ftpPath
	 *            FTP路径
	 * @return 解码后的路径
	 */
	public String decodeFTPPath(String ftpPath) {
		try {
			String str = Util.isNotNull(encode) ? new String(ftpPath.getBytes("iso_8859_1"), encode) : ftpPath;
			return str;
		} catch (UnsupportedEncodingException e) {
			logger.error(keyId + "设置的编码不正确:" + encode, e);
		}
		return ftpPath;
	}

	/**
	 * 下载单个文件
	 * 
	 * @param path
	 *            单个文件的FTP绝对路径
	 * @param localPath
	 *            本地文件夹
	 * @param fileName
	 *            文件名
	 * @param remoteLength
	 * @return 是否下载成功
	 */
	private boolean downSingleFile(String path, String localPath, String fileName, DownStructer downStruct, long remoteLength) {
		boolean result = false;
		boolean ex = false;
		logger.debug(keyId + "开始下载:" + path);
		double downStartTime = System.currentTimeMillis();
		boolean end = true;
		String singlePath = encodeFTPPath(path);
		File tdFile = null;
		InputStream in = null;
		OutputStream out = null;
		long length = remoteLength;
		if (length < 0) {
			logger.warn(keyId + "remote file lenght=" + length + ", file=" + path);
		} else {
			logger.debug(keyId + "remote file lenght=" + length + ", file=" + path);
		}
		double downTime = 0.0;
		long tdLength = 0;
		try {
			File dir = new File(localPath);
			if (!dir.exists()) {
				if (!dir.mkdirs()) {
					throw new Exception(keyId + "创建文件夹时异常:" + dir.getAbsolutePath());
				}
			}
			tdFile = new File(dir, fileName + ".td_" + Util.getDateString_yyyyMMddHH(taskinInfo.getLastCollectTime()));
			if (!tdFile.exists()) {
				if (!tdFile.createNewFile()) {
					throw new Exception(keyId + "创建临时文件失败:" + tdFile.getAbsolutePath());
				}
			} else {
				logger.debug(keyId + "文件已存在,文件名=" + tdFile.getAbsolutePath() + ",字节=" + tdFile.length());
			}
			tdLength = tdFile.length();
			if (tdLength >= length) {
				end = true;
			}
			in = ftp.retrieveFileStream(singlePath);
			if (tdLength > 0) {
				logger.debug(keyId + "文件已存在,文件名=" + tdFile.getAbsolutePath() + ",跳过字节数量=" + tdFile.length());
				in.skip(tdLength);
				logger.debug(keyId + "文件已存在,文件名=" + tdFile.getAbsolutePath() + ",跳过字节成功");
			}
			out = new FileOutputStream(tdFile, true);
			int buffSize = 1024;

			if (downCfg != null) {
				buffSize = downCfg.getBufferSize();;
			}

			byte[] bytes = new byte[buffSize];
			int c;
			while ((c = in.read(bytes)) != -1) {
				out.write(bytes, 0, c);
			}
			if (tdFile.length() < length) {
				end = false;
				logger.warn(keyId + tdFile.getAbsoluteFile() + ":文件下载不完整，理论长度:" + length + "，实际下载长度:" + tdFile.length());
			}
			double downEndTime = System.currentTimeMillis();
			downTime = (downEndTime - downStartTime) / 1000;
		} catch (Exception e) {
			ex = true;
			if (in == null) {
				logger.error(keyId + "FTP服务器返回输入流为null，可能文件不存在 - " + path, e);
			} else {
				logger.error(keyId + "下载单个文件时异常:" + path, e);
			}
			result = false;
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
			try {
				if (in != null)
					ftp.completePendingCommand();
			} catch (IOException e1) {
			}
			if (out != null) {
				try {
					out.flush();
					out.close();
				} catch (IOException e) {
				}
			}
			if (!ex && (end || tdLength < 0)) {
				if (in != null) {
					File f = new File(localPath, fileName);
					if (f.exists()) {
						f.delete();
					}
					boolean bRename = tdFile.renameTo(f);
					if (!bRename) {
						logger.error(keyId + "将" + tdFile.getAbsolutePath() + "重命名为" + f.getAbsolutePath() + "时失败，" + f.getAbsolutePath() + "被占用");
					} else {
						tdFile.delete();
						logger.debug(keyId + "下载成功(耗时" + downTime + "秒):" + path + "  本地路径:" + f.getAbsolutePath() + " 文件大小:" + f.length() + "字节。");
						result = true;

						if (f.length() == 0) {
							if (!downStruct.getFail().contains(singlePath))
								downStruct.getFail().add(singlePath);
							if (downStruct.getLocalFail().contains(f.getAbsolutePath()))
								downStruct.getLocalFail().add(f.getAbsolutePath());
							logger.error(keyId + ": 文件 " + f.getAbsolutePath() + " 长度为0");
							return false;
						}

					}
				} else {
					result = false;
				}
			}

		}
		return result;
	}

	/**
	 * 自动设置FTP服务器类型
	 */
	private FTPClientConfig setFTPClientConfig() {
		FTPClientConfig cfg = null;
		try {
			ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_UNIX));
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_NT));
			} else {
				logger.debug(keyId + "ftp type:UNIX");
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_AS400));
			} else {
				logger.debug(keyId + "ftp type:NT");
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_L8));
			} else {
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_MVS));
			} else {
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_NETWARE));
			} else {
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_OS2));
			} else {
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_OS400));
			} else {
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_VMS));
			} else {
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_NT));
				logger.debug(keyId + "ftp type:NT...last");
				return cfg;
			}
		} catch (Exception e) {
			logger.error("配置FTP客户端时异常", e);
			ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_UNIX));
		}
		return cfg;
	}

	public static void main(String[] args) throws Exception {
		String aa = "lrwxrwxrwx    1 516      516            28 Feb 22 02:14 20120222_947193.txt -> /data/zl/20120222_947193.txt";
		UnixFTPEntryParser p = new UnixFTPEntryParser();
		FTPFile ff = p.parseFTPEntry(aa);
		System.out.println(ff.getLink());

		CollectObjInfo info = new CollectObjInfo(333);
		info.setLastCollectTime(new Timestamp(Util.getDate1("2010-01-01 12:00:00").getTime()));
		DevInfo di = new DevInfo();
		di.setHostPwd("1");
		di.setHostUser("administrator");
		di.setIP("192.168.0.170");
		info.setDevInfo(di);
		info.setDevPort(21);
		FTPTool t = new FTPTool(info);

		t.login(2000, 3);
		// System.out.println(t.getFtpClient().getLocalPort());

		DownStructer fs = t.downFile("/ACIE/ACIE_NLexport_Dir1/Adjacency.csv", "E:\\datacollector_path\\eric");
		for (String s : fs.getSuc()) {
			System.out.println(s);
		}
		t.disconnect();
	}

	public void finalize() throws Throwable {
		disconnect();
	}
}

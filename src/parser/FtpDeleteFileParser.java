package parser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import util.LogMgr;
import util.Util;
import collect.FTPTool;

/**
 * 用于删除FTP服务器上的文件
 * 
 * @author lijiayu @ 2013-12-4
 */
public class FtpDeleteFileParser extends Parser {

	private static final Logger LOGGER = LogMgr.getInstance().getSystemLogger();

	private CollectObjInfo taskInfo;

	private String logKey;

	private FTPTool ftpTool;

	public FtpDeleteFileParser() {
	}

	public FtpDeleteFileParser(CollectObjInfo task) {
		this.taskInfo = task;
		this.ftpTool = new FTPTool(task);
		long id = task.getTaskID();
		if (task instanceof RegatherObjInfo)
			id = task.getKeyID() - 10000000;
		this.logKey = task.getTaskID() + "-" + id;
		this.logKey = String.format("[%s][%s]", this.logKey, Util.getDateString(task.getLastCollectTime()));
	}

	/**
	 * 删掉Ftp服务器上的文件 add by lijiayu 2013-12-04<br>
	 * 标识配置支持按 年Y，月M，日D，时H，分m，周W<br>
	 * 如按日为 'deleteFile,3D'<br>
	 * 
	 * @return 文件删除是否成功标志
	 */
	public boolean deleteFiles() {
		info("FtpDeleteFileParser begin to delete File");
		// 1. 验证任务配置
		if (!validTaskInfo())
			return false;
		try {
			// 2. 登录FTP
			boolean bOK = ftpTool.login(30000, 3);
			if (!bOK) {
				error("FtpDeleteFileParser FTP多次尝试登陆失败!");
				return false;
			}
			debug("FtpDeleteFileParser FTP登陆成功.");
			// 3. 删除文件,按单个采集路径删除
			String[] paths = taskInfo.getCollectPath().split(";");
			for (String gatherPath : paths) {
				deleteFile(gatherPath);
			}
			return true;
		} catch (Exception e) {
			error("FtpDeleteFileParser 删除文件异常!", e);
		} finally {
			ftpTool.disconnect();
		}

		return false;
	}

	/**
	 * 删除单个路径下面的文件
	 * 
	 * @param gatherPath
	 * @throws IOException
	 */
	private void deleteFile(String gatherPath) throws IOException {
		info("FtpDeleteFileParser deleteFile gatherPath:" + gatherPath);
		// 得到所有同类文件路径
		String filePath = gatherPath.substring(0, gatherPath.lastIndexOf("/") + 1) + "*" + gatherPath.substring(gatherPath.lastIndexOf("."));
		// 从采集路径的通配符里取日期
		String regex = getDateRegex(gatherPath);
		// 服务器要删除的文件
		FTPClient ftpClient = ftpTool.getFtpClient();
		FTPFile[] ftpFiles = ftpClient.listFiles(ftpTool.encodeFTPPath(filePath));
		for (FTPFile f : ftpFiles) {
			if (f.isFile() || f.isSymbolicLink()) {
				// 得到文件在服务器上的路径
				String name = ftpTool.decodeFTPPath(f.isSymbolicLink() ? f.getLink() : f.getName());
				name = name.substring(name.lastIndexOf("/") + 1, name.length());
				String singlePath = gatherPath.substring(0, gatherPath.lastIndexOf("/") + 1) + name;
				singlePath = ftpTool.encodeFTPPath(singlePath);
				// 校验是否为最新的文件
				if (isRecentlyFile(singlePath, regex, gatherPath))
					continue;
				else
					ftpClient.deleteFile(singlePath);
				info("FtpDeleteFileParser delete file success file is:" + singlePath);
			}
		}
	}

	private boolean validTaskInfo() {
		// 根据标识判断 解析完是否要删掉服务器上的文件
		String strShellCmdFinish = taskInfo.getShellCmdFinish();
		if (Util.isNull(strShellCmdFinish) || !strShellCmdFinish.trim().toUpperCase().startsWith("DELETEFILE")) {
			error("delete file failed! field shell_cmd_finish is incorrect!");
			return false;
		}
		String[] paths = taskInfo.getCollectPath().split(";");
		for (String gatherPath : paths) {
			if (gatherPath.indexOf("%%") < 0) {
				error("delete file failed! field collect_path must contain '%%'!");
				return false;
			}
		}
		return true;
	}

	/**
	 * 根据采集路径里面的通配符，得到正则表达式
	 * 
	 * @param num 如果采集是多少路径，要标明是哪个路径
	 * @return
	 */
	private String getDateRegex(String gatherPath) {
		int lastLength = getLastLength(gatherPath);
		String regex = gatherPath.substring(gatherPath.indexOf("%%"), gatherPath.lastIndexOf("%%") + lastLength);
		info("gather path begin to convert regex:" + regex);
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
		info("gather path convert to regex:" + regex);
		return regex;
	}

	/**
	 * 最后的一个通配符跟着字符串长度
	 * 
	 * @param gatherPath
	 * @return
	 */
	private int getLastLength(String gatherPath) {
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
	 * 把通配符转换为 时期格式化
	 * 
	 * @return
	 */
	public String getDateFormatStr(String gatherPath) {

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

	/**
	 * 验证一个文件是否为最新文件，以当前日期减传入的天数 为基准
	 * 
	 * @param fileName 文件名称
	 * @param regex 用于从文件里取日期的正则表达式
	 * @param gatherPath 原始的采集路径 单个的
	 * @return true 文件是属于最新文件 false 文件需要删除
	 */
	private boolean isRecentlyFile(String fileName, String regex, String gatherPath) {
		// 得到文件要保留的天数
		String[] signs = taskInfo.getShellCmdFinish().split(",");
		// 如果没有配保留日期,则全删除
		if (signs.length < 2) {
			return false;
		}
		String times = signs[1];
		// 文件要保留的时间单位
		int retentionTime = Integer.parseInt(times.substring(0, times.length() - 1));
		// 文件要保留的时间单位
		String dateUnitStr = times.substring(times.length() - 1);
		SimpleDateFormat simpleFormat = new SimpleDateFormat(getDateFormatStr(gatherPath));
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
	 * 记录INFO级别日志。
	 * 
	 * @param msg 日志信息。
	 */
	protected void info(Object msg) {
		LOGGER.info(logKey + msg);
	}

	/**
	 * 记录DEBUG级别日志。
	 * 
	 * @param msg 日志信息。
	 */
	protected void debug(Object msg) {
		LOGGER.debug(logKey + msg);
	}

	/**
	 * 记录ERROR级别日志。
	 * 
	 * @param msg 日志信息。
	 * @param ex 异常信息。
	 */
	protected void error(Object msg, Throwable ex) {
		if (ex == null)
			LOGGER.error(logKey + msg);
		else {
			LOGGER.error(logKey + msg, ex);
			LOGGER.error(ex, ex);
		}
	}

	/**
	 * 记录ERROR级别日志。
	 * 
	 * @param msg 日志信息。
	 */
	protected void error(Object msg) {
		error(msg, null);
	}

	protected void warn(Object msg) {
		LOGGER.warn(logKey + msg);
	}

	@Override
	public boolean parseData() throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

}

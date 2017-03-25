package util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.nfunk.jep.JEP;

import framework.SystemConfig;

/**
 * 公共方法类
 * 
 * @author YangJian
 * @version 1.0
 */
public class Util {

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	/** 获取本地计算机名 */
	public static String getHostName() {
		String strHostName = null;
		try {
			strHostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			// log.error("获取hostname时异常", e);
			strHostName = Util.nvl(e.getMessage(), "").trim();
			try {
				strHostName = strHostName.split(":")[0].trim();
			} catch (Exception exx) {
			}
		}

		return strHostName;
	}

	/** 显示当前系统物理状况 */
	public static void showOSState() {
		long maxMemory = Runtime.getRuntime().maxMemory() / 1024;
		long totalMemory = Runtime.getRuntime().totalMemory() / 1024;
		long freeMemory = Runtime.getRuntime().freeMemory() / 1024;
		long usedMemory = totalMemory - freeMemory;
		freeMemory = maxMemory - usedMemory;

		log.debug("OS State: mem used:" + usedMemory + "KB,mem free:" + freeMemory + "KB,mem total:" + totalMemory + "KB.");
	}

	/** 显示 程序运行的 系统环境 信息 */
	public static void printEnvironmentInfo() {
		try {
			Properties props = System.getProperties();

			StringBuffer sb = new StringBuffer();

			sb.append("\n----------------------Environment Info--------------------------\n");
			sb.append("os.name : " + props.getProperty("os.name") + "\n"); // 操作系统名称
			sb.append("os.arch : " + props.getProperty("os.arch") + "\n"); // 操作系统构架
			sb.append("os.version : " + props.getProperty("os.version") + "\n"); // 操作系统版本

			sb.append("java.version : " + props.getProperty("java.version") + "\n"); // Java 运行时环境版本
			sb.append("java.vendor : " + props.getProperty("java.vendor") + "\n"); // Java 运行时环境供应商
			sb.append("java.vm.name : " + props.getProperty("java.vm.name") + "\n"); // Java 虚拟机实现名称
			sb.append("java.home : " + props.getProperty("java.home") + "\n"); // Java
			// 安装目录
			sb.append("java.class.path : " + props.getProperty("java.class.path") + "\n"); // Java 类路径
			sb.append("java.library.path : " + props.getProperty("java.library.path") + "\n"); // 加载库时搜索的路径列表

			sb.append("user.name : " + props.getProperty("user.name") + "\n"); // 用户的账户名称
			sb.append("user.home : " + props.getProperty("user.home") + "\n"); // 用户的主目录
			sb.append("user.dir : " + props.getProperty("user.dir") + "\n"); // 用户的当前工作目录

			sb.append("file.encoding : " + props.getProperty("file.encoding") + "\n");

			sb.append("--------Disk information---------\n");
			printDiskInfo(sb);

			sb.append("----------------------------------------------------------------");

			log.info(sb.toString());
		} catch (Exception e) {
		}
	}

	/** 记录磁盘信息 */
	public static void printDiskInfo(StringBuffer sb) {
		try {
			File[] roots = File.listRoots();
			for (File _file : roots) {
				sb.append(_file.getPath() + "\n");
				sb.append("Free space = " + _file.getFreeSpace() + "\n");
				sb.append("Usable space = " + _file.getUsableSpace() + "\n");
				sb.append("Total space = " + _file.getTotalSpace() + "\n");
				sb.append("\n");
			}
		} catch (Exception e) {
		}
	}

	/**
	 * 在标准输出端口上打印当前内存占用情况。
	 */
	public static void printMemoryStatus() {
		float maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
		float totalMemory = Runtime.getRuntime().totalMemory() / (1024 * 1024);
		float freeMemory = Runtime.getRuntime().freeMemory() / (1024 * 1024);
		float usedMemory = totalMemory - freeMemory;
		freeMemory = maxMemory - usedMemory;
		System.out.println("已使用: " + usedMemory + "M  剩余: " + freeMemory + "M  最大内存: " + maxMemory + "M");
	}

	/**
	 * @param b
	 *            byte[]
	 * @return String
	 */
	public static String string2Hex(String b) {
		if (b == null || b.equals(""))
			return null;

		return bytes2Hex(b.getBytes());
	}

	/**
	 * 字节转化为十六进制
	 * 
	 * @param b
	 *            byte[]
	 * @return String
	 */
	public static String bytes2Hex(byte[] b) {
		if (b == null)
			return null;
		String ret = "";
		for (int i = 0; i < b.length; i++) {
			String hex = Integer.toHexString(b[i] & 0xFF);
			if (hex.length() == 1) {
				hex = '0' + hex;
			}
			ret += hex.toUpperCase();
		}
		return ret;
	}

	/** 执行外部命令 */
	public static int execExternalCmd(String cmd) throws Exception {
		ExternalCmd externalCmd = new ExternalCmd();

		return externalCmd.execute(cmd);
	}

	/** 字符串 null 或 空 判断 */
	public static boolean isNull(String str) {
		boolean bReturn = false;

		if (str == null || str.trim().equals("")) {
			bReturn = true;
		}

		return bReturn;
	}

	/** 字符串 非null 和 非空 判断 */
	public static boolean isNotNull(String str) {
		boolean bReturn = false;

		if (str != null && !str.trim().equals("")) {
			bReturn = true;
		}

		return bReturn;
	}

	/** 转换 byte数组 为 char数组 */
	public static char[] bytesToChars(byte[] bytes) {
		String s = new String(bytes);
		char[] c = s.toCharArray();

		return c;
	}

	/** 计算字符串表达式的值 ，比如 字符串"1+2" 计算后为3 */
	public static int parseExpression(String str) {
		JEP myParser = new JEP();

		myParser.parseExpression(str);

		return (int) myParser.getValue();
	}

	/** 当时间为0-9的时候返回 00-09，为10-23的时候返回10-23 */
	public static String trimHour(int hour) {
		String str = String.valueOf(hour);
		if (str.length() == 1) {
			str = "0" + str;
		}

		return str;
	}

	/** 判断文件是否为压缩文件 */
	public static boolean isZipFile(String strFileName) {
		boolean bReturn = false;

		if (strFileName == null || strFileName.equals(""))
			return false;

		int nExIndex = strFileName.lastIndexOf(".");

		String strExName = "";
		if (nExIndex > 0)
			strExName = strFileName.substring(nExIndex).toLowerCase();

		if (strExName.equals(".zip") || strExName.equals(".tar") || strExName.equals(".tar.z") || strExName.equals(".gz")
				|| strExName.equals(".tar.bz2") || strExName.equals(".rar") || strExName.equals(".z") || strExName.equals(".tgz")) {
			bReturn = true;
		}
		// liangww add 2012-09-12 增加配置zip文件解析后缀
		else {
			String zipFileSuffixs = SystemConfig.getInstance().getZipFileSuffixs();
			if (zipFileSuffixs != null) {
				String[] array = zipFileSuffixs.split(",");
				for (int i = 0; i < array.length; i++) {
					if (array[i].equals(strExName)) {
						bReturn = true;
						break;
					}
				}// for(int i=0; i < array.length; i++)
			}// if(zipFileSuffix != null)
		}

		return bReturn;
	}

	/**
	 * 将list转换为字符串数组
	 * 
	 * @param list
	 * @return
	 */
	public static String[] list2Array(List<String> list) {
		if (list == null)
			return null;
		int size = list.size();
		String[] values = new String[size];

		for (int i = 0; i < size; i++) {
			values[i] = list.get(i);
		}
		return values.length > 0 ? values : null;
	}

	/**
	 * 将list转换为字符串数组
	 * 
	 * @param list
	 * @return
	 */
	public static String list2String(List<String> list, String split) {
		if (list == null)
			return null;
		StringBuilder sb = new StringBuilder();
		for (String s : list) {
			sb.append(s).append(split);
		}
		return sb.toString();
	}

	/** 数组转化为字符串 */
	public static String array2String(String[] strs) {
		if (strs == null)
			return null;
		StringBuilder sb = new StringBuilder();
		for (String s : strs) {
			sb.append(s).append(";");
		}
		return sb.toString();
	}

	/** 判断是否为windows操作系统 */
	public static boolean isWindows() {
		boolean bReturn = false;

		String os = System.getProperties().getProperty("os.name").toLowerCase();
		if (os == null && "".equals(os))
			return true;

		if (os.indexOf("window") >= 0) {
			bReturn = true;
		}

		return bReturn;
	}

	/** 是否为 oracle 数据库 */
	public static boolean isOracle() {
		String strDriver = SystemConfig.getInstance().getDbDriver();

		return strDriver.contains("oracle");
	}

	/** 是否为 sybase 数据库 */
	public static boolean isSybase() {
		String strDriver = SystemConfig.getInstance().getDbDriver();

		return strDriver.contains("sybase");
	}

	/** 是否为 mysql 数据库 */
	public static boolean isMysql() {
		String strDriver = SystemConfig.getInstance().getDbDriver();

		return strDriver.contains("mysql");
	}

	/** 是否为 sqlserver 数据库 */
	public static boolean isSqlServer() {
		String strDriver = SystemConfig.getInstance().getDbDriver();

		return strDriver.contains("sqlserver");
	}

	/***************************************************************************
	 * 以下为 时间相关 方法
	 **************************************************************************/

	/** 转换时间为字符串格式 yyyy-MM-dd HH:mm:ss */
	public static String getDateString(Date date) {
		String pattern = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat f = new SimpleDateFormat(pattern);

		return f.format(date);
	}

	/** 转换时间为字符串格式 yyyy-MM-dd HH:mm:ss */
	public static String getDateString(Timestamp tsp) {
		Date date = new Date(tsp.getTime());

		return getDateString(date);
	}

	/** 转换时间为字符串格式 yyyyMMddHHmmss */
	public static String getDateString_yyyyMMddHHmmss(Date date) {
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMddHHmmss");
		String strTime = f.format(date);

		return strTime;
	}

	/** 转换时间为字符串格式 yyyyMMddHHmm */
	public static String getDateString_yyyyMMddHHmm(Date date) {
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMddHHmm");
		String strTime = f.format(date);

		return strTime;
	}

	/** 转换时间为字符串格式 yyyyMMddHH */
	public static String getDateString_yyyyMMddHH(Date date) {
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMddHH");
		String strTime = f.format(date);

		return strTime;
	}

	/** 转换时间为字符串格式 yyyyMMdd */
	public static String getDateString_yyyyMMdd(Date date) {
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
		String strTime = f.format(date);

		return strTime;
	}

	/** 转换时间为字符串格式 yyyyMMddHHmmssSSS */
	public static String getDateString_yyyyMMddHHmmssSSS(Date date) {
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		String strTime = f.format(date);

		return strTime;
	}

	/** 转换时间为字符串格式 yyyyMMddHHmmss */
	public static String getDateString_yyyyMMddHHmmss(Timestamp tsp) {
		Date date = new Date(tsp.getTime());

		return getDateString_yyyyMMddHHmmss(date);
	}

	/** 把 字符串 转换成 时间 */
	public static Date getDate(String str, String pattern) throws ParseException {
		SimpleDateFormat f = new SimpleDateFormat(pattern);
		Date d = f.parse(str);

		return d;
	}

	/** 把 yyyy-MM-dd HH:mm:ss形式的字符串 转换成 时间 */
	public static Date getDate1(String str) throws ParseException {
		String pattern = "yyyy-MM-dd HH:mm:ss";

		return getDate(str, pattern);
	}

	/** 把 yyyyMMddhhmmss形式的字符串 转换成 时间 */
	public static Date getDate2(String str) throws ParseException {
		String pattern = "yyyyMMddhhmmss";

		return getDate(str, pattern);
	}

	// 通过正则表达式查找
	public static String findByRegex(String str, String regEx, int group) {
		String resultValue = null;
		if (regEx == null || (regEx != null && "".equals(regEx.trim()))) {
			return resultValue;
		}
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);

		boolean result = m.find();// 查找是否有匹配的结果
		if (result) {
			resultValue = m.group(group);// 找出匹配的结果
		}
		return resultValue;
	}

	public static long crc32(String str) {
		java.util.zip.CRC32 x = new java.util.zip.CRC32();
		x.update(str.getBytes());
		return x.getValue();
	}

	/**
	 * 把一组pojo对象转换为xml格式
	 * 
	 * @param pojos
	 * @return
	 */
	public static <T> Document pojosToXML(List<T> pojos) {
		Document doc = DocumentHelper.createDocument();
		Element root = doc.addElement("pojos");
		for (Object pojo : pojos) {
			Element child = root.addElement("pojo");
			Class<?> cls = pojo.getClass();
			Field[] fields = cls.getDeclaredFields();

			final String DATE1 = "java.sql.Timestamp";
			final String DATE2 = "java.util.Date";

			for (Field f : fields) {
				f.setAccessible(true);
				String name = f.getName();
				String value = null;
				String type = f.getType().getName();
				if (type.equals(DATE1) || type.equals(DATE2)) {
					try {
						Object obj = f.get(pojo);
						if (obj == null) {
							value = "";
						} else {
							Date d = (Date) obj;
							value = getDateString(d);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

				} else {
					try {
						Object obj = f.get(pojo);
						if (obj == null) {
							value = "";
						} else {
							value = obj.toString();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
				Element e = child.addElement(name);
				e.setText(value);
			}
		}
		return doc;
	}

	/**
	 * 把一个pojo对象转换为xml格式
	 * 
	 * @param pojo
	 * @return
	 */
	public static Document pojoToXML(Object pojo) {
		List<Object> list = new ArrayList<Object>();
		list.add(pojo);
		return pojosToXML(list);
	}

	/**
	 * 将字符串转为md5码，如果传入的字符串为null，将返回null
	 * 
	 * @param s
	 *            字符串
	 * @return md5码
	 */
	public static String toMD5(String s) {
		if (s == null) {
			return null;
		}
		StringBuilder result = new StringBuilder();

		byte[] bs = null;
		try {
			bs = MessageDigest.getInstance("md5").digest(s.getBytes());
		} catch (NoSuchAlgorithmException e) {
			log.error("转md5时异常", e);
			return null;
		}

		String tmp = null;
		for (int i = 0; i < bs.length; i++) {
			tmp = (Integer.toHexString(bs[i] & 0xFF));
			if (tmp.length() == 1) {
				result.append("0");
			}
			result.append(tmp);
		}

		return result.toString();
	}

	// 单元测试
	public static void main(String[] args) throws Exception {
	}

	public static boolean isFileNotNull(FTPFile[] fs, String ip) {
		if (fs == null) {
			// if ( ip != null )
			// {
			// log.debug(ip + " - test - fs == null!");
			// }
			return false;
		}
		if (fs.length == 0) {
			// if ( ip != null )
			// {
			// log.debug(ip + " - test - fs.length == 0!");
			// }
			return false;
		}
		boolean b = false;
		for (FTPFile f : fs) {
			// if ( f != null && ip != null )
			// {
			// log.debug(ip + " - test - [" + f.getName() + "]");
			// }
			// else if ( f == null )
			// {
			// log.debug(ip + " - test f == null!");
			// }

			if ((f != null && Util.isNotNull(f.getName()) && !f.getName().contains("\t"))
					|| (f != null && Util.isNotNull(f.getName()) && f.getName().contains(".loc"))) {
				return true;
			}

		}
		return b;
	}

	public static boolean isFileNotNull(FTPFile[] fs) {

		return isFileNotNull(fs, null);
	}

	private static FTPClient newFTP(String ip, int port, String user, String pwd, String type) throws Exception {
		FTPClient ftp = new FTPClient();
		ftp.connect(ip, port);
		ftp.login(user, pwd);
		ftp.configure(new FTPClientConfig(type));
		return ftp;
	}

	public static FTPClient setFTPClientConfig(FTPClient ftp, String ip, int port, String user, String pwd) throws Exception {

		// 修改成与FTPTool一样的设置顺序--liangww modify 2012-11-21
		ftp.configure(new FTPClientConfig(FTPClientConfig.SYST_UNIX));
		if (!isFileNotNull(ftp.listFiles("/*"))) {
			ftp.disconnect();
			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_NT);
		} else {
			return ftp;
		}
		if (!isFileNotNull(ftp.listFiles("/*"))) {
			ftp.disconnect();
			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_AS400);
		} else {
			return ftp;
		}
		if (!isFileNotNull(ftp.listFiles("/*"))) {
			ftp.disconnect();
			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_L8);
		} else {
			return ftp;
		}
		if (!isFileNotNull(ftp.listFiles("/*"))) {
			ftp.disconnect();
			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_MVS);
		} else {
			return ftp;
		}
		if (!isFileNotNull(ftp.listFiles("/*"))) {
			ftp.disconnect();
			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_NETWARE);
		} else {
			return ftp;
		}
		if (!isFileNotNull(ftp.listFiles("/*"))) {
			ftp.disconnect();
			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_OS2);
		} else {
			log.debug("use SYST_NETWARE");
		}
		if (!isFileNotNull(ftp.listFiles("/*"))) {
			ftp.disconnect();
			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_OS400);
		} else {
			return ftp;
		}
		if (!isFileNotNull(ftp.listFiles("/*"))) {
			ftp.disconnect();
			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_VMS);
		} else {
			return ftp;
		}
		return ftp;
	}

	/**
	 * 用于在FTP下载目录，将目录中有通配符的采集路径，展开为正常的（目录中无通配符）多条路径。
	 * 
	 * @param collectPath
	 *            采集任务中的一条采集路径
	 * @param ip
	 *            FTP的IP
	 * @param port
	 *            端口
	 * @param user
	 *            用户名
	 * @param pwd
	 *            密码
	 * @param encode
	 *            编码
	 * @param reConnect
	 *            是否重连
	 * @return
	 */
	public static List<String> listFTPDirs(String collectPath, String ip, int port, String user, String pwd, String encode, boolean reConnect,
			int parserId) throws Exception {
		List<String> result = new ArrayList<String>();
		String tmp = collectPath.substring(0, collectPath.lastIndexOf("/"));
		if (!tmp.contains("*") && !tmp.contains("?")) {
			result.add(collectPath);
			return result;
		}
		if (parserId == 18 || parserId == 19 || parserId == 4001) {
			result.add(collectPath);
			return result;
		}
		FTPClient ftp = new FTPClient();
		try {
			ftp.connect(ip, port);
			ftp.login(user, pwd);
			ftp = setFTPClientConfig(ftp, ip, port, user, pwd);
			if (!SystemConfig.getInstance().isFtpPortMode())
				ftp.enterLocalPassiveMode();
			ftp.setFileType(FTPClient.BINARY_FILE_TYPE);// 设置为二进制传输模式

			ftp.setDataTimeout(3600 * 1000);
			ftp.setDefaultTimeout(3600 * 1000);
			String[] split = collectPath.split("/");

			List<String> temp = new ArrayList<String>();
			for (int i = 0; i < split.length; i++) {
				String s = split[i];
				if (isNotNull(s)) {
					if (!s.startsWith("/")) {
						s = "/" + s;
					}

					if (temp.size() > 0) {
						List<String> temp2 = new ArrayList<String>(temp);
						temp.clear();
						for (String t : temp2) {
							String str = t + s;
							if (!s.contains("*") && !s.contains("?")) {
								result.add(str);
								temp.add(str);
								continue;
							}
							str = isNotNull(encode) ? new String(str.getBytes(encode), "iso_8859_1") : str;
							log.debug("listing - " + str);
							FTPFile[] fs = ftp.listFiles(str);
							if (fs.length == 0 && reConnect) {
								for (int times = 0; times < 3; times++) {
									int delay = (times + 1) * 1500;
									Thread.sleep(delay);
									ftp.disconnect();
									Thread.sleep(500);
									ftp.connect(ip, port);
									ftp.login(user, pwd);
									ftp = Util.setFTPClientConfig(ftp, ip, port, user, pwd);
									ftp.enterLocalPassiveMode();
									ftp.setFileType(FTPClient.BINARY_FILE_TYPE);// 设置为二进制传输模式
									ftp.setDataTimeout(3600 * 1000);
									ftp.setDefaultTimeout(3600 * 1000);
									fs = ftp.listFiles(str);
									if (fs.length > 0) {
										break;
									}
								}
							}
							for (FTPFile f : fs) {
								log.debug("找到目录 - " + f.getName() + "    parent - " + str + "  raw - " + f.getRawListing());
								if (f.isDirectory() && !f.getName().endsWith(".") && !f.getName().endsWith("..")) {
									String name = f.getName();
									name = name.substring(name.lastIndexOf("/") + 1, name.length());
									name = isNotNull(encode) ? new String(name.getBytes("iso_8859_1"), encode) : name;
									if (!name.startsWith("/")) {
										result.add(t + "/" + name);
										temp.add(t + "/" + name);
									} else {
										result.add(t + name);
										temp.add(t + name);
									}
								}
							}
						}
					} else {
						if (!s.contains("*") && !s.contains("?")) {
							result.add(s);
							temp.add(s);
							continue;
						}
						s = isNotNull(encode) ? new String(s.getBytes(encode), "iso_8859_1") : s;

						log.debug("listing(s) - " + s);
						FTPFile[] fs = ftp.listFiles(s);
						if (fs.length == 0 && reConnect) {
							for (int times = 0; times < 3; times++) {
								int delay = (times + 1) * 1500;
								Thread.sleep(delay);
								ftp.disconnect();
								Thread.sleep(500);
								ftp.connect(ip, port);
								ftp.login(user, pwd);
								ftp = Util.setFTPClientConfig(ftp, ip, port, user, pwd);
								ftp.enterLocalPassiveMode();
								ftp.setFileType(FTPClient.BINARY_FILE_TYPE);// 设置为二进制传输模式
								ftp.setDataTimeout(3600 * 1000);
								ftp.setDefaultTimeout(3600 * 1000);
								fs = ftp.listFiles(s);
								if (fs.length > 0) {
									break;
								}
							}
						}
						for (FTPFile f : fs) {
							if (f.isDirectory() && !f.getName().equals(".") && !f.getName().equals("..")) {
								String name = f.getName();
								name = isNotNull(encode) ? new String(name.getBytes("iso_8859_1"), encode) : name;
								result.add("/" + name);
								temp.add("/" + name);
							}
						}
					}
				}
			}

			List<String> tmpResult = new ArrayList<String>(result);

			for (String s : result) {
				if (s.split("/").length != split.length - 1) {
					tmpResult.remove(s);
				}
			}

			result.clear();

			for (String s : tmpResult) {
				result.add(s + "/" + split[split.length - 1]);
			}

		} finally {
			ftp.disconnect();
		}
		return result;
	}

	/**
	 * 用于在FTP下载目录，将目录中有通配符的采集路径，展开为正常的（目录中无通配符）多条路径。
	 * 
	 * @param collectPath
	 *            采集任务中的一条采集路径
	 * @param ip
	 *            FTP的IP
	 * @param port
	 *            端口
	 * @param user
	 *            用户名
	 * @param pwd
	 *            密码
	 * @param encode
	 *            编码
	 * @param parserId
	 *            解析方式
	 * @return
	 */
	public static List<String> listFTPDirs(String collectPath, String ip, int port, String user, String pwd, String encode, int parserId)
			throws Exception {
		return listFTPDirs(collectPath, ip, port, user, pwd, encode, true, parserId);
	}

	/**
	 * 判断一个字符，是否可作为ORACLE的NUMBER类型数据，并返回相应的{@link Double}对象。如果传入的字符串为 <code>null</code>或空字符串，将返回 <code>null</code>.
	 * 
	 * @param str
	 *            需要判断的字符串。
	 * @return {@link Double}对象。
	 * @author ChenSijiang 2011-01-04 17:23
	 */
	public static Double isOracleNumberString(String str) {
		if (Util.isNull(str))
			return null;
		String s = str.trim();
		try {
			Double dou = Double.parseDouble(s);
			return dou;
		} catch (Exception unused) {
			return null;
		}
	}

	/**
	 * <p>
	 * 关闭{@link Closeable}.
	 * </p>
	 * 
	 * @param closeable
	 *            {@link Closeable}对象
	 * @author ChenSijiang 2011-01-04 18:52
	 */
	public static void closeCloseable(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (IOException unused) {
			}
		}
	}

	/**
	 * 空字符串处理方法。当<code>str</code>为<code>null</code>或空字符串时，返回<code>replace</code> ，否则返回 <code>str</code>.
	 * 
	 * @param str
	 *            需要进行处理的字符串。
	 * @param replace
	 *            当<code>str</code>为<code>null</code>或空字符串时的替换值。
	 * @return 当<code>str</code>为<code>null</code>或空字符串时，返回<code>replace</code> ，否则返回 <code>str</code>.
	 */
	public static String nvl(String str, String replace) {
		return Util.isNull(str) ? replace : str;
	}

	public static String listStringArray(String[] strArr, char splitChar) {
		if (strArr == null || strArr.length == 0)
			return "";

		StringBuilder sb = new StringBuilder();

		for (String s : strArr) {
			sb.append(s).append(splitChar);
		}
		sb.delete(sb.length() - 1, sb.length());
		return sb.toString();
	}

	/**
	 * 从data中提取字符串
	 * 
	 * @param data
	 * @param beginStr
	 *            开始标记
	 * @param endStr
	 *            结束标记
	 * @return
	 */
	public static String getStr(String data, String beginStr, String endStr) {
		return getStr(data, "", beginStr, endStr);
	}

	/**
	 * 从data中提取字符串
	 * 
	 * @param data
	 * @param begin1Str
	 *            开始1标记
	 * @param begin2Str
	 *            开始2标记
	 * @param endStr
	 *            结束标记
	 * @return
	 */
	public static String getStr(String data, String begin1Str, String begin2Str, String endStr) {
		if (data == null || begin1Str == null || begin2Str == null || endStr == null) {
			return null;
		}

		int begin1 = data.indexOf(begin1Str);
		if (begin1 == -1) {
			return null;
		}

		int begin2 = data.indexOf(begin2Str, begin1 + begin1Str.length());
		if (begin2 == -1) {
			return null;
		}

		int end = data.indexOf(endStr, begin2 + begin2Str.length());
		if (end == -1) {
			return null;
		}

		return data.substring(begin2 + begin2Str.length(), end);
	}

	/**
	 * 从尾总开始的提取data数据
	 * 
	 * @param data
	 * @param begin1Str
	 *            从尾总查找的开始1标记
	 * @param begin2Str
	 *            从尾总查找的开始2标记
	 * @param endStr
	 *            从尾总查找的结束标记
	 * @return
	 */
	public static String getStrFromLast(String data, String begin1Str, String begin2Str, String endStr) {
		int begin1 = data.lastIndexOf(begin1Str);
		if (begin1 == -1) {
			return null;
		}

		int begin2 = data.substring(0, begin1).lastIndexOf(begin2Str);
		if (begin2 == -1) {
			return null;
		}

		int end = data.substring(0, begin2).lastIndexOf(endStr);
		if (end == -1) {
			return null;
		}

		return data.substring(end + endStr.length(), begin2);
	}

	/**
	 * 判断是否是32位操作系统
	 * 
	 * @return
	 */
	public static boolean is32Digit() {
		int os_digit = Integer.parseInt(System.getProperty("sun.arch.data.model"));
		log.debug("操作系统位数：" + os_digit);
		if (os_digit == 32)
			return true;
		return false;
	}

	/**
	 * 解析英文格式的时间 如：16-JUL-13 01.40.37.076000 AM
	 * 
	 * @param time
	 * @return
	 * @throws ParseException
	 */
	public static String parseEnglishTime(String time) throws ParseException {
		time = time.replace("000 ", " ");
		DateFormat df1 = new SimpleDateFormat("dd-MMM-yy hh.mm.ss.SSS aa", Locale.ENGLISH);
		Date date = df1.parse(time);
		df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return df1.format(date);
	}

	/**
	 * 字符串拆分
	 * 
	 * @param string
	 * @param perfix
	 * @param maxArrayLength
	 * @return 拆分后的数组
	 */
	public static final String[] split(String string, String perfix) {
		if (isNull(string))
			return null;
		if (perfix == null || perfix.length() == 0)
			return new String[]{string};
		int index = string.indexOf(perfix);
		if (index < 0)
			return new String[]{string};
		List<String> splitList = new ArrayList<String>();
		while ((index = string.indexOf(perfix)) != -1) {
			splitList.add(string.substring(0, index));
			string = string.substring(index + perfix.length());
		}
		splitList.add(string.substring(0));
		String[] array = new String[splitList.size()];
		return splitList.toArray(array);
	}
}

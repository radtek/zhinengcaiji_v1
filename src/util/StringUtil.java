package util;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 字符串工具类。
 * 
 * @author ChenSijiang 2012-10-29
 */
public final class StringUtil {

	/**
	 * 日志
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(StringUtil.class);

	/**
	 * 从路径中获取文件名
	 * 
	 * @param ftpPath
	 * @return filename (without path)
	 */
	public static String getFilename(String ftpPath) {
		return FilenameUtils.getName(ftpPath);
	}

	/**
	 * 转换采集路径，将“%%”占位符转换为实际值。
	 * 
	 * @param raw
	 *            原始路径。
	 * @param date
	 *            时间点。
	 * @return 转换后的值。
	 */
	public static String convertCollectPath(String raw, Date date) {
		if (raw == null || date == null)
			return raw;
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		String s = raw;
		s = s.replaceAll("%%Y", String.format("%04d", cal.get(Calendar.YEAR)));
		s = s.replaceAll("%%y", String.format("%04d", cal.get(Calendar.YEAR)));
		s = s.replaceAll("%%M", String.format("%02d", cal.get(Calendar.MONTH) + 1));
		s = s.replaceAll("%%D", String.format("%02d", cal.get(Calendar.DAY_OF_MONTH)));
		s = s.replaceAll("%%d", String.format("%02d", cal.get(Calendar.DAY_OF_MONTH)));
		s = s.replaceAll("%%H", String.format("%02d", cal.get(Calendar.HOUR_OF_DAY)));
		s = s.replaceAll("%%h", String.format("%02d", cal.get(Calendar.HOUR_OF_DAY)));
		s = s.replaceAll("%%m", String.format("%02d", cal.get(Calendar.MINUTE)));
		s = s.replaceAll("%%S", String.format("%02d", cal.get(Calendar.SECOND)));
		s = s.replaceAll("%%s", String.format("%02d", cal.get(Calendar.SECOND)));
		String em = new SimpleDateFormat("MMM", Locale.ENGLISH).format(date);
		s = s.replaceAll("%%EM", em);
		return s;
	}

	public static String getPattern(String target, String regex) {
		try {
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(target);
			while (matcher.find()) {
				// 只提取第一次满足匹配的字符串
				return matcher.group();
			}
		} catch (Throwable e) {
			// 没找到匹配的字符串
		}
		return null;
	}

	public static Date parseZteFileDate(String filename) {
		try {
			String name = FilenameUtils.getBaseName(filename);
			String str = getPattern(name, "\\d{4}[-]\\d{2}[-]\\d{2}[_]\\d{2}[-]\\d{2}");
			Date date = new SimpleDateFormat("yyyy-MM-dd_HH-mm").parse(str);
			return date;
		} catch (Exception e) {
			return new Date(0);
		}
	}

	public static Date parseZteCdrFileDate(String filename) {
		try {
			String name = FilenameUtils.getBaseName(filename);
			String str = getPattern(name, "\\d{14}");
			Date date = new SimpleDateFormat("yyyyMMddHHmmSSS").parse(str);
			return date;
		} catch (Exception e) {
			return new Date(0);
		}
	}

	public static Date parseZteMRFileDate(String filename) {
		try {
			String name = FilenameUtils.getBaseName(filename);
			String str = getPattern(name, "\\d{12}");
			Date date = new SimpleDateFormat("yyyyMMddHHmm").parse(str);
			return date;
		} catch (Exception e) {
			return new Date(0);
		}
	}

	public static Date parseZte3GMRFileDate(String filename) {
		try {
			String name = FilenameUtils.getBaseName(filename);
			String str = getPattern(name, "\\d{4}[-]\\d{2}[-]\\d{2}[-]\\d{2}[-]\\d{2}[-]\\d{2}");
			Date date = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").parse(str);
			return date;
		} catch (Exception e) {
			return new Date(0);
		}
	}

	public static Date parseCvicMRFileDate(String filename) {
		try {
			String name = FilenameUtils.getBaseName(filename);
			String str = getPattern(name, "\\d{8}[_]\\d{4}");
			Date date = new SimpleDateFormat("yyyyMMdd_HHmm").parse(str);
			return date;
		} catch (Exception e) {
			return new Date(0);
		}
	}

	public static Date parseAluFileDate(String filename) {
		try {
			String name = FilenameUtils.getBaseName(filename);
			String str = getPattern(name, "\\d{8}");
			Date date = new SimpleDateFormat("yyMMddHH").parse(str);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			calendar.add(Calendar.HOUR_OF_DAY, -1);
			return calendar.getTime();
		} catch (Exception e) {
			return new Date(0);
		}
	}

	public static Date parseAluDoFileDate(String filename) {
		try {
			String name = FilenameUtils.getBaseName(filename);
			String str = getPattern(name, "\\d{10}");
			if (str == null)
				str = getPattern(name, "\\d{8}") + "00";
			Date date = new SimpleDateFormat("yyMMddHHmm").parse(str);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			calendar.add(Calendar.HOUR_OF_DAY, -1);
			return calendar.getTime();
		} catch (Exception e) {
			return new Date(0);
		}
	}

	public static Date parseAluMRFileDate(String filename) {
		try {
			String name = FilenameUtils.getBaseName(filename);
			String str = getPattern(name, "\\d{8}[.]\\d{4}");
			Date date = new SimpleDateFormat("yyyyMMdd.HHmm").parse(str);
			return date;
		} catch (Exception e) {
			return new Date(0);
		}
	}

	public static Date parseHwFileDate(String filename) {
		try {
			String name = FilenameUtils.getBaseName(filename);
			String str = getPattern(name, "\\d{12}");
			Date date = new SimpleDateFormat("yyyyMMddHHmm").parse(str);
			return date;
		} catch (Exception e) {
			return new Date(0);
		}
	}

	public static Date parseEricssonFileDate(String filename) {
		try {
			String name = FilenameUtils.getBaseName(filename);
			String str = getPattern(name, "\\d{8}[.]\\d{4}");
			Date date = new SimpleDateFormat("yyyyMMdd.HHmm").parse(str);
			return date;
		} catch (Exception e) {
			return new Date(0);
		}
	}

	public static Date parseNokiaFileDate(String filename) {
		try {
			String name = FilenameUtils.getBaseName(filename);
			String str = getPattern(name, "\\d{4}[_]\\d{2}[_]\\d{2}[_]\\d{2}[_]\\d{2}");
			Date date = new SimpleDateFormat("yyyy_MM_dd_HH_mm").parse(str);
			return date;
		} catch (Exception e) {
			return new Date(0);
		}
	}

	/*
	 * 解析诺西 WCAS文件 文件样式：2014_01_02_09_44.str.gz
	 */
	public static Date parseNokiaCdtFileDate(String filename) {
		try {
			String name = FilenameUtils.getBaseName(filename);
			String str = getPattern(name, "\\d{4}[_]\\d{2}[_]\\d{2}[_]\\d{2}[_]\\d{2}[.]");
			Date date = new SimpleDateFormat("yyyy_MM_dd_HH_mm").parse(str);
			return date;
		} catch (Exception e) {
			return new Date(0);
		}
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
		if (isEmpty(string))
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

	/**
	 * 字符串格式数字小数点精度处理
	 * 
	 * @param num
	 * @param radixNum
	 * @return 处理精度后的数字字符串
	 */
	public static String formatNumberRadix(double num, int radixNum) {
		String str = String.valueOf(num);
		if (radixNum < 0)
			return str;
		int radixIndex = str.indexOf('.');
		if (radixIndex < 0)
			return str;
		if (radixNum == 0)
			return str.substring(0, radixIndex);
		int endIndex = radixIndex + radixNum + 1;
		if (endIndex > str.length())
			endIndex = str.length();
		str = str.substring(0, endIndex);
		return str;
	}

	/**
	 * 判断字符串是否为空
	 * 
	 * @param string
	 * @return boolean 如果字符串为Null、""或者空白字符串 返回true.否则返回false
	 */
	public static boolean isEmpty(String string) {
		return string == null || string.trim().length() == 0;
	}

	public static boolean isNotEmpty(String string) {
		return string != null && string.trim().length() > 0;
	}

	public static String nvl(String str, String replace) {
		return isEmpty(str) ? replace : str;
	}

	/**
	 * 判断字符串是否是数字
	 * 
	 * @param str
	 * @return true or false
	 */
	public static boolean isNum(String str) {
		return str.matches("^[-+]?(([0-9]+)([.]([0-9]+))?|([.]([0-9]+))?)$");
	}

	/**
	 * 编码一条FTP路径
	 * 
	 * @param ftpPath
	 *            FTP路径
	 * @return 编码后的路径
	 */
	public static String encodeFTPPath(String ftpPath, String encode) {
		try {
			String str = StringUtil.isNotEmpty(encode) ? new String(ftpPath.getBytes(encode), "iso_8859_1") : ftpPath;
			// String path = new String(ftpPath.getBytes(),encode);
			// String str = StringUtil.isNotEmpty(encode) ? new String(path.getBytes(encode), "iso_8859_1") : new String(ftpPath.getBytes(),
			// "iso_8859_1");
			return str;
		} catch (UnsupportedEncodingException e) {
			LOGGER.error("设置的编码不正确:" + encode, e);
		}
		return ftpPath;
	}

	/**
	 * @param str
	 * @param strCharset
	 * @return
	 */
	public static String decodeToLocal(String str, String strCharset) {
		try {
			return new String(str.getBytes(strCharset));
		} catch (UnsupportedEncodingException e) {
			LOGGER.error("设置的编码不正确:" + strCharset, e);
			return null;
		}
	}

	/**
	 * 解码一条FTP路径
	 * 
	 * @param ftpPath
	 *            FTP路径
	 * @return 解码后的路径
	 */
	public static String decodeFTPPath(String ftpPath) {
		try {
			String str = StringUtil.isNotEmpty(ftpPath) ? new String(ftpPath.getBytes("iso_8859_1"), "utf-8") : ftpPath;
			return str;
		} catch (UnsupportedEncodingException e) {
			return ftpPath;
		}
	}

	/**
	 * 解码一条FTP路径
	 * 
	 * @param ftpPath
	 *            FTP路径
	 * @param charset
	 *            FTP服务端编码集，若设置为null或""默认为JVM本地默认编码
	 * @return 解码后的路径
	 * @author Niow 2014-6-11
	 */
	public static String decodeFTPPath(String ftpPath, String charset) {
		try {
			String str = ftpPath;
			if (isNotEmpty(charset)) {
				str = StringUtil.isNotEmpty(ftpPath) ? new String(ftpPath.getBytes("ISO_8859_1"), charset) : ftpPath;
			} else {
				str = StringUtil.isNotEmpty(ftpPath) ? new String(ftpPath.getBytes("ISO_8859_1")) : ftpPath;
			}

			return str;
		} catch (UnsupportedEncodingException e) {
			return ftpPath;
		}
	}

	/**
	 * 字符串截取方法<br>
	 * 从原始string中截图begin和end之间的部分
	 * 
	 * @param string
	 * @param begin
	 * @param end
	 * @return begin和end之间字符串
	 */
	public static String substring(String string, String begin, String end) {
		if (isEmpty(string))
			return "";
		int beginIndex = string.indexOf(begin);
		if (beginIndex == -1)
			return "";
		int endIndex = string.indexOf(end, beginIndex + begin.length());
		if (endIndex == -1)
			return string.substring(beginIndex + begin.length());
		return string.substring(beginIndex + begin.length(), endIndex);
	}

	/**
	 * 字符串截取方法<br>
	 * 带有偏移量<br>
	 * 从原始string中截图begin和end之间的部分
	 * 
	 * @param string
	 * @param begin
	 * @param offset
	 *            偏移量，从截取开始处为begin末尾+offset位
	 * @param end
	 * @return begin和end之间字符串
	 */
	public static String substring(String string, String begin, int offset, String end) {
		if (isEmpty(string))
			return "";
		int beginIndex = string.indexOf(begin);
		if (beginIndex == -1)
			return "";
		int endIndex = string.indexOf(end, beginIndex + begin.length() + offset);
		if (endIndex == -1)
			return string.substring(beginIndex + begin.length());
		return string.substring(beginIndex + begin.length() + offset, endIndex);
	}

	/**
	 * 字符串截取方法<br>
	 * 从原始string中截取begin到字符串末尾
	 * 
	 * @param string
	 * @param begin
	 * @return
	 */
	public static String substring(String string, String begin) {
		if (isEmpty(string))
			return "";
		int beginIndex = string.indexOf(begin);
		if (beginIndex == -1)
			return "";
		return string.substring(beginIndex + begin.length());
	}

	/**
	 * 通配符匹配
	 * 
	 * @param src
	 *            带通配符的字符串
	 * @param dest
	 *            不带通配符的字符串
	 * @param wildCard
	 *            通配符
	 * @return
	 */
	public static boolean wildCardMatch(String src, String dest, String wildCard) {
		String[] fieldName = StringUtil.split(src, wildCard);
		int start = -1;
		boolean flag = true;
		for (int n = 0; n < fieldName.length; n++) {
			if ("".equals(fieldName[n]))
				continue;
			int index = dest.indexOf(fieldName[n]);
			if (index > start) {
				start = index;
				continue;
			} else {
				flag = false;
				break;
			}
		}
		return flag;
	}

	public static void main(String[] arg1) {
		String str = "?/小区:eNodeB名称=?, 本地小区标识=?, 小区名称=?, eNodeB标识=?, 小区双工模式=?";
		String[] valList = StringUtil.split(str, "?");
		int n = 0;
		for (String val : valList) {
			System.out.println(++n + ":" + val);
		}
	}

}

package parser.hw.dt;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

/**
 * 路测CDMA解析工具类 CdmaUtil<br>
 * 因解析类太过庞大，抽出固定业务功能代码
 * 
 * @author lijiayu
 * @date 2014年8月12日
 */
public class CdmaUtil {

	/**
	 * 在做固定字段填充时，用于过滤只有三个固定字段的表，一般是5个固定字段
	 */
	public static String[] fixThreeFieldTables = {"CLT_DT_TP_MOC", "CLT_DT_TP_MTC", "CLT_DT_TP_FTPDL", "CLT_DT_TP_FTPUL", "CLT_DT_TP_PING",
			"CLT_DT_TP_HTTPP", "CLT_DT_TP_HTTPDL", "CLT_DT_TP_POP3", "CLT_DT_TP_SMTP", "CLT_DT_TP_SMS", "CLT_DT_TP_MMSS", "CLT_DT_TP_MMSR",
			"CLT_DT_TP_VS", "CLT_DT_TP_TR", "CLT_DT_TP_GPSO", "CLT_DT_TP_QC", "CLT_DT_TP_WAPP", "CLT_DT_TP_WAPL", "CLT_DT_TP_WAPDL"};

	/** 需要回填经纬度的表 */
	public static Set<String> LONGITUDE_LATITUDE_TABLES = new HashSet<String>();
	static {
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_EVT");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CSC");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CPM");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CDM");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CAP");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CHP");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CPCP");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CASF");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CQOS");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_C1XFCH");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_C1XSCH");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CEVS");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_DORLC");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CL3");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CEVQC");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CEVST");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CEVAC");
		LONGITUDE_LATITUDE_TABLES.add("CLT_DT_CEVTCA");
	}

	/**
	 * 校验是否为只三个固定字段的表
	 * 
	 * @param tableName
	 * @return boolean
	 */
	public static boolean isThreeFieldTables(String tableName) {
		if (tableName == null)
			return false;
		for (int i = 0; i < fixThreeFieldTables.length; i++)
			if (tableName.equals(fixThreeFieldTables[i]))
				return true;
		return false;
	}

	/**
	 * 用于预留空间的空格，直接用字符串速度会比较快
	 * 
	 * @param num
	 *            空格的数量
	 * @return 一长串空格
	 */
	public static String getSpaceStr(int num) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < num; i++)
			sb.append(" ");
		return sb.toString();
	}

	/**
	 * 对固定字段做预留空间处理，数据没达到最大值的，补空格
	 * 
	 * @param str
	 * @param length
	 * @return
	 */
	public static String supplementLength(String str, int length) {

		int chineseAmount = getChineseAmount(str);
		int totalAmount = str.length() + chineseAmount;
		if (totalAmount > length) {
			str = str.substring(0, str.length() - chineseAmount);
		}
		// 如果有中文 填充的数据要减 chineseAmount * 2
		// 这里有疑点，原理上不用*2，但是出现中文，会把后面的文字向前移动，所有要*2，避免覆盖
		if (str.length() < length)
			str += getSpaceStr(length - str.length() - (chineseAmount * 2));
		return str;
	}

	/**
	 * 获取中文个数
	 */
	public static int getChineseAmount(String str) {
		char[] chars = str.toCharArray();
		int chineseAmount = 0;
		for (int i = 0, len = chars.length; i < len; i++) {
			// >='\u4E00' && <='\u9FFF' unicode汉字范围
			if (chars[i] >= '\u4E00' && chars[i] <= '\u9FFF') {
				chineseAmount++;
			}
		}
		return chineseAmount;
	}

	/**
	 * 获取全局的日期 拼接上时分秒
	 * 
	 * @param time
	 * @return YYYY-MM-DD HH24:MI:SS
	 */
	public static String getFixDateTime(String time, String dateStr) {
		StringBuffer bf = new StringBuffer();
		bf.append(dateStr);
		if (time.length() >= 6)
			bf.append(" ").append(time.substring(0, 2)).append(":").append(time.substring(2, 4)).append(":").append(time.substring(4, 6));
		return bf.toString();
	}

	/**
	 * COVER_TYPE（覆盖类型） 解析文件名中第四个"_"与第五个"_"之间的数据
	 * 
	 * @param str
	 * @return
	 */
	public static String getCoverType(String fileName) {
		for (int i = 0; i < 4; i++) {
			fileName = fileName.substring(fileName.indexOf("_") + 1);
		}
		fileName = fileName.substring(0, fileName.indexOf("_"));
		return fileName;
	}

	/**
	 * SERVICETYPE_SV（业务类型） 解析文件名中第五个"_"后的数据
	 * 
	 * @param str
	 * @return
	 */
	public static String getServiceTypeSV(String fileName) {
		return fileName.substring(fileName.lastIndexOf("_") + 1);
	}

	/**
	 * FILE_VENDOR（采集仪表厂家标识） 解析文件名中最开始俩个字符
	 * 
	 * @param str
	 * @return
	 */
	public static String getFileVendor(String fileName) {
		return fileName.substring(0, 2);
	}

	/**
	 * FILE_DEVICETYPE（采集仪表类型） 解析文件名中第三个字符的数据
	 * 
	 * @param str
	 * @return
	 */
	public static String getFileDeviceType(String fileName) {
		return fileName.substring(2, 3);
	}

	/**
	 * FILE_NETWORKTYPE（网络类型）解析文件名中第三个"_"与第四个"_"之间的数据
	 * 
	 * @param str
	 * @return
	 */
	public static String getFileNetWorkType(String fileName) {
		for (int i = 0; i < 3; i++) {
			fileName = fileName.substring(fileName.indexOf("_") + 1);
		}
		fileName = fileName.substring(0, fileName.indexOf("_"));
		return fileName;
	}

	/**
	 * 为字符串数子补0
	 */
	public static String supplyZero(String strNum, int count) {
		String str = strNum.trim();
		int len = str.length();
		if (strNum.trim().length() >= count) {
			return str;
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < (count - len); i++) {
			sb.append("0");
		}
		return sb.append(str).toString();
	}

	/**
	 * 用于gpsLinkSeq List的排序
	 * 
	 * @author lijiayu @ 2013年9月11日
	 */
	public static Comparator<Integer> getComparatorGpsLinkSeq() {
		return new Comparator<Integer>() {

			@Override
			public int compare(Integer o1, Integer o2) {
				return o1.compareTo(o2);
			}
		};
	}

	public static Comparator<String> getComparatorStrTime() {
		return new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return Integer.valueOf(o1).compareTo(Integer.valueOf(o2));
			}
		};
	}

	/**
	 * 拆分字符串
	 * 
	 * @param srcStr
	 *            原字符串
	 * @param split
	 *            拆分字符
	 * @return String[] 拆分后的字符串数组
	 * @Date 2013-9-16
	 * @author lijiayu
	 */
	public final static String[] split(String str, String symbol) {
		class InnerSplit {

			int size;

			// 要返回的字符串数组
			String[] elements = new String[10];

			public String[] split(String str, String symbol) {
				int symbolLength = symbol.length();
				int index = str.indexOf(symbol);
				int beginIndex = 0;
				int i = 0;
				while (index != -1) {
					ensureCapacity(i++, str.substring(beginIndex, index));
					beginIndex = index + symbolLength;
					// 如果是连续的分割符，这里设置为 ""
					while ((index = str.indexOf(symbol, beginIndex)) == beginIndex) {
						ensureCapacity(i++, "");
						beginIndex = index + symbolLength;
					}
				}
				// 最后一个字符串
				if (beginIndex <= str.length()) {
					ensureCapacity(i++, str.substring(beginIndex));
				}
				// 返回之前去掉为空的
				return Arrays.copyOf(elements, size);
			}

			/**
			 * 自行拓展elements容量，与添加一个字符串元素
			 * 
			 * @param i
			 *            目前的字符串元素的人数
			 * @param str
			 *            要添加的字符串元素
			 */
			public void ensureCapacity(int i, String str) {
				int minCapacity = i + 1;
				size++;
				int oldCapacity = elements.length;
				if (oldCapacity < minCapacity) {
					int newCapacity = (oldCapacity * 3) / 2 + 1;
					if (newCapacity < minCapacity)
						newCapacity = minCapacity;
					// newCapacity通常是最接近minCapacity的
					elements = Arrays.copyOf(elements, newCapacity);
				}
				elements[i] = str;
			}
		}
		return new InnerSplit().split(str, symbol);
	}
}

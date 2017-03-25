package util;

import java.io.IOException;
import java.util.Vector;

import util.opencsv.CSVParser;

/**
 * 解析标准csv文件中的一行
 * 
 * @author ChenSijiang 20100719
 * @since 1.0
 */
public class CSVLineParser {

	/**
	 * 解析csv文件中的一行，以逗号作为分隔符
	 * 
	 * @param src
	 *            csv文件中的一行
	 * @return 分隔后的结果
	 * @throws NullPointerException
	 *             传入的数据为null时
	 */
	public static String[] splitCSV(String src, boolean blindfault) {
		return splitCSV(src, ',', blindfault);
	}

	/**
	 * 解析csv文件中的一行
	 * 
	 * @param src
	 *            csv文件中的一行
	 * @param splitChar
	 *            分隔符
	 * @return 分隔后的结果
	 * @throws NullPointerException
	 *             传入的数据为null时
	 */
	public static String[] splitCSV(String src, char splitChar, boolean blindfault) {
		if (blindfault) {
			try {
				CSVParser p = new CSVParser(splitChar, '\0');
				String[] result = p.parseLine(src);
				p = null;
				return result;
			} catch (Exception e) {
				LogMgr.getInstance().getSystemLogger().error("解析单行数据出错，src=" + src + "，split=" + splitChar, e);
				return new String[0];
			}
		} else {
			if (src == null)
				throw new NullPointerException();
			src = src.trim();
			StringBuffer st = new StringBuffer();
			Vector<String> result = new Vector<String>();
			boolean beginWithQuote = false;
			for (int i = 0; i < src.length(); i++) {
				char ch = src.charAt(i);
				if (ch == '\"') {
					if (beginWithQuote) {
						i++;
						if (i >= src.length()) {
							result.addElement(st.toString());
							st = new StringBuffer();
							beginWithQuote = false;
						} else {
							ch = src.charAt(i);
							if (ch == '\"') {
								st.append(ch);
							} else if (ch == splitChar) {
								result.addElement(st.toString());
								st = new StringBuffer();
								beginWithQuote = false;
							} else {
								/*
								 * throw new RuntimeException( "一个字段中，不应该出现单个双引号，只能是两个双引号的转义形式。" + (result.size() + 1) + "at:" + i + "\n source:" +
								 * src);
								 */
							}
						}
					} else if (st.length() == 0) {
						beginWithQuote = true;
					} else {
						// throw new
						// RuntimeException("双引号不能出现在没有被双引号包裹的字段中。 at:"
						// + (result.size() + 1) + "\n source:" + src);
					}
				} else if (ch == splitChar) {
					if (beginWithQuote) {
						st.append(ch);
					} else {
						result.addElement(st.toString());
						st = new StringBuffer();
						beginWithQuote = false;
					}
				} else {
					st.append(ch);
				}
			}
			if (st.length() != 0) {
				if (beginWithQuote) {
					try {
						String[] values = new CSVParser().parseLine(src);
						for (int j = 0; j < values.length; j++) {
							if (values[j] != null) {
								values[j] = values[j].trim();
								if (values[j].startsWith("\"")) {
									values[j] = values[j].substring(1);
								}
								if (values[j].endsWith("\"")) {
									values[j] = values[j].substring(0, values[j].length() - 1);
								}
							}
						}
						return values;
					} catch (IOException e) {
						// throw new RuntimeException(e);
						LogMgr.getInstance().getSystemLogger().error("解析单行数据出错，src=" + src + "，split=" + splitChar, e);
						return new String[0];
					}
				} else {
					result.addElement(st.toString());
				}
			}
			String rs[] = new String[result.size()];
			for (int i = 0; i < rs.length; i++) {
				rs[i] = (String) result.elementAt(i);
			}
			return rs;
		}
	}

	public static void main(String[] args) throws Exception {

		String s = "abc,\"dd,e\",\"d,l\",ld";
		String[] sp = new CSVParser(',').parseLine(s);
		for (String x : sp)
			System.out.println(x);
	}
}

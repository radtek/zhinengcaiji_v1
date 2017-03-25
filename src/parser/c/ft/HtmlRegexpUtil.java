package parser.c.ft;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import util.Util;

final class HtmlRegexpUtil {

	private final static String regxpForHtml = "<([^>]*)>"; // 过滤所有以<开头以>结尾的标签

	private final static String regxpForImgTag = "<\\s*img\\s+([^>]*)\\s*>"; // 找出IMG标签

	private final static String regxpForImaTagSrcAttrib = "src=\"([^\"]+)\""; // 找出IMG标签的SRC属性

	/**
	 *
	 */
	public HtmlRegexpUtil() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * 基本功能：替换标记以正常显示
	 * <p>
	 * 
	 * @param input
	 * @return String
	 */
	public String replaceTag(String input) {
		if (!hasSpecialChars(input)) {
			return input;
		}
		StringBuffer filtered = new StringBuffer(input.length());
		char c;
		for (int i = 0; i <= input.length() - 1; i++) {
			c = input.charAt(i);
			switch (c) {
				case '<' :
					filtered.append("&lt;");
					break;
				case '>' :
					filtered.append("&gt;");
					break;
				case '"' :
					filtered.append("&quot;");
					break;
				case '&' :
					filtered.append("&amp;");
					break;
				default :
					filtered.append(c);
			}
		}
		return (filtered.toString());
	}

	/**
	 * 基本功能：判断标记是否存在
	 * <p>
	 * 
	 * @param input
	 * @return boolean
	 */
	public boolean hasSpecialChars(String input) {
		boolean flag = false;
		if ((input != null) && (input.length() > 0)) {
			char c;
			for (int i = 0; i <= input.length() - 1; i++) {
				c = input.charAt(i);
				switch (c) {
					case '>' :
						flag = true;
						break;
					case '<' :
						flag = true;
						break;
					case '"' :
						flag = true;
						break;
					case '&' :
						flag = true;
						break;
				}
			}
		}
		return flag;
	}

	/**
	 * 基本功能：过滤所有以"<"开头以">"结尾的标签
	 * <p>
	 * 
	 * @param str
	 * @return String
	 */
	public static String filterHtml(String str) {
		if (Util.isNull(str))
			return "";
		Pattern pattern = Pattern.compile(regxpForHtml);
		Matcher matcher = pattern.matcher(str);
		StringBuffer sb = new StringBuffer();
		boolean result1 = matcher.find();
		while (result1) {
			matcher.appendReplacement(sb, " ");
			result1 = matcher.find();
		}
		matcher.appendTail(sb);
		return sb.toString().replace("&nbsp;", "").replace(";", "");
	}

	/**
	 * 基本功能：过滤指定标签
	 * <p>
	 * 
	 * @param str
	 * @param tag
	 *            指定标签
	 * @return String
	 */
	public static String fiterHtmlTag(String str, String tag) {
		String regxp = "<\\s*" + tag + "\\s+([^>]*)\\s*>";
		Pattern pattern = Pattern.compile(regxp);
		Matcher matcher = pattern.matcher(str);
		StringBuffer sb = new StringBuffer();
		boolean result1 = matcher.find();
		while (result1) {
			matcher.appendReplacement(sb, "");
			result1 = matcher.find();
		}
		matcher.appendTail(sb);
		return sb.toString();
	}

	/**
	 * 基本功能：替换指定的标签
	 * <p>
	 * 
	 * @param str
	 * @param beforeTag
	 *            要替换的标签
	 * @param tagAttrib
	 *            要替换的标签属性值
	 * @param startTag
	 *            新标签开始标记
	 * @param endTag
	 *            新标签结束标记
	 * @return String
	 * @如：替换img标签的src属性值为[img]属性值[/img]
	 */
	public static String replaceHtmlTag(String str, String beforeTag, String tagAttrib, String startTag, String endTag) {
		String regxpForTag = "<\\s*" + beforeTag + "\\s+([^>]*)\\s*>";
		String regxpForTagAttrib = tagAttrib + "=\"([^\"]+)\"";
		Pattern patternForTag = Pattern.compile(regxpForTag);
		Pattern patternForAttrib = Pattern.compile(regxpForTagAttrib);
		Matcher matcherForTag = patternForTag.matcher(str);
		StringBuffer sb = new StringBuffer();
		boolean result = matcherForTag.find();
		while (result) {
			StringBuffer sbreplace = new StringBuffer();
			Matcher matcherForAttrib = patternForAttrib.matcher(matcherForTag.group(1));
			if (matcherForAttrib.find()) {
				matcherForAttrib.appendReplacement(sbreplace, startTag + matcherForAttrib.group(1) + endTag);
			}
			matcherForTag.appendReplacement(sb, sbreplace.toString());
			result = matcherForTag.find();
		}
		matcherForTag.appendTail(sb);
		return sb.toString();
	}

	public static void main(String[] args) {
		System.out
				.println(filterHtml("<font size=\"2\" face=\"Arial\">SZ-LG-PHSZ-0003&nbsp;</font></td><td style=\"background-color:transparent;\"><font size=\"2\" face=\"Arial\">121.15.185.176</font></td><td style=\"background-color:transparent;\"><font size=\"2\" face=\"Arial\">鹏海山庄</font></td><td style=\"background-color:transparent;\"><font size=\"2\" face=\"Arial\">ADSLS3498162</font></td></tr><td style=\"background-color:transparent;\"><font size=\"2\" face=\"Arial\">AP无法连通告警</font></td></tbody>~!~~!~ ~!~1~!~ ~!~2~!~2~!~1~!~Jun 13 2009 10:11:21:186PM~!~Aug 13 2009 10:23:34:726AM~!~1500033764~!~张行军~!~ ~!~Jun 13 2009 10:11:21:186PM~!~0~!~0~!~0~!~0~!~0~!~1~!~200~!~网络侧故障~!~20001~!~网络侧设备~!~~!~2~!~2~!~2~!~2~!~2~!~300~!~~!~~!~~!~~!~~!~2~!~~!~755000440~!~整个热点离线/~!~~!~0~!~-3552~!~3852~!~~!~-3492~!~~!~~!~~!~~!~~!~~!~~!~~!~1500033764~!~张行军~!~~!~2~!~~!~~!~~!~~!~~!~~!~~!~~!~2~!~~!~~!~~!~~!~~!~~!~~!~~$~4379646~!~1~!~7~!~Jun 20 2010  4:02:48:356AM~!~1~!~650369~!~/网络侧故障(跨网)/CDMA/传输---光缆故障/~!~0~!~3870881~!~本地N级光缆~!~23~!~1010002~!~省无线~!~省无线~!~Jun 15 2009  2:26:00:000PM~!~Jun 15 2009  2:26:00:000PM~!~1200000945~!~清远无线网络运营中心~!~0~!~清远无线分中心~!~3331281~!~~!~~!~~!~~!~~!~~!~1~!~~!~480~!~0~!~ ~!~0~!~ ~!~0~!~ ~!~WX-20090615-0018~!~测试无线中心是否可以派单到清远监控中心~!~测试无线中心是否可以派单到清远监控中心~!~~!~ ~!~2~!~测试无线中心是否可以派单到清远监控中心~!~1~!~2~!~1~!~Jun 15 2009  2:31:01:956PM~!~Jun 20 2010  4:02:48:356AM~!~796826~!~清远无线分中心~!~3331281~!~Jun 15 2009  2:31:01:956PM~!~1~!~1~!~1~!~0~!~0~!~1~!~100~!~ ~!~10006~!~接入型业务~!~~!~2~!~2~!~2~!~2~!~2~!~450~!~~!~~!~~!~~!~~!~2~!~~!~99970000~!~其他/~!~~!~0~!~-6418~!~6868~!~~!~-6388~!~~!~~!~~!~~!~~!~~!~~!~~!~796826~!~清远无线分中心~!~~!~2~!~~!~~!~~!~~!~~!~~!~~!~~!~2~!~~!~~!~~!~~!~~!~~!~~!~~$~4421268~!~1~!~7~!~Jun 16 2009  9:48:26:433AM~!~1~!~755000007~!~/网络侧/网络故障/传输/~!~0~!~755000003~!~C2级~!~6~!~1010001~!~深圳~!~深圳~!~Jun 16 2009  9:06:00:000AM~!~Jun 16 2009  9:06:00:000AM~!~1500031679~!~无线网络维护室~!~0~!~刘波~!~ 28812050~!~~!~~!~~!~~!~~!~~!~1~!~~!~360~!~0~!~ ~!~0~!~ ~!~0~!~ ~!~SZ-20090616-0027~!~BSC6-67 麒麟工业区~!~BSC6-67 麒麟工业区，断站，调度单号：2003维356~!~~!~ ~!~1~!~ ~!~2~!~2~!~1~!~Jun 16 2009  9:08:50:713AM~!~Aug 13 2009 10:23:34:726AM~!~1500033759~!~刘波~!~ ~!~Jun 16 2009  9:08:50:713AM~!~0~!~0~!~0~!~0~!~0~!~1~!~200~!~网络侧故障~!~20001~!~网络侧设备~!~~!~2~!~2~!~2~!~2~!~2~!~240~!~~!~~!~~!~~!~~!~2~!~~!~755000029~!~其他/~!~~!~0~!~201~!~39~!~~!~321~!~~!~~!~~!~~!~~!~~!~~!~~!~1500033759~!~刘波~!~~!~2~!~~!~~!~~!~~!~~!~~!~~!~~!~2~!~~!~~!~~!~~!~~!~~!~~!~~$~4429347~!~1~!~3~!~Jun 16 2009 12:13:45:423PM~!~1~!~763000011~!~/网络侧/网络故障/动力/~!~0~!~763000004~!~C~!~7~!~1020003~!~清远~!~清远~!~Jun 16 2009 10:20:00:000AM~!~Jun 16 2009 10:20:00:000AM~!~1200000750~!~网络监控中心~!~0~!~吴战平~!~0763-3369982~!~~!~~!~~!~~!~~!~~!~1~!~~!~1440~!~0~!~~!~0~!~ ~!~0~!~ ~!~QY-20090616-0067~!~  第1路交流低电压告警状态低于下限~!~【告警信息】: 2 Mbit-s Loss of input signal<br />"));
	}
}

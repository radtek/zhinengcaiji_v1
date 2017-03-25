package parser.eric.pm.parser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import util.Util;

/**
 * XmlAsTxtParser
 * 
 * @author YangJian
 * @since 1.0
 */
public class XmlAsTxtParser {

	public void parse(String fileName, XmlAsTextHandler handler) {
		if (handler == null || Util.isNull(fileName))
			return;

		XmlAsTextHandler xmlHandler = handler;

		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			xmlHandler.startDocument();
			String strLine = null;
			while ((strLine = br.readLine()) != null) {
				strLine = strLine.trim();
				// 如果首字母不等于'<'就跳过，非法的标签
				if (strLine.length() <= 2 || strLine.charAt(0) != '<')
					continue;

				if (strLine.charAt(1) == '?' || strLine.charAt(1) == '!')
					continue;

				String tagName = null;
				char secondChar = strLine.charAt(1);
				int ePos = strLine.indexOf(">");
				if (ePos <= 0)
					continue;
				// 表示结束符
				if (secondChar == '/') {
					// 为关闭标签
					tagName = strLine.substring(strLine.indexOf("/") + 1, ePos);
					xmlHandler.endElement(tagName);
				} else {
					// 为开始标签
					tagName = strLine.substring(1, ePos);
					xmlHandler.startElement(tagName);

					// 表示一行只有一个开始标签或者关闭标签
					if (ePos == (strLine.length() - 1))
						continue;

					// 获取标签的内容部分
					String tmpStrLine = strLine.substring(ePos + 1);
					int tPos = tmpStrLine.indexOf("</");
					String tmpContent = tmpStrLine.substring(0, tPos);
					xmlHandler.content(tmpContent);

					// 处理结束标签-- 开始标签和结束标签在一行的情况
					tagName = tmpStrLine.substring(tPos + 2, tmpStrLine.length() - 1);
					xmlHandler.endElement(tagName);
				}
			}
			xmlHandler.endDocument();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}
		}
	}

}

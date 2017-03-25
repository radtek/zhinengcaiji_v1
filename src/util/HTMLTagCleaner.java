package util;

import java.io.UnsupportedEncodingException;

import org.htmlparser.Parser;
import org.htmlparser.util.ParserException;
import org.htmlparser.util.Translate;
import org.htmlparser.visitors.TextExtractingVisitor;

/**
 * HTML标签和预置字符清除器
 * 
 * @author niow
 * @date 2014-7-1
 * 
 */
public class HTMLTagCleaner {

	private Parser parser;

	private TextExtractingVisitor visitor;

	public HTMLTagCleaner() {
		parser = new Parser();
	}

	/**
	 * 清除所有HTML标签和预置字符，只保留文本内容<br>
	 * <p>
	 * 例如：
	 * </p>
	 * {@code <head></head><body>
	 * 
	 * <PRE>大声的发生的法士:大夫<br/>&nbsp; &nbsp &nbsp &nbsp 的发的发:舍得发<br/></PRE>
	 * 
	 * </body></html>}
	 * <p>
	 * 清除结果为：<br>
	 * {@code 大声的发生的法士:大夫        的发的发:舍得发}
	 * </p>
	 * 
	 * @param inputHTML
	 *            HTML输入内容
	 * @return html的文本内容
	 * @throws ParserException
	 */
	public String cleanTag(String inputHTML) throws ParserException {
		if (inputHTML == null)
			return "";
		visitor = new TextExtractingVisitor();
		parser.setInputHTML(inputHTML);
		parser.visitAllNodesWith(visitor);
		return Translate.decode(visitor.getExtractedText());
	}

	public static void main(String[] args) throws ParserException,
			UnsupportedEncodingException {
		String szContent = "<head></head><body><PRE>大声的发生的法士:大夫<br/>&nbsp; &nbsp &nbsp &nbsp 的发的发:舍得发<br/></PRE></body></html>";
		HTMLTagCleaner cl = new HTMLTagCleaner();
		System.out.println(cl.cleanTag(szContent));
		System.out.println(cl.cleanTag(szContent));
		System.out.println(cl.cleanTag(szContent));
	}

}

package parser.eric.cm;

import java.io.File;
import java.sql.Timestamp;
import java.util.Date;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import util.LogMgr;

/**
 * <p>
 * 爱立信参数解析实现，SAX解析方式。
 * </p>
 * <p>
 * 解析思路：每个vsDataType标签内容，都是一种对象类型，以此类型区分表。每种类型的VsDataContainer标签下的属性不同，也就相当于不同的列 。
 * </p>
 * 
 * @author chensj 2010-3-7
 */
public class EricssonCmParserSaxImp implements EricssonCmParser {

	private final Logger logger = LogMgr.getInstance().getSystemLogger();

	@Override
	public void parse(String file, int omcId, Timestamp stampTime, long taskID) throws Exception {
		try {
			SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
			parser.parse(new File(file), new CmHandler());
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	class CmHandler extends DefaultHandler {

		String currentText;

		String currentTagName;

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			currentText = new String(ch);
			if (!currentTagName.equals("bulkCmConfigDataFile") && !currentTagName.equals("configData") && !currentTagName.equals("xn:SubNetwork")) {
				logger.debug(currentTagName + "  =  " + currentText);
			}

		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			currentTagName = qName;

		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {

		}
	}

	public static void main(String[] args) {
//		EricssonCmParser parser = new EricssonCmParserSaxImp();
//		try {
//			parser.parse("D:\\陈思江\\爱立信数据\\czrnc01_cm_exp_20100224_160312.xml", 11, new Timestamp(new Date().getTime()), 989);
//		} catch (EricssonCmParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
}

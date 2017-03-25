package parser.c.wlan.bean;

import parser.Parser;
import parser.c.wlan.AbstractWlanParser;
import parser.c.wlan.WlanParserFactory;

public class CV2XML_HP_CM extends Parser {

	@Override
	public boolean parseData() throws Exception {
		AbstractWlanParser parser = new WlanParserFactory(getCollectObjInfo(), getFileName()).createWlanParser(WlanParserFactory.HP_CM);
		return parser.parse();
	}

}

package parser.lucent.cm.cdma;

import parser.Parser;

public class CV1XML extends Parser {

	@Override
	public boolean parseData() throws Exception {
		CMParser parser = new CMParser();
		boolean b = parser.parse(collectObjInfo, fileName);
		return b;
	}

}

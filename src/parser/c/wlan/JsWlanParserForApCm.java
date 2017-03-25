package parser.c.wlan;

import task.CollectObjInfo;

public class JsWlanParserForApCm extends AbstractWlanParser {

	protected JsWlanParserForApCm(CollectObjInfo taskInfo, String file) {
		super(taskInfo, file);
	}

	@Override
	public String getRowFlag() {
		return "ApCMData";
	}

	@Override
	public String getTableName() {
		return "CLT_CM_JSWLAN_AP";
	}

}

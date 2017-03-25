package parser.c.wlan;

import task.CollectObjInfo;

public class JsWlanParserForApPm extends AbstractWlanParser {

	protected JsWlanParserForApPm(CollectObjInfo taskInfo, String file) {
		super(taskInfo, file);
	}

	@Override
	public String getRowFlag() {
		return "ApData";
	}

	@Override
	public String getTableName() {
		return "CLT_PM_JSWLAN_AP";
	}

}

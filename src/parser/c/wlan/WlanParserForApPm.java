package parser.c.wlan;

import task.CollectObjInfo;

class WlanParserForApPm extends AbstractWlanParser {

	protected WlanParserForApPm(CollectObjInfo taskInfo, String file) {
		super(taskInfo, file);
	}

	@Override
	public String getRowFlag() {
		return "ApData";
	}

	@Override
	public String getTableName() {
		return "CLT_PM_WLAN_AP";
	}

}

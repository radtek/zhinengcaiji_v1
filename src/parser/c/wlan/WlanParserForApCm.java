package parser.c.wlan;

import task.CollectObjInfo;

class WlanParserForApCm extends AbstractWlanParser {

	protected WlanParserForApCm(CollectObjInfo taskInfo, String file) {
		super(taskInfo, file);
	}

	@Override
	public String getRowFlag() {
		return "ApCMData";
	}

	@Override
	public String getTableName() {
		return "CLT_CM_WLAN_AP";
	}

}

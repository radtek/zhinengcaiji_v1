package parser.c.wlan;

import task.CollectObjInfo;

class WlanParserForApFm extends AbstractWlanParser {

	protected WlanParserForApFm(CollectObjInfo taskInfo, String file) {
		super(taskInfo, file);
	}

	@Override
	public String getRowFlag() {
		return "ApData";
	}

	@Override
	public String getTableName() {
		return "CLT_FM_WLAN_AP";
	}

}

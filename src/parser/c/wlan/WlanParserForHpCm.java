package parser.c.wlan;

import task.CollectObjInfo;

class WlanParserForHpCm extends AbstractWlanParser {

	protected WlanParserForHpCm(CollectObjInfo taskInfo, String file) {
		super(taskInfo, file);
	}

	@Override
	public String getRowFlag() {
		return "HpCMData";
	}

	@Override
	public String getTableName() {
		return "CLT_CM_WLAN_HP";
	}

}

package parser.c.wlan;

import task.CollectObjInfo;

public class JsWlanParserForHpCm extends AbstractWlanParser {

	protected JsWlanParserForHpCm(CollectObjInfo taskInfo, String file) {
		super(taskInfo, file);
	}

	@Override
	public String getRowFlag() {
		return "HpCMData";
	}

	@Override
	public String getTableName() {
		return "clt_cm_jswlan_hp";
	}

}

package parser.c.wlan;

import java.sql.Timestamp;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Date;

import task.CollectObjInfo;
import task.DevInfo;

public class WlanParserFactory {

	public static final int HP_CM = 0;

	public static final int AP_CM = 1;

	public static final int AP_FM = 2;

	public static final int AP_PM = 3;

	public static final int JS_AP_CM = 4;

	public static final int JS_AP_PM = 5;

	public static final int JS_HP_CM = 6;

	private CollectObjInfo taskInfo;

	private String file;

	public WlanParserFactory(CollectObjInfo taskInfo, String file) {
		super();
		this.taskInfo = taskInfo;
		this.file = file;
	}

	@Override
	public String toString() {
		return "WlanParserFactory [file=" + file + ", taskInfo=" + taskInfo + "]";
	}

	public AbstractWlanParser createWlanParser(int type) {
		switch (type) {
			case HP_CM :
				return new WlanParserForHpCm(taskInfo, file);
			case AP_CM :
				return new WlanParserForApCm(taskInfo, file);
			case AP_FM :
				return new WlanParserForApFm(taskInfo, file);
			case AP_PM :
				return new WlanParserForApPm(taskInfo, file);
			case JS_AP_CM :
				return new JsWlanParserForApCm(taskInfo, file);
			case JS_AP_PM :
				return new JsWlanParserForApPm(taskInfo, file);
			case JS_HP_CM :
				return new JsWlanParserForHpCm(taskInfo, file);
			default :
				return null;
		}

	}

	public static void main(String[] args) {
		StringCharacterIterator sc = new StringCharacterIterator("C:\\Users\\ChenSijiang\\Desktop\\wlan数据样例--天津\\AP_CM_27_1_20110423.xml");
		for (char c = sc.first(); c != CharacterIterator.DONE; c = sc.next()) {
			System.out.println(c);
		}

		CollectObjInfo c = new CollectObjInfo(1122);
		DevInfo d = new DevInfo();
		d.setOmcID(99);
		c.setDevInfo(d);
		c.setLastCollectTime(new Timestamp(new Date().getTime()));

		String fileName = "C:\\Users\\ChenSijiang\\Desktop\\wlan数据样例--天津\\AP_CM_27_1_20110423.xml";
		try {
			System.out.println(new WlanParserFactory(c, fileName).createWlanParser(WlanParserFactory.JS_AP_CM).parse());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

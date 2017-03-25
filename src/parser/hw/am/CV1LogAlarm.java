package parser.hw.am;

import java.sql.Timestamp;
import java.text.ParseException;

import task.CollectObjInfo;
import task.DevInfo;

/**
 * 华为告警 FTP文件
 * 
 * @author liuwx May 30, 2011
 */
public class CV1LogAlarm extends HWAlarmLog {

	public CV1LogAlarm() {

	}

	@Override
	public boolean parseData() throws Exception {
		return this.commonParse();
	}

	public static void main(String s[]) {
		CV1LogAlarm a = new CV1LogAlarm();
		String filename = "C:\\Documents and Settings\\wenxiang\\桌面\\2011-05-26\\AlarmLog.2012081712";
		CollectObjInfo collectObjInfo = new CollectObjInfo(1);
		try {
			collectObjInfo.setLastCollectTime(new Timestamp(util.Util.getDate("2011-06-09", "yyyy-MM-dd").getTime()));
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		a.setFileName(filename);
		DevInfo d = new DevInfo();
		d.setOmcID(1);
		collectObjInfo.setDevInfo(d);
		a.setCollectObjInfo(collectObjInfo);
		try {
			a.parseData();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

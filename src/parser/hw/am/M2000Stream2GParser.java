package parser.hw.am;

import java.io.IOException;

import util.Util;
import cn.uway.alarmbox.protocol.monitor.OriAlarm;

/**
 * 
 * M2000Stream2GParser
 * 
 * @author liangww 2012-6-21
 */
public class M2000Stream2GParser extends M2000StreamParser {

	public M2000Stream2GParser() {
		super();
	}

	/**
	 * 
	 */
	protected void handle(String buff) {
		if (Util.isNull(buff) || buff.contains("告警握手"))
			return;

		String data = buff.trim();
		RawDataEntry entry = parseRawData(data);
		// 如果为空
		if (entry == null) {
			return;
		}

		this.list.add(entry);
		if (list.size() >= 100) {
			insert(String.format(SQL, "clt_hw_g_am_test"), list);
			list.clear();
		}

	}

}

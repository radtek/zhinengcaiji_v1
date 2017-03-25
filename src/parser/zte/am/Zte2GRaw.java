package parser.zte.am;

import java.io.ByteArrayInputStream;
import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.io.IOUtils;

import util.Util;

public class Zte2GRaw {

	public String PACKET;

	public String MODULE;

	public String ALARMID;

	public String MOUDLETYPE;

	public String EVENTTYPE;

	public String CAUSE;

	public Timestamp EVENTTIME;

	public Timestamp CANCELTIME;

	public String SEVERITY;

	public String ALARMTEXT;

	public String CODE;

	public String SOURCE;

	public String POSITION;

	public String SOLUTION;

	public static Zte2GRaw fromRaw(String raw) {

		List<String> lines = null;
		try {
			lines = IOUtils.readLines(new ByteArrayInputStream(raw.getBytes()));

			StringBuilder ready = new StringBuilder();
			for (String line : lines) {
				if (Util.isNull(line) || line.trim().equals(GV1CorbaSdAmParser.END_FLAG))
					continue;
				line = line.trim();
				if (line.contains("=")) {
					ready.append("\n");

				}
				ready.append(" ").append(line);
			}
			lines.clear();
			lines = IOUtils.readLines(new ByteArrayInputStream(ready.toString().getBytes()));
			StringBuilder remain = new StringBuilder();
			Zte2GRaw entry = new Zte2GRaw();
			for (String line : lines) {
				line = line.trim();
				if (line.equals(GV1CorbaSdAmParser.END_FLAG)) {
					remain.setLength(0);
				} else if (line.contains("=")) {
					String[] kv = line.split("=");
					String key = kv[0].trim();
					String value = (kv.length > 1 ? kv[1] : "");
					if (key.equals("PACKET")) {
						entry.PACKET = value;
					} else if (key.equals("MODULE")) {
						entry.MODULE = value;
					} else if (key.equals("ALARMID")) {
						entry.ALARMID = value;
					} else if (key.equals("MOUDLETYPE")) {
						entry.MOUDLETYPE = value;
					} else if (key.equals("EVENTTYPE")) {
						entry.EVENTTYPE = value;
					} else if (key.equals("CAUSE")) {
						entry.CAUSE = value;
					} else if (key.equals("EVENTTIME")) {
						try {
							if (Util.isNotNull(value))
								entry.EVENTTIME = new Timestamp(Util.getDate1(value).getTime());
						} catch (Exception e) {
						}
					} else if (key.equals("CANCELTIME")) {
						try {
							if (Util.isNotNull(value))
								entry.CANCELTIME = new Timestamp(Util.getDate1(value).getTime());
						} catch (Exception e) {
						}
					} else if (key.equals("SEVERITY")) {
						entry.SEVERITY = value;
					} else if (key.equals("ALARMTEXT")) {
						entry.ALARMTEXT = value;
					} else if (key.equals("CODE")) {
						entry.CODE = value;
					} else if (key.equals("SOURCE")) {
						entry.SOURCE = value;
					} else if (key.equals("POSITION")) {
						entry.POSITION = value;
					} else if (key.equals("SOLUTION")) {
						entry.SOLUTION = value;
					}
				} else {
					remain.append(line);
				}
			}
			return entry;
		} catch (Exception e) {
			return null;
		} finally {
			if (lines != null)
				lines.clear();
		}
	}

	@Override
	public String toString() {
		return "Zte2GRaw [PACKET=" + PACKET + ", MODULE=" + MODULE + ", ALARMID=" + ALARMID + ", MOUDLETYPE=" + MOUDLETYPE + ", EVENTTYPE="
				+ EVENTTYPE + ", CAUSE=" + CAUSE + ", EVENTTIME=" + EVENTTIME + ", CANCELTIME=" + CANCELTIME + ", SEVERITY=" + SEVERITY
				+ ", ALARMTEXT=" + ALARMTEXT + ", CODE=" + CODE + ", SOURCE=" + SOURCE + ", POSITION=" + POSITION + ", SOLUTION=" + SOLUTION + "]";
	}

	public static void main(String[] args) throws Exception {

		String str = "PACKET=001\n" + "MODULE=0000053780\n" + "ALARMID=0243433011\n" + "MOUDLETYPE=BSC\n" + "EVENTTYPE=0000000000\n"
				+ "CAUSE=1. 对接两端中继帧格式不一致\n" + "2. 对端中继设备发送端故障\n" + "3. 本端中继设备故障\n" + "4. 两端设备接地不好\n" + "5. 阻抗不匹配\n"
				+ "EVENTTIME=2012-06-21 14:50:14\n" + "CANCELTIME=\n" + "SEVERITY=0000000002\n" + "ALARMTEXT=中继电路异常\n" + "CODE=0198000520\n"
				+ "SOURCE=OMCBSCID103\n" + "POSITION=OMCBSCID103,RACKNO1,SHELFNO1,SLOTNO5,CPUNO1\n"
				+ "SOLUTION=在告警管理界面,查看告警的详细信息,在告警的详细信息里,给出了告警产生的错误类型,根据错误类型,可以快速定位到具体的处理方法.\n" + "每个错误类型对应的处理建议描述如下:\n"
				+ "1. 错误类型:E1中继帧丢失,E1中继复帧丢失,E1?";
		Zte2GRaw raw = fromRaw(str);
		System.out.println(raw);
	}
}

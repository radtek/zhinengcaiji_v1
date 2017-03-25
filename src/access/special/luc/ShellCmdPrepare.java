package access.special.luc;

import java.util.ArrayList;
import java.util.List;

import util.LogMgr;
import util.Util;

public class ShellCmdPrepare {

	String cmd;

	String sct;

	String log;

	String raw;

	public ShellCmdPrepare(String cmd, String sct, String log) {
		super();
		this.cmd = cmd;
		this.sct = sct;
		this.log = log;
	}

	public String getCmd() {
		return cmd;
	}

	public String getSct() {
		return sct;
	}

	public String getLog() {
		return log;
	}

	public String getRaw() {
		return raw;
	}

	public static ShellCmdPrepare parse(String content) {
		if (Util.isNull(content))
			return null;

		String trimed = content.trim();

		List<String> tmp = new ArrayList<String>();
		String[] sp = trimed.split(" ");
		for (String s : sp) {
			if (Util.isNotNull(s))
				tmp.add(s.trim());
		}
		try {
			ShellCmdPrepare scp = new ShellCmdPrepare(tmp.get(0), tmp.get(2), tmp.get(4));
			scp.raw = content;
			return scp;
		} catch (Exception e) {
			LogMgr.getInstance().getSystemLogger().error("解析shell_cm_prepare失败 - [" + content + "]", e);
			return null;
		}
	}

	public static List<ShellCmdPrepare> parseAll(String content) {
		if (Util.isNull(content))
			return null;

		List<ShellCmdPrepare> list = new ArrayList<ShellCmdPrepare>();
		String[] sp = content.split(";");
		for (String x : sp) {
			if (Util.isNotNull(x)) {
				list.add(parse(x.trim()));
			}
		}

		return list;
	}

	@Override
	public String toString() {
		return "ShellCmdPrepare [cmd=" + cmd + ", log=" + log + ", sct=" + sct + "]";
	}

	public static void main(String[] args) {
		System.out.println(parse("DBsurvey -i /home/wxwyzh01/uway/luc_ceqface3g.sct -o /home/wxwyzh01/uway/luc_ceqface3g.log   \n"));
	}
}

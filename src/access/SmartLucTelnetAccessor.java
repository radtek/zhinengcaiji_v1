package access;

import java.util.Map;

import parser.SmartLucTelnetParser;
import access.special.luc.LucTelnetCollect;
import access.special.luc.ShellCmdPrepare;
import collect.ProtocolTelnet;

public class SmartLucTelnetAccessor extends TelnetAccessor {

	private String sctName;

	private String logName;

	Map<Integer, String> sctMap;

	SmartLucTelnetParser p;

	@Override
	public void parse(char[] chData, int len) throws Exception {
		if (p == null) {
			p = (SmartLucTelnetParser) parser;
		}
		if (p != null) {
			p.parse(new String(chData, 0, len));
		}
	}

	@Override
	public void configure() throws Exception {

		// try
		// {
		// if ( p == null )
		// p = (SmartLucTelnetParser) parser;
		// }
		// catch (ClassCastException e)
		// {
		// log.error(name + " 类转换异常，请确定igp_conf_task表中的parserid字段是否配置正确。", e);
		// }
		//
		// // 获取sct模板。
		// ShellCmdPrepare scp =
		// ShellCmdPrepare.parse(taskInfo.getShellCmdPrepare());
		// sctName = scp.getSct();
		// logName = scp.getLog();
		//
		// boolean contain = false;
		// synchronized (SmartLucTelnetParser.SCT_CACHE)
		// {
		// contain = SmartLucTelnetParser.SCT_CACHE.containsKey(sctName);
		// }
		// if ( contain )
		// {
		// synchronized (SmartLucTelnetParser.SCT_CACHE)
		// {
		// sctMap = SmartLucTelnetParser.SCT_CACHE.get(sctName);
		// }
		// }
		// else
		// {
		// ProtocolTelnet pt = null;
		// try
		// {
		// log.debug(name + " 开始获取sct文件 - " + sctName);
		// pt = new ProtocolTelnet();
		// boolean b = pt.Login(taskInfo.getDevInfo().getIP(),
		// taskInfo.getDevPort(), taskInfo.getDevInfo().getHostUser(),
		// taskInfo.getDevInfo().getHostPwd(),
		// taskInfo.getDevInfo().getHostSign(), "ANSI",
		// taskInfo.getCollectTimeOut());
		// if ( b )
		// {
		// pt.sendCmd("cat " + sctName);
		// Thread.sleep(2000);
		// int ret = -1;
		// byte[] buffer = new byte[4096];
		// StringBuilder sb = new StringBuilder();
		// while ((ret = pt.readData(buffer)) != -1)
		// {
		// sb.append(new String(buffer, 0, ret));
		// }
		// sctMap = LucTelnetCollect.parseSCT(sb.toString());
		// synchronized (SmartLucTelnetParser.SCT_CACHE)
		// {
		// SmartLucTelnetParser.SCT_CACHE.put(sctName, sctMap);
		// }
		// }
		// else
		// {
		// log.warn(name + " 获取sct文件时，telnet登录失败 - " + sctName);
		// }
		// }
		// catch (Exception e)
		// {
		// log.error(name + " 获取sct内容时出现异常 - " + sctName, e);
		// }
		// finally
		// {
		// pt.Close();
		// }
		// }
		//
		// if ( p != null )
		// {
		// p.setSctInfo(sctName, logName);
		// }
	}

	@Override
	public void doSqlLoad() throws Exception {
		super.doSqlLoad();
	}
}

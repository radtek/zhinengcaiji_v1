package console.commands;

import cn.uway.console.io.CommandIO;

public class HelpCommand extends BasicCommand {

	private static String HELP_INFO = "list          列出系统当前正处理的任务\r\n" + "   kill id       强行终止指定任务(id为任务编号或者补采编号)\r\n"
			+ "   os            获取操作系统信息\r\n" + "   jvm           获取JVM的版本信息及JVM内存消耗情况\r\n" + "   ver           获取采集系统版本信息\r\n"
			+ "   disk          获取磁盘信息\r\n" + "   stop          停止采集系统 (等待任务执行完后在停止)\r\n" + "   stop -i       立即停止采集系统\r\n"
			+ "   date          获取服务器当前时间\r\n" + "   host          获取服务器机器名\r\n" + "   sys           获取系统信息\r\n"
			+ "   error         获取采集系统标准错误端信息\r\n" + "   thread -c     获取采集系统内部线程个数\r\n" + "   whoami        获取当前用户信息\r\n"
			+ "   exit          退出会话\r\n" + "   help/?        获取帮助";

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		io.println(HELP_INFO);
		return true;
	}

}

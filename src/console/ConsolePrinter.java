package console;

import java.io.PrintWriter;

/**
 * 控制台信息打印类
 * 
 * @author YangJian
 * @since DC3.1
 */
public class ConsolePrinter {

	private PrintWriter pw = null;

	private static final String BLANK = "   ";

	public ConsolePrinter(PrintWriter pw) {
		super();
		this.pw = pw;

		printHello();
	}

	public void close() {
		pw.close();
	}

	public void printHello() {
		println();
		println("----------------------------------------------------");
		println("                Welcome to IGP Console              ");
		println("     Copyright 2004-2020 Uway All Rights Reserved   ");
		println("----------------------------------------------------");
		println();
		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void printHelp() {
		println("list          列出系统当前正处理的任务");
		println("kill id       强行终止指定任务(id为任务编号或者补采编号)");
		println("os            获取操作系统信息");
		println("jvm           获取JVM的版本信息及JVM内存消耗情况");
		println("ver           获取采集系统版本信息");
		println("disk          获取磁盘信息");
		println("stop          停止采集系统 (等待任务执行完后在停止)");
		println("stop -i       立即停止采集系统");
		println("date          获取服务器当前时间");
		println("host          获取服务器机器名");
		println("sys           获取系统信息");
		println("error         获取采集系统标准错误端信息");
		println("thread -c     获取采集系统内部线程个数");
		println("whoami        获取当前用户信息");
		println("exit          退出会话");
		println("help/?        获取帮助");
	}

	/**
	 * 不支持的命令
	 * 
	 * @param cmd
	 */
	public void printUnSupportCmd(String cmd) {
		println("不支持的命令 " + cmd);
		println("输入help或者?获取帮助");
	}

	public void println() {
		pw.println(BLANK);
		pw.flush();
	}

	public void println(String content) {
		pw.println(BLANK + content);
		pw.flush();
	}

	public void print(String content) {
		pw.print(BLANK + content);
		pw.flush();
	}

	public void backspace() {
		pw.print(' ');
		pw.print('\b');
		pw.flush();
	}

	public void maskChar() {
		pw.print('\b');
		pw.print(' ');
		pw.flush();
	}

	public void printNull() {
		pw.print(' ');
		pw.flush();
	}

	public void printPrompt() {
		print("> ");
	}
}

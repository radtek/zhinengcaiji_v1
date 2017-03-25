package console;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import util.Util;
import db.dao.UserDAO;
import db.pojo.User;

/**
 * 客户端请求处理类
 * 
 * @author YangJian
 * @since DC3.1
 */
public class RequestHandler implements Runnable {

	private Socket socket;

	private ConsolePrinter printer = null;

	private CmdHandler cmdHandler = null;

	// 用户登陆信息
	private String userName;

	private String userPwd;

	private Date loginTime; // 登陆时间

	private boolean loginFlag = false; // 是否已经登陆标记，默认为false

	private int loginTimes = 0; // 登陆尝试次数

	private static final int MAX_LOGIN_TIMES = 3; // 最大允许尝试登陆次数

	public static final List<Socket> SOCKETS = new ArrayList<Socket>();

	public RequestHandler(Socket soeckt) {
		super();
		this.socket = soeckt;
		if (soeckt != null) {
			synchronized (SOCKETS) {
				SOCKETS.add(soeckt);
			}
		}
	}

	@Override
	public void run() {
		try {
			printer = new ConsolePrinter(new PrintWriter(socket.getOutputStream()));
			cmdHandler = new CmdHandler(printer);
			InputStream in = socket.getInputStream();
			// 处理登陆
			handleLogin(in);
			if (!loginFlag)
				return;
			// 处理用户输入的命令
			String strLine = cmdHandler.getInput(in);
			while ((strLine != null && !strLine.trim().equalsIgnoreCase("exit"))) {
				handleCommand(strLine, in);
				printer.printPrompt();
				strLine = cmdHandler.getInput(in);
			}
		} catch (SocketException se) {
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (printer != null)
				printer.close();

			if (socket != null)
				try {
					socket.close();
					synchronized (SOCKETS) {
						SOCKETS.remove(socket);
					}
				} catch (IOException e) {
				}
		}
	}

	/**
	 * 处理登陆
	 * 
	 * @param in
	 * @throws Exception
	 */
	private void handleLogin(InputStream in) throws Exception {
		printer.print("Login: ");

		String strLine = cmdHandler.getInput(in);
		while (strLine != null) {
			userName = strLine.trim(); // 获取输入的用户名
			// 用户如果什么都没有输入而是直接回车，则继续提示login
			if (userName.length() == 0) {
				printer.print("Login: ");
				strLine = cmdHandler.getInput(in);
				continue;
			}

			printer.print("Password: ");
			strLine = cmdHandler.getInput(in, false);
			if (strLine == null) {
				printer.print("Login: ");
				strLine = cmdHandler.getInput(in);
				continue;
			} else {
				userPwd = strLine.trim(); // 获取用户输入的密码

				// 验证用户名和密码的正确性
				User u = new User();
				u.setUserName(userName);
				u.setUserPwd(userPwd);
				loginFlag = new UserDAO().checkAccount(u);
				if (loginFlag) // 登陆成功
				{
					loginTime = new Date();
					String hostName = Util.getHostName();
					Properties props = System.getProperties();
					String osName = props.getProperty("os.name");
					String osVersion = props.getProperty("os.version");

					printer.println("Hello " + userName + ".   " + osName + " " + osVersion + "   " + hostName + "   "
							+ Util.getDateString(loginTime));
					printer.printPrompt();
					break;
				} else
				// 登陆失败
				{
					loginTimes++;
					if (loginTimes >= MAX_LOGIN_TIMES)
						break;
					else {
						printer.println("Login incorrect");
						printer.print("Login: ");
						strLine = cmdHandler.getInput(in);
						continue;
					}
				}
			}
		}
	}

	/**
	 * 处理客户端发过来的请求命令
	 * 
	 * @param cmd
	 *            命令
	 * @param in
	 *            输入流,有时候需要传入输入流，因为有的命令需要和用户交互
	 */
	private void handleCommand(String cmd, InputStream in) {
		if (!loginFlag)
			return;

		if (Util.isNull(cmd))
			return;

		if (cmd.equalsIgnoreCase("list")) // 列出系统当前正处理的任务
		{
			cmdHandler.list();
		} else if (cmd.startsWith("kill")) // 强行终止指定任务
		{
			cmdHandler.kill(cmd, in);
		} else if (cmd.equalsIgnoreCase("os")) // 打印操作系统信息
		{
			cmdHandler.os();
		} else if (cmd.equalsIgnoreCase("jvm")) // 打印JVM的版本信息及JVM内存消耗情况
		{
			cmdHandler.jvm();
		} else if (cmd.equalsIgnoreCase("ver")) // 打印采集系统当前的版本号
		{
			cmdHandler.ver();
		} else if (cmd.equalsIgnoreCase("disk")) // 获取磁盘信息
		{
			cmdHandler.disk();
		} else if (cmd.equalsIgnoreCase("date")) // 获取服务器当前时间
		{
			cmdHandler.date();
		} else if (cmd.equalsIgnoreCase("host")) // 获取服务器机器名
		{
			cmdHandler.host();
		} else if (cmd.equalsIgnoreCase("sys")) // 获取系统信息
		{
			cmdHandler.sys();
		} else if (cmd.equalsIgnoreCase("error")) // 获取采集系统标准错误端信息
		{
			cmdHandler.error();
		} else if (cmd.equalsIgnoreCase("whoami")) // 获取当前用户信息
		{
			printer.println(userName + "    login time: " + Util.getDateString(loginTime));
		} else if (cmd.startsWith("thread")) // 获取采集系统内部线程信息
		{
			cmdHandler.thread(cmd);
		} else if (cmd.startsWith("stop")) // 停止采集系统
		{
			cmdHandler.stop(cmd, in);
		} else if (cmd.equalsIgnoreCase("help") || cmd.equalsIgnoreCase("?")) // 获取帮助
		{
			printer.printHelp();
		} else {
			printer.printUnSupportCmd(cmd);
		}
	}

}

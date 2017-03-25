package console;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import task.TaskMgr;
import util.CommonDB;
import util.DbPool;
import util.DbPoolManager;
import util.Util;
import alarm.AlarmMgr;
import datalog.DataLogMgr;
import framework.DataLifecycleMgr;
import framework.ScanThread;
import framework.SystemConfig;

/**
 * 命令处理类
 * 
 * @author YangJian
 * @since 3.1
 */
public class CmdHandler {

	private ConsolePrinter printer = null;

	/**
	 * 换行符
	 */
	static final char N_CHAR = 0x0a;

	/**
	 * 回车符
	 */
	static final char R_CHAR = 0x0d;

	/**
	 * 退格符
	 */
	static final char B_CHAR = 0x08;

	public CmdHandler(ConsolePrinter printer) {
		super();
		this.printer = printer;
	}

	/**
	 * 获取当前系统运行的任务列表
	 */
	public void list() {
		List<CollectObjInfo> lst = TaskMgr.getInstance().list();
		if (lst.size() == 0) {
			printer.println("暂无运行任务");
			return;
		}

		long now = System.currentTimeMillis();
		for (CollectObjInfo obj : lst) {
			long taskID = obj.getTaskID();
			String des = obj.getDescribe();
			Timestamp dataTime = obj.getLastCollectTime();

			String flag = "";
			if (obj instanceof RegatherObjInfo)
				flag = "-" + String.valueOf(obj.getKeyID() - 10000000);
			String cost = "";
			long fast = now - obj.startTime.getTime();
			// 小于一分钟之内使用秒为单位
			if (fast < (1000 * 60))
				cost = Math.round(fast / 1000) + " 秒";
			// 大于一分钟的使用分钟作为单位
			else {
				cost = Math.round(fast / (1000 * 60)) + " 分钟";
			}
			printer.println(taskID + flag + "   " + dataTime + "   " + des + "  " + cost);
		}
		printer.println("总计： " + lst.size() + " 个");
	}

	/**
	 * 读取标准输入端的输入，并且回显
	 * 
	 * @param in
	 *            输入流
	 * @return 输入的字符串
	 * @throws IOException
	 */
	String getInput(InputStream in) throws IOException {
		return getInput(in, true);
	}

	/**
	 * 读取标准输入端的输入
	 * 
	 * @param in
	 *            输入流
	 * @param echo
	 *            是否回显
	 * @return 输入的字符串
	 * @throws IOException
	 */
	String getInput(InputStream in, boolean echo) throws IOException {
		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// 缓存字符串，存入输入的有效字符，最终组成一条命令字符串
		StringBuilder buffer = new StringBuilder();

		int i = -1;
		// 以回车符作为结束符，读取输入一行命令
		while ((i = in.read()) != R_CHAR && i > -1) {
			// 是否回显字符到输出端
			if (!echo) {
				printer.maskChar();
			}

			// 当输入的字符不是退格也不是换行符时，认为是有效字符，存入缓存
			if (i != B_CHAR && i != N_CHAR) {
				buffer.append((char) i);
			}
			// 缓存长度大于0时，退格
			else if (buffer.length() > 0 && buffer.charAt(buffer.length() - 1) != (char) N_CHAR) {
				printer.backspace();
				buffer.deleteCharAt(buffer.length() - 1);
			}
			// 防止不停按退格键时，光标一直往左移
			else if (i != N_CHAR) {
				printer.printNull();
			}
			/*
			 * 此处，为了防止非正常情况下，结束会话时（强行关掉CMD窗），程序陷入死循环， 所以，加一个换行键，作为标记，指示当前还没有关闭CMD窗口
			 */
			else if (buffer.length() == 0) {
				buffer.append((char) N_CHAR);
			}
		}

		// 此种情况，缓存长度为0，即未加换行符作为标记，也就是说CMD窗口是被强关闭的，所以发送exit命令，退出会话。
		if (buffer.length() == 0) {
			return "exit";
		}

		String str = buffer.toString().trim();
		return str;
	}

	/**
	 * 强行终止指定任务(kill 任务编号)
	 * 
	 * @param cmd
	 *            用户输入的kill命令
	 * @param in
	 *            输入流
	 */
	public void kill(String cmd, InputStream in) {
		if (cmd.length() <= 4) {
			printer.println("kill语法错误,缺少任务编号. 输入help/?获取命令帮助");
			return;
		}

		String[] strs = cmd.split(" ");
		if (strs.length != 2) {
			printer.println("kill语法错误. 输入help/?获取命令帮助");
			return;
		}
		if (!strs[0].equalsIgnoreCase("kill")) {
			printer.printUnSupportCmd(cmd);
		}

		// 检查任务编号合法性
		String strTaskID = strs[1];
		int taskID = -1;
		try {
			taskID = Integer.parseInt(strTaskID);
		} catch (NumberFormatException e) {
		}
		if (taskID == -1) {
			printer.println("kill语法错误,任务编号输入有误. 输入help/?获取命令帮助");
			return;
		}

		CollectObjInfo obj = TaskMgr.getInstance().getObjByID(taskID);
		if (obj == null) {
			obj = TaskMgr.getInstance().getObjByID(taskID + 10000000); // 看是否是补采任务,补采id要加10000000
			if (obj == null) {
				printer.println("指定的任务编号当前不在运行状态或者任务编号不存在");
				return;
			}
		}

		// 询问用户是否杀死指定任务
		String des = obj.getDescribe();
		Timestamp dataTime = obj.getLastCollectTime();
		printer.print("是否要杀死任务(" + taskID + ", " + dataTime + ", " + des + " )?   [y|n]  ");
		String strLine = null;
		try {
			strLine = getInput(in);
		} catch (IOException e) {
			printer.println("操作失败,内部错误:" + e.getMessage());
			return;
		}
		strLine = strLine == null ? "" : strLine;
		// 放弃停止
		if (strLine.equalsIgnoreCase("n") || strLine.equalsIgnoreCase("no"))
			return;
		// 输入非法
		if (!strLine.equalsIgnoreCase("n") && !strLine.equalsIgnoreCase("no") && !strLine.equalsIgnoreCase("y") && !strLine.equalsIgnoreCase("yes")) {
			printer.println("非法输入,放弃操作.");
			return;
		}

		// 杀死指定任务
		Thread thrd = obj.getCollectThread();
		if (thrd != null) {
			thrd.interrupt();
			thrd = null;
			TaskMgr.getInstance().delActiveTask(taskID, (obj instanceof RegatherObjInfo));
		}
	}

	/**
	 * 停止采集系统
	 * 
	 * @param cmd
	 *            用户输入的停止命令
	 * @param in
	 *            输入流
	 */
	public void stop(String cmd, InputStream in) {
		if (!cmd.equalsIgnoreCase("stop") && !cmd.equalsIgnoreCase("stop -i")) {
			printer.println("stop语法错误. 输入help/?获取命令帮助");
			return;
		}

		int taskCount = TaskMgr.getInstance().size();
		// 询问用户是否真的立即退出
		printer.print("当前有" + taskCount + "个任务在运行,是否要立即退出? [y|n]  ");
		String strLine = null;
		try {
			strLine = getInput(in);
		} catch (IOException e) {
			printer.println("操作失败,内部错误:" + e.getMessage());
			return;
		}
		strLine = strLine == null ? "" : strLine;
		// 放弃停止
		if (strLine.equalsIgnoreCase("n") || strLine.equalsIgnoreCase("no"))
			return;
		// 输入非法
		if (!strLine.equalsIgnoreCase("n") && !strLine.equalsIgnoreCase("no") && !strLine.equalsIgnoreCase("y") && !strLine.equalsIgnoreCase("yes")) {
			printer.println("非法输入,放弃停止采集系统.");
			return;
		}

		// 等待当前所有任务执行完毕后在停止采集系统
		if (cmd.equalsIgnoreCase("stop")) {
			awaitStop();
		}
		// 立即停止
		else if (cmd.equalsIgnoreCase("stop -i")) {
			// 如果还有任务在系统里执行，则返回-1，否则返回0,表示正常退出（因为此时已经没有任务在执行了，也就无所谓业务意义的失败了）
			if (taskCount == 0)
				System.exit(0);
			else
				System.exit(-1);
		}
	}

	/**
	 * 等待任务执行完毕后停止采集系统
	 */
	private void awaitStop() {
		ScanThread scanThread = ScanThread.getInstance();
		scanThread.setEndAction(new ScanThread.ScanEndAction() {

			@Override
			public void actionPerformed(TaskMgr taskMgr) {
				List<CollectObjInfo> tasks = taskMgr.list();
				for (CollectObjInfo task : tasks) {
					String taskType = (task instanceof RegatherObjInfo ? "补采任务" : "采集任务");
					Thread th = task.getCollectThread();
					long id = task.getKeyID();
					printer.print("正在等待" + taskType + "(id:" + id + ")结束");
					try {
						th.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					th.interrupt();
					printer.println("......已结束");
				}

				// 结束告警,生命周期线程，连接池
				DataLifecycleMgr.getInstance().stop();
				AlarmMgr.getInstance().shutdown();
				DataLogMgr.getInstance().commit(); // 提交尚未入库的数据库日志
				CommonDB.closeDbConnection();
				DbPool.close();

				
				DbPoolManager.closeAllConnectionPool();

				// 结束控制台线程
				Console.getInstance().stop();
				
			
				System.exit(0);

			}
		});
		scanThread.stopScan();

	}

	/**
	 * 获取采集系统标准错误端信息
	 */
	public void error() {
		String stdErrFile = "." + File.separator + "log" + File.separator + "error.log";
		File fError = new File(stdErrFile);
		if (fError.exists() && fError.isFile()) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(fError)));
				String strLine = null;
				while ((strLine = br.readLine()) != null) {
					printer.println(strLine);
				}
			} catch (FileNotFoundException e) {
				printer.println("标准错误端文件 " + stdErrFile + " 不存在");
			} catch (IOException e) {
				printer.println("获取标准错误端信息时异常,原因: " + e.getMessage());
			} finally {
				if (br != null)
					try {
						br.close();
					} catch (IOException e) {
					}
			}
		} else
			printer.println("标准错误端文件 " + stdErrFile + " 不存在");
	}

	/**
	 * 获取采集系统内部线程信息
	 */
	public void thread(String cmd) {
		if (!cmd.equalsIgnoreCase("thread") && !cmd.equalsIgnoreCase("thread -c")) {
			printer.println("thread语法错误. 输入help/?获取命令帮助");
			return;
		}

		// 获取系统内部线程信息
		if (cmd.equalsIgnoreCase("thread")) {
			// TODO 暂无实现
		}
		// 获取系统内部线程个数
		else if (cmd.equalsIgnoreCase("thread -c")) {
			printer.println("active thread count: " + Thread.activeCount());
		}
	}

	/**
	 * 获取操作系统信息
	 */
	public void os() {
		Properties props = System.getProperties();
		String osName = props.getProperty("os.name");
		String osArch = props.getProperty("os.arch");
		String osVersion = props.getProperty("os.version");

		printer.println(osName + "  " + osArch + "  " + osVersion);
	}

	/**
	 * 获取JAVA虚拟机信息
	 */
	public void jvm() {
		float maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
		float totalMemory = Runtime.getRuntime().totalMemory() / (1024 * 1024);
		float freeMemory = Runtime.getRuntime().freeMemory() / (1024 * 1024);
		float usedMemory = totalMemory - freeMemory;
		freeMemory = maxMemory - usedMemory;

		printer.println("jvm memory usage: ");
		printer.println("已使用: " + usedMemory + "M  剩余: " + freeMemory + "M  最大内存: " + maxMemory + "M");
	}

	/**
	 * 获取服务器当前时间
	 */
	public void date() {
		printer.println(Util.getDateString(new Date()));
	}

	/**
	 * 获取服务器机器名
	 */
	public void host() {
		printer.println(Util.getHostName());
	}

	/**
	 * 获取系统信息
	 */
	public void sys() {
		Date sDate = null;
		try {
			sDate = (Date) Class.forName("framework.IGP").getDeclaredField("SYS_START_TIME").get(null);
		} catch (Exception e) {
			printer.println("错误,原因:" + e.getMessage());
		}
		String sysStartTime = Util.getDateString(sDate);
		long fast = System.currentTimeMillis() - sDate.getTime();
		String cost = "";
		// 小于1小时的使用分钟为单位
		if (fast < (1000 * 60 * 60)) {
			cost = Math.round(fast / (1000 * 60)) + " 分钟";
		}
		// 其他使用小时为单位
		else {
			cost = Math.round(fast / (1000 * 60 * 60)) + " 小时";
		}
		printer.println("系统启动时间: " + sysStartTime + "  已运行: " + cost);
	}

	/**
	 * 获取磁盘信息
	 */
	public void disk() {
		try {
			File[] roots = File.listRoots();
			for (File f : roots) {
				float total = f.getTotalSpace();
				if (total == 0)
					continue;
				float remain = f.getFreeSpace();
				int u = Math.round((remain / total) * 100);

				printer.println(f.getPath() + "  " + (remain / (1024 * 1024 * 1024)) + "GB可用   共 " + (total / (1024 * 1024 * 1024)) + "GB   剩余: " + u
						+ "%");
			}
		} catch (Exception e) {
		}
	}

	/**
	 * 获取采集系统版本信息
	 */
	public void ver() {
		String edition = SystemConfig.getInstance().getEdition();
		String releaseTime = SystemConfig.getInstance().getReleaseTime1();

		printer.println(edition + "  " + releaseTime);
	}

}

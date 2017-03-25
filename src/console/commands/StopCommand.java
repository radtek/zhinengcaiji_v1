package console.commands;

import java.util.List;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import task.TaskMgr;
import util.CommonDB;
import util.DbPool;
import util.DbPoolManager;
import util.LogMgr;
import alarm.AlarmMgr;
import cn.uway.console.io.CommandIO;
import console.ConsoleMgr;
import datalog.DataLogMgr;
import framework.DataLifecycleMgr;
import framework.ScanThread;

public class StopCommand extends BasicCommand {

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		String i = null;
		int taskCount = TaskMgr.getInstance().size();
		if (args != null && args.length > 0) {
			i = args[0].trim();
			i = i.equalsIgnoreCase("-i") ? i : null;
		}

		if (i != null) {
			// 如果还有任务在系统里执行，则返回-1，否则返回0,表示正常退出（因为此时已经没有任务在执行了，也就无所谓业务意义的失败了）
			if (taskCount == 0)
				System.exit(0);
			else
				System.exit(-1);
		}

		else if (args == null || args.length < 1) {

			// 询问用户是否真的立即退出
			String strLine = io.readLine("当前有" + taskCount + "个任务在运行,是否要立即退出? [y|n]  ");

			// 放弃停止
			if (strLine.equalsIgnoreCase("n") || strLine.equalsIgnoreCase("no"))
				return true;
			// 输入非法
			if (!strLine.equalsIgnoreCase("n") && !strLine.equalsIgnoreCase("no") && !strLine.equalsIgnoreCase("y")
					&& !strLine.equalsIgnoreCase("yes")) {
				io.println("非法输入,放弃停止采集系统.");
				return true;
			}
			awaitStop(io);
		}

		return true;
	}

	private void awaitStop(final CommandIO printer) {
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
				LogMgr.getInstance().getDBLogger().dispose();
				CommonDB.closeDbConnection();
				DbPool.close();
				
				DbPoolManager.closeAllConnectionPool();

				// 结束控制台线程
				ConsoleMgr.getInstance().stop();

				System.exit(0);

			}
		});
		scanThread.stopScan();

	}

}

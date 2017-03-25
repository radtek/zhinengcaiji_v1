package console.commands;

import java.sql.Timestamp;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import task.TaskMgr;
import cn.uway.console.io.CommandIO;

public class KillCommand extends BasicCommand {

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		if (args == null || args.length < 1) {
			io.println("kill语法错误,缺少任务编号. 输入help/?获取命令帮助");
			return true;
		}

		// 检查任务编号合法性
		String strTaskID = args[0];
		int taskID = -1;
		try {
			taskID = Integer.parseInt(strTaskID);
		} catch (NumberFormatException e) {
		}
		if (taskID == -1) {
			io.println("kill语法错误,任务编号输入有误. 输入help/?获取命令帮助");
			return true;
		}

		CollectObjInfo obj = TaskMgr.getInstance().getObjByID(taskID);
		if (obj == null) {
			obj = TaskMgr.getInstance().getObjByID(taskID + 10000000); // 看是否是补采任务,补采id要加10000000
			if (obj == null) {
				io.println("指定的任务编号当前不在运行状态或者任务编号不存在");
				return true;
			}
		}

		// 询问用户是否杀死指定任务
		String des = obj.getDescribe();
		Timestamp dataTime = obj.getLastCollectTime();
		String strLine = io.readLine("是否要杀死任务(" + taskID + ", " + dataTime + ", " + des + " )?   [y|n]  ");

		// 放弃停止
		if (strLine.equalsIgnoreCase("n") || strLine.equalsIgnoreCase("no"))
			return true;
		// 输入非法
		if (!strLine.equalsIgnoreCase("n") && !strLine.equalsIgnoreCase("no") && !strLine.equalsIgnoreCase("y") && !strLine.equalsIgnoreCase("yes")) {
			io.println("非法输入,放弃操作.");
			return true;
		}

		// 杀死指定任务
		Thread thrd = obj.getCollectThread();
		if (thrd != null) {
			thrd.interrupt();
			thrd = null;
			TaskMgr.getInstance().delActiveTask(taskID, (obj instanceof RegatherObjInfo));
		}
		return true;
	}

}

package console.commands;

import java.sql.Timestamp;
import java.util.List;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import task.TaskMgr;
import cn.uway.console.io.CommandIO;

public class ListCommand extends BasicCommand {

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		List<CollectObjInfo> lst = TaskMgr.getInstance().list();
		if (lst.size() == 0) {
			io.println("暂无运行任务");
			return true;
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
			io.println(taskID + flag + "   " + dataTime + "   " + des + "  " + cost);
		}
		io.println("总计： " + lst.size() + " 个");
		return true;
	}

}

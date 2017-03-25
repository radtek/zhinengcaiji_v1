package console.commands;

import cn.uway.console.io.CommandIO;

public class JvmCommand extends BasicCommand {

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		float maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
		float totalMemory = Runtime.getRuntime().totalMemory() / (1024 * 1024);
		float freeMemory = Runtime.getRuntime().freeMemory() / (1024 * 1024);
		float usedMemory = totalMemory - freeMemory;
		freeMemory = maxMemory - usedMemory;

		io.println("jvm memory usage: ");
		io.println("已使用: " + usedMemory + "M  剩余: " + freeMemory + "M  最大内存: " + maxMemory + "M");
		return true;
	}

}

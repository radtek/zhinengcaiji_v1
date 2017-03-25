package console.commands;

import cn.uway.console.io.CommandIO;

public class ThreadCommand extends BasicCommand {

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		if (args == null || args.length < 1 || !args[0].trim().toLowerCase().equals("-c")) {
			io.println("thread语法错误. 输入help/?获取命令帮助");
			return true;
		}
		io.println("active thread count: " + Thread.activeCount());
		return true;
	}

}

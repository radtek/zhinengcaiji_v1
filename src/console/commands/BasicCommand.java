package console.commands;

import cn.uway.console.command.CommandAction;
import cn.uway.console.io.CommandIO;

public abstract class BasicCommand implements CommandAction {

	@Override
	public boolean handleCommand(String[] args, CommandIO io) throws Exception {
		io.setPrefix("  >");
		return doCommand(args, io);
	}

	public abstract boolean doCommand(String[] args, CommandIO io) throws Exception;
}

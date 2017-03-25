package console.commands;

import cn.uway.console.io.CommandIO;

public class ExitCommand extends BasicCommand {

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		io.println("Bye.");
		return false;
	}

}

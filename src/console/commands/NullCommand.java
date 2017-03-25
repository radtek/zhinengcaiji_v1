package console.commands;

import cn.uway.console.io.CommandIO;

public class NullCommand extends BasicCommand {

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		return true;
	}

}

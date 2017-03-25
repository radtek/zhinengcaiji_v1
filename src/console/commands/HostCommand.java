package console.commands;

import util.Util;
import cn.uway.console.io.CommandIO;

public class HostCommand extends BasicCommand {

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		io.println(Util.getHostName());
		return true;
	}

}

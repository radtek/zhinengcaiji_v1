package console.commands;

import cn.uway.console.io.CommandIO;
import framework.SystemConfig;

public class VerCommand extends BasicCommand {

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		String edition = SystemConfig.getInstance().getEdition();
		String releaseTime = SystemConfig.getInstance().getReleaseTime1();

		io.println(edition + "  " + releaseTime);
		return true;
	}

}

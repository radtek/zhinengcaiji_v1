package console.commands;

import java.util.Date;

import util.Util;
import cn.uway.console.io.CommandIO;

public class DateCommand extends BasicCommand {

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		io.println(Util.getDateString(new Date()));
		return true;
	}

}

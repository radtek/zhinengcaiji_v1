package console;

import cn.uway.console.command.CommandAction;
import cn.uway.console.io.CommandIO;

class NotFindAction implements CommandAction {

	@Override
	public boolean handleCommand(String[] args, CommandIO io) throws Exception {
		io.setPrefix("  >");
		io.println("您输入的命令不存在，请输入help或?获取帮助");
		return true;
	}

}

package console;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cn.uway.console.Console;
import cn.uway.console.command.Command;
import console.commands.DateCommand;
import console.commands.DiskCommand;
import console.commands.ErrorCommand;
import console.commands.ExitCommand;
import console.commands.HelpCommand;
import console.commands.HostCommand;
import console.commands.JvmCommand;
import console.commands.KillCommand;
import console.commands.ListCommand;
import console.commands.NullCommand;
import console.commands.OsCommand;
import console.commands.StopCommand;
import console.commands.SysCommand;
import console.commands.ThreadCommand;
import console.commands.VerCommand;
import framework.SystemConfig;

public final class ConsoleMgr {

	private Console console;

	private static ConsoleMgr instance;

	private static String WELCOME_INFO = "  ---------------------------------------"
			+ "-------------\r\n                  Welcome to IGP Console              \r\n   "
			+ "    Copyright 2004-2020 Uway All Rights Reserved   \r\n  ----------------------" + "------------------------------\r\n";

	// 单例
	private ConsoleMgr() {
		int port = SystemConfig.getInstance().getCollectPort();
		Map<String, Command> cmds = new HashMap<String, Command>();
		cmds.put("?", new Command("?", new HelpCommand()));
		cmds.put("help", new Command("help", new HelpCommand()));
		cmds.put("date", new Command("date", new DateCommand()));
		cmds.put("disk", new Command("disk", new DiskCommand()));
		cmds.put("error", new Command("error", new ErrorCommand()));
		cmds.put("exit", new Command("exit", new ExitCommand()));
		cmds.put("host", new Command("host", new HostCommand()));
		cmds.put("jvm", new Command("jvm", new JvmCommand()));
		cmds.put("kill", new Command("kill", new KillCommand()));
		cmds.put("list", new Command("list", new ListCommand()));
		cmds.put("os", new Command("os", new OsCommand()));
		cmds.put("stop", new Command("stop", new StopCommand()));
		cmds.put("sys", new Command("sys", new SysCommand()));
		cmds.put("thread", new Command("thread", new ThreadCommand()));
		cmds.put("ver", new Command("ver", new VerCommand()));
		cmds.put("", new Command("", new NullCommand()));
		try {
			console = new Console(port, cmds, new NotFindAction(), new LoginAction(), WELCOME_INFO);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public synchronized static ConsoleMgr getInstance() {
		if (instance == null) {
			instance = new ConsoleMgr();
		}
		return instance;
	}

	public void start() {
		if (console != null) {
			try {
				console.start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void stop() {
		if (console != null) {
			console.stop();
		}
	}

	public static void main(String[] args) {
		ConsoleMgr.getInstance().start();
	}
}

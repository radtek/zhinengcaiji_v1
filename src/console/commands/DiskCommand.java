package console.commands;

import java.io.File;

import cn.uway.console.io.CommandIO;

public class DiskCommand extends BasicCommand {

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		try {
			File[] roots = File.listRoots();
			for (File f : roots) {
				float total = f.getTotalSpace();
				if (total == 0)
					continue;
				float remain = f.getFreeSpace();
				int u = Math.round((remain / total) * 100);

				io.println(f.getPath() + "  " + (remain / (1024 * 1024 * 1024)) + "GB可用   共 " + (total / (1024 * 1024 * 1024)) + "GB   剩余: " + u
						+ "%");
			}
		} catch (Exception e) {
		}
		return true;
	}

}

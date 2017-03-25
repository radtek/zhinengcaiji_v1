package console.commands;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import cn.uway.console.io.CommandIO;

public class ErrorCommand extends BasicCommand {

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		String stdErrFile = "." + File.separator + "log" + File.separator + "error.log";
		File fError = new File(stdErrFile);
		if (fError.exists() && fError.isFile()) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(fError)));
				String strLine = null;
				while ((strLine = br.readLine()) != null) {
					io.println(strLine);
				}
			} catch (FileNotFoundException e) {
				io.println("标准错误端文件 " + stdErrFile + " 不存在");
			} catch (IOException e) {
				io.println("获取标准错误端信息时异常,原因: " + e.getMessage());
			} finally {
				if (br != null)
					try {
						br.close();
					} catch (IOException e) {
					}
			}
		} else
			io.println("标准错误端文件 " + stdErrFile + " 不存在");
		return true;
	}

}

package delayprobe;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

import util.Util;
import framework.SystemConfig;

/**
 * 用于记录延时日志
 * 
 * @author ChenSijiang 2010-08-09
 * @version 1.1
 */
public class ProbeLogger {

	private long taskId;

	private FileWriter writer;

	private File logDir;

	private File logFile;

	private static final String SEPARATOR = File.separator;

	private static final String LINE_SEP = System.getProperty("line.separator");

	private static final int MAX_LENGTH = 10 * 1024 * 1024;

	public ProbeLogger(long taskId) {
		if (!SystemConfig.getInstance().isEnableProbeLog()) {
			return;
		}
		this.taskId = taskId;

		logDir = new File("." + SEPARATOR + "log" + SEPARATOR + "delay_log" + SEPARATOR + this.taskId);
		if (!logDir.exists()) {
			logDir.mkdirs();
		}
		logFile = new File(logDir, "delay.log");
		if (logFile.exists() && logFile.length() >= MAX_LENGTH) {
			File history = new File(logDir, "delay.log." + Util.getDateString_yyyyMMddHHmmssSSS(new Date()) + ".history");
			logFile.renameTo(history);
			logFile.delete();
			try {
				logFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		try {
			if (!logFile.exists()) {
				logFile.createNewFile();
			}
			writer = new FileWriter(logFile, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void println(String s) {
		if (!SystemConfig.getInstance().isEnableProbeLog()) {
			return;
		}
		if (writer != null) {
			try {
				writer.write("[" + Util.getDateString(new Date()) + "] " + s + LINE_SEP);
				writer.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void dispose() {
		if (!SystemConfig.getInstance().isEnableProbeLog()) {
			return;
		}
		if (writer != null) {
			try {
				writer.flush();
				writer.close();
				writer = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {

		ProbeLogger log = new ProbeLogger(12);
		log.println("asdf有人");
		log.dispose();
	}
}

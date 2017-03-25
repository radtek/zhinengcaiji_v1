package util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

/**
 * 外部命令 类
 * 
 * @author YangJian
 * @version 1.0
 */
public class ExternalCmd {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	public String _cmd;

	public long taskID;

	public ExternalCmd() {
		super();
	}

	public void setCmd(String cmd) {
		_cmd = cmd;
	}

	/** 执行命令 */
	public int execute(String cmd) throws Exception {
		_cmd = cmd;
		return execute();
	}

	/** 执行命令 */
	public int execute() throws Exception {
		if (Util.isNull(_cmd))
			return 0;

		int retCode = -1;
		Process proc = null;
		try {
//			ProcessBuilder pb = new ProcessBuilder(_cmd);
//			pb.redirectErrorStream(true);
//			proc = pb.start();
			proc = Runtime.getRuntime().exec(_cmd);
			new StreamGobbler(proc.getErrorStream()).start();
			new StreamGobbler(proc.getInputStream()).start();
			retCode = proc.waitFor();
		} catch (Exception e) {
			throw e;
		} finally {
			if (proc != null)
				proc.destroy();
		}

		return retCode;
	}

	public class StreamGobbler extends Thread {

		InputStream is;

		public StreamGobbler(InputStream is) {
			this.is = is;
			setDaemon(true);
		}

		public void run() {
			BufferedReader br = null;

			StringBuilder b = new StringBuilder();
			try {
				br = new BufferedReader(new InputStreamReader(is));
				String line;
				while ((line = br.readLine()) != null) {
					b.append(line).append("\r\n");
				}
				//这里不用打日志，通常没什么有用的信息；
				//log.debug(taskID+",StreamGobbler run() debug:" + b.toString());
			} catch (Exception e) {
				log.error(taskID+",StreamGobbler run() error:"+b, e);
			} finally {
				try {
					if (br != null) {
						br.close();
					}
				} catch (Exception e) {
				}
			}
		}
	}

}

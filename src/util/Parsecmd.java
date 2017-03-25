package util;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import framework.ConstDef;
import framework.SystemConfig;

/**
 * 解析时用到的语句
 * 
 * @author user
 */
public class Parsecmd {

	private HashMap<String, String> movefilemap = new HashMap<String, String>();

	private ArrayList<String> filelist;

	private boolean zipflags;

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public void addfile(String sourfile, String newPath) {
		movefilemap.put(sourfile, newPath);
	}

	public static void main(String[] args) {
		BalySqlloadThread thread = new BalySqlloadThread();
		try {
			thread.runcmd("G:\\data\\test.bat");
		} catch (Exception e1) {
			e1.printStackTrace();
		}

	}

	public void comitmovefiles() {
		if (movefilemap.size() < 1)
			return;
		String[] keys = (String[]) movefilemap.keySet().toArray(new String[0]);
		filelist = new ArrayList<String>();
		for (int i = 0; i < keys.length; i++) {
			try {
				zipflags = (SystemConfig.getInstance().getMRZipFlag() == 1) ? true : false;
				Parsecmd.movefile(keys[i], (String) movefilemap.get(keys[i]), zipflags);
			} catch (Exception e) {
				filelist.add(keys[i]);
				e.printStackTrace();
			}
			File f = new File(keys[i]);
			filelist.add((String) movefilemap.get(keys[i]) + File.separator + f.getName());
		}
	}

	public static boolean movefile(String sourfile, String newPath) {
		return movefile(sourfile, newPath, false);
	}

	public static boolean movefile(String sourfile, String newPath, boolean zflag) {
		Process ldr = null;
		try {
			// 将文件移到新文件里
			String os = System.getProperty("os.name").toLowerCase();
			String cmd = null;
			int nSucceed = 0;
			if (-1 != os.indexOf("windows")) {
				cmd = "cmd /c move " + sourfile + " " + newPath;
			} else {
				if (zflag) {
					cmd = "gzip " + sourfile;
					// 防止文件句柄未释放gzip失败
					Thread.sleep(5000);
					ldr = Runtime.getRuntime().exec(cmd);
					try {
						ldr.waitFor();
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
					log.debug("zflag1=" + zflag + ";" + cmd + ";exitvalue=" + ldr.exitValue());
					if (ldr.exitValue() == 0)
						sourfile += ".gz";
				}
				cmd = "mv " + sourfile + " " + newPath;
				log.debug(cmd);
			}

			ldr = Runtime.getRuntime().exec(cmd);
			try {
				nSucceed = ldr.waitFor();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			if (nSucceed == 0) {
				// 文件原地址
				File oldFile = new File(sourfile);
				// new一个新文件夹
				File fnewpath = new File(newPath);
				// 判断文件夹是否存在
				if (!fnewpath.exists())
					fnewpath.mkdirs();

				// 写入.ack文件，标明移出的文件完整
				String ackName = newPath + File.separator + oldFile.getName() + ".ack";
				oldFile = null;
				File ackFile = new File(ackName);
				if (!ackFile.createNewFile()) {
				}

				log.debug("mv " + sourfile + " to " + newPath + " success");
			}
		} catch (Exception e) {
			log.error(" ", e);
			return false;
		} finally{
			if(ldr != null){
				ldr.destroy();
			}
		}
		return true;
	}

	public static boolean ExecShellCmdByFtp(String strCmdList, Timestamp timestamp) {
		boolean bSuccesed = true;
		Process ldr = null;
		try {
			int sucess = 1;
			Runtime runtime = Runtime.getRuntime();
			
			String[] strCmd = strCmdList.split(";");
			for (int i = 0; i < strCmd.length; i++) {
				if (strCmd[i] != null && !strCmd[i].equals("")) {
					String strSql = ConstDef.ParseFilePath(strCmd[i], timestamp);
					log.debug("执行脚本：" + strSql);
					ldr = runtime.exec(strSql);
					try {
						sucess = ldr.waitFor();
					} catch (InterruptedException e) {
						log.error("InterruptedException", e);
						bSuccesed = false;
					}
					log.debug("执行脚本完成：" + strSql + " exitValue：" + sucess);
					if (sucess != 0)
						bSuccesed = false;
				}
			}

		} catch (Exception e) {
			log.error("执行脚本异常。", e);
			bSuccesed = false;
		}finally{
			if(ldr != null)
				ldr.destroy();
		}
		return bSuccesed;
	}

	public static boolean ExecShellCmdByFtp1(String strCmdList, Timestamp timestamp) {
		boolean bSuccesed = true;
		BalySqlloadThread thread = new BalySqlloadThread();
		try {
			int sucess = 1;

			String[] strCmd = strCmdList.split(";");
			for (int i = 0; i < strCmd.length; i++) {
				if (strCmd[i] != null && !strCmd[i].equals("")) {
					String strSql = ConstDef.ParseFilePath(strCmd[i], timestamp);
					log.debug("执行脚本：" + strSql);
					sucess = thread.runcmd(strSql);
					log.debug("执行脚本完成：" + strSql + " exitValue：" + sucess);
					if (sucess != 0)
						bSuccesed = false;
				}
			}

		} catch (Exception e) {
			log.error("执行脚本异常。", e);
			bSuccesed = false;
		}
		return bSuccesed;
	}

	public ArrayList<String> getFilelist() {
		return filelist;
	}

	public void setFilelist(ArrayList<String> filelist) {
		this.filelist = filelist;
	}

	public boolean isZipflags() {
		return zipflags;
	}

	public void setZipflags(boolean zipflags) {
		this.zipflags = zipflags;
	}

}

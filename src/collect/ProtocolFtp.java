package collect;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import util.LogMgr;
import util.Util;

public class ProtocolFtp {

	private FTPClient FTP;

	private String host;

	private int port;

	private String user;

	private String pwd;

	private int taskid;

	private String taskdate;

	// 定义ftp登陆
	private boolean forceexit;

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public String toString() {
		return host + ":" + port + "@" + user + "/" + pwd;
	}

	public boolean Login(String strHost, int nPort, String strUser, String strPwd) {
		host = strHost;
		port = nPort;
		user = strUser;
		pwd = strPwd;

		boolean bOK = false;
		try {
			if (FTP == null) {
				FTP = new FTPClient();
			} else {
				try {
					FTP.disconnect();
				} catch (Exception e) {
				}
			}

			FTP.connect(strHost, nPort);

			int reply = FTP.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				FTP.disconnect();
				log.error("FTP server refused connection.");
				return false;
			}

			bOK = FTP.login(strUser, strPwd);
			if (bOK) {
				FTP = Util.setFTPClientConfig(FTP, host, port, user, pwd);
				FTP.enterLocalPassiveMode();
				FTP.setControlEncoding("GBK");// 设置编码方式，解决中文乱码
				FTP.setFileType(FTPClient.BINARY_FILE_TYPE);// 设置为二进制传输模式
				FTP.setDataTimeout(3600 * 1000);
				FTP.setDefaultTimeout(3600 * 1000);
			} else {
				log.error("FTP server Login Failure Code:" + FTP.getReplyCode());
			}
		} catch (SocketException se) {
			log.error("FTP login", se);
		} catch (Exception e) {
			log.error("FTP login", e);
		}

		return bOK;
	}

	/**
	 * FTP登陆,如果登陆失败则进行最大次数的重试
	 * 
	 * @param task
	 * @param iSleepTime
	 *            每次重新登陆的间隔时间
	 * @param maxTryTimes
	 *            最大重试次数
	 * @return
	 */
	@SuppressWarnings("static-access")
	public boolean login(CollectObjInfo task, int iSleepTime, byte maxTryTimes) {
		if (task == null)
			return false;

		boolean bOK = false;

		bOK = Login(task.getDevInfo().getIP(), task.getDevPort(), task.getDevInfo().getHostUser(), task.getDevInfo().getHostPwd());

		if (!bOK) {
			String strLog = task.getSysName();

			log.error(strLog + ": FTP登陆失败,尝试重新登陆 ... ");

			byte tryTimes = 0;
			int sleepTime = iSleepTime;
			while (tryTimes < maxTryTimes && !ftpValidate()) {
				try {
					Thread.currentThread().sleep(sleepTime);
				} catch (InterruptedException e) {
					break;
				}

				bOK = Login(task.getDevInfo().getIP(), task.getDevPort(), task.getDevInfo().getHostUser(), task.getDevInfo().getHostPwd());

				tryTimes++;

				if (!bOK) {
					log.error(strLog + ": 尝试重新登陆FTP失败 (" + tryTimes + ") ... ");
				} else
					break;

				sleepTime += sleepTime * 2;
			}

			if (!bOK) {
				log.error(strLog + ": " + tryTimes + "次FTP登陆重试失败.");
			} else {
				log.info(strLog + ": FTP登陆重试成功(" + tryTimes + ").");
			}
		}

		return bOK;
	}

	public void Close() {
		try {
			if (FTP != null) {
				FTP.logout();
				FTP.disconnect();
			}

		} catch (Exception e) {
		}
	}

	// 判断文件是否存在
	/*
	 * public boolean FindFile(String strFile) { boolean isExist = false; try { if((FTP != null) && (FTP.isConnected()) ) { /*InputStream ftpIn =
	 * FTP.retrieveFileStream(strFile); if(ftpIn == null) { return false; } ftpIn.close(); isExist = true;
	 */
	/*
	 * String [] FileName = FTP.listNames(strFile); if(FileName.length == 1) { int nIndex = FileName[0].indexOf("No such file or directory");
	 * if(nIndex>=0) isExist= false; else isExist= true; } else { isExist= false; } return isExist; } } catch(IOException e) {
	 * Log.AddLog(ConstDef.COLLECT_LOGTYPE_ERROR," hava no file "+strFile+" :"+ e.getMessage()); e.printStackTrace(); return false; } catch(Exception
	 * e) { Log.AddLog(ConstDef.COLLECT_LOGTYPE_ERROR," hava no file "+strFile+" :"+ e.getMessage()); e.printStackTrace(); return false; } finally {
	 * //System.out.println("find file finish"); } return isExist; }
	 */

	private boolean ftpValidate() {
		if ((FTP != null) && (FTP.isConnected()))
			return true;
		else
			return false;
	}

	public boolean ReLogin() {
		int i = 1;
		boolean ret = false;

		if (ftpValidate()) {
			return true;
		}

		while (!ftpValidate()) {
			if (i > 3) {
				return ret;
			}

			log.debug(getTaskid() + ": 第" + i + "次ReLogin登陆ftp:" + host + "," + user);

			try {
				Thread.sleep(1000 * i * 30);
			} catch (Exception e) {
				log.error(getTaskid() + ": ftp relogin failed. ", e);
			}

			ret = Login(host, port, user, pwd);
			if (ret)
				break;

			i++;
		}

		return ret;
	}

	class MonitorThread extends Thread {

		FTPClient ftpClient = null;

		int nSeconds = 0;

		int nTaskID = 0;

		Thread ftpThread = null;

		public MonitorThread(int nSeconds, FTPClient ftp, int nTaskID, Thread t) {
			ftpClient = ftp;
			this.nSeconds = nSeconds;
			this.nTaskID = nTaskID;
			this.ftpThread = t;
		}

		public void run() {
			if (nSeconds > 0) {
				try {
					log.debug(getTaskid() + ": sleep 开始");
					Thread.sleep(((long) nSeconds) * 1000);
					log.debug(getTaskid() + ": sleep 结束");
				} catch (InterruptedException e) {
					log.debug(getTaskid() + ": Monitor thread interrupted by ftp thread");
					return;
				}

				try {
					log.debug("Task " + nTaskID + " ftp timeout for " + nSeconds + " seconds, interrupt ftp thread");

					ftpThread.interrupt();
				} catch (Exception e) {
					log.error("Interrupt ftp error", e);
				}
			}
		}
	}

	/** 从服务器下载指定的文件到本地，返回本地文件名称，如果返回为null，则表示下载没成功 */
	public String[] downFile(String strFile, String strLocalPath, String encode) throws Exception {
		if (!checkConnection()) {
			return null;
		}

		String[] retFiles = null;

		try {
			if ((FTP != null) && (FTP.isConnected())) {
				FTP.setControlEncoding("GBK"); // 支持中文文件名

				String strPath = "";

				int nFind = strFile.lastIndexOf('\\');
				if (nFind != -1) {
					strPath = strFile.substring(0, nFind + 1);
				} else {
					nFind = strFile.lastIndexOf('/');
					if (nFind != -1) // 获取文件名称,有可能下载的文件为全路径
						strPath = strFile.substring(0, nFind + 1);
				}

				String[] fileNames = null;
				try {
					fileNames = listNames(strFile, encode);
				} catch (Exception e) {
					throw e;
				}

				if (fileNames == null || fileNames.length == 0) {
					for (int times = 0; times < 3; times++) {
						int delay = (times + 1) * 1500;
						log.error(getTaskid() + ": " + FTP.getRemoteAddress().toString() + ":" + strFile + " 不存在，开始重试，次数:" + (times + 1));
						Thread.sleep(delay);
						FTP.disconnect();
						FTP.connect(host, port);
						FTP.login(user, pwd);
						FTP = Util.setFTPClientConfig(FTP, host, port, user, pwd);
						FTP.setControlEncoding("GBK");// 设置编码方式，解决中文乱码
						FTP.setFileType(FTPClient.BINARY_FILE_TYPE);// 设置为二进制传输模式
						FTP.setDataTimeout(3600 * 1000);
						FTP.setDefaultTimeout(3600 * 1000);
						Thread.sleep(500);
						fileNames = listNames(strFile, encode);
						if (fileNames != null && fileNames.length > 0) {
							break;
						}
					}
					if (fileNames == null || fileNames.length == 0) {
						log.error(getTaskid() + ": " + FTP.getRemoteAddress().toString() + ":" + strFile + " 不存在，已重试3次");
						return null;
					}
				}

				retFiles = new String[fileNames.length];

				Long beg = System.currentTimeMillis();

				for (int i = 0; i < fileNames.length; ++i) {
					String strFileName = fileNames[i];
					nFind = strFileName.lastIndexOf('\\');
					if (nFind != -1) // 获取文件名称,有可能下载的文件为全路径
					{
						strFileName = strFileName.substring(nFind + 1);
					} else {
						nFind = strFileName.lastIndexOf('/');
						if (nFind != -1) // 获取文件名称,有可能下载的文件为全路径
							strFileName = strFileName.substring(nFind + 1);
					}

					File lpath = new File(strLocalPath);

					if (!lpath.exists())
						lpath.mkdir();

					if (downloadOneFile(strFileName, strLocalPath, strPath, encode)) {
						// 本地rename,前提是先关闭文件流
						retFiles[i] = strLocalPath + File.separator + strFileName;
					} else {
						// 删掉本地文件
						if (ReLogin()) {
							if (!downloadOneFile(strFileName, strLocalPath, strPath, encode)) {
								log.error(getTaskid() + ": 下载文件失败" + strFileName);
								log.debug(getTaskid() + " 删掉本地文件: " + fileNames[i]);
								File f = new File(strLocalPath + File.separator + strFileName);
								f.delete();
								retFiles[i] = null;
								// return null;
								throw new Exception("下载文件失败.");
							}
						} else {
							throw new Exception("下载文件失败后重新登陆失败.");
						}
					}
				}

				log.debug(getTaskid() + " : 数据时间=" + this.taskdate + " 文件下载完成; 耗时:" + ((System.currentTimeMillis() - beg) / 1000));
			} // if
		} catch (Exception e) {
			log.error(getTaskid() + ": down file error :" + strFile + " loacl file :" + strLocalPath, e);
			retFiles = null;
			throw e;
		} finally {
			// this.Close();
		}

		return retFiles;
	}

	/** 检查连接,如果连接不正常则重连,最大重连次数为3 */
	private boolean checkConnection() {
		boolean bReturn = true;

		this.forceexit = false;

		if (!ftpValidate()) {
			if (!ReLogin()) {
				this.forceexit = true;
				bReturn = false;
			}
		}

		return bReturn;
	}

	private String[] listNames(String strFile, String encode) throws Exception {
		if (!checkConnection()) {
			return null;
		}

		String[] names = null;
		try {

			names = FTP.listNames(new String(strFile.getBytes(Util.isNotNull(encode) ? encode : "GBK"), "iso-8859-1"));
			if (names == null || names.length == 0) {
				FTPFile[] fs = FTP.listFiles(new String(strFile.getBytes(Util.isNotNull(encode) ? encode : "GBK"), "iso-8859-1"));
				List<String> tmp = new ArrayList<String>();
				for (FTPFile f : fs) {
					if (f.isFile()) {
						tmp.add(f.getName());
					}
				}
				names = tmp.toArray(new String[0]);
			}
		} catch (Exception e) {
			log.error(getTaskid() + " : error when FTP list names. " + strFile, e);

			// listNames失败后重新检查连接以及尝试重新登陆
			if (checkConnection()) {
				try {
					FTP.disconnect();
					Thread.sleep(500);
					FTP.connect(host, port);
					FTP.login(user, pwd);
					FTP = Util.setFTPClientConfig(FTP, host, port, user, pwd);
					FTP.enterLocalPassiveMode();
					FTP.setFileType(FTPClient.BINARY_FILE_TYPE);// 设置为二进制传输模式

					FTP.setDataTimeout(3600 * 1000);
					FTP.setDefaultTimeout(3600 * 1000);
					names = FTP.listNames(new String(strFile.getBytes("GBK"), "iso-8859-1"));
				} catch (Exception e1) {
					log.error(getTaskid() + " : 尝试再次listNames失败.", e1);
					throw e1;
				}
			} else {
				log.error(getTaskid() + " : listNames失败后尝试重连失败.");
				throw e;
			}
		}

		return names;
	}

	/** 下载一个文件 */
	private boolean downloadOneFile(String fileName, String localPath, String path, String encode) {
		boolean result = false;
		long ftpFileLength = -1;
		String fullName = path + fileName;
		String tmp = fullName;
		try {
			fullName = new String(fullName.getBytes(Util.isNull(encode) ? "gbk" : encode), "iso_8859_1");
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}
		try {
			FTPFile[] fs = FTP.listFiles(fullName);
			if (fs.length == 0) {
				for (int times = 0; times < 3; times++) {
					int delay = (times + 1) * 1500;
					Thread.sleep(delay);
					FTP.disconnect();
					Thread.sleep(500);
					FTP.connect(host, port);
					FTP.login(user, pwd);
					FTP = Util.setFTPClientConfig(FTP, host, port, user, pwd);
					FTP.enterLocalPassiveMode();
					FTP.setFileType(FTPClient.BINARY_FILE_TYPE);// 设置为二进制传输模式
					FTP.setDataTimeout(3600 * 1000);
					FTP.setDefaultTimeout(3600 * 1000);
					fs = FTP.listFiles(fullName);
					if (fs.length > 0) {
						break;
					}
				}
			}

			if (fs.length == 0) {
				for (int times = 0; times < 3; times++) {
					int delay = (times + 1) * 1500;
					Thread.sleep(delay);
					FTP.disconnect();
					Thread.sleep(500);
					FTP.connect(host, port);
					FTP.login(user, pwd);
					FTP = Util.setFTPClientConfig(FTP, host, port, user, pwd);
					FTP.setDataTimeout(3600 * 1000);
					FTP.setDefaultTimeout(3600 * 1000);
					fs = FTP.listFiles(fullName);
					if (fs.length > 0) {
						break;
					}
				}
			}

			if (fs.length > 0) {
				for (FTPFile f : fs) {
					if (!f.getName().equals(".") && !f.getName().equals("..")) {
						ftpFileLength = f.getSize();
						log.debug(tmp + "在FTP上的文件大小为：" + ftpFileLength);
					}
				}
			}

			if (fs.length == 0 || ftpFileLength == -1) {
				throw new Exception("未能在FTP上找到文件:" + tmp);
			}
		} catch (Exception e1) {
			log.error("ftp,fileLength=-1");
		}
		File tdFile = null;
		try {
			File dir = new File(localPath);
			if (!dir.exists()) {
				dir.mkdirs();
			}
			tdFile = new File(localPath, fileName + ".td_" + Util.getDateString_yyyyMMddHH(Util.getDate1(taskdate)));
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		if (!tdFile.exists()) {
			try {
				tdFile.createNewFile();
			} catch (IOException e) {
				log.error("临时文件创建失败：" + tdFile.getAbsoluteFile(), e);
				return false;
			}
		} else {
			if (tdFile.length() >= ftpFileLength) {
				log.debug(getTaskid() + " 文件下载成功: " + fileName);
				result = true;
				return result;
			}
		}

		FileOutputStream fos = null;

		MonitorThread monitor = null;
		InputStream in = null;
		try {
			fos = new FileOutputStream(tdFile, true);
			log.debug(getTaskid() + ": 开始下载文件:" + path + fileName);
			// 监视下载过程，防止任务死掉，1小时超时，自动关闭
			monitor = new MonitorThread(3600, FTP, this.getTaskid(), Thread.currentThread());
			monitor.start();
			in = FTP.retrieveFileStream(new String((path + fileName).getBytes(Util.isNull(encode) ? "GBK" : encode), "iso-8859-1"));
			long tdLength = tdFile.length();
			in.skip(tdLength);
			byte[] bytes = new byte[1024];
			int c;
			while ((c = in.read(bytes)) != -1) {
				fos.write(bytes, 0, c);
			}
			result = ((tdFile.length() >= ftpFileLength) || ftpFileLength == -1);

			if (result) {
				fos.flush();
				log.debug(getTaskid() + "-文件下载成功: " + fileName);
			} else {
				log.error(getTaskid() + "-文件未完整下载，文件大小不一致，" + tmp + ":" + ftpFileLength + "," + tdFile.getAbsoluteFile() + ":" + tdFile.length());
			}
		} catch (ClosedByInterruptException e) {
			log.error("任务 " + getTaskid() + " ftp时由于超时被MonitorThread中断.");
			result = false;
		} catch (Exception e) {
			log.error(getTaskid() + ": downloadOneFile: down file error :" + fileName + " loacl file :" + localPath, e);
			result = false;
		} finally {
			if (in != null) {
				try {
					in.close();
					FTP.completePendingCommand();
				} catch (Exception e) {
				}
			}
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					fos = null;
				}
			}

			if (result) {
				File f = new File(localPath, fileName);
				if (f.exists()) {
					f.delete();
				}
				boolean b = tdFile.renameTo(f);
				if (!b) {
					result = false;
					log.error("将" + tdFile.getAbsolutePath() + "重命名为" + f.getAbsolutePath() + "时失败，" + f.getAbsolutePath() + "被占用");
				}
				tdFile.delete();
			}

			if (monitor != null)
				monitor.interrupt();
		}

		return result;
	}

	public boolean isForceexit() {
		return forceexit;
	}

	public void setForceexit(boolean forceexit) {
		this.forceexit = forceexit;
	}

	public int getTaskid() {
		return taskid;
	}

	public void setTaskid(int taskid) {
		this.taskid = taskid;
	}

	public String getTaskdate() {
		return taskdate;
	}

	public void setTaskdate(String taskdate) {
		this.taskdate = taskdate;
	}

	// 单元测试
	public static void main(String[] args) {
		// String logstr = "10.195.184.4,21,oracle,oracle";
		// String remotefile = "/usr/oracle/miniz/";
		// String local = "C:\\TEMP";
		// String loginfo[] = logstr.split(",");
		// ProtocolFtp ftp = new ProtocolFtp();
		// java Collect.ProtocolFtp 10.12.80.3,21,zxt2000,zxt2000
		// /e:/data/20090413/*.gz D:\\ftp
		// ftp.Login(loginfo[0], Integer.parseInt(loginfo[1]), loginfo[2],
		// loginfo[3]);
		// ftp.DownFile(remotefile, local);
		// ftp.DownFile(remotefile, local);
		// ftp.Login( "10.12.80.3", 21, "zxt2000", "zxt2000" );
		// String [] ret = ftp.DownFile( "/e:/data/20090413/*.gz", "D:\\ftp" );
	}
}

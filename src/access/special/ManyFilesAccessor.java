package access.special;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import task.TaskMgr;
import util.LogMgr;
import util.Util;
import collect.FTPConfig;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * <p>
 * 对大数量文件处理。
 * </p>
 * <p>
 * 按常规的方式，是先下载完所有文件，然后逐个解析入库，这样效率太低。现在改为多线程同时下载，同时解析，且每个时间点的文件，都累积到一起入库， 而不是每个文件执行一次入库。
 * </p>
 */
public abstract class ManyFilesAccessor {

	/**
	 * 任务信息对象。
	 */
	protected CollectObjInfo task;

	protected String encode;

	protected String logKey;

	protected FTPClient ftp;

	protected FTPConfig downCfg;

	// 阻塞队列，用于放入已经准备好的文件。文件解析者从此队列中不停的获取文件，进行解析。
	// private BlockingQueue<File> filesQueue;

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final SystemConfig cfg = SystemConfig.getInstance();
	
	// 厂家文件下载到本地后，如果按原始路径存放，文件夹过多。如果把原始路径拼接成长文件名又可能会超过系统对文件名长度的限制
	// 现在统一设置上限为255个字符
	private static final int MAX_FILE_NAME_LEN= 255;

	/**
	 * 构造方法，需指定任务信息。
	 * 
	 * @param task
	 *            任务信息。
	 */
	public ManyFilesAccessor(CollectObjInfo task) {
		super();
		this.task = task;
		this.encode = task.getDevInfo().getEncode();
		long id = task.getTaskID();
		if (task instanceof RegatherObjInfo)
			id = task.getKeyID() - 10000000;
		this.logKey = task.getTaskID() + "-" + id;
		this.logKey = String.format("[%s][%s]", this.logKey, Util.getDateString(task.getLastCollectTime()));
		this.downCfg = FTPConfig.getFTPConfig(task.getTaskID());
		if (downCfg != null) {
			logger.debug(logKey + "注意：此任务将使用ftpConfig.xml中的配置 - " + downCfg);
		}
	}

	/**
	 * 开始处理一个时间点的数据。
	 * 
	 * @return 是否处理成功。
	 */
	public boolean handle() {
		boolean isSuccess = true;
		try {
			try {
				if (!login(2000, 3))
					return false;
			} catch (Exception e1) {
				logger.error(logKey + "FTP登陆异常。", e1);
			}
			logger.debug(logKey + "FTP连接已建立。");
			String collectPath = task.getCollectPath();
			if (Util.isNull(collectPath)) {
				error("collect_path为空");
				return false;
			}
			String[] splited = collectPath.split(";");
			List<String> paths = new ArrayList<String>();
			for (String s : splited) {
				if (Util.isNotNull(s))
					paths.add(ConstDef.ParseFilePath(s.trim(), task.getLastCollectTime()));
			}

			for (String path : paths) {
				FTPFile[] ftpFiles = null;
				try {
					ftpFiles = listFTPFiles(path);
					logger.info(logKey + "listFiles reply:" + ftp.getReplyString());
				} catch (IOException e1) {
					logger.error(logKey + "listFiles异常。" + path, e1);
					return false;
				}

				if (ftpFiles != null) {
					List<FTPFile> tmpList = Arrays.asList(ftpFiles);
					Collections.reverse(tmpList);
					ftpFiles = tmpList.toArray(new FTPFile[0]);
					final List<Thread> parseThreads = new ArrayList<Thread>();
					int timeOut = SystemConfig.getInstance().getFtpSingleFileTimeOut();
					debug("开始下载并处理文件，文件个数 - " + ftpFiles.length);
					for (FTPFile ftpFile : ftpFiles) {
						File tmpFile = null;
						if(timeOut<=0){
							tmpFile = download(path, ftpFile);
						}else{
							tmpFile = download(path, ftpFile, timeOut);
						}
						debug("下载本地地址： - " + (tmpFile == null ? null : tmpFile.getPath()));
						if (tmpFile == null) {
							String remoteDir = FilenameUtils.getFullPath(ftpFile.getName().contains("/") ? ftpFile.getName() : path);
							String fileName = FilenameUtils.getName(ftpFile.getName());
							String downpath = remoteDir + fileName;
							for (int times = 1; times <= 4; times++) {
								debug("尝试重试下载 - " + times + " - " + downpath);

								disconnect();
								try {
									Thread.sleep(5000);
								} catch (InterruptedException e) {
								}
								try {
									login(2000, 2);
								} catch (Exception e) {
									logger.error(logKey + "ftp登陆失败。", e);
								}
								
								if(timeOut<=0){
									tmpFile = download(path, ftpFile);
								}else{
									tmpFile = download(path, ftpFile, timeOut);
								}
								if (tmpFile != null) {
									debug("重试下载成功 - " + times + " - " + downpath);
									break;
								}
								if (times == 4 && tmpFile == null) {
									debug("重试下载失败 - " + downpath);
									TaskMgr.getInstance().newRegather(task, downpath, "下载失败");
								}
							}
						}
						if(null == tmpFile){
							continue;
						}
						final File file = tmpFile;
						Thread parseThread = new Thread(new Runnable() {
							@Override
							public void run() {
								parse(file);
								debug("文件解析完毕 - " + file.getAbsolutePath());
							}
						}, "parse - " + file.getAbsolutePath());
						parseThread.start();
						parseThreads.add(parseThread);
					}
					debug("文件下载完毕等待处理，文件个数 - " + ftpFiles.length);
					for (Thread th : parseThreads) {
						try {
							th.join();
						} catch (InterruptedException e) {
							error("线程被中断 - handle()", e);
							return false;
						}
					}
					debug("文件处理完毕,开始入库");
					parse(null);
					debug("入库完毕");
					parseThreads.clear();
				}
			}
		} finally {
			// 返回之前，必须提交补采任务。
			TaskMgr.getInstance().commitRegather(task, task.getLastCollectTime().getTime());
			disconnect();
			logger.debug(logKey + "FTP连接已断开。");
		}
		return isSuccess;
	}

	/**
	 * 解析一个文件。注意，此方法会被并发调用。如果传入<code>null</code>，表示所有文件已经解析完了。
	 * 
	 * @param file
	 *            需解析的文件。
	 * @return 是否解析成功。
	 */
	protected abstract boolean parse(File file);

	/**
	 * 下载文件到本地磁盘。如果下载失败，则返回<code>null</code>.
	 * 
	 * @param path FTP原始路径。
	 * @param ftpFile list出来的FTP文件信息。
	 * @param timeOut 下载超时时间
	 * @return 本地文件路径。
	 */
	protected File download(final String path, final FTPFile ftpFile,int timeOut) {
		//重载download(String path, FTPFile ftpFile);
		//解决问题：
		//不管是主动还是被动模式，都存在ftp.retrieveFile和ftp.listFiles
		//不返回的情况，
		final ExecutorService exec = Executors.newSingleThreadExecutor();
        Callable<File> call = new Callable<File>() {
            public File call() throws Exception {
            	File file = download( path,  ftpFile);
                return file;
            }
        };
        File file = null;
        try {
            Future<File> future = exec.submit(call);
            file = future.get(timeOut, TimeUnit.SECONDS); // 任务处理超时时间设置
        } catch (TimeoutException ex) {
            error("文件["+ftpFile.getName()+"]下载超时，下载时间为:"+timeOut+" s.",ex);
        } catch (Exception e) {
            error("文件["+ftpFile.getName()+"]下载失败，原因:",e);
        }finally{
        	exec.shutdown();
        }
		return file;
	}
	
	/**
	 * 下载文件到本地磁盘。如果下载失败，则返回<code>null</code>.
	 * 
	 * @param path
	 *            FTP原始路径。
	 * @param ftpFile
	 *            list出来的FTP文件信息。
	 * @return 本地文件路径。
	 */
	@SuppressWarnings("deprecation")
	protected File download(String path, FTPFile ftpFile) {
		String remoteDir = FilenameUtils.getFullPath(ftpFile.getName().contains("/") ? ftpFile.getName() : path);
		String fileName = FilenameUtils.getName(ftpFile.getName());
		// 解码
		// remoteDir = decodePath(remoteDir);
		// fileName = decodePath(fileName);

		String downpath = remoteDir + fileName;
		String localpath = cfg.getCurrentPath() + File.separator + task.getTaskID();

		if(remoteDir.length()+fileName.length()+1>MAX_FILE_NAME_LEN){
			localpath += remoteDir;
		}else{
			fileName = remoteDir.replace("/", "_") + "_" + fileName;
		}
		File localDir = new File(localpath);
		if (!localDir.exists())
			localDir.mkdirs();
		File localFile = null;
		if (Util.isWindows())
			localFile = new File(localDir, fileName);
		else
			localFile = new File(localDir, URLEncoder.encode(fileName));
		FileOutputStream out = null;
		boolean isDownloaded = false;
		try {
			out = new FileOutputStream(localFile);
		} catch (Exception e) {
			error("打开本地文件失败，localFile=" + localFile + "，localDir=" + localDir + "，fileName=" + fileName + "，remoteDir=" + remoteDir + "，ftpFile="
					+ ftpFile, e);
		}
		try {
			if (!ftp.retrieveFile(encodePath(downpath), out))
				error("文件下载失败 - " + downpath);
			else
				isDownloaded = true;
		} catch (Exception e) {
			error("下载文件失败 - " + downpath, e);
			return null;
		} finally {
			try {
				if (out != null)
					out.flush();
			} catch (Exception e) {
			}
			IOUtils.closeQuietly(out);
		}
		// if ( !isDownloaded )
		// TaskMgr.getInstance().newRegather(task, downpath, "下载失败");
		return (!isDownloaded ? null : localFile);
	}

	/**
	 * 在FTP上列出指定路径的文件。如果失败，将返回<code>null</code>.
	 * 
	 * @param path
	 *            FTP文件路径。
	 * @return FTP文件路径。
	 * @throws IOException
	 * @throws SocketException
	 */
	protected FTPFile[] listFTPFiles(String path) throws SocketException, IOException {
		FTPFile[] ftpFiles = null;
		boolean isException = false;
		try {
			ftpFiles = ftp.listFiles(encodePath(path));
		} catch (Exception e) {
			error("listFiles异常 - " + path, e);
			isException = true;
		}

		if (!isFilesNotNull(ftpFiles)) {
			debug(logKey + "重试listFiles - " + path);
			if (isException)
				login(2000, 2);
			try {
				ftpFiles = ftp.listFiles(encodePath(path));
			} catch (Exception e) {
				error("重试listFiles异常 - " + path, e);
			}

			if (isFilesNotNull(ftpFiles)) {
				debug(logKey + "重试listFiles成功 - " + path + " 文件个数 - " + ftpFiles.length);
			} else {
				error(logKey + "重试listFiles失败 - " + path);
				TaskMgr.getInstance().newRegather(task, path, String.format("listFiles失败(ftpFiles%s)", ftpFiles == null ? "为null" : "长度为0"));
				return null;
			}
		} else {
			debug(logKey + "listFiles成功 - " + path + " 文件个数 - " + ftpFiles.length);
		}

		return ftpFiles;
	}

	/**
	 * 登录到FTP.
	 * 
	 * @return 是否登录成功。
	 * @throws IOException
	 * @throws SocketException
	 */
	public boolean login(int sleepTime, int tryTimes) throws SocketException, IOException {
		boolean b = false;
		if (login()) {
			logger.debug(logKey + "ftp登录成功");
			return true;
		}
		int st = sleepTime;
		int tt = tryTimes;
		if (tt > 0) {
			for (int i = 0; i < tt; i++) {
				if (st > 0) {
					logger.debug(logKey + "尝试重新登录，次数:" + (i + 1));
					try {
						Thread.sleep(st);
					} catch (InterruptedException e) {
						logger.error(logKey + "休眠时线程被中断");
					}
					b = login();
					if (b) {
						logger.debug(logKey + "重新登录成功");
						break;
					}
				}
			}
		}
		if (!b) {
			logger.debug(logKey + "重新登录失败");
		}
		return b;
	}

	public boolean login() throws SocketException, IOException {
		ftp = new FTPClient();
		ftp.connect(task.getDevInfo().getIP());
		if (encode != null && encode.length() > 0)
			ftp.setControlEncoding(encode.trim());
		boolean flag = ftp.login(task.getDevInfo().getHostUser().trim(), task.getDevInfo().getHostPwd().trim());
		if (flag) {
			/* ftpConfig.xml中配置了此任务使用PASV模式 */
			if (downCfg != null && downCfg.getTransMode().equalsIgnoreCase(FTPConfig.TRANS_MODE_PASV)) {
				ftp.enterLocalPassiveMode();
				logger.debug(logKey + "进入FTP被动模式。ftp entering passive mode");
			}
			/* ftpConfig.xml中未配置，并且config.xml中没有指定使用port模式。 */
			else if (downCfg == null && !SystemConfig.getInstance().isFtpPortMode()) {
				ftp.enterLocalPassiveMode();
				logger.debug(logKey + "进入FTP被动模式。ftp entering passive mode");
			} else {
				logger.debug(logKey + "进入FTP主动模式。ftp entering local mode");
			}
			try {
				ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
			} catch (IOException e) {
				logger.error(logKey + "设置FileType时异常", e);
			}
		} else {
			String rep = "";
			try {
				rep = ftp.getReplyString().trim();
			} catch (Exception e) {
			}
			logger.warn(logKey + "注意：用户名/密码可能错误，服务器返回信息：" + rep);
		}
		return flag;
	}

	/**
	 * 记录INFO级别日志。
	 * 
	 * @param msg
	 *            日志信息。
	 */
	protected void info(Object msg) {
		logger.info(logKey + msg);
	}

	/**
	 * 记录DEBUG级别日志。
	 * 
	 * @param msg
	 *            日志信息。
	 */
	protected void debug(Object msg) {
		logger.debug(logKey + msg);
	}

	/**
	 * 记录ERROR级别日志。
	 * 
	 * @param msg
	 *            日志信息。
	 * @param ex
	 *            异常信息。
	 */
	protected void error(Object msg, Throwable ex) {
		if (ex == null)
			logger.error(logKey + msg);
		else
			logger.error(logKey + msg, ex);
	}

	/**
	 * 记录ERROR级别日志。
	 * 
	 * @param msg
	 *            日志信息。
	 */
	protected void error(Object msg) {
		error(msg, null);
	}

	/**
	 * 判断listFiles出来的FTP文件，是否不是空的。
	 * 
	 * @param fs
	 *            listFiles出来的FTP文件。
	 * @return listFiles出来的FTP文件，是否不是空的。
	 */
	private boolean isFilesNotNull(FTPFile[] fs) {
		if (fs == null) {
			return false;
		}
		if (fs.length == 0) {
			return false;
		}
		for (FTPFile f : fs) {
			if (f == null) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 编码一条FTP路径
	 * 
	 * @param ftpPath
	 *            FTP路径
	 * @return 编码后的路径
	 */
	private String encodePath(String path) {
		try {
			return Util.isNotNull(encode) ? new String(path.getBytes(encode), "iso_8859_1") : path;
		} catch (UnsupportedEncodingException e) {
			logger.error(logKey + "设置的编码不正确:" + encode, e);
		}
		return path;
	}

	/**
	 * 解码一条FTP路径
	 * 
	 * @param ftpPath
	 *            FTP路径
	 * @return 解码后的路径
	 */
	// private String decodePath(String path) {
	// try {
	// return Util.isNotNull(encode) ? new String(path.getBytes("iso_8859_1"), encode) : path;
	// } catch (UnsupportedEncodingException e) {
	// logger.error(task.getTaskID() + "设置的编码不正确:" + encode, e);
	// }
	// return path;
	// }

	/**
	 * 断开FTP连接
	 */
	public void disconnect() {
		if (ftp != null) {
			try {
				ftp.logout();
				// liangww add 2012-05-23 增加logout时打印ftp返回的信息
				// liangww add 2012-05-23 增加logout时打印ftp返回的信息
				logger.debug(logKey + "【FTP退出信息。】 ftp logout" + ftp.getReplyString() + "  thread:" + Thread.currentThread());
			} catch (Exception e) {
				logger.error(logKey + " 【FTP断开时报错，不影响采集】ftp error logout" + ftp.getReplyString() + "  thread:" + Thread.currentThread(), e);
			}
			try {
				ftp.disconnect();
			} catch (Exception e) {
				logger.error(logKey + "【FTP断开时报错，不影响采集】ftp error disconnect" + ftp.getReplyString() + "  thread:" + Thread.currentThread(), e);
			}
			ftp = null;
		}
	}
}

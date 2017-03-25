package collect;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;

import util.LogMgr;

/**
 * FTP文件上传下载
 * 
 * @author liuwx May 30, 2011
 */
public class FtpUtils {

	protected int timeout = 10; // 默认为10分钟

	protected String ip;

	protected int port;

	protected String user;

	protected String pwd;

	protected FTPClient ftp;

	protected FTPClientConfig ftpCfg;

	protected static final String ISOCHARSET = "iso-8859-1";

	protected String encode;

	protected String keyId;

	protected boolean bPasv = false;

	public Logger LOG = LogMgr.getInstance().getSystemLogger();

	public FtpUtils(String ip, int port, String user, String pwd, String encode, boolean bPasv) {
		keyId = "";
		this.ip = ip;
		this.port = port;
		this.user = user;
		this.pwd = pwd;
		this.encode = encode;// 默认，需改为配置
		this.bPasv = bPasv;
	}

	/**
	 * 连接FTP服务器
	 */
	public void connectServer() {
		if (ftp == null) {
			login();
		}
	}

	/**
	 * 登录到FTP服务器
	 * 
	 * @return 是否成功
	 */
	public boolean login() {
		closeConnect();
		ftp = new FTPClient();// new UwayFTPClient();
		// ftp.setRemoteVerificationEnabled(false);
		ftp.setDataTimeout(timeout * 60 * 1000);
		ftp.setDefaultTimeout(timeout * 60 * 1000);
		// ftp.setControlEncoding(encode);
		boolean b = false;
		try {
			LOG.debug(keyId + "正在连接到 - " + ip + ":" + port);
			ftp.connect(ip, port);
			LOG.debug(keyId + "ftp connected");
			LOG.debug(keyId + "正在进行安全验证 - " + user + " " + pwd);
			b = ftp.login(user, pwd);
			LOG.debug(keyId + "ftp logged in");
		} catch (Exception e) {
			LOG.error(keyId + "登录FTP服务器时异常", e);
		}
		if (b) {
			ftp.enterLocalPassiveMode();
			LOG.debug(keyId + "ftp entering passive mode");
			// if (this.ftpCfg == null) {
			// this.ftpCfg = setFTPClientConfig();
			// } else {
			// ftp.configure(this.ftpCfg);
			// }
			try {
				ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return b;
	}

	/**
	 * 自动设置FTP服务器类型
	 */
	private FTPClientConfig setFTPClientConfig() {
		FTPClientConfig cfg = null;
		try {
			ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_UNIX));
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_NT));
			} else {
				LOG.debug(keyId + "ftp type:UNIX");
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_AS400));
			} else {
				LOG.debug(keyId + "ftp type:NT");
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_L8));
			} else {
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_MVS));
			} else {
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_NETWARE));
			} else {
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_OS2));
			} else {
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_OS400));
			} else {
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_VMS));
			} else {
				return cfg;
			}
			if (!isFilesNotNull(ftp.listFiles("/*"))) {
				ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_UNIX));
			}
		} catch (Exception e) {
			LOG.error("配置FTP客户端时异常", e);
			ftp.configure(cfg = new FTPClientConfig(FTPClientConfig.SYST_UNIX));
		}
		return cfg;
	}

	/**
	 * 下载指定的文件
	 * 
	 * @param ftpPath
	 *            需要下载的文件绝对路径
	 * @param localPath
	 *            放置下载后文件的本地文件夹
	 * @return 下载到的所有文件的本地路径，如果返回null，则表示下载失败
	 */
	public DownStructer downFile(String ftpPath, String localPath) {
		connectServer();
		String aFtpPath = ftpPath;

		/* 主被动模式切换。 */
		if (this.bPasv) {
			this.ftp.enterLocalPassiveMode();
			LOG.debug("使用FTP被动模式。");
		} else {
			this.ftp.enterLocalActiveMode();
			LOG.debug("使用FTP主动模式。");
		}

		/*
		 * /w/zte/!20110209{888}!/00.txt !20110209{888}!中20110209是实际的FTP路径，888是本地的路径。 程序会把/w/zte/!20110209{888}
		 * !/00.txt转化为/w/zte/20110209/00.txt，然后下载到....\w\zte\888/00.txt
		 */
		if (util.Util.isNotNull(ftpPath)) {
			if (ftpPath.contains("!") && ftpPath.contains("{") && ftpPath.contains("}")) {
				int begin = ftpPath.indexOf("!");
				int end = ftpPath.lastIndexOf("!");
				if (begin > -1 && end > -1 && begin < end) {
					String content = ftpPath.substring(begin, end + 1);
					int cBegin = content.indexOf("!");
					int cEnd = content.indexOf("{");
					if (cBegin > -1 && cEnd > -1 && cBegin < cEnd) {
						String dir = content.substring(cBegin + 1, cEnd);
						aFtpPath = aFtpPath.replace(content, dir);
					}
				}
			}
		}

		FTPFile[] ftpFiles = null;
		DownStructer downStruct = new DownStructer();
		try {

			boolean isEx = false;
			try {
				aFtpPath = aFtpPath.replace("\\\\", "/").replace("\\", "/");
				int index = aFtpPath.lastIndexOf("/");

				String dir = "/";
				if (index != 0 && index > 0) {
					dir = aFtpPath.substring(0, index);
				}

				ftp.changeWorkingDirectory(dir);

				String encodeFileName = encodeFTPPath(aFtpPath);
				ftpFiles = ftp.listFiles(encodeFileName);
			} catch (Exception e) {
				LOG.error(keyId + "listFiles失败:" + aFtpPath, e);
				isEx = true;
			}
			if (!isFilesNotNull(ftpFiles)) {
				for (int i = 0; i < 3; i++) {
					int sleepTime = 2000 * (i + 1);
					if (isEx) {
						LOG.debug(keyId + "listFiles异常，断开重连");
						login();
					}
					// login();
					LOG.debug(keyId + "重新尝试listFiles: " + aFtpPath + ",次数:" + (i + 1));
					Thread.sleep(sleepTime);
					try {
						ftpFiles = ftp.listFiles(encodeFTPPath(aFtpPath));
					} catch (Exception e) {
						LOG.error("listFiles失败：" + aFtpPath, e);
					}
					if (isFilesNotNull(ftpFiles)) {
						LOG.debug(keyId + "重试listFiles成功：" + aFtpPath);
						break;
					}
				}
				if (!isFilesNotNull(ftpFiles)) {
					LOG.warn(keyId + "重试3次listFiles失败，不再尝试：" + aFtpPath);
					return downStruct;
				}
			}
			LOG.debug(keyId + "listFiles成功,文件个数:" + ftpFiles.length + " (" + encodeFTPPath(aFtpPath) + ")");
			for (FTPFile f : ftpFiles) {
				if (f.isFile()) {
					String name = decodeFTPPath(f.getName());
					name = name.substring(name.lastIndexOf("/") + 1, name.length());
					String singlePath = aFtpPath.substring(0, aFtpPath.lastIndexOf("/") + 1) + name;
					String fpath = localPath + File.separator + name.replace(":", "");
					boolean b = downSingleFile(singlePath, localPath, name.replace(":", ""), downStruct);
					if (!b) {
						LOG.error(keyId + "下载单个文件时失败:" + singlePath + ",开始重试");
						for (int i = 0; i < 3; i++) {
							int sleepTime = 2000 * (i + 1);
							LOG.debug(keyId + "重试下载:" + singlePath + ",次数:" + (i + 1));
							Thread.sleep(sleepTime);
							login();
							if (downSingleFile(singlePath, localPath, name.replace(":", ""), downStruct)) {
								b = true;
								LOG.debug(keyId + "重试下载成功:" + singlePath);
								break;
							}
						}
						if (!b) {
							LOG.debug(keyId + "重试3次失败:" + singlePath);
							return downStruct;
						} else {
							if (!downStruct.getLocalFail().contains(fpath))
								downStruct.getSuc().add(fpath);
						}
					} else {
						if (!downStruct.getLocalFail().contains(fpath))
							downStruct.getSuc().add(fpath);
					}
				}
			}
		} catch (Exception e) {
			LOG.error(keyId + "下载文件时异常", e);
		}

		return downStruct;
	}

	/**
	 * 解码一条FTP路径
	 * 
	 * @param ftpPath
	 *            FTP路径
	 * @return 解码后的路径
	 */
	private String decodeFTPPath(String ftpPath) {
		try {
			String str = util.Util.isNotNull(encode) ? new String(ftpPath.getBytes("iso_8859_1"), encode) : ftpPath;
			return str;
		} catch (UnsupportedEncodingException e) {
			LOG.error(keyId + "设置的编码不正确:" + encode, e);
		}
		return ftpPath;
	}

	/**
	 * 下载单个文件
	 * 
	 * @param path
	 *            单个文件的FTP绝对路径
	 * @param localPath
	 *            本地文件夹
	 * @param fileName
	 *            文件名
	 * @return 是否下载成功
	 */
	private boolean downSingleFile(String path, String localPath, String fileName, DownStructer downStruct) {
		boolean result = false;
		boolean ex = false;
		LOG.debug(keyId + "开始下载:" + path);
		double downStartTime = System.currentTimeMillis();
		boolean end = true;
		String singlePath = encodeFTPPath(path);
		File tdFile = null;
		InputStream in = null;
		OutputStream out = null;
		long length = getFileSize(path);
		if (length < 0)
			LOG.warn("lenght=" + length);
		double downTime = 0.0;
		long tdLength = 0;
		try {
			File dir = new File(localPath);
			if (!dir.exists()) {
				if (!dir.mkdirs()) {
					throw new Exception(keyId + "创建文件夹时异常:" + dir.getAbsolutePath());
				}
			}
			tdFile = new File(dir, fileName + ".td");
			if (!tdFile.exists()) {
				if (!tdFile.createNewFile()) {
					throw new Exception(keyId + "创建临时文件失败:" + tdFile.getAbsolutePath());
				}
			}
			tdLength = tdFile.length();
			if (tdLength >= length) {
				end = true;
			}
			in = ftp.retrieveFileStream(singlePath);
			if (in != null) {
				if (tdLength > -1) {
					in.skip(tdLength);
				}
				out = new FileOutputStream(tdFile, true);
				byte[] bytes = new byte[1024];
				int c;
				while ((c = in.read(bytes)) != -1) {
					out.write(bytes, 0, c);
				}
				if (tdFile.length() < length) {
					end = false;
					LOG.warn(keyId + tdFile.getAbsoluteFile() + ":文件下载不完整，理论长度:" + length + "，实际下载长度:" + tdFile.length());
				}
				double downEndTime = System.currentTimeMillis();
				downTime = (downEndTime - downStartTime) / 1000;
			}
		} catch (Exception e) {
			ex = true;
			LOG.error(keyId + "下载单个文件时异常:" + path, e);
			result = false;
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
			try {
				ftp.completePendingCommand();
			} catch (IOException e1) {
			}
			if (out != null) {
				try {
					out.flush();
					out.close();
				} catch (IOException e) {
				}
			}
			if (!ex && (end || tdLength < 0)) {
				if (in != null) {
					File f = new File(localPath, fileName);
					if (f.exists()) {
						f.delete();
					}
					boolean bRename = tdFile.renameTo(f);
					if (!bRename) {
						LOG.error(keyId + "将" + tdFile.getAbsolutePath() + "重命名为" + f.getAbsolutePath() + "时失败，" + f.getAbsolutePath() + "被占用");
					} else {
						tdFile.delete();
						LOG.debug(keyId + "下载成功(耗时" + downTime + "秒):" + path + "  本地路径:" + f.getAbsolutePath() + " 文件大小:" + f.length());
						result = true;

						// if (f.length() == 0) {
						// if (!downStruct.getFail().contains(singlePath))
						// downStruct.getFail().add(singlePath);
						// if (downStruct.getLocalFail().contains(
						// f.getAbsolutePath()))
						// downStruct.getLocalFail().add(
						// f.getAbsolutePath());
						// LOG.error(keyId + ": 文件 " + f.getAbsolutePath()
						// + " 长度为0");
						// return false;
						// }

					}
				} else {
					result = false;
				}
			}

		}
		return result;
	}

	/**
	 * 编码一条FTP路径
	 * 
	 * @param ftpPath
	 *            FTP路径
	 * @return 编码后的路径
	 */
	private String encodeFTPPath(String ftpPath) {
		try {
			String str = util.Util.isNotNull(encode) ? new String(ftpPath.getBytes(encode), ISOCHARSET) : ftpPath;// iso_8859_1
			return str;
		} catch (UnsupportedEncodingException e) {
			LOG.error(keyId + "设置的编码不正确:" + encode, e);
		}
		return ftpPath;
	}

	/**
	 * 获取FTP上的一个文件的长度
	 * 
	 * @param path
	 *            FTP文件路径
	 * @return 文件长度，如果为-1，表示未找到
	 */
	private long getFileSize(String path) {
		try {
			FTPFile[] fs = ftp.listFiles(encodeFTPPath(path));
			if (!isFilesNotNull(fs)) {
				return -1;
			}
			for (FTPFile f : fs) {
				String name = f.getName();
				name = name.substring(name.lastIndexOf("/") + 1, name.length());
				if (!name.equals(".") && !name.equals("..")) {
					return f.getSize();
				}
			}
		} catch (Exception e) {
		}
		return -1;
	}

	/**
	 * 判断listFiles出来的FTP文件，是否不是空的
	 * 
	 * @param fs
	 *            listFiles出来的FTP文件
	 * @return listFiles出来的FTP文件，是否不是空的
	 */
	private boolean isFilesNotNull(FTPFile[] fs) {
		return isFileNotNull(fs);
	}

	public static boolean isFileNotNull(FTPFile[] fs) {
		if (fs == null) {
			return false;
		}
		if (fs.length == 0) {
			return false;
		}
		boolean b = false;
		for (FTPFile f : fs) {
			if (f != null && util.Util.isNotNull(f.getName()) && !f.getName().contains("\t") && !f.getName().contains(" ")) {
				return true;
			}

		}
		return b;
	}

	/**
	 * 上传文件
	 * 
	 * @param localFilePath
	 *            本地文件
	 * @param newFileName
	 *            FTP文件
	 * @param path
	 * @throws IOException
	 * @throws UnsupportedEncodingException
	 */
	// public boolean uploadFile(String localFilePath, String newFileName,
	// String path) throws Exception
	// {
	// connectServer();
	// // 上传文件
	// BufferedInputStream buffIn = null;
	// OutputStream out = null;
	// try
	// {
	// buffIn = new BufferedInputStream(new FileInputStream(localFilePath));
	// String tmpFilename = new String((newFileName + ".tmp").getBytes(encode),
	// ISOCHARSET);// 上传到ftp上的临时文件
	// out = ftp.storeFileStream(tmpFilename);
	// if (out == null)
	// {
	// throw new Exception("上传文件时远程FTP服务器目录{"+path+"}不存在或者流建立失败！");
	// }
	// long len = IOUtils.copyLarge(buffIn, out);
	// IOUtils.closeQuietly(buffIn);
	// IOUtils.closeQuietly(out);
	// boolean succ = ftp.completePendingCommand();
	// long localLen = new File(localFilePath).length();
	// if (len == localLen && succ)
	// {
	// // 举例:将文件a.txt.tmp命名为a.txt,目的是为了防止上传到ftp上的文件不完整
	// newFileName = new String((newFileName).getBytes(encode), ISOCHARSET);
	// boolean b = ftp.rename(tmpFilename, newFileName);
	// LOG.debug("FTP://" + ip + "/" + newFileName + " ,重命名状态:" + b);
	// return b;
	// }
	// }
	// finally
	// {
	// IOUtils.closeQuietly(out);
	// IOUtils.closeQuietly(buffIn);
	// }
	// return false;
	// }

	// public boolean uploadFile(String localFilePath, String newFileName,
	// String path) throws Exception {
	// connectServer();
	// // 上传文件
	// BufferedInputStream buffIn = null;
	// OutputStream out = null;
	// try {
	//
	// buffIn = new BufferedInputStream(new FileInputStream(localFilePath));
	// // String tmpFilename = new String(
	// // (newFileName + ".tmp").getBytes(encode), ISOCHARSET);// 上传到ftp上的临时文件
	//
	// //modify
	// String tmpFilename = new String(
	// (newFileName + ".tmp").getBytes(encode), ISOCHARSET);// 上传到ftp上的临时文件
	// //
	// // tmpFilename = new String(
	// // (tmpFilename).getBytes(), "gbk");// 上传到ftp上的临时文件
	//
	//
	//
	// //
	//
	// out = ftp.storeFileStream(tmpFilename);
	// if (out == null) {
	// throw new Exception("上传文件时远程FTP服务器目录{" + path + "}不存在或者流建立失败！");
	// }
	// long len = IOUtils.copyLarge(buffIn, out);
	//
	// IOUtils.closeQuietly(buffIn);
	// IOUtils.closeQuietly(out);
	// boolean succ = ftp.completePendingCommand();
	// long localLen = new File(localFilePath).length();
	// if (len == localLen && succ) {
	// // 举例:将文件a.txt.tmp命名为a.txt,目的是为了防止上传到ftp上的文件不完整
	// // newFileName = new String((newFileName).getBytes(encode),
	// // ISOCHARSET);
	//
	// // if(ftp.listFiles(tmpFilename).length>0)
	// // ftp.deleteFile(tmpFilename);
	// // if(ftp.listFiles(newFileName).length>0)
	// // ftp.deleteFile(newFileName);
	//
	// boolean b = ftp.rename(tmpFilename, newFileName);
	// LOG.debug(" FTP://" + ip + newFileName + " ,重命名状态:" + b);
	// return b;
	// }
	// } finally {
	// IOUtils.closeQuietly(out);
	// IOUtils.closeQuietly(buffIn);
	// }
	// return false;
	// }

	/**
	 * 上传文件
	 * 
	 * @param localFilePath
	 *            本地文件
	 * @param newFileName
	 *            FTP文件
	 * @throws IOException
	 * @throws UnsupportedEncodingException
	 */
	public boolean uploadFile(String localFilePath, String newFileName) throws UnsupportedEncodingException, IOException {
		connectServer();
		// 上传文件
		BufferedInputStream buffIn = null;
		OutputStream out = null;
		String ext = ".tmp";// 上传到ftp上的临时文件
		try {
			newFileName = newFileName.replace("\\", "/");

			String remoteFile = "";

			if (newFileName.contains("/")) {
				remoteFile = newFileName.substring(newFileName.lastIndexOf("/") + 1);

				if (!createDirecroty(newFileName, ftp)) {
					LOG.error("远程 创建目录失败." + "FTP://" + ip + "/" + newFileName);
					return false;
				}
			}

			buffIn = new BufferedInputStream(new FileInputStream(localFilePath));

			out = ftp.storeFileStream(new String((remoteFile + ext).getBytes(encode), ISOCHARSET));
			long len = IOUtils.copyLarge(buffIn, out);
			IOUtils.closeQuietly(buffIn);
			IOUtils.closeQuietly(out);
			boolean succ = ftp.completePendingCommand();
			long localLen = new File(localFilePath).length();
			if (len == localLen && succ) {
				String ftmp = new String((remoteFile).getBytes(encode), ISOCHARSET);
				if (ftp.listFiles(ftmp).length > 0) {

					boolean bFlg = ftp.deleteFile(ftmp);
					LOG.debug("FTP://" + ip + "/" + newFileName + " , 文件已经存在,文件删除状态:  " + bFlg);
				}
				// 举例:将文件a.txt.tmp命名为a.txt,目的是为了防止上传到ftp上的文件不完整
				boolean b = ftp.rename(new String((remoteFile + ext).getBytes(encode), ISOCHARSET), new String((remoteFile).getBytes(encode),
						ISOCHARSET));
				LOG.debug("FTP://" + ip + "/" + newFileName + " ,重命名状态:" + b);
				return b;
			}
		} finally {
			IOUtils.closeQuietly(out);
			IOUtils.closeQuietly(buffIn);
		}
		return false;
	}

	/**
	 * 递归创建远程服务器目录
	 * 
	 * @param remote
	 *            远程服务器文件绝对路径
	 * @param ftpClient
	 *            FTPClient对象
	 * @return 目录创建是否成功
	 * @throws IOException
	 */
	public boolean createDirecroty(String remote, FTPClient ftpClient) throws IOException {
		boolean uploadFlag = true;
		String directory = remote.substring(0, remote.lastIndexOf("/") + 1);
		if (!directory.equalsIgnoreCase("/") && !ftpClient.changeWorkingDirectory(new String(directory.getBytes(encode), "iso-8859-1"))) {
			// 如果远程目录不存在，则递归创建远程服务器目录
			int start = 0;
			int end = 0;
			if (directory.startsWith("/")) {
				start = 1;
			} else {
				start = 0;
			}
			end = directory.indexOf("/", start);
			while (true) {
				String subDirectory = new String(remote.substring(start, end).getBytes(encode), "iso-8859-1");
				if (!ftpClient.changeWorkingDirectory(subDirectory)) {
					if (ftpClient.makeDirectory(subDirectory)) {
						ftpClient.changeWorkingDirectory(subDirectory);
					} else {
						System.out.println("创建目录失败");
						return uploadFlag;
					}
				}

				start = end + 1;
				end = directory.indexOf("/", start);

				// 检查所有目录是否创建完毕
				if (end <= start) {
					break;
				}
			}
		}
		return uploadFlag;
	}

	private boolean appendFile(String localFilePath, String tmpFilename, long localLen) throws Exception {
		boolean retVal = false;
		RandomAccessFile raf = null;
		OutputStream out = null;

		try {
			FTPFile[] files = ftp.listFiles(tmpFilename);
			if (files.length == 1) {
				long remoteSize = files[0].getSize();
				if (localLen == remoteSize) {
					retVal = true;
				} else if (localLen > remoteSize) {
					raf = new RandomAccessFile(new File(localFilePath), "r");
					raf.seek(remoteSize);

					out = ftp.appendFileStream(tmpFilename);
					ftp.setRestartOffset(remoteSize);

					byte[] bytes = new byte[1024 * 10];
					int c;
					while ((c = raf.read(bytes)) != -1) {
						out.write(bytes, 0, c);
					}

					retVal = true;
				}
			}
		} catch (Exception e) {
			LOG.debug(localFilePath + "断点续传发生异常", e);
		} finally {
			IOUtils.closeQuietly(out);
			if (raf != null) {
				raf.close();
			}
		}

		return retVal;
	}

	/**
	 * 断开FTP连接
	 */
	public void closeConnect() {
		try {
			if (ftp != null) {
				ftp.logout();
				ftp.disconnect();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		ftp = null;
	}

	public static void main(String[] args) throws Exception, IOException {
		FtpUtils u = new FtpUtils("132.228.39.154", 21, "ftpuser", "Js!QAZ7", "gbk", false);
		u.downFile("/InterfaceFiles/AH201211220002/调整详情_1.txt", "c:/");//
		u.uploadFile("D:\\SoftWare\\上传.txt", "/InterfaceFiles/AH201211220002/上传.txt");

		// String ftpPath=new String("调整说明".getBytes("gbk"));
		// String tt=u.encodeFTPPath(ftpPath);
		// System.out.println(tt);
		// String n=u.decodeFTPPath(tt);
		// System.out.println(n);
		// FTPClient f=new FTPClient();
		// f.connect("132.228.39.154");
		// f.login("ftpuser", "Js!QAZ7");
		// FTPFile
		// []fs=f.listFiles("/InterfaceFiles/AH201211220002/AR_AH201211220002_testOk_1.txt")
		// ;
		//
		// System.out.println(fs[0].getName());
		// AR_AH201211220002_testOk_1
	}

}

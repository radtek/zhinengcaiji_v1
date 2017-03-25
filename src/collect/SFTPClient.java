package collect;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

public class SFTPClient {

	protected String host;

	protected int port;

	protected String user;

	protected String pass;

	protected String charset;

	protected Session session;

	protected ChannelSftp channel;

	protected String currSftpDirPath;

	private static final Logger LOGGER = LoggerFactory.getLogger(SFTPClient.class);
	
//	public static void main(String[] args) throws Exception {
//		SFTPClient sftp = new SFTPClient("192.168.15.64", 22, "test", "test", "UTF-8");
//		sftp.connectServer();
//		sftp.listSFTPFile("/w/");
//	}

	/**
	 * List出来的文件对象 @ 2014-8-19
	 */
	public static class SFTPFileEntry {

		public String fileName;

		public boolean isDirectory;

		public long fileSize;
	}

	private static class SFTPFileComparator implements Comparator<SFTPFileEntry> {

		@Override
		public int compare(SFTPFileEntry o1, SFTPFileEntry o2) {
			if (o1 == null && o2 == null)
				return 0;
			if (o1 == null)
				return -1;
			if (o2 == null)
				return 1;
			String name1 = (o1.fileName != null ? o1.fileName : "");
			String name2 = (o2.fileName != null ? o2.fileName : "");
			return name1.compareTo(name2);
		}
	}

	private static SFTPFileComparator sftpfileComparator = new SFTPFileComparator();

	public SFTPClient(String host, int port, String user, String pass, String charset) {
		this.host = host;
		this.port = port;
		this.user = user;
		this.pass = pass;
		this.charset = charset;
	}

	/**
	 * 连接到服务器
	 * 
	 * @return
	 */
	public boolean connectServer() {
		try {
			Properties sshConfig = new Properties();
			sshConfig.put("StrictHostKeyChecking", "no");

			LOGGER.debug("开始尝试连接SFTP服务器. {}@{}:{} password:{} ...", new Object[]{user, host, port, pass});

			JSch jsch = new JSch();
			this.session = jsch.getSession(user, host, port);
			this.session.setPassword(pass);
			this.session.setConfig(sshConfig);
			this.session.connect();

			this.channel = (ChannelSftp) this.session.openChannel("sftp");
			this.channel.connect();
			this.channel.setFilenameEncoding(this.charset);

			if (this.channel.isConnected()) {
				LOGGER.debug("成功登陆到SFTP服务器. {}@{}::{}", new Object[]{user, host, port});
				return true;
			}
		} catch (Exception e) {
			LOGGER.error("连接SFTP服务器失败.", e);
		}

		return false;
	}

	public boolean isAvaliable() {
		if (this.session != null && this.channel != null) {
			return this.session.isConnected() && this.channel.isConnected();
		}

		return false;
	}

	/**
	 * 将client归还到连接池中，之前调用工作;
	 */
	public void beforeReturnPool() {
		this.currSftpDirPath = null;
	}

	/**
	 * 关闭client
	 */
	public void close() {
		this.currSftpDirPath = null;
		if (this.channel != null && this.channel.isConnected()) {
			this.channel.disconnect();
			this.channel = null;
		}

		if (this.session != null && this.session.isConnected()) {
			this.session.disconnect();
			this.session = null;
		}
	}

	public void changeDir(String dir) throws Exception {
		if (!isAvaliable()) {
			throw new Exception("SFTP连接已失效.");
		}

		// 如果文件目录改变了，则进入工作目录，避免每下一个文件，重复执行cd命令
		if (dir != null && !dir.equals(this.currSftpDirPath)) {
			this.channel.cd(dir);
			this.currSftpDirPath = dir;
		}
	}

	/**
	 * 下载指定文件，到目标文件
	 * 
	 * @param sftpPathFile
	 *            服务器上的原文件
	 * @param targetFile
	 *            目录文件
	 * @throws Exception
	 */
	public void downRemoteFile(String sftpPathFile, String targetFile) throws Exception {
		if (!isAvaliable()) {
			throw new Exception("SFTP连接已失效.");
		}

		String pathfile = sftpPathFile.replace("\\", "/");
		String filePath = null;
		String fileName = pathfile;
		int nLastPathSpliterPos = pathfile.lastIndexOf('/');
		if (nLastPathSpliterPos >= 0) {
			filePath = pathfile.substring(0, nLastPathSpliterPos);
			fileName = pathfile.substring(nLastPathSpliterPos + 1);
		}

		changeDir(filePath);

		File file = new File(targetFile);
		if (!file.exists()) {
			file.createNewFile();
		}
		FileOutputStream os = new FileOutputStream(file);
		boolean bOnErrOccured = false;
		try {
			this.channel.get(fileName, os);
		} catch (Exception e) {
			bOnErrOccured = true;
			throw e;
		} finally {
			IOUtils.closeQuietly(os);
			// 下载出错，将本地文件删除掉.
			if (bOnErrOccured) {
				file.deleteOnExit();
			}
		}
	}

	/**
	 * 获取SFTP指定路径下的目录
	 * 
	 * @param sftpPath
	 * @return
	 * @throws SftpException
	 */
	@SuppressWarnings("unchecked")
	public List<SFTPFileEntry> listSFTPDirectory(String sftpPath) throws Exception {
		if (!isAvaliable()) {
			throw new Exception("SFTP连接已失效.");
		}

		List<SFTPFileEntry> files = new ArrayList<SFTPFileEntry>();
		String parentPath = sftpPath;
		if (!sftpPath.endsWith("/"))
			parentPath = FilenameUtils.getFullPath(sftpPath);

		Vector<ChannelSftp.LsEntry> filesEntry = new Vector<ChannelSftp.LsEntry>();
		for (int n = 0; n < 3; n++) {
			try {
				filesEntry = this.channel.ls(sftpPath);
				break;
			} catch (Exception e) {
				if (n == 2)
					break;
				this.close();
				Thread.sleep(1000);
				LOGGER.debug("sftp连接出现异常，重新获取连接，第" + (n + 1) + "次尝试", e.getMessage());
				this.connectServer();
			}
		}
		for (ChannelSftp.LsEntry entry : filesEntry) {
			if (entry.getFilename().equals(".") || entry.getFilename().equals(".."))
				continue;

			SftpATTRS fileAttribute = entry.getAttrs();
			if (fileAttribute.isDir()) {
				SFTPFileEntry sftpFileEntry = new SFTPFileEntry();
				sftpFileEntry.fileName = parentPath + entry.getFilename();
				sftpFileEntry.fileSize = fileAttribute.getSize();
				sftpFileEntry.isDirectory = fileAttribute.isDir();
				files.add(sftpFileEntry);
			}
		}

		return files;
	}

	/**
	 * 获取SFTP指定路长下的所有文件
	 * 
	 * @param sftpPath
	 * @return
	 * @throws SftpException
	 */
	@SuppressWarnings("unchecked")
	public List<SFTPFileEntry> listSFTPFile(String path) throws Exception {
		if (!isAvaliable()) {
			throw new Exception("SFTP连接已失效.");
		}

		List<SFTPFileEntry> files = new ArrayList<SFTPFileEntry>();
		// 获取文件的全路径(不包含文件名)
		String parentPath = FilenameUtils.getFullPath(path);
		// 目录无通配符的情况，直接返回。
		if (!parentPath.contains("*") && !parentPath.contains("?")) {
			Vector<ChannelSftp.LsEntry> filesEntry = new Vector<ChannelSftp.LsEntry>();
			for (int n = 0; n < 3; n++) {
				try {
					filesEntry = this.channel.ls(path);
					break;
				} catch (Exception e) {
					if (n == 2)
						break;
					this.close();
					Thread.sleep(1000);
					LOGGER.debug("sftp连接出现异常，重新获取连接，第" + (n + 1) + "次尝试", e.getMessage());
					this.connectServer();
				}
			}
			for (ChannelSftp.LsEntry entry : filesEntry) {
				SftpATTRS fileAttribute = entry.getAttrs();
				if (!fileAttribute.isDir()) {
					SFTPFileEntry sftpFileEntry = new SFTPFileEntry();
					sftpFileEntry.fileName = parentPath + entry.getFilename();
					sftpFileEntry.fileSize = fileAttribute.getSize();
					sftpFileEntry.isDirectory = fileAttribute.isDir();
					files.add(sftpFileEntry);
				}
			}

			return files;
		}

		String fileExt = path.substring(parentPath.length());
		List<String> parsedDirs = extractDirPath(parentPath);
		LOGGER.debug("本次一共搜索到{}个符合通匹符规则:\"{}\"的子文件夹，igp将开始逐一在这些子文件夹下，查找符合规则的所有文件。 fileExt=\"{}\"", new Object[]{parsedDirs.size(), parentPath,
				fileExt});

		for (int i = 0; i < parsedDirs.size(); i++) {
			String subPath = parsedDirs.get(i);
			if (!subPath.endsWith("/"))
				subPath += "/";
			subPath += fileExt;

			List<SFTPFileEntry> tmp = listSFTPFile(subPath);
			if (tmp != null)
				files.addAll(tmp);
		}

		Collections.sort(files, sftpfileComparator);
		return files;
	}

	/**
	 * 根据采集路径，在FTP服务器上遍历出所有文件路径
	 * 
	 * @param gatherPath
	 * @param ftp
	 * @return 返回本地编码的文件路径列表，下载的时候需要encode成ftp编码
	 */
	public List<String> extractDirPath(String gatherPath) {
		gatherPath = gatherPath.replaceAll("[\\\\]", "/");
		if (!gatherPath.startsWith("/")) {
			gatherPath = "/" + gatherPath;
		}
		if (!gatherPath.endsWith("/")) {
			gatherPath = gatherPath.substring(0, gatherPath.lastIndexOf('/') + 1);
		}

		List<String> dirList = new ArrayList<String>();
		if (!gatherPath.contains("*")) {
			try {
				this.changeDir(gatherPath);
				LOGGER.debug(">>扫描到符合通匹符规则的子文件夹：{}", gatherPath);
				dirList.add(gatherPath);
			} catch (Exception e) {
			}

			return dirList;
		}

		String[] splitPath = gatherPath.split("/");
		String path = "";

		// 循环从第2个开始，因为第一个是/,
		for (int i = 1; i < splitPath.length; ++i) {
			String subDir = splitPath[i];
			if (subDir == null || subDir.trim().isEmpty()) {
				continue;
			}

			// 有通配符
			if (subDir.contains("*")) {
				boolean bNeedMatch = false;
				String encodeDir = path + "/";
				if (!subDir.equals("*")) {
					bNeedMatch = true;
				}

				// log.debug("开始在FTP路径：\"{}\" 下查找子件夹...", encodeDir);
				// encodeDir = StringUtil.encodeFTPPath(encodeDir, charset);
				List<SFTPFileEntry> ftpDirList = null;
				try {
					ftpDirList = listSFTPDirectory(encodeDir);
				} catch (Exception e) {
					// 异常时，返回null，告知调用者listFiles失败，有可能是网络原因，可重试。
					LOGGER.warn("SFTP listDirectories时发生异常。", e);
					return dirList;
				}
				for (SFTPFileEntry ftpDir : ftpDirList) {
					String subPath = ftpDir.fileName;
					for (int j = i + 1; j < splitPath.length; ++j) {
						subPath += "/";
						subPath += splitPath[j];
					}
					subPath += "/";

					if (!bNeedMatch || wildcardMatch(subPath, subDir)) {
						dirList.addAll(extractDirPath(subPath));
					}
				}
				// 碰到有通匹符的，直接通过递归将扫描到的目录加入到tempDirList中，后面的目录就不需要再去管了，由递归去完成。
				return dirList;
			} else {
				path += "/" + subDir;
			}
		}

		// 实际应用中，理论上代码不会执行到此处
		if (path.length() < 1)
			path = "/";

		try {
			this.changeDir(path);
			LOGGER.debug(">>扫描到符合通匹符规则的子文件夹：{}", path);
			dirList.add(path);
		} catch (Exception e) {
		}

		return dirList;
	}

	public static boolean wildcardMatch(String fileName, final String parten) {
		if (FilenameUtils.wildcardMatch(fileName, parten))
			return true;

		return false;
	}

}

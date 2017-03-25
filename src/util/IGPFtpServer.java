package util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.ftplet.FtpSession;
import org.apache.ftpserver.ftplet.Ftplet;
import org.apache.ftpserver.ftplet.FtpletContext;
import org.apache.ftpserver.ftplet.FtpletResult;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.ClearTextPasswordEncryptor;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.log4j.Logger;

import framework.SystemConfig;

public class IGPFtpServer {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	private static final String key = "[FTP服务]";

	private static IGPFtpServer _instance;

	private static final SystemConfig cfg = SystemConfig.getInstance();

	private String username;

	private String password;

	private String propPrefix;

	private String dir;

	private boolean canWrite;

	private FtpServer server;

	private int port = 0;

	private String serverAddress = null;

	private IGPFtpServer() {
		super();
		this.username = cfg.getFtpServerUsername();
		this.password = cfg.getFtpServerPassword();
		this.propPrefix = "ftpserver.user." + this.username + ".";
		this.dir = cfg.getFtpServerRootDir();
		this.canWrite = cfg.isFtpServerCanWrite();
	}

	public synchronized static IGPFtpServer getInstance() {
		if (_instance == null)
			_instance = new IGPFtpServer();
		return _instance;
	}

	public synchronized void stopServer() {
		if (this.server == null)
			return;
		if (this.server.isStopped())
			return;
		this.server.stop();
		this.server = null;
	}

	public synchronized boolean startServer() {
		if (!cfg.isEnabelFtpServer())
			return false;
		if (this.server != null && !this.server.isStopped())
			return true;
		File prop = new File(cfg.getCurrentPath() + File.separator + "ftpserver.ini");
		Properties p = new Properties();
		p.put(propPrefix + "userpassword", password);
		p.put(propPrefix + "homedirectory", dir);
		p.put(propPrefix + "writepermission", String.valueOf(canWrite));
		OutputStream out = null;
		try {
			out = new FileOutputStream(prop);
			p.store(out, "");
			out.flush();
		} catch (Exception e) {
			log.warn(key + "FTP服务初始化失败。", e);
			return false;
		} finally {
			IOUtils.closeQuietly(out);
		}

		FtpServerFactory serverFactory = new FtpServerFactory();
		ListenerFactory lisFactory = new ListenerFactory();
		lisFactory.setPort(21);
		serverFactory.addListener("default", lisFactory.createListener());
		PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
		userManagerFactory.setPasswordEncryptor(new ClearTextPasswordEncryptor());
		userManagerFactory.setFile(prop);
		Map<String, Ftplet> ftplets = new LinkedHashMap<String, Ftplet>();
		ftplets.put(FtpletNotification.class.getName(), new FtpletNotification());
		serverFactory.setFtplets(ftplets);
		serverFactory.setUserManager(userManagerFactory.createUserManager());
		this.server = serverFactory.createServer();

		try {
			this.server.start();
			port = serverFactory.getListener("default").getPort();
			serverAddress = serverFactory.getListener("default").getServerAddress();
			log.info("FTP服务已启动，端口：" + port);
		} catch (Exception e) {
			log.warn(key + "启动失败。", e);
		}
		return true;
	}

	static class FtpletNotification implements Ftplet {

		@Override
		public void init(FtpletContext ftpletContext) throws FtpException {
		}

		@Override
		public void destroy() {
		}

		@Override
		public FtpletResult beforeCommand(FtpSession session, FtpRequest request) throws FtpException, IOException {
			return null;
		}

		@Override
		public FtpletResult afterCommand(FtpSession session, FtpRequest request, FtpReply reply) throws FtpException, IOException {
			return null;
		}

		@Override
		public FtpletResult onConnect(FtpSession session) throws FtpException, IOException {
			log.debug(key + "收到一个FTP连接：" + session.getClientAddress());
			return null;
		}

		@Override
		public FtpletResult onDisconnect(FtpSession session) throws FtpException, IOException {
			log.debug(key + "一个FTP连接已断开：" + session.getClientAddress());
			return null;
		}
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public String getDir() {
		return dir;
	}

	public boolean isCanWrite() {
		return canWrite;
	}

	public FtpServer getServer() {
		return server;
	}

	public int getPort() {
		return port;
	}

	public String getServerAddress() {
		return serverAddress;
	}

	public static void main(String[] args) {
		IGPFtpServer.getInstance().startServer();
	}

}

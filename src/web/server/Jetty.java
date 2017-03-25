package web.server;

import java.util.Map;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

/**
 * Jetty Http服务器实现
 * 
 * @author ltp
 * @since 1.0
 */
public class Jetty implements HttpServer {

	private Server server;

	private int port;

	private static final int DEFAULT_PORT = 8080; // 默认端口8080

	private String contextPath;

	private String webApp;

	private boolean uninitialized = true;

	public Jetty(int port) {
		this.port = port;
	}

	public Jetty() {
		this(DEFAULT_PORT);
	}

	public Jetty(int port, String contextPath, String webApp) {
		this.port = port;
		this.contextPath = contextPath;
		this.webApp = webApp;
	}

	@Override
	public boolean restart() throws Exception {
		if (uninitialized) {
			return false;
		}

		stop();
		server.start();

		return server.isRunning();
	}

	@Override
	public boolean start() throws Exception {
		if (uninitialized) {
			return false;
		}

		if (!server.isStarted()) {
			if (!server.isStarting()) {
				server.start();
			}
		}

		return server.isRunning();

	}

	@Override
	public boolean stop() throws Exception {
		if (uninitialized) {
			return false;
		}

		if (!server.isStopped()) {
			if (!server.isStopping()) {
				server.stop();
			}
		}

		return server.isStarted();
	}

	public String getContextPath() {
		return contextPath;
	}

	public void setContextPath(String contextPath) {
		this.contextPath = contextPath;
	}

	public String getWebApp() {
		return webApp;
	}

	public void setWebApp(String webApp) {
		this.webApp = webApp;
	}

	/** 初始化服务器 */
	@Override
	public void init(Map<String, String> params) {
		port = Integer.parseInt(params.get("port"));
		contextPath = params.get("contextPath");
		webApp = params.get("webApp");

		server = new Server(port);
		// 设置web.xml的路径
		WebAppContext context = new WebAppContext(webApp, contextPath);
		server.addHandler(context);

		uninitialized = false;

	}

}

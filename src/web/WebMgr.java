package web;

import java.util.HashMap;
import java.util.Map;

import web.server.HttpServer;
import framework.SystemConfig;

/**
 * Web管理器
 * 
 * @author ltp
 * @since 1.0
 */
public class WebMgr {

	private HttpServer server;

	private static WebMgr instance = null;

	private WebMgr() {
		try {
			SystemConfig sysConf = SystemConfig.getInstance();
			int port = sysConf.getWebPort();
			String contextPath = sysConf.getWebContextPath();
			String webApp = sysConf.getWebApp();
			String serverClass = sysConf.getWebServerClass();

			// 设置启动参数
			Map<String, String> params = new HashMap<String, String>();
			params.put("port", String.valueOf(port));
			params.put("contextPath", contextPath);
			params.put("webApp", webApp);

			server = (HttpServer) Class.forName(serverClass).newInstance();
			server.init(params);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static synchronized WebMgr getInstance() {
		return instance == null ? instance = new WebMgr() : instance;
	}

	/** 启动Http Server */
	public boolean startServer() throws Exception {
		if (!SystemConfig.getInstance().isEnableWeb())
			return false;

		return server.start();
	}

	/** 停止Http Server */
	public boolean stopServer() throws Exception {
		return server.stop();
	}

	/** 重启Http Server */
	public boolean restartServer() throws Exception {
		if (!SystemConfig.getInstance().isEnableWeb())
			return false;

		return server.restart();
	}

	public static void main(String[] args) {
		try {
			WebMgr.getInstance().startServer();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

package console;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import util.LogMgr;
import framework.SystemConfig;

/**
 * 控制台模块
 * 
 * @author YangJian
 * @since 3.1
 */
public class Console {

	private ServerSocket serverSocket;

	private static Console instance = null;

	private boolean runFlag = true;

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private RequestAccept accept;

	private ExecutorService executor;

	private int maxClientCount; // 最大客户端个数

	private Console() {
		super();
	}

	public static synchronized Console getInstance() {
		if (instance == null)
			instance = new Console();

		return instance;
	}

	public void start() throws IOException {
		if (serverSocket == null) {
			int port = SystemConfig.getInstance().getCollectPort();
			serverSocket = new ServerSocket(port);
			listen();
		}
	}

	private synchronized boolean isRun() {
		return this.runFlag;
	}

	private void listen() {
		if (serverSocket == null)
			return;
		try {
			logger.info("开始侦听控制台命令，请使用telnet登录到:" + InetAddress.getByName(null).getHostAddress() + " " + serverSocket.getLocalPort());
		} catch (UnknownHostException e) {
		}
		int maxClientCount = Runtime.getRuntime().availableProcessors() + 1;
		executor = Executors.newFixedThreadPool(maxClientCount);

		accept = new RequestAccept();
		accept.start();
	}

	public void stop() {
		if (executor != null) {
			// 拒绝传入任务
			executor.shutdown();
			for (Socket s : RequestHandler.SOCKETS) {
				if (s != null) {
					try {
						s.close();
					} catch (Exception e) {
					}
				}
			}
			executor.shutdownNow(); // 取消所有遗留的任务
		}
	}

	/**
	 * 客户端请求接收器
	 * 
	 * @author YangJian
	 * @since 3.1
	 */
	class RequestAccept extends Thread {

		@Override
		public void run() {
			while (isRun()) {
				try {
					Socket s = serverSocket.accept();
					RequestHandler reqHandler = new RequestHandler(s);
					executor.execute(reqHandler);
				} catch (IOException e) {
					logger.error("控制台异常,原因:", e);
				}
			}
		}
	}

	// 单元测试
	public static void main(String[] args) {
		try {
			Console.getInstance().start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int getMaxClientCount() {
		return maxClientCount;
	}
}

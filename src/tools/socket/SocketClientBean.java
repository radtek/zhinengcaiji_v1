package tools.socket;

import java.net.Socket;

/**
 * @author yuy socket客户端实体封装类
 */
public class SocketClientBean {

	private String ip;

	private int port;

	private int timeout = 60 * 1000;

	private Socket socketClientConn;

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public Socket getSocketClientConn() {
		return socketClientConn;
	}

	public void setSocketClientConn(Socket socketClientConn) {
		this.socketClientConn = socketClientConn;
	}

}

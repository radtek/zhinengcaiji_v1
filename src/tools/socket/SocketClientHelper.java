package tools.socket;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import util.LogMgr;
import util.Util;

/**
 * @author yuy socket客户端帮助类
 */
public class SocketClientHelper {

	protected Logger LOGGER = LogMgr.getInstance().getSystemLogger();

	public static SocketClientHelper socketClientHelper = new SocketClientHelper();

	public static String charset = "UTF-8";

	public static String endSign = "\n";

	public static String splitSign = ",";

	public static synchronized SocketClientHelper getInstance() {
		if (socketClientHelper == null)
			socketClientHelper = new SocketClientHelper();
		return socketClientHelper;
	}

	/**
	 * 获取连接
	 * 
	 * @param scBean
	 */
	public void getSocketClientConn(SocketClientBean scBean) {
		try {
			Socket client = new Socket(scBean.getIp(), scBean.getPort());
			client.setSoTimeout(scBean.getTimeout());
			scBean.setSocketClientConn(client);
			LOGGER.info("getSocketClientConn(ip:" + scBean.getIp() + ",port=" + scBean.getPort() + ")连接成功");
		} catch (UnknownHostException e) {
			LOGGER.error("ip地址" + scBean.getIp() + "不正确", e);
		} catch (IOException e) {
			LOGGER.error("连接" + scBean.getIp() + "出现异常", e);
		}
	}

	/**
	 * 发送消息
	 * 
	 * @param client
	 * @param message
	 * @throws IOException
	 */
	public void sendMessage(Socket client, String message) throws IOException {
		if (client == null)
			return;
		if (Util.isNull(message))
			return;
		OutputStream out = null;
		out = client.getOutputStream();
		PrintStream ps = new PrintStream(out, false, charset);
		// 发送
		ps.write(message.getBytes());
		ps.flush();
	}

	/**
	 * 关闭连接
	 * 
	 * @param scBean
	 */
	public void close(SocketClientBean scBean) {
		if (scBean.getSocketClientConn() == null)
			return;
		try {
			scBean.getSocketClientConn().close();
		} catch (IOException e) {
			LOGGER.error("close socket(" + scBean.getIp() + ")出现异常", e);
		}
	}

	/**
	 * 处理消息（获取连接，发送，关闭）
	 * 
	 * @param bean
	 * @param message
	 */
	public void handleMessage(SocketClientBean bean, String message) {
		// 绑定连接
		this.getSocketClientConn(bean);
		try {
			// send
			this.sendMessage(bean.getSocketClientConn(), message);
			LOGGER.info("sendMessage(ip:" + bean.getIp() + ",port=" + bean.getPort() + ")发送成功");
		} catch (IOException e) {
			LOGGER.error("sendMessage(ip:" + bean.getIp() + ",port=" + bean.getPort() + ")出现异常，发送失败", e);
		} finally {
			// close
			this.close(bean);
		}
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		SocketClientBean bean = new SocketClientBean();
		bean.setIp("192.168.15.130");
		bean.setPort(2021);
		// bean.setTimeout(5 * 1000);
		SocketClientHelper helper = SocketClientHelper.getInstance();
		helper.getSocketClientConn(bean);
		helper.sendMessage(bean.getSocketClientConn(), "hello,地球!~~");
		helper.close(bean);
	}

}

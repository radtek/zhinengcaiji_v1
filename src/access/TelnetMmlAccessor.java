package access;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.telnet.TelnetClient;
import org.apache.log4j.Logger;

import parser.AbstractStreamParser;
import parser.Parser;
import util.LogMgr;
import framework.Factory;

public class TelnetMmlAccessor extends AbstractAccessor {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	private static final int timeout = 5;// 默认超时时间5分钟

	TelnetClient telnetClient = null;

	InputStream in = null;

	PrintStream out = null;

	public TelnetMmlAccessor() {
		super();
	}

	public boolean doBeforeAccess() throws Exception {
		super.doBeforeAccess();
		String strHostIP = taskInfo.getDevInfo().getIP();
		int nHostPort = taskInfo.getDevPort();
		String strUser = taskInfo.getDevInfo().getHostUser();
		String strPassword = taskInfo.getDevInfo().getHostPwd();
		String strHostSign = taskInfo.getDevInfo().getHostSign();
		int gatherTimeout = taskInfo.getCollectTimeOut();

		try {
			telnetClient = new TelnetClient("ANSI");

			telnetClient.setReaderThread(true);
			telnetClient.setReceiveBufferSize(1024 * 4);
			telnetClient.connect(strHostIP, nHostPort);

			int timeoutSec = gatherTimeout * 60;
			timeoutSec = (timeoutSec > 0) ? timeoutSec : timeout * 60;
			telnetClient.setSoTimeout(timeoutSec * 1000);
			// log.info("[{}]设置超时，{}s", taskInfo.getTaskID(), timeoutSec);
			log.info("taskId:[" + taskInfo.getTaskID() + "]设置超时，" + timeoutSec + "s");

			in = telnetClient.getInputStream();
			out = new PrintStream(telnetClient.getOutputStream());

			if (in == null)
				return false;
			// log.debug("telnet开始登录：{}:{}，用户名：{}，密码：{}", new Object[]{strHostIP, nHostPort, strUser, strPassword});
			log.info("telnet开始登录:" + strHostIP + ":" + nHostPort + ",用户名：" + strUser + ",密码:" + strPassword);
			if (waitForString("ogin:", 5000))// 如果服务器返回login,则发送用户名
			{
				out.println(strUser);
				out.flush();
			} else
				return false;

			if (waitForString("assword:", 5000))// 如果服务器返回password,则发送用户密码
			{
				out.println(strPassword);
				out.flush();
			} else {
				return false;
			}

			if (!waitForString(strHostSign, 5000)) // 等待用户登陆成功信息
			{
				return false;
			}
		} catch (Exception e) {
			log.warn("taskId:[" + taskInfo.getTaskID() + "]-telnet 登陆失败", e);
			close();
			return false;
		}

		// log.info("[{}]telnet 登陆成功", taskInfo.getTaskID());
		log.info("taskId:[" + taskInfo.getTaskID() + "]-telnet 登陆成功");
		return true;
	}

	// 等待TELNET命令提示符号返回
	public boolean waitForString(String end, long timeout) throws Exception {
		byte[] buffer = new byte[1024 * 10];

		long starttime = System.currentTimeMillis();

		try {
			String readbytes = new String();
			// 循环读取，直到超时或者遇到结束符号
			while ((readbytes.indexOf(end) < 0) && ((System.currentTimeMillis() - starttime) < timeout * 1000)) {
				if (in.available() > 0) {
					int ret_read = in.read(buffer);
					if (ret_read != -1)
						readbytes = readbytes + new String(buffer, 0, ret_read);
				} else {
					Thread.sleep(500);
				}
			}

			if (readbytes.indexOf(end) >= 0) {
				return (true);
			} else {
				return (false);
			}
		} catch (Exception e) {
			log.error("Telnet> waitForString error.", e);
			return false;
		}
	}

	String receiveUntilTimeout(String keyWords, long timeout) throws Exception {
		byte[] buffer = new byte[1024];
		long startTime = System.currentTimeMillis();
		String readbytes = "";
		// 循环读取直到超时或者遇到相应的关键字
		while ((readbytes.indexOf(keyWords) < 0)) {
			if (isTimeout(startTime, timeout * 1000L))
				throw new Exception("连接Telnet服务器超时,timeout=" + timeout);
			// 如果输入流没有内容 线程休眠0.5秒
			if (in.available() <= 0) {
				Thread.sleep(500);
				continue;
			}
			int redBytes = in.read(buffer);
			if (redBytes != -1)
				readbytes = readbytes + new String(buffer, 0, redBytes);
		}
		// log.debug("keyWords={},back={}", new Object[]{keyWords, readbytes});
		log.info("keyWords=" + keyWords + ",back=" + readbytes);
		return readbytes;
	}

	boolean isTimeout(long startTime, long timeout) {
		return System.currentTimeMillis() - startTime >= timeout;
	}

	@Override
	public boolean access() throws Exception {
		// 是不是solaris，否则认为是linux.
		boolean isSunOs = false;
		log.debug("发送命令“uname”……");
		out.println("uname");
		out.flush();
		try {
			String recv = receiveUntilTimeout(taskInfo.getDevInfo().getHostSign(), 5);
			log.info("uname返回内容为:" + recv);
			if (recv != null && recv.toLowerCase().contains("sunos"))
				isSunOs = true;
		} catch (Exception e) {
			log.error("接收uname的响应失败。", e);
			close();
			return false;
		}

		// 检测parser
		Parser parser = Factory.createParser(this.taskInfo);
		if (parser == null || !(parser instanceof AbstractStreamParser)) {
			log.error("taskId:[" + taskInfo.getTaskID() + "],不是有效任务，原因，parser配置不对");
			close();
			return false;
		}
		try {
			AbstractStreamParser abstractParser = (AbstractStreamParser) parser;
			abstractParser.parse(in, out);
		} catch (Exception e) {
			log.debug("taskId:[" + taskInfo.getTaskID() + "],解析或入库发生异常", e);
			close();
			return false;
		}

		return true;
	}

	@Override
	public void doFinishedAccess() throws Exception {
		super.doFinishedAccess();
		close();
	}

	public void close() throws Exception {
		IOUtils.closeQuietly(in);
		IOUtils.closeQuietly(out);

		if (telnetClient != null) {
			try {
				telnetClient.disconnect();
			} catch (IOException e) {
			}
		}
	}

	@Override
	public void configure() throws Exception {
		// TODO Auto-generated method stub

	}

}

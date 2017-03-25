package collect;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.SocketTimeoutException;

import org.apache.commons.net.telnet.TelnetClient;
import org.apache.log4j.Logger;

import util.LogMgr;
import alarm.AlarmMgr;

public class ProtocolTelnet {

	private BufferedReader br = null;

	private InputStream in;

	private PrintStream out;

	private TelnetClient client;

	private boolean proxy = false;

	private Logger log = LogMgr.getInstance().getSystemLogger();

	public void Close() {
		try {
			if (proxy) // 如果是通过代理连接的，先要发送exit命令，端口远程连接才退出
				sendCmd("exit");

			if (in != null)
				in.close();

			if (out != null)
				out.close();

			if (br != null)
				br.close();

			if (client != null && client.isConnected())
				client.disconnect();
		} catch (Exception e) {
			log.error("Telnet> Close error.", e);
		}
	}

	/** 发送ＴＥＬＮＥＴ命令 */
	public boolean sendCmd(String strCmd) {
		boolean bResult = true;

		try {
			out.println(strCmd);
			out.flush();
		} catch (Exception e) {
			log.error("Telnet> sendCmd error. " + strCmd, e);
			bResult = false;
		}

		return bResult;
	}

	// 在TELNET下执行SHELL,并等待执行完成
	// 如果在指定的时间内没有执行完成，则返回失败
	public boolean ExecuteShell(String strShell, String strEnd, int TimeOut) {
		boolean bResult = false;

		try {
			sendCmd(strShell);
			bResult = waitForString(strEnd, TimeOut); // 等到返回，直到超时或者执行完成
		} catch (Exception e) {
			log.error("Telnet> ExecuteShell error.", e);
		}

		return bResult;
	}

	// 通过代理服务器进行连接
	public boolean ProxyLogin(String strHost, int nPort, String strUser, String strPwd, String strSign, String strTermType) {
		try {
			proxy = true;
			if (client.isConnected()) {
				if (in == null)
					return false;

				String strConnect;
				strConnect = String.format("telnet %s %d", strHost, nPort);
				sendCmd(strConnect); // 发送telnet命令到服务器，进行代理连接

				if (waitForString("ogin:", 5000))// 如果服务器返回login,则发送用户名
					sendCmd(strUser);
				else
					return false;

				if (waitForString("assword:", 5000))// 如果服务器返回password,则发送用户密码
					sendCmd(strPwd);
				else
					return false;

				if (!waitForString(strSign, 5000))// 等待用户登陆成功信息
					return false;

			} else {
				return false;
			}
		} catch (Exception e) {
			log.error("Telnet> ProxyLogin error.", e);
			return false;
		}

		return true;

	}

	// TELNET连接到服务器
	public boolean Login(String strHost, int nPort, String strUser, String strPwd, String strSign, String strTermType, int nTimeOut) {
		try {
			proxy = false;
			client = new TelnetClient(strTermType);
			client.setReaderThread(true);
			client.connect(strHost, nPort);
			in = client.getInputStream();
			out = new PrintStream(client.getOutputStream());

			// 设置Telnet读取数据超时时长，如果超时间，读取失败
			client.setSoTimeout(nTimeOut * 1000);

			if (in == null)
				return false;
			br = new BufferedReader(new InputStreamReader(in));
			if (waitForString("ogin:", 5000))// 如果服务器返回login,则发送用户名
				sendCmd(strUser);
			else
				return false;

			if (waitForString("assword:", 5000))// 如果服务器返回password,则发送用户密码
				sendCmd(strPwd);
			else
				return false;

			if (!waitForString(strSign, 5000)) // 等待用户登陆成功信息
				return false;

		} catch (Exception e) {
			log.error("Telnet> login error.", e);
			return false;
		}

		return true;
	}

	/**
	 * 接收数据到接收buffer,如果成功，返回接收到的字节数， 如果失败有可能是接收超时或者网络端口连接
	 */
	public int readData(byte[] buffer) {
		int nLen = -1;
		try {
			if (client.isConnected()) {
				nLen = in.read(buffer);
			}
		} catch (Exception e) {
			if (e instanceof SocketTimeoutException) {

				// 到达指定的超时时间，中断读取。
				log.debug("Telnet read end.");
			} else {
				AlarmMgr.getInstance().insert(2008, "telnet", "telnet", "采集问题", 1000);
				log.error("Telnet> readData error.", e);
			}
		}

		return nLen;
	}

	/**
	 * 接收数据到接收buffer,如果成功，返回接收到的字节数， 如果失败有可能是接收超时或者网络端口连接
	 */
	public int readCharData(char[] buffer) {
		int nLen = -1;
		try {
			if (client.isConnected()) {
				nLen = br.read(buffer);
			}
		} catch (Exception e) {
			AlarmMgr.getInstance().insert(2008, "telnet", "telnet", "采集问题", 1000);
			log.error("Telnet> readData error.", e);
		}

		return nLen;
	}

	public String receiveUntilTimeout(String keyWords, long timeout) throws Exception {
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
		log.debug("keyWords=" + keyWords + ",back={}" + readbytes);
		return readbytes;
	}

	boolean isTimeout(long startTime, long timeout) {
		return System.currentTimeMillis() - startTime >= timeout;
	}

	public boolean findFile(String strName, String end, long timeout) {
		boolean bIsExist = false;

		try {
			sendCmd("ls " + strName);

			Thread.sleep(2000);

			byte[] buffer = new byte[1024 * 10];

			int ret = readData(buffer);

			if (ret != -1) {
				String strRet = new String(buffer, 0, ret);
				// log.debug(strRet);

				if (strRet.indexOf("No such file or directory") == -1 && strRet.indexOf("not found") == -1) {
					bIsExist = true;
				}
			}
		} catch (Exception e) {
			log.error("Telnet> findFile error.", e);
		}

		return bIsExist;
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
					int ret_read = readData(buffer);
					if (ret_read != -1)
						readbytes = readbytes + new String(buffer, 0, ret_read);
				} else {
					// System.out.println(readbytes+" waitForString timeout
					// "+timeout);
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

	/**
	 * 获取输入流---liangww add 2012-09-26
	 * 
	 * @return
	 */
	public InputStream getInpustream() {
		return in;
	}

}

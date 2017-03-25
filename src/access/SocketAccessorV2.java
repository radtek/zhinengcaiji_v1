package access;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import parser.AbstractStreamParser;
import parser.Parser;
import task.CollectObjInfo;
import util.Parsecmd;
import util.Util;
import framework.Factory;

/**
 * socket client 连接方式
 * 
 * @author liangww 2012-4-17
 * @since 1.0.0<br>
 *        1.0.1 liangww 2012-06-25 openSocket增加超时机制<br>
 */
public class SocketAccessorV2 extends AbstractAccessor {

	// 默认超时时间，分钟为单位
	private final static int DEFAULT_TIMEOUT_MIN = 30;

	private Socket socket = null;

	private InputStream in = null;

	private OutputStream out = null;

	private AbstractStreamParser parser = null;

	@Override
	public boolean validate() {
		// TODO Auto-generated method stub
		if (this.getDataSourceConfig() == null || getDataSourceConfig().getDatas() == null) {
			setDataSourceConfig(GenericDataConfig.wrap("empty"));
		}

		if (!super.validate()) {
			return false;
		}

		// liangww add 2012-04-27 初始化name
		this.name = this.taskInfo.getFullName();

		// 检测parser
		Parser tmpParser = Factory.createParser(this.taskInfo);
		if (tmpParser == null || !(tmpParser instanceof AbstractStreamParser)) {
			log.error("taskId-" + taskInfo.getTaskID() + ":不是有效任务，原因，parser配置不对");
			return false;
		}

		//
		parser = (AbstractStreamParser) tmpParser;

		return true;
	}

	@Override
	public boolean access() throws Exception {
		while (true) {
			try {
				// 打开socket
				openSocket();
				parser.parse(in, out);
			} catch (IOException e) {
				// TODO: handle exception
				log.warn(name, e);
			} finally {
				// 关闭socket
				closeSocket();
			}

			int second = 5;
			log.info(String.format("%s 休息%s", name, second));
			try {
				Thread.sleep(second * 1000);
			} catch (InterruptedException e) {
				break;
			}
		}// end while(true)

		return true;
	}

	@Override
	public void configure() throws Exception {

	}

	@Override
	public boolean doAfterAccess() throws Exception {
		// 采集之后执行的Shell命令
		String strShellCmdFinish = taskInfo.getShellCmdFinish();
		if (Util.isNotNull(strShellCmdFinish)) {
			Parsecmd.ExecShellCmdByFtp(strShellCmdFinish, taskInfo.getLastCollectTime());
		}

		return true;
	}

	/**
	 * 打开socket
	 * 
	 * @return
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	private boolean openSocket() throws UnknownHostException, IOException {
		int port = taskInfo.getDevPort();
		String host = taskInfo.getDevInfo().getIP();

		socket = new Socket(host, port);

		// liangww add 2012-06-25 增加超时机制
		int timeOutMin = taskInfo.getCollectTime() > 0 ? taskInfo.getCollectTime() : DEFAULT_TIMEOUT_MIN;
		socket.setSoTimeout(timeOutMin * 60 * 1000);

		in = socket.getInputStream();
		out = socket.getOutputStream();

		log.info("连接建立成功 - " + getDeviceInfo(taskInfo));
		return true;
	}

	/**
	 * 关于socket
	 * 
	 * @return
	 */
	private boolean closeSocket() {
		Util.closeCloseable(in);
		Util.closeCloseable(out);

		if (socket != null) {
			try {
				socket.close();
			} catch (Exception e) {
			}
		}

		log.info(name + " 关闭连接成功 - " + getDeviceInfo(taskInfo));
		return true;
	}

	public static String getDeviceInfo(CollectObjInfo taskInfo) {
		int port = taskInfo.getDevPort();
		String host = taskInfo.getDevInfo().getIP();
		String encode = taskInfo.getDevInfo().getEncode();
		String name = taskInfo.getDevInfo().getName();
		int omcId = taskInfo.getDevInfo().getOmcID();

		String format = "[encode=%s, ip=%s, name=%s, omcId=%s, port=%s]";
		return String.format(format, encode, host, name, omcId, port);
	}

}

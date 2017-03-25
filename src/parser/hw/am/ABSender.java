package parser.hw.am;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import util.LogMgr;
import cn.uway.alarmbox.protocol.DataPacket;
import cn.uway.alarmbox.protocol.DataPacketV1;
import cn.uway.alarmbox.protocol.DataStruct;
import cn.uway.alarmbox.protocol.UserInfo;

/**
 * 
 * 向AlarmBox发送。 ABSender
 * 
 * @author
 * @version 1.0<br>
 *          1.0.1 liangww 2012-04-28 增加name成员，并写log时增加相关任务信息<br>
 *          1.0.2 liangww 2012-06-04 修改send函数的参数为dataStruct<br>
 */
public class ABSender implements Closeable {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	private Socket socket;

	private String host;

	private int port;

	private DataPacketV1 dp;

	private boolean isSentUserInfo;

	private UserInfo ui;

	private OutputStream out;

	private InputStream in;

	private String name = null;

	private String logKey = null;

	public ABSender(String host, int port, String name, String logKey) {
		super();
		this.host = host;
		this.port = port;
		ui = new UserInfo();
		ui.setUserType(UserInfo.UT_MONITOR);
		// liangww add 2012-04-17 增加设置用户名字
		ui.setName("ha_" + name);
		dp = new DataPacketV1();
		dp.setCompress();

		this.name = name;
		this.logKey = logKey;
	}

	public boolean send(DataStruct dataStruct) {
		if (socket == null) {
			try {
				socket = new Socket(host, port);
				// liangww add 2012-04-17 设置20分钟为超时
				socket.setSoTimeout(1000 * 60 * 20);
				out = socket.getOutputStream();
				in = socket.getInputStream();
			} catch (Exception e) {
				log.error(logKey + "连接与AlarmBox的连接失败。", e);
				return false;
			}
		}

		// 如果没有发送用户信息
		if (!isSentUserInfo) {
			try {
				dp.setDataStruct(ui);
				dp.write(out);
				out.flush();

				// liangww add 2012-04-2-13 发送用户验证信息必须读取发送状态，
				// 暂时不处理判断，因为只要socket正常，这个验证状态必是通过正常验证
				DataPacket dataPacket = DataPacket.getInstance(in);

				isSentUserInfo = true;
			} catch (Exception e) {
				log.warn(logKey + "向AlarmBox发送UserInfo时出错异常。", e);
				return false;
			}
		}

		try {
			dp.setDataStruct(dataStruct);
			dp.write(out);
			out.flush();

			// liangww add 2012-04-2-13 发送完必须读取发送状态，
			// 暂时不处理判断，因为只要socket正常，这个状态必是发送正常
			DataPacket dataPacket = DataPacket.getInstance(in);
		} catch (Exception e) {
			log.error(logKey + "向AlarmBox发送告警时异常。数据=" + dataStruct, e);
			return false;
		}
		return true;
	}

	@Override
	public void close() throws IOException {
		// liangww moidfy 2012-04-13 修改关闭的流置为Null,并修改isSendtuserInfo的为false
		IOUtils.closeQuietly(out);
		IOUtils.closeQuietly(in);

		if (socket != null) {
			try {
				socket.close();
			} catch (IOException e) {
			}
		}

		this.out = null;
		this.in = null;
		this.socket = null;
		this.isSentUserInfo = false;
	}

	public boolean send(DataStruct dataStruct, int times) {
		for (int i = 0; i < 3; i++) {
			// 如果发送成功直接返回
			if (send(dataStruct)) {
				return true;
			}
			// 发送失败了，先把sender close
			int second = 5;
			log.warn(String.format("%s 休息%s秒, 第%s次重连", logKey, second, i + 1));
			try {
				close();
			} catch (IOException e1) {
			}
			try {
				Thread.sleep(1000 * second);
			} catch (Exception e) {
			}
		}//

		return false;
	}
}

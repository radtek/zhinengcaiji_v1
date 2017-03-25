package tools.socket;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

import org.apache.log4j.Logger;

import util.LogMgr;

/**
 * @author yuy socket服务端帮助类
 */
public class SocketServerHelper {

	protected static Logger LOGGER = LogMgr.getInstance().getSystemLogger();

	public static void main(String[] args) throws IOException {
		ServerSocket server = new ServerSocket(2021);
		server.setSoTimeout(0);
		Socket client = null;
		while (true) {
			try {
				client = server.accept();
				// 此处可以优化，用线程去做
				InputStream in = client.getInputStream();
				LOGGER.info("接收了一条客户端连接");
				while (true) {
					// 读
					byte[] bb = readByte(in);
					if (bb == null || bb.length == 0)
						break;
					byte[] bs = deleteEndSign(bb);
					LOGGER.info("接收到的信息：" + new String(bs));
				}
			} catch (SocketTimeoutException s) {
				LOGGER.info(s.getMessage());
				continue;
			} finally {
				client.close();
			}
		}
	}

	public static byte[] readByte(InputStream in) throws IOException {
		byte[] b = new byte[16 * 1024];
		int size = 0;
		int lastpos = 0;
		byte[] bb = null;
		byte[] tmpb = null;
		while ((size = in.read(b)) != -1) {
			if (bb != null) {
				tmpb = bb;
			}
			bb = new byte[lastpos + size];
			if (tmpb != null) {
				System.arraycopy(tmpb, 0, bb, 0, tmpb.length);
			}
			System.arraycopy(b, 0, bb, lastpos, size);
			lastpos = size;
			if (new String(b, 0, size).endsWith("\n"))
				break;
		}
		return bb;
	}

	public static byte[] deleteEndSign(byte[] bb) {
		int len = bb.length - "\n".getBytes().length;
		byte[] bs = new byte[len];
		System.arraycopy(bb, 0, bs, 0, len);
		return bs;
	}

}

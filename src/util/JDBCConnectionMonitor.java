package util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.Socket;
import java.sql.Connection;

import org.apache.log4j.Logger;

/**
 * 作为守护线程，监视socket连接，达到超时时间后强行断开。
 * 
 * @author ChenSijiang 2012-7-17
 */
public class JDBCConnectionMonitor extends Thread {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	private long taskId;

	private Connection con;

	private Socket socket;

	private long timeoutMill;

	public JDBCConnectionMonitor(long taskId, Connection con, long timeoutMill) {
		super("ConMonitor-" + taskId);
		setDaemon(true);
		this.taskId = taskId;
		this.con = con;
		this.timeoutMill = timeoutMill;
	}

	public void startMonit() {
		if (con == null || timeoutMill <= 0) {
			log.warn(taskId + " 因为连接对象为空或时间小于等于0，数据库连接监控将不启动。con=" + con + "，timeoutMill=" + timeoutMill);
			return;
		}
		this.socket = findSocket(this.con);
		if (this.socket == null) {
			log.warn(taskId + " 因为未能从连接中找到原始socket，数据库连接监控将不启动。con=" + con);
			return;
		}
		start();
	}

	public void endMonit() {
		this.interrupt();
	}

	@Override
	public void run() {
		try {
			log.debug(taskId + " 开始监控数据库连接，socket=" + this.socket + "，timeoutMill=" + timeoutMill);
			Thread.sleep(this.timeoutMill);
			try {
				this.socket.close();
				log.debug(taskId + " 因为达到超时时间，socket被强行关闭。");
			} catch (IOException e) {
				log.error(taskId + " 强行关闭socket时，发生异常。", e);
			}
		} catch (InterruptedException e) {
			log.debug(taskId + " 连接监控正常结束。");
			return;
		}
	}

	@Override
	public void finalize() throws Throwable {
		super.finalize();
		this.endMonit();
	}

	public static Socket findSocket(Connection connection) {
		if (connection == null)
			return null;
		try {
			if (_driverClassExists("com.microsoft.jdbc.sqlserver.SQLServerConnection")
					&& connection.getClass().getName().equals("com.microsoft.jdbc.sqlserver.SQLServerConnection")) {
				// sqlserver2000
				return _findSocket(connection, "implConnection.conn.socket");
			} else if (_driverClassExists("com.microsoft.sqlserver.jdbc.SQLServerConnection")
					&& connection.getClass().getName().equals("com.microsoft.sqlserver.jdbc.SQLServerConnection")) {
				// sqlserver2005
				return _findSocket(connection, "tdsChannel.tcpSocket");
			} else if (_driverClassExists("oracle.jdbc.driver.OracleConnection")
					&& connection.getClass().getName().equals("oracle.jdbc.driver.OracleConnection")) {
				// oracle
				return _findSocket(connection, "db_access.net.addrRes.cs.copt.nt.socket");
			} else if (_driverClassExists("com.sybase.jdbc3.jdbc.SybConnection")
					&& connection.getClass().getName().equals("com.sybase.jdbc3.jdbc.SybConnection")) {
				// sybase
				return _findSocket(connection, "_pc._inMgr.a._socket");
			}

		} catch (Exception e) {
			return null;
		}
		return null;
	}

	private static Socket _findSocket(Connection connection, String socketPath) throws Exception {
		String[] paths = socketPath.split("\\.");
		Field field = null;
		Object fieldInstance = connection;
		for (String fieldName : paths) {
			if (fieldInstance == null)
				return null;
			try {
				field = fieldInstance.getClass().getDeclaredField(fieldName);
			} catch (NoSuchFieldException e) {
				// 在父类尝试找一次。
				field = fieldInstance.getClass().getSuperclass().getDeclaredField(fieldName);
			}
			field.setAccessible(true);
			fieldInstance = field.get(fieldInstance);
		}
		if (fieldInstance != null)
			return (Socket) fieldInstance;
		return null;
	}

	private static boolean _driverClassExists(String clz) {
		try {
			Class.forName(clz);
		} catch (ClassNotFoundException e) {
			return false;
		}
		return true;
	}
}

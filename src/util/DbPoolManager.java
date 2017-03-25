package util;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.log4j.Logger;

import framework.SystemConfig;

/**
 * 数据库连接池管理器
 * 
 * @author yuy @ 2014-1-10
 */
public class DbPoolManager {

	/**
	 * 数据库连接池缓存器
	 */
	private static Map<Long, BasicDataSource> dataSourceCacher = new HashMap<Long, BasicDataSource>();

	private static final Logger LOGGER = LogMgr.getInstance().getSystemLogger();

	/**
	 * 获取一个jdbc连接
	 * 
	 * @return Connection
	 */
	public static Connection getConnection(String driver, String url, String usr, String pwd) {
		if (Util.isNull(url) || Util.isNull(usr) || Util.isNull(pwd)) {
			return null;
		}
		if (url.indexOf("jdbc:") < 0) {
			return null;
		}
		// 获取数据源
		DataSource dataSource = getDataSource(driver, url, usr, pwd);
		if (dataSource == null) {
			return null;
		}
		/*
		 * LOGGER.debug( "OracleJdbcExporter:准备获取Oracle连接……当前oracle连接数：{}，最大oracle连接数：{}", new Object[]{dataSource.getNumActive(),
		 * dataSource.getMaxActive()});
		 */
		// 获取连接
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
		} catch (Exception e) {
			LOGGER.error("DbPoolManager: error when got a connection from DB pool.", e);
			try {
				BasicDataSource bds = ((BasicDataSource) dataSource);
				LOGGER.error("数据库连接池信息：当前活动连接数=" + bds.getNumActive() + ", 当前空闲连接数=" + bds.getNumIdle() + ", 设定的最大活动连接数=" + bds.getMaxActive()
						+ ", 当前获取连接的线程=" + Thread.currentThread());
			} catch (Exception ignore) {
			}
		}
		return conn;
	}

	/**
	 * 获取数据源
	 * 
	 * @param url
	 * @param usr
	 * @param pwd
	 * @return
	 */
	public static DataSource getDataSource(String driver, String url, String usr, String pwd) {
		// 获取key码
		long keyCode = getKeyCode(url, usr, pwd);

		BasicDataSource dataSource = dataSourceCacher.get(keyCode);
		if (dataSource == null) {
			synchronized (dataSourceCacher) {
				dataSource = dataSourceCacher.get(keyCode);
				if (dataSource == null) {
					dataSource = createDataSource(driver, url, usr, pwd);
					dataSourceCacher.put(keyCode, dataSource);
				}
			}
		}
		return dataSource;
	}

	/**
	 * 创建一个新的数据库连接池
	 * 
	 * @param version
	 *            JNDI的搜索名
	 * @param record
	 *            数据库连接池的基本信息
	 * @param constants
	 *            系统内置的环境管理类
	 * @return 新的数据库连接池，null表示创建失败
	 */
	private static BasicDataSource createDataSource(String driver, String url, String usr, String pwd) {
		String name = url;
		BasicDataSource dataSource = null;
		try {
			LOGGER.debug("creating dbpool...");
			SystemConfig cfg = SystemConfig.getInstance();
			Properties p = new Properties();
			p.put("name", name);
			p.put("type", "javax.sql.DataSource");
			p.put("driverClassName", driver);
			p.put("url", url);
			p.put("username", usr);
			p.put("password", pwd);
			p.put("maxActive", String.valueOf(cfg.getPoolMaxActiveRemote()));
			p.put("maxIdle", String.valueOf(cfg.getPoolMaxIdleRemote()));
			p.put("maxWait", String.valueOf(cfg.getPoolMaxWaitRemote()));
			p.put("validationQuery", DatabaseUtil.getMyValidationQuery(driver));
			p.put("testOnBorrow", "true");
			p.put("testOnReturn", "true");
			p.put("testWhileIdle", "true");
			dataSource = (BasicDataSource) BasicDataSourceFactory.createDataSource(p);
			LOGGER.debug("DbPoolManager：创建数据库连接池：" + name + "，用户：" + usr);
		} catch (Exception e) {
			LOGGER.error("DbPoolManager：创建数据源 " + name + " 失败：", e);
		}
		return dataSource;
	}

	/**
	 * 关闭现有的数据库连接池
	 * 
	 * @param dataSource
	 *            要关闭的数据库连接池
	 */
	public static void close(String url, String usr, String pwd) {
		// 获取key码
		long keyCode = getKeyCode(url, usr, pwd);

		DataSource dataSource = dataSourceCacher.get(keyCode);
		try {
			if (dataSource != null) {
				Class<?> classz = dataSource.getClass();
				Class<?>[] types = new Class[0];
				Method method = classz.getDeclaredMethod("close", types);
				if (method != null) {
					method.setAccessible(true);
					Object[] args = new Object[0];
					method.invoke(dataSource, args);
				}
			}
		} catch (Exception e) {
			LOGGER.error("DbPoolManager: 尝试关闭原有的数据库连接池 [" + dataSource.getClass().getName() + "]时失败.", e);
		} finally {
			dataSource = null;
		}
	}

	/**
	 * 获取key码
	 * 
	 * @param url
	 * @param user
	 * @param pwd
	 * @return
	 */
	public static long getKeyCode(String url, String user, String pwd) {
		return crc32(url.trim() + user.trim() + pwd.trim());
	}

	/**
	 * compute the CRC-32 of a string
	 * 
	 * @param str
	 * @return
	 */
	public static long crc32(String str) {
		java.util.zip.CRC32 x = new java.util.zip.CRC32();
		x.update(str.getBytes());
		return x.getValue();
	}

	public static void closeAllConnectionPool() {

		if (dataSourceCacher != null) {
			Iterator<Entry<Long, BasicDataSource>> it = dataSourceCacher.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Long, BasicDataSource> en = it.next();
				DataSource dataSource = en.getValue();

				try {
					if (dataSource != null) {
						Class<?> classz = dataSource.getClass();
						Class<?>[] types = new Class[0];
						Method method = classz.getDeclaredMethod("close", types);
						if (method != null) {
							method.setAccessible(true);
							Object[] args = new Object[0];
							method.invoke(dataSource, args);
						}
					}
				} catch (Exception e) {
					LOGGER.error("DbPool: 尝试关闭原所有的数据库连接池 [" + dataSource.getClass().getName() + "]时失败.", e);
				} finally {
					dataSource = null;
				}

			}
		}
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		/*
		 * for (int i = 0; i < 50; i++) { Connection conn = DbPoolManager.getConnection("jdbc:oracle:thin:@132.120.32.26:1521:orcl", "gd_lte",
		 * "gd_lte"); //DbUtil.close(null, null, conn); LOGGER.debug("Conn is null:{}", (conn == null)); LOGGER.debug("Conn is close:{}",
		 * conn.isClosed()); LOGGER.debug("============================"); //ThreadUtil.sleep(500); }
		 */
	}

}

package util;

/*
 * Copyright 2008 BeiJing ZCTT Co. Ltd.
 * All right reserved. 
 * File Name: TomcatServer.java
 * Create Date: 2008-03-05
 */
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.log4j.Logger;

import framework.SystemConfig;

/**
 * dbcp数据库连接池的实现类
 * 
 * @author miniz
 * @version 1.0
 */
public class DbPool {

	static DataSource dataSource = null;

	static BasicDataSource basicDS = null;

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	/**
	 * 关闭现有的数据库连接池
	 * 
	 * @param dataSource
	 *            要关闭的数据库连接池
	 */
	public static void close() {
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
			log.error("DbPool: 尝试关闭原有的数据库连接池 [" + dataSource.getClass().getName() + "]时失败.", e);
		} finally {
			dataSource = null;
		}
	}

	public synchronized static Connection getConn() {
		Connection conn = null;
		try {
			if (dataSource == null) {
				DbPool.createDataSource();
			}
			conn = dataSource.getConnection();

		} catch (Exception e) {
			log.error("DbPool: error when got a connection from DB pool.", e);
			try {
				BasicDataSource bds = ((BasicDataSource) dataSource);
				log.error("数据库连接池信息：当前活动连接数=" + bds.getNumActive() + ", 设定的最大活动连接数=" + bds.getMaxActive() + ", 当前获取连接的线程=" + Thread.currentThread());
			} catch (Exception ignore) {
			}
		}
		// printPoolInfo();
		return conn;
	}

	public static void printPoolInfo() {
		if (basicDS == null) {
			if (dataSource != null) {
				basicDS = (BasicDataSource) dataSource;
			}
		}
		if (basicDS != null) {
			int maxActive = basicDS.getMaxActive();
			int maxIdle = basicDS.getMaxIdle();
			int active = basicDS.getNumActive();
			int idle = basicDS.getNumIdle();
			log.info(String.format("连接池信息:活动连接(当前/最大)=%s/%s,空闲连接(当前/最大)=%s/%s", active, maxActive, idle, maxIdle));
		}
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
	private static DataSource createDataSource() {
		String name = "";
		try {
			log.debug("creating dbpool...");
			Properties p = new Properties();
			SystemConfig cfg = SystemConfig.getInstance();
			p.put("name", cfg.getPoolName());
			p.put("type", cfg.getPoolType());
			p.put("driverClassName", cfg.getDbDriver());
			p.put("url", cfg.getDbUrl());
			p.put("maxActive", String.valueOf(cfg.getPoolMaxActive()));
			p.put("username", cfg.getDbUserName());
			p.put("password", cfg.getDbPassword());
			p.put("maxIdle", String.valueOf(cfg.getPoolMaxIdle()));
			p.put("maxWait", "240000");
			p.put("validationQuery", SystemConfig.getInstance().getDbValidationQueryString());
			p.put("testOnBorrow", "true");
			p.put("testOnReturn", "true");
			p.put("testWhileIdle", "true");
			name = SystemConfig.getInstance().getPoolName();
			dataSource = BasicDataSourceFactory.createDataSource(p);
			log.debug("DbPool: 创建数据库连接池：" + name);
		} catch (Exception e) {
			log.error("DbPool: 创建数据源 " + name + " 失败：", e);
		}

		return dataSource;
	}

}

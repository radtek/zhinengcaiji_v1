package framework;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import store.StoreFactory;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.PropertiesXML;
import util.Util;
import cn.uway.des.DESDecryptor;

/**
 * 系统配置类
 * <p>
 * 对应 conf/config.xml 文件
 * </p>
 * 
 * @author 陈思江
 * @since 1.0
 */
public class SystemConfig {

	// 以键值对方式读写xml文件的工具类
	private PropertiesXML propertiesXML;

	private static final String SYSTEMFILE = "." + File.separator + "conf" + File.separator + "config.xml";

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static SystemConfig instance = null;

	private String realDbUser;

	private String realDbPwd;

	private String realUrl;

	private String realService;

	private SystemConfig() throws SystemConfigException {

		propertiesXML = new PropertiesXML(SYSTEMFILE);

		// 2010-11-4 陈思江
		// 如果数据库用户密码为login/login，则为解密方式，从login用户的project_login_info同义词中取加密码字符串，解密得到真正的用户名与密码
		String dbUser = getDbUserName().toLowerCase();
		String dbPwd = getDbPassword().toLowerCase();

		String dbNum = getDBNum().toLowerCase();
		String dbConnType = getDBConnType().toLowerCase();
		String dbType = getDBConnType().toLowerCase();
		
		
		if (dbUser.equals("login") && dbPwd.equals("login") && (util.Util.isNotNull(dbNum) && util.Util.isNotNull(dbType)&& util.Util.isNotNull(dbConnType))) {
			String sql = "SELECT * FROM TB_LOGIN_INFO  where db_num=? and db_type=? and conn_type =? ";  //
			Connection con = CommonDB.getConnection(0, propertiesXML.getProperty("config.db.driverClassName"),
					propertiesXML.getProperty("config.db.url"), "login", "login");
			PreparedStatement st = null;
			ResultSet rs = null;
			try {
				String loginInfo = null;
				st = con.prepareStatement(sql);

				st.setString(1, dbNum);

				st.setString(2, dbType);
				
				st.setString(3, dbConnType);
				logger.debug("query for - " + sql);
				rs = st.executeQuery();
				if (rs.next()) {
					loginInfo = rs.getString("LOGIN_INFO");
					logger.debug("query ok - " + sql);

				} else {
					throw new Exception("select出来的记录数为0");
				}

				DESDecryptor desDecryptor = new DESDecryptor();
				String result = desDecryptor.desDecrypt(loginInfo, "UWAY@SOFT2009");
				if (desDecryptor.getLastException() != null) {
					throw desDecryptor.getLastException();
				}
				// result = result.toLowerCase().replace("user id=", "").replace("password=", "");
				result = result.toLowerCase();

				String[] split = result.split(";");
				for (String name : split) {
					if (name.startsWith("url")) {
						name = name.replace("url={", "");
						StringBuilder sb = new StringBuilder(name);
						sb.deleteCharAt(name.length() - 1);
						realUrl = sb.toString();

						if (sb.toString().contains("@") && sb.toString().toUpperCase().contains("DESCRIPTION"))
							realService = sb.substring(sb.lastIndexOf("@") + 1);

					} else if (name.startsWith("username")) {
						name = name.replace("username={", "");
						StringBuilder sb = new StringBuilder(name);
						sb.deleteCharAt(name.length() - 1);
						realDbUser = sb.toString();
					} else if (name.startsWith("password")) {
						name = name.replace("password={", "");
						StringBuilder sb = new StringBuilder(name);
						sb.deleteCharAt(name.length() - 1);
						realDbPwd = sb.toString();
					}
				}
				DbPool.close();
			} catch (Throwable e) {
				logger.error("在tb_login_info同义词中获取数据库账户时出现异常:" + sql, e);
			} finally {
				CommonDB.close(rs, st, con);
			}
		}

		// 自动刷新配置文件
		// Timer timer = new Timer();
		// timer.schedule(new TimerTask()
		// {
		//
		// @Override
		// public void run()
		// {
		// try
		// {
		// propertiesXML = new PropertiesXML(SYSTEMFILE);
		// logger.info("已重新加载config.xml");
		// }
		// catch (SystemConfigException e)
		// {
		// logger.warn("重新加载config.xml时异常，重新加载未生效。", e);
		// }
		// }
		// }, 3 * 60 * 1000, 3 * 60 * 1000);
	}

	/**
	 * 获取实例
	 * 
	 * @return
	 */
	public static SystemConfig getInstance() {
		if (instance == null) {
			try {
				instance = new SystemConfig();
			} catch (SystemConfigException e) {
				logger.error("创建SystemConfig对象时出现异常", e);
				return null;
			}
		}
		return instance;
	}

	public static void setInstance(SystemConfig instance) {
		SystemConfig.instance = instance;
	}

	/**
	 * 获取连接池名称，默认值：DC_POOL
	 * 
	 * @return
	 */
	public String getPoolName() {
		String dbName = propertiesXML.getProperty("config.db.name");

		if (Util.isNull(dbName)) {
			dbName = "DC_POOL";
		}

		return dbName;
	}

	/**
	 * 获取连接池类型，默认值：javax.sql.DataSource
	 * 
	 * @return
	 */
	public String getPoolType() {
		String name = propertiesXML.getProperty("config.db.type");

		if (Util.isNull(name)) {
			name = "javax.sql.DataSource";
		}

		return name;
	}

	/**
	 * 获取数据库驱动类，默认值：oracle.jdbc.driver.OracleDriver
	 * 
	 * @return
	 */
	public String getDbDriver() {
		String d = propertiesXML.getProperty("config.db.driverClassName");

		if (Util.isNull(d)) {
			d = "oracle.jdbc.driver.OracleDriver";
		}

		return d;
	}

	/**
	 * 数据库号:用于标识一个数据库下的一个用户 （会有重复）--符合主键
	 * 
	 * @return
	 */
	public String getDBNum() {
		String db_num = propertiesXML.getProperty("config.db.db_num");

		if (Util.isNull(db_num)) {
			db_num = "";
		}

		return db_num;
	}

	/**
	 * 取的数据库连接类型 连接类型:区分不同连接请求： 例如 jdbc sqlldr ado 等 --符合主键
	 * 
	 * @return
	 */
	public String getDBConnType() {
		String connType = propertiesXML.getProperty("config.db.conn_type");

		if (Util.isNull(connType)) {
			connType = "";
		}

		return connType;
	}
	
	public String getDBType() {
		String connType = propertiesXML.getProperty("config.db.db_type");

		if (Util.isNull(connType)) {
			connType = "";
		}

		return connType;
	}
	

	/**
	 * 获取数据库连接字符串，默认值为空字符串
	 * 
	 * @return
	 */
	public String getDbUrl() {
		if (realUrl != null)
			return realUrl;
		String url = propertiesXML.getProperty("config.db.url");

		if (Util.isNull(url)) {
			url = "";
		}

		return url;
	}

	/**
	 * 获取数据库服务名，默认为空字符串
	 * 
	 * @return
	 */
	public String getDbService() {
		if (realService != null)
			return realService;
		String service = propertiesXML.getProperty("config.db.service");
		if (Util.isNull(service)) {
			service = "";
		}
		return service;
	}

	/**
	 * 获取数据库用户名，默认为空字符串
	 * 
	 * @return
	 */
	public String getDbUserName() {
		if (realDbUser != null) {
			return realDbUser;
		}
		String user = propertiesXML.getProperty("config.db.user");
		if (Util.isNull(user)) {
			user = "login";
		}
		return user;
	}

	/**
	 * 获取数据库密码，默认为空字符串
	 * 
	 * @return
	 */
	public String getDbPassword() {
		if (realDbPwd != null) {
			return realDbPwd;
		}
		String pwd = propertiesXML.getProperty("config.db.password");
		if (Util.isNull(pwd)) {
			pwd = "login";
		}
		return pwd;
	}

	/**
	 * 获取连接池最大活动连接数，默认值：12
	 * 
	 * @return
	 */
	public int getPoolMaxActive() {
		int ma = 12;
		try {
			ma = Integer.parseInt(propertiesXML.getProperty("config.db.maxActive"));
		} catch (Exception e) {
		}
		if (ma <= 0) {
			ma = 12;
		}
		return ma;
	}

	/**
	 * 获取连接池最大活动空闲连接数，默认值：5
	 * 
	 * @return
	 */
	public int getPoolMaxIdle() {
		int maxIdle = 5;
		try {
			maxIdle = Integer.parseInt(propertiesXML.getProperty("config.db.maxIdle"));
		} catch (Exception e) {
		}
		if (maxIdle <= 0) {
			maxIdle = 5;
		}
		return maxIdle;
	}

	/**
	 * 获取连接池最大等待数，默认值：10000
	 * 
	 * @return
	 */
	public int getPoolMaxWait() {
		int maxWait = 10000;
		try {
			maxWait = Integer.parseInt(propertiesXML.getProperty("config.db.maxWait"));
		} catch (Exception e) {
		}
		if (maxWait <= 0) {
			maxWait = 10000;
		}
		return maxWait;
	}

	/**
	 * 执行SELECT时的超时时间，单位为秒，默认1800秒
	 * 
	 * @return
	 */
	public int getQueryTimeout() {
		int timeout = 7200;
		try {
			timeout = Integer.parseInt(propertiesXML.getProperty("config.db.queryTimeout"));
		} catch (Exception e) {
		}
		if (timeout < 7200) {
			timeout = 7200;
		}
		return timeout;
	}

	/**
	 * 获取数据库连接验证语句，默认值：select sysdate from dual
	 * 
	 * @return
	 */
	public String getDbValidationQueryString() {
		String sql = propertiesXML.getProperty("config.db.validationQuery");
		if (Util.isNull(sql)) {
			sql = "select sysdate from dual";
		}
		return sql;
	}

	/**
	 * 获取程序名，用于在project_login_info表查找DB账户，默认值为IGP
	 * 
	 * @return
	 */
	public String getProjectName() {
		String projectName = propertiesXML.getProperty("config.system.projectName");
		if (Util.isNull(projectName)) {
			projectName = "IGP";
		}
		return projectName;
	}

	/**
	 * 获取采集工作路径，默认值为空字符串
	 * 
	 * @return
	 */
	public String getCurrentPath() {
		String path = propertiesXML.getProperty("config.system.currentPath");
		if (Util.isNull(path)) {
			path = "";
		}
		return path;
	}
	
	/**
	 * 获取获取资源路径，默认值为空字符串
	 * 
	 * @return
	 */
	public String getCurrentSourcePath() {
		String path = propertiesXML.getProperty("config.system.currentSourcePath");
		if (Util.isNull(path)) {
			path = "";
		}
		return path;
	}

	/**
	 * 获取本机连接单个设备ip的最大会话数
	 * 
	 * @return
	 */
	public int getMaxSessionCount() {
		String maxSessionCount = propertiesXML.getProperty("config.system.maxSessionCount");
		if (Util.isNull(maxSessionCount)) {
			return 0;
		}
		return Integer.parseInt(maxSessionCount);
	}

	/**
	 * 获取调用本地sqlldr的最大进程数,默认是200
	 * 
	 * @return
	 */
	public int getMaxSqlldrProCount() {
		String maxLocalInstanceCount = propertiesXML.getProperty("config.system.sqlldr.maxSqlldrProCount");
		if (Util.isNull(maxLocalInstanceCount) || Integer.parseInt(maxLocalInstanceCount) == 0) {
			return 200;
		}
		return Integer.parseInt(maxLocalInstanceCount);
	}

	/**
	 * 获取采集工作路径，默认值为空字符串
	 * 
	 * @return
	 */
	public String getEric_w_pm_isDeal() {
		String value = propertiesXML.getProperty("config.eric_w_pm.isDeal");
		if (Util.isNull(value)) {
			value = "";
		}
		return value;
	}

	/**
	 * 判断是否是4G的配置，如果是，要做特殊处理
	 * 
	 * @return
	 */
	public String getLteIs4G() {
		String value = propertiesXML.getProperty("config.lte.is4G");
		if (Util.isNull(value)) {
			value = "";
		}
		return value;
	}

	/**
	 * 获取fd文件生成路径，默认值为空字符串
	 * 
	 * @return
	 */
	public String getFdPath() {
		String path = propertiesXML.getProperty("config.system.fdPath");
		if (Util.isNull(path)) {
			path = "";
		}
		return path;
	}

	/**
	 * 获取MR输出路径，默认值为空字符串
	 * 
	 * @return
	 */
	public String getMROutputPath() {
		String str = propertiesXML.getProperty("config.mr.mrOutputPath");
		if (Util.isNull(str)) {
			str = "";
		}
		return str;
	}

	/**
	 * 获取模板文件目录，默认值为空字符串
	 * 
	 * @return
	 */
	public String getTempletPath() {
		String str = propertiesXML.getProperty("config.system.templetFilePath");
		if (Util.isNull(str)) {
			str = "";
		}
		return str;
	}

	/**
	 * 获取采集绑定端口，默认值：0
	 * 
	 * @return
	 */
	public int getCollectPort() {
		int port = 0;
		try {
			port = Integer.parseInt(propertiesXML.getProperty("config.system.port"));
		} catch (Exception e) {
		}
		if (port <= 0) {
			port = 0;
		}
		return port;
	}

	/**
	 * 是否删除采集临时文件，默认值：true
	 * 
	 * @return
	 */
	public boolean isDeleteLog() {
		boolean b = true;

		try {
			b = Boolean.parseBoolean(propertiesXML.getProperty("config.externalTool.sqlldr.isDelLog"));
		} catch (Exception e) {
		}
		return b;
	}

	/**
	 * 获取mrSource，默认值：1
	 * 
	 * @return
	 */
	public int getMRSource() {
		int i = 1;
		try {
			i = Integer.parseInt(propertiesXML.getProperty("config.mr.mrSource"));
		} catch (Exception e) {
		}
		if (i <= 0) {
			i = 1;
		}
		return i;
	}

	/**
	 * 获取sqlldr字符集，默认值：ZHS16GBK
	 * 
	 * @return
	 */
	public String getSqlldrCharset() {
		String s = propertiesXML.getProperty("config.externalTool.sqlldr.charset");

		if (Util.isNull(s)) {
			s = "ZHS16GBK";
		}

		return s;
	}
	
	/**
	 * 获取当系统调用sqlldr返回128时重试次数，
	 * @return
	 */
	public int getSqlldrRetrytims() {
		//最大不超过10(超过时取余)；默认为3(没有配置此节点的情况)；小于等于0时代表取消重试功能
		int i = 3;
		try {
			i = Integer.parseInt(propertiesXML.getProperty("config.externalTool.sqlldr.retrytims"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return i%11;
	}

	/**
	 * 获取前端机数量，默认值：1
	 * 
	 * @return
	 */
	public int getFrontNum() {
		int i = 1;
		try {
			i = Integer.parseInt(propertiesXML.getProperty("config.mr.frontNum"));
		} catch (Exception e) {
		}
		if (i <= 0) {
			i = 1;
		}
		return i;
	}

	/**
	 * 是否采集汇总一体化，默认为:true
	 * 
	 * @return
	 */
	public boolean isMRSingleCal() {
		boolean b = true;
		try {
			b = Boolean.parseBoolean(propertiesXML.getProperty("config.mr.mrSingleCal"));
		} catch (Exception e) {
		}
		return b;
	}

	/**
	 * 获取winrar安装目录，默认值为空字符串
	 * 
	 * @return
	 */
	public String getWinrarPath() {
		String str = propertiesXML.getProperty("config.system.zipTool");
		if (Util.isNull(str)) {
			str = "";
		}
		return str;
	}

	/**
	 * 获取TraceFilter2.exe程序路径，默认值为空字符串
	 * 
	 * @return
	 */
	public String getTraceFileter2Path() {
		String str = propertiesXML.getProperty("config.externalTool.traceFileter2Path");
		if (Util.isNull(str)) {
			str = "";
		}
		return str;
	}

	/**
	 * 获取ParseFix.exe程序路径，默认值为空字符串
	 * 
	 * @return
	 */
	public String getParseFix() {
		String str = propertiesXML.getProperty("config.externalTool.parseFix");
		if (Util.isNull(str)) {
			str = "";
		}
		return str;
	}

	/**
	 * 获取采集最大线程数，默认值：15
	 * 
	 * @return
	 */
	public int getMaxThread() {
		int i = 15;
		try {
			i = Integer.parseInt(propertiesXML.getProperty("config.system.maxThreadCount"));
		} catch (Exception e) {
		}
		if (i < 0) {
			i = 0;
		}
		return i;
	}

	/**
	 * 扫描补采任务时最大select数，默认值：10
	 * 
	 * @return
	 */
	public int getMaxCountPerRegather() {
		int i = 10;
		try {
			i = Integer.parseInt(propertiesXML.getProperty("config.system.maxCountPerRegather"));
		} catch (Exception e) {
		}
		if (i <= 0) {
			i = 10;
		}
		return i;
	}

	/**
	 * 获取mrProcessId，默认值：0
	 * 
	 * @return
	 */
	public int getMRProcessId() {
		int i = 0;
		try {
			i = Integer.parseInt(propertiesXML.getProperty("config.mr.mrProcessId"));
		} catch (Exception e) {
		}
		return i;
	}

	/**
	 * 获取mrZipFlag，默认值：0
	 * 
	 * @return
	 */
	public int getMRZipFlag() {
		int i = 0;
		try {
			i = Integer.parseInt(propertiesXML.getProperty("config.mr.mrZipFlag"));
		} catch (Exception e) {
		}
		return i;
	}

	/**
	 * 获取siteDistRange，默认值：0
	 * 
	 * @return
	 */
	public float getSiteDistRange() {
		float f = 0.00f;

		try {
			f = Float.parseFloat(propertiesXML.getProperty("config.mr.siteDistRange"));
		} catch (Exception e) {
		}

		return f;
	}

	/**
	 * 获取时间戳文件扩展名，默认值：.flag
	 * 
	 * @return
	 */
	public String getLifecycleFileExt() {
		String str = propertiesXML.getProperty("config.module.dataFileLifecycle.fileExt");
		if (Util.isNull(str)) {
			str = ".flag";
		}
		return str;
	}

	/**
	 * 获取文件生成周期，默认值：7天
	 * 
	 * @return
	 */
	public int getFilecycle() {
		int i = 20;
		try {
			i = Integer.parseInt(propertiesXML.getProperty("config.module.dataFileLifecycle.lifecycle"));
		} catch (Exception e) {
		}
		if (i < 0) {
			i = 20;
		}
		return i;
	}

	/**
	 * 是否在关闭数据周期模板时删除数据，默认值：true
	 * 
	 * @return
	 */
	public boolean isDeleteWhenOff() {
		boolean b = true;

		try {
			b = Boolean.parseBoolean(propertiesXML.getProperty("config.module.dataFileLifecycle.delWhenOff"));
		} catch (Exception e) {
		}
		return b;
	}

	/**
	 * 获取locateJava，默认值：1
	 * 
	 * @return
	 */
	public int getLocateJava() {
		int i = 1;
		try {
			i = Integer.parseInt(propertiesXML.getProperty("config.mr.location.locateJava"));
		} catch (Exception e) {
		}

		return i;
	}

	/**
	 * 获取locate，默认值：1
	 * 
	 * @return
	 */
	public int getLocate() {
		int i = 1;
		try {
			i = Integer.parseInt(propertiesXML.getProperty("config.mr.location.locate"));
		} catch (Exception e) {
		}

		return i;
	}

	/**
	 * 获取版本号，默认空字符串
	 * 
	 * @return
	 */
	public String getEdition() {
		String e = propertiesXML.getProperty("config.system.version.edition");
		if (Util.isNull(e)) {
			e = "";
		}
		return e;
	}

	/**
	 * 获取发布时间（yyyy-mm-dd hh:mm:ss），默认空字符串
	 * 
	 * @return
	 */
	public String getReleaseTime() {
		String d = propertiesXML.getProperty("config.system.version.releaseTime");
		if (Util.isNull(d)) {
			return "";
		}
		try {
			d = Util.getDateString(Util.getDate1(d));
		} catch (Exception e) {
			return "";
		}
		return d;
	}

	/**
	 * 获取发布时间,默认空字符串(配置文件里面是什么样子就是什么样子)
	 * 
	 * @return
	 */
	public String getReleaseTime1() {
		String d = propertiesXML.getProperty("config.system.version.releaseTime");
		if (Util.isNull(d)) {
			return "";
		}
		return d;
	}

	/**
	 * 是否MR快速定位，默认值：true
	 * 
	 * @return
	 */
	public boolean isMRFast() {
		boolean b = true;
		b = Boolean.parseBoolean(propertiesXML.getProperty("config.mr.location.fast"));
		return b;
	}

	// 以下为邮件相关信息

	/**
	 * 是否开启邮件发送模块 true 开启, false 关闭
	 */
	public boolean isEnableAlarm() {
		// chensijiang 20120531，不使用此功能，不稳定。
		return false;
	}

	/**
	 * 消息过滤器 如果过滤器为空，返回长度为0的list
	 * 
	 * @return
	 */
	public List<String> getFilters() {
		return propertiesXML.getPropertyes("config.module.alarm.filters.newAlarm.filter");
	}

	/**
	 * 邮件发送器
	 * 
	 * @return
	 */
	public String getSender() {
		String sender = null;
		sender = propertiesXML.getProperty("config.module.alarm.senderBean");
		return sender;
	}

	/**
	 * 获取smtp服务器地址，默认值为空字符串
	 * 
	 * @return
	 */
	public String getMailSMTPHost() {
		String host = null;
		host = propertiesXML.getProperty("config.externalTool.mail.smtp_host");
		return host;
	}

	/**
	 * 获取发送邮件的账户名，默认值为空字符串
	 * 
	 * @return
	 */
	public String getMailAccount() {
		String account = null;
		account = propertiesXML.getProperty("config.externalTool.mail.user");
		return account;
	}

	/**
	 * 获取发送邮件的密码，默认值为空字符串
	 * 
	 * @return
	 */
	public String getMailPassword() {
		String pwd = null;
		pwd = propertiesXML.getProperty("config.externalTool.mail.password");
		return pwd;
	}

	/**
	 * 获取所有邮件收件人地址，如长度为0，则表示没有
	 * 
	 * @return
	 */
	public String[] getMailTO() {
		String[] tos = null;
		String to = propertiesXML.getProperty("config.externalTool.mail.to");
		if (Util.isNotNull(to)) {
			tos = to.split(";");
		}
		return tos;
	}

	/**
	 * 获取所有邮件抄送地址，如长度为0，则表示没有
	 * 
	 * @return
	 */
	public String[] getMailCC() {
		String[] ccs = null;
		String cc = propertiesXML.getProperty("config.externalTool.mail.cc");
		if (Util.isNotNull(cc)) {
			ccs = cc.split(";");
		}
		return ccs;
	}

	/**
	 * 获取所有邮件暗送地址，如长度为0，则表示没有
	 * 
	 * @return
	 */
	public String[] getMailBCC() {
		String[] bccs = null;
		String bcc = propertiesXML.getProperty("config.externalTool.mail.bcc");
		if (Util.isNotNull(bcc)) {
			bccs = bcc.split(";");
		}
		return bccs;
	}

	/**
	 * 是否开启文件周期模块，默认为false
	 * 
	 * @return
	 */
	public boolean isEnableDataFileLifecycle() {
		/* 20120528,chensijiang,此功能有严重问题，不再使用，用TempFileCleaner代替。 */
		return false;
	}

	/**
	 * 获取字段名相似度，最大值1.0f，默认值：0.8f
	 * 
	 * @return 字段名相似度
	 */
	public float getFieldMatch() {
		String str = propertiesXML.getProperty("config.system.fieldMatch");
		float f = 0.8f;
		try {
			f = Float.parseFloat(str);
		} catch (Exception e) {
			return 0.8f;
		}
		return f;
	}

	/**
	 * 获取最大正常任务线程数，默认200
	 * 
	 * @return 最大正常任务线程数，默认200
	 */
	public int getMaxCltCount() {
		String str = propertiesXML.getProperty("config.system.maxCltCount");
		int i = 200;
		try {
			i = Integer.parseInt(str);
		} catch (Exception e) {
		}
		return i;
	}

	/**
	 * 获取最大补采任务线程数，默认10
	 * 
	 * @return 最大补采任务线程数，默认10
	 */
	public int getMaxRecltCount() {
		String str = propertiesXML.getProperty("config.system.maxRecltCount");
		int i = 10;
		try {
			i = Integer.parseInt(str);
		} catch (Exception e) {
		}
		return i;
	}

	// web配置开始-----------------------------------
	/**
	 * 是否开启Web模块，默认为true | on
	 * 
	 * @return
	 */
	public boolean isEnableWeb() {
		boolean b = false;
		String str = propertiesXML.getProperty("config.module.web.enable");
		if (Util.isNull(str)) {
			return b;
		}
		str = str.toLowerCase().trim();
		if (str.equals("on") || str.equals("true")) {
			b = true;
		} else if (str.equals("off") || str.equals("false")) {
			b = false;
		}
		return b;
	}

	public int getWebPort() {
		int port = 8080;
		try {
			port = Integer.parseInt(propertiesXML.getProperty("config.module.web.port"));
		} catch (Exception e) {
		}
		if (port <= 0) {
			port = 8080;
		}
		return port;
	}

	public String getWebServerClass() {
		return propertiesXML.getProperty("config.module.web.httpServer.class");
	}

	public String getWebApp() {
		return propertiesXML.getProperty("config.module.web.httpServer.webapp");
	}

	public String getWebContextPath() {
		return propertiesXML.getProperty("config.module.web.httpServer.contextpath");
	}

	public String getWebCharset() {
		return propertiesXML.getProperty("config.module.web.charset");
	}

	/** 用户自定义log级别，0为debug，1为info，2为warn，3为error，4为fatal */
	public String getWebServerLogLevel() {
		String str = propertiesXML.getProperty("system.web.httpServer.loglevel");
		if (str == null || str.equals("") || str.equalsIgnoreCase("info"))
			str = "1";
		else if (str.equalsIgnoreCase("debug"))
			str = "0";
		else if (str.equalsIgnoreCase("warn"))
			str = "2";
		else if (str.equalsIgnoreCase("error"))
			str = "3";
		else if (str.equalsIgnoreCase("fatal"))
			str = "4";
		else
			str = "1";

		return str;
	}

	/**
	 * 获取数据库日志提交间隔，默认100
	 * 
	 * @return 数据库日志提交间隔，默认100
	 */
	public int getDataLogInterval() {
		int interval = 100;
		try {
			interval = Integer.parseInt(propertiesXML.getProperty("config.module.dataLog.interval"));
		} catch (Exception e) {
		}
		if (interval <= 0) {
			interval = 100;
		}
		return interval;
	}

	/**
	 * 是否开启数据库日志，默认false
	 * 
	 * @return 是否开启数据库日志，默认false
	 */
	public boolean isEnableDataLog() {
		// chensijiang 20120531
		// ，暂不启用此功能，因为无人使用，并且记在数据库中的日志也没人，而且日志表没有被维护，数量会一直增加。
		return false;
	}

	/**
	 * 是否以sqlldr方式入库数据库日志，默认false
	 * 
	 * @return 是否以sqlldr方式入库数据库日志，默认false
	 */
	public boolean isSqlldrDataLog() {
		String str = propertiesXML.getProperty("config.module.dataLog.sqlldrMode");
		if (Util.isNull(str)) {
			return false;
		}
		return str.trim().equalsIgnoreCase("true");
	}

	/**
	 * 是否删除数据库日志的临时文件，默认true
	 * 
	 * @return 是否删除数据库日志的临时文件，默认true
	 */
	public boolean isDelDataLogTmpFile() {
		String str = propertiesXML.getProperty("config.module.dataLog.delTmpFile");
		if (Util.isNull(str)) {
			return true;
		}
		return str.trim().equalsIgnoreCase("true");
	}

	/**
	 * 是否开启延时探针，默认false
	 * 
	 * @return 是否开启延时探针，默认false
	 */
	public boolean isEnableDelayProbe() {
		// chensijiang 20120531，不使用此功能，对厂家数据库或FTP造成压力，并且影响效率，及正常采集的成功率。
		return false;
	}

	/**
	 * 延时探针的探测次数，即连续探测到几次数据相等时确定数据已生成。默认值5
	 * 
	 * @return
	 */
	public int getDelayProbeTimes() {
		String str = propertiesXML.getProperty("config.module.delayProbe.probeTimes");

		try {
			int times = Integer.parseInt(str);
			return times;
		} catch (Exception e) {
		}

		return 5;
	}

	/**
	 * 探针间隔时间，单位分钟，默认5分钟
	 * 
	 * @return
	 */
	public int getProbeInterval() {
		String str = propertiesXML.getProperty("config.module.delayProbe.interval");

		try {
			int interval = Integer.parseInt(str);
			if (interval <= 0) {
				return 5;
			}
			return interval;
		} catch (Exception e) {
		}

		return 5;
	}

	/**
	 * 是否对FTP方式采集进行探测，默认false
	 * 
	 * @return
	 */
	public boolean isProbeFTP() {
		String str = propertiesXML.getProperty("config.module.delayProbe.ftp");
		try {
			return str.trim().equalsIgnoreCase("true");
		} catch (Exception e) {
		}
		return false;
	}

	public List<String> getReservALRnc() {
		List<String> list = new ArrayList<String>();
		String str = propertiesXML.getProperty("config.w-al-reserv-rnc");
		if (Util.isNotNull(str)) {
			String[] ss = str.split(",");
			for (String s : ss) {
				if (Util.isNotNull(s))
					list.add(s.trim());
			}
		}
		return list;
	}

	/**
	 * 是否开启探针日志，默认true
	 * 
	 * @return
	 */
	public boolean isEnableProbeLog() {
		String str = propertiesXML.getProperty("config.module.delayProbe.log");
		try {
			return str.trim().equalsIgnoreCase("true");
		} catch (Exception e) {
		}
		return true;
	}

	public boolean isFtpPortMode() {
		String str = propertiesXML.getProperty("config.system.ftpPortMode");
		try {
			// liangww add 2012-05-21 增加str null的判断
			return (Util.isNotNull(str) && str.trim().equalsIgnoreCase("true"));
		} catch (Exception e) {
		}

		return false;
	}
	
	public int getFtpSingleFileTimeOut() {
		String str = propertiesXML.getProperty("config.system.ftpSingleFileTimeOut");
		int timeOut = -1;
		try {
			if(Util.isNotNull(str) )
				timeOut = Integer.parseInt(str);
		} catch (Exception e) {
		}
		return timeOut;
	}

	public boolean isSPAS() {
		String str = propertiesXML.getProperty("config.system.isSPAS");
		try {
			return (Util.isNotNull(str) && str.equals("true"));
		} catch (Exception e) {
		}
		return false;
	}

	public int getSpasLoggingPort() {
		int val = 514;
		String str = propertiesXML.getProperty("config.system.spasLoggingPort");
		try {
			val = Integer.parseInt(str);
		} catch (Exception e) {
		}
		if (val <= 0)
			val = 514;
		return val;
	}

	/** 是否开启临时文件清理功能，默认不开启。 */
	public boolean isEnableTempFileCleaner() {
		String str = propertiesXML.getProperty("config.tempFileCleaner.enable");
		try {
			if (str.equalsIgnoreCase("on") || str.equalsIgnoreCase("true"))
				return true;
		} catch (Exception e) {
		}
		return false;
	}

	/**
	 * 临时文件清理功能的文件保留时间，单位为分钟。默认4320分钟（即3天）。
	 * 
	 * @return
	 */
	public int getTempFileCleanerTimeMinutes() {
		String str = propertiesXML.getProperty("config.tempFileCleaner.timeMinutes");
		int i = 4320;
		try {
			i = Integer.parseInt(str);
		} catch (Exception e) {
		}
		return i > 60 ? i : 4320;
	}

	/**
	 * 文件清理功能的文件扩展名。
	 * 
	 * @return
	 */
	public String[] getTempFileCleanerExtensions() {
		String[] exs = {"ctl", "txt", "log", "bad"};
		String str = propertiesXML.getProperty("config.tempFileCleaner.extensions");
		try {
			return str.split(",");
		} catch (Exception e) {
		}
		return exs;
	}

	/**
	 * 文件清理功能的清理间隔时间。默认60分钟。
	 * 
	 * @return
	 */
	public int getTempFileCleanerIntervalMinutes() {
		int i = 60;
		String str = propertiesXML.getProperty("config.tempFileCleaner.intervalMinutes");
		try {
			i = Integer.parseInt(str);
		} catch (Exception e) {
		}
		return i >= 60 ? i : 60;
	}

	public boolean isEnabelFtpServer() {
		String str = propertiesXML.getProperty("config.ftpServer.enable");
		if (str != null && (str.trim().equalsIgnoreCase("true") || str.trim().equalsIgnoreCase("on"))) {
			return true;
		}
		return false;
	}

	public String getFtpServerRootDir() {
		String str = propertiesXML.getProperty("config.ftpServer.rootDir");
		return str;
	}

	public int getFtpServerPort() {
		String str = propertiesXML.getProperty("config.ftpServer.port");
		try {
			return Integer.parseInt(str);
		} catch (Exception e) {
			return 21;
		}
	}

	public String getFtpServerUsername() {
		String str = propertiesXML.getProperty("config.ftpServer.username");
		return str;
	}

	public String getFtpServerPassword() {
		String str = propertiesXML.getProperty("config.ftpServer.password");
		return str;
	}

	public boolean isFtpServerCanWrite() {
		String str = propertiesXML.getProperty("config.ftpServer.canWrite");
		if (str != null) {
			str = str.trim().toLowerCase();
			if (str.equals("true"))
				return true;
		}
		return false;
	}

	/**
	 * @return
	 */
	public String getGpfdistRootUrl() {
		return propertiesXML.getProperty("config.storeDb.gpfdistRootUrl");
	}

	/**
	 * @return
	 */
	public String getGpfdistLocalPath() {
		return propertiesXML.getProperty("config.storeDb.gpfdistLocalPath");
	}

	/**
	 * 获取存储类型，默认是sqlldr
	 * 
	 * @return
	 */
	public int getStoreType() {
		String driver = getStoreDbDriver();
		if (Util.isNull(driver)) {
			return StoreFactory.SQLLDR_STORE_TYPE;
		}

		if (driver.indexOf("oracle") != -1) {
			return StoreFactory.SQLLDR_STORE_TYPE;
		} else if (driver.indexOf("org.postgresql.Driver") != -1) {
			return StoreFactory.GP_STORE_TYPE;
		}

		return 0;
	}

	/**
	 * 获取数据库驱动类，默认值：oracle.jdbc.driver.OracleDriver
	 * 
	 * @return
	 */
	public String getStoreDbDriver() {
		String d = propertiesXML.getProperty("config.storeDb.driverClassName");

		if (Util.isNull(d)) {
			d = "oracle.jdbc.driver.OracleDriver";
		}

		return d;
	}

	/**
	 * 数据库采集查询对方库最大时间
	 * 
	 * @return
	 */
	public int getMaxCltSelectTime() {
		String d = propertiesXML.getProperty("config.system.maxCltSelectTime");

		if (Util.isNull(d)) {
			d = "30";
		}

		return Integer.valueOf(d.trim());
	}

	/**
	 * 数据库采集查询对方库最大select并行数
	 * 
	 * @return
	 */
	public int getMaxCltSelectParallelCount() {
		String d = propertiesXML.getProperty("config.system.maxCltSelectParallelCount");

		if (Util.isNull(d)) {
			d = "2";
		}

		return Integer.valueOf(d.trim());
	}

	/**
	 * 获取数据库连接字符串，默认值为空字符串
	 * 
	 * @return
	 */
	public String getStoreDbUrl() {
		String url = propertiesXML.getProperty("config.storeDb.url");

		if (Util.isNull(url)) {
			url = "";
		}

		return url;
	}

	/**
	 * 获取数据库用户名，默认为空字符串
	 * 
	 * @return
	 */
	public String getStoreDbUserName() {
		return propertiesXML.getProperty("config.storeDb.user");
	}

	/**
	 * 获取数据库密码，默认为空字符串
	 * 
	 * @return
	 */
	public String getStoreDbPassword() {
		return propertiesXML.getProperty("config.storeDb.password");
	}

	/**
	 * 获取数据库密码，默认为空字符串
	 * 
	 * @return
	 */
	public String getZipFileSuffixs() {
		return propertiesXML.getProperty("config.system.zipFileSuffixs");
	}

	public String getProvince() {
		return propertiesXML.getProperty("config.system.province");
	}

	/**
	 * 获取厂家数据库连接池是否启用，默认值：关闭(false/off)
	 * 
	 * @return
	 */
	public boolean getPoolMaxActiveRemoteEnable() {
		boolean flag = false;
		String enable = propertiesXML.getProperty("config.db.remote.enable");
		if ("true".equalsIgnoreCase(enable) || "on".equalsIgnoreCase(enable)) {
			flag = true;
		}
		return flag;
	}

	/**
	 * 获取连接池最大活动连接数，默认值：12
	 * 
	 * @return
	 */
	public int getPoolMaxActiveRemote() {
		int ma = 12;
		try {
			ma = Integer.parseInt(propertiesXML.getProperty("config.db.remote.maxActive"));
		} catch (Exception e) {
		}
		if (ma <= 0) {
			ma = 12;
		}
		return ma;
	}

	/**
	 * 获取连接池最大等待数，默认值：240000
	 * 
	 * @return
	 */
	public int getPoolMaxWaitRemote() {
		int maxWait = 240000;
		try {
			maxWait = Integer.parseInt(propertiesXML.getProperty("config.db.remote.maxWait"));
		} catch (Exception e) {
		}
		if (maxWait <= 0) {
			maxWait = 240000;
		}
		return maxWait;
	}

	/**
	 * 获取连接池最大活动空闲连接数，默认值：5
	 * 
	 * @return
	 */
	public int getPoolMaxIdleRemote() {
		int maxIdle = 5;
		try {
			maxIdle = Integer.parseInt(propertiesXML.getProperty("config.db.remote.maxIdle"));
		} catch (Exception e) {
		}
		if (maxIdle <= 0) {
			maxIdle = 5;
		}
		return maxIdle;
	}

	/**
	 * 路测采集，采集中用于配制 异常事件代码
	 * 
	 * @return
	 */
	public String getHwdtCdmaProblemCodes() {
		String codes = propertiesXML.getProperty("config.hwdtcdma.codes");
		if (Util.isNull(codes)) {
			codes = "1007,1013,1015,1303,106B,106E,1305,1328,1338";
		}
		return codes;
	}

	/**
	 * W网华为性能第三方ip
	 * 
	 * @return
	 */
	public String getHwPmSocketIp() {
		String ip = propertiesXML.getProperty("config.socketInfo.hwPm.ip");
		return ip;
	}

	/**
	 * W网爱立信性能第三方ip
	 * 
	 * @return
	 */
	public String getEricPmSocketIp() {
		String ip = propertiesXML.getProperty("config.socketInfo.ericPm.ip");
		return ip;
	}

	/**
	 * W网华为性能第三方port
	 * 
	 * @return
	 */
	public int getHwPmSocketPort() {
		int port = 0;
		try {
			port = Integer.parseInt(propertiesXML.getProperty("config.socketInfo.hwPm.port"));
		} catch (Exception e) {
		}
		return port;
	}

	/**
	 * W网爱立信性能第三方port
	 * 
	 * @return
	 */
	public int getEricPmSocketPort() {
		int port = 0;
		try {
			port = Integer.parseInt(propertiesXML.getProperty("config.socketInfo.ericPm.port"));
		} catch (Exception e) {
		}
		return port;
	}

	public String getTmpTestOmcId() {
		String ip = propertiesXML.getProperty("config.tmp.omc_id");
		return ip;
	}

	public String getTmpTestpath() {
		String ip = propertiesXML.getProperty("config.tmp.src_path");
		return ip;
	}

	public String getTmpTestDataTime() {
		String ip = propertiesXML.getProperty("config.tmp.data_time");
		return ip;
	}

	public String getTmpTestTaskId() {
		String ip = propertiesXML.getProperty("config.tmp.task_id");
		return ip;
	}

	public static void main(String[] args) {
		System.out.println(SystemConfig.getInstance().getSqlldrRetrytims());
		// System.out.println(SystemConfig.getInstance().getDbPassword());
		// SystemConfig c = SystemConfig.getInstance();
		// Class<?> clz = c.getClass();
		// Method[] ms = clz.getDeclaredMethods();
		// for (Method m : ms)
		// {
		// if ( m.getName().startsWith("get") || m.getName().startsWith("is") )
		// {
		// try
		// {
		// logger.info(m.getName() + "  =  " + m.invoke(c));
		// }
		// catch (Exception e)
		// {
		// e.printStackTrace();
		// }
		//
		// }
		// }

		// System.out.println(""+SystemConfig.getInstance().getMaxCltSelectParallelCount());
		// DESDecryptor des = new DESDecryptor();
		// System.out.println(des.desDecrypt("YCQWxx09MAq/crZbLGBnEvpyc2kZwa4JcY5a+YMpkPvL49j4fwMgLA==", "UWAY@SOFT2009"));

	}
}

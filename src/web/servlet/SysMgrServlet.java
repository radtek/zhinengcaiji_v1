package web.servlet;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import util.LogMgr;
import util.PropertiesXML;
import db.pojo.ActionResult;
import framework.IGPError;
import framework.SystemConfig;
import framework.SystemConfigException;

/**
 * 系统管理 参数设置
 * 
 * @author liuwx 2010-6-7
 */
public class SysMgrServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7132118517401814450L;

	private static final String configPath = "./conf/config.xml";

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		doPost(req, resp);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		req.setCharacterEncoding("gbk");
		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");

		String action = req.getParameter("action");

		PropertiesXML properties = null;

		if (action != null) {

			if (action.equals("show")) {
				// 数据库配置模块管理
				req.setAttribute("name", SystemConfig.getInstance().getDbUserName());
				req.setAttribute("type", SystemConfig.getInstance().getPoolType());
				req.setAttribute("driverClassName", SystemConfig.getInstance().getDbDriver());
				req.setAttribute("url", SystemConfig.getInstance().getDbUrl());
				req.setAttribute("service", SystemConfig.getInstance().getDbService());

				req.setAttribute("user", SystemConfig.getInstance().getDbUserName());
				req.setAttribute("password", SystemConfig.getInstance().getDbPassword());
				req.setAttribute("maxActive", SystemConfig.getInstance().getPoolMaxActive());
				req.setAttribute("maxIdle", SystemConfig.getInstance().getPoolMaxIdle());
				req.setAttribute("maxWait", SystemConfig.getInstance().getPoolMaxWait());
				req.setAttribute("validationQuery", SystemConfig.getInstance().getDbValidationQueryString());

				// web模块配置管理
				req.setAttribute("webServerClass", SystemConfig.getInstance().getWebServerClass());
				req.setAttribute("webApp", SystemConfig.getInstance().getWebApp());
				req.setAttribute("webContextPath", SystemConfig.getInstance().getWebContextPath());
				req.setAttribute("loglevel", SystemConfig.getInstance().getWebServerLogLevel());
				req.setAttribute("isEnableWeb", SystemConfig.getInstance().isEnableWeb() == true ? "true" : "false");

				req.setAttribute("charset", SystemConfig.getInstance().getWebCharset());

				// 告警配置管理
				req.setAttribute("alarmEnable", SystemConfig.getInstance().isEnableAlarm() == true ? "true" : "false");

				req.setAttribute("senderBean", SystemConfig.getInstance().getSender());
				List<String> alarmFilters = SystemConfig.getInstance().getFilters();
				StringBuilder sb = new StringBuilder();
				for (String filter : alarmFilters) {
					sb.append(filter + "\r\n");
				}
				req.setAttribute("filters", sb.toString());
				sb.delete(0, sb.length());
				req.setAttribute("dataFileLifecycleEnable", SystemConfig.getInstance().isEnableDataFileLifecycle() == true ? "true" : "false");
				req.setAttribute("fileExt", SystemConfig.getInstance().getLifecycleFileExt());
				req.setAttribute("lifecycle", SystemConfig.getInstance().getFilecycle());
				req.setAttribute("delWhenOff", SystemConfig.getInstance().isDeleteWhenOff() == true ? "true" : "false");

				// 系统配置管理
				req.setAttribute("currentPath", SystemConfig.getInstance().getCurrentPath());
				req.setAttribute("templetFilePath", SystemConfig.getInstance().getTempletPath());
				req.setAttribute("fieldMatch", SystemConfig.getInstance().getFieldMatch());
				req.setAttribute("port", SystemConfig.getInstance().getCollectPort());
				req.setAttribute("winrarPath", SystemConfig.getInstance().getWinrarPath());
				req.setAttribute("maxThreadCount", SystemConfig.getInstance().getMaxThread());
				req.setAttribute("maxCltCount", SystemConfig.getInstance().getMaxCltCount());
				req.setAttribute("maxRecltCount", SystemConfig.getInstance().getMaxRecltCount());

				req.setAttribute("maxCountPerRegather", SystemConfig.getInstance().getMaxCountPerRegather());
				req.setAttribute("edition", SystemConfig.getInstance().getEdition());
				req.setAttribute("releaseTime", SystemConfig.getInstance().getReleaseTime());

			}

			else {

				try {
					properties = new PropertiesXML(configPath);
				} catch (SystemConfigException e1) {
					e1.printStackTrace();
				}
				if (action.equals("system")) {
					try {
						// 系统配置更新
						properties.setProperty("config.system.currentPath", (String) req.getParameter("currentPath"));
						properties.setProperty("config.system.templetFilePath", (String) req.getParameter("templetFilePath"));
						properties.setProperty("config.system.fieldMatch", (String) req.getParameter("fieldMatch"));
						properties.setProperty("config.system.port", (String) req.getParameter("port"));
						properties.setProperty("config.system.zipTool", (String) req.getParameter("winrarPath"));
						properties.setProperty("config.system.maxThreadCount", (String) req.getParameter("maxThreadCount"));
						properties.setProperty("config.system.maxCltCount", (String) req.getParameter("maxCltCount"));
						properties.setProperty("config.system.maxRecltCount", (String) req.getParameter("maxRecltCount"));
						properties.setProperty("config.system.maxCountPerRegather", (String) req.getParameter("maxCountPerRegather"));
					} catch (SystemConfigException e) {
						logger.error("系统配置更新失败", e);
					}
				} else if (action.equals("db")) {
					// 数据库设置更新
					try {
						properties.setProperty("config.db.type", (String) req.getParameter("type"));
						properties.setProperty("config.db.driverClassName", (String) req.getParameter("driverClassName"));
						properties.setProperty("config.db.url", (String) req.getParameter("url"));
						properties.setProperty("config.db.service", (String) req.getParameter("service"));
						properties.setProperty("config.db.user", (String) req.getParameter("dbname"));
						properties.setProperty("config.db.password", (String) req.getParameter("password"));
						properties.setProperty("config.db.maxActive", (String) req.getParameter("maxActive"));
						properties.setProperty("config.db.maxIdle", (String) req.getParameter("maxIdle"));
						properties.setProperty("config.db.maxWait", (String) req.getParameter("maxWait"));
					} catch (SystemConfigException e) {
						logger.error("数据库配置更新失败", e);
					}
				} else if (action.equals("module")) {
					// web模块更新
					try {
						// properties.setProperty("config.module.web.httpServer.class", (String) req.getParameter("webServerClass"));
						// properties.setProperty("config.module.web.httpServer.webapp", (String) req.getParameter("webApp"));
						// properties.setProperty("config.module.web.httpServer.contextpath", (String) req.getParameter("webContextPath"));
						String loglevel = null;
						String str = (String) req.getParameter("loglevel");
						if (str == null || str.equals("") || str.equalsIgnoreCase("1"))
							loglevel = "INFO";
						else if (str.equalsIgnoreCase("0"))
							loglevel = "DEBUG";
						else if (str.equalsIgnoreCase("2"))
							loglevel = "WARN";
						else if (str.equalsIgnoreCase("3"))
							loglevel = "ERROR";
						else if (str.equalsIgnoreCase("4"))
							loglevel = "FATAL";
						properties.setProperty("config.module.web.httpServer.loglevel", loglevel);

						String alarmEnable = (String) req.getParameter("alarmEnable");
						String alarmEnableValue = "off";
						if (alarmEnable.trim().equals("true")) {
							alarmEnableValue = "on";
						}
						properties.setProperty("config.module.alarm.enable", alarmEnableValue);
						properties.setProperty("config.module.web.httpServer.loglevel", loglevel);
						String enable = (String) req.getParameter("isEnableWeb");
						String enableValue = "off";
						if (enable.trim().equals("true")) {
							enableValue = "on";
						}
						properties.setProperty("config.module.web.enable", enableValue);
						properties.setProperty("config.module.web.charset", (String) req.getParameter("charset"));
						// properties.setProperty("config.module.alarm.senderBean", (String) req.getParameter("senderBean"));
						// properties.setProperty("config.module.alarm.filters.newAlarm.filter", (String) req.getParameter("filters"));
						properties.setProperty("config.module.dataFileLifecycle.lifecycle", (String) req.getParameter("lifecycle"));
						properties.setProperty("config.module.dataFileLifecycle.delWhenOff", (String) req.getParameter("delWhenOff"));

					} catch (SystemConfigException e) {
						logger.error("web或告警配置更新失败", e);
					}
				}
			}
			ActionResult result = new ActionResult();
			result.setError(new IGPError());
			result.setData("成功");
			result.setForwardURL(forwardURL);
			result.setReturnURL(returnURL);
			req.setAttribute("result", result);
			req.getRequestDispatcher(forwardURL).forward(req, resp);
		}
	}
}

package util;

import java.io.File;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * 日志管理器
 * 
 * @author YangJian
 * @version 1.0
 */
public class LogMgr {

	private static LogMgr instance = null;

	private Logger systemLog; // 系统日志

	private LogMgr() {
		try {
			File f = new File("." + File.separator + "conf" + File.separator + "config.xml");
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(f);
			Element log4jEl = (Element) doc.getDocumentElement().getElementsByTagName("log4j").item(0);
			log4jEl = (Element) log4jEl.getElementsByTagName("log4j:configuration").item(0);
			DOMConfigurator.configure(log4jEl);
			systemLog = Logger.getLogger("system");
		} catch (Exception e) {
			System.err.println("配置log4j时发生异常请检查conf目录下的config.xml文件，节点：/config/log4j/configuration");
			e.printStackTrace();
		}
	}

	public synchronized static LogMgr getInstance() {
		if (instance == null) {
			instance = new LogMgr();
		}

		return instance;
	}

	/** 获取系统日志 */
	public Logger getSystemLogger() {
		return systemLog;
	}

	public DBLogger getDBLogger() {
		return DBLogger.getInstance();
	}

	public SPASLogger getSPASLogger() {
		return SPASLogger.getInstance();
	}
}

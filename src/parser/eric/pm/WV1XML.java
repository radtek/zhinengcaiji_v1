package parser.eric.pm;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

import collect.FTPConfig;

import parser.Parser;
import util.LogMgr;
import framework.SystemConfig;

public class WV1XML extends Parser {

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	@Override
	public boolean parseData() {
		logger.debug(collectObjInfo.getTaskID() + ":开始解析W网爱立信FTP性能：" + fileName);
		File f = new File(fileName);
		InputStream in = null;
		if (!f.exists()) {
			logger.error(collectObjInfo.getTaskID() + ":文件不存在：" + fileName);
			return false;
		}

		try {
			EricssonWPmXMLParser p = new EricssonWPmXMLParser();
			p.parse(fileName, collectObjInfo.getDevInfo().getOmcID(), collectObjInfo.getLastCollectTime(), collectObjInfo.getTaskID());
			logger.debug(collectObjInfo.getTaskID() + ":W网爱立信FTP性能解析完成：" + fileName);
			return true;
		} catch (Exception e) {
			logger.error(collectObjInfo.getTaskID() + ":解析W网爱立信FTP性能时异常：" + fileName, e);
			return false;
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
			FTPConfig cfg = FTPConfig.getFTPConfig(collectObjInfo.getTaskID());
			boolean isDel = SystemConfig.getInstance().isDeleteWhenOff();
			if (cfg != null)
				isDel = cfg.isDelete();
			if (isDel) {
				f.delete();
			}
		}

	}

}

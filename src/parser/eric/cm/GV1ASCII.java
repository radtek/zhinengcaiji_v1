package parser.eric.cm;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

import parser.Parser;
import util.LogMgr;
import collect.FTPConfig;
import framework.SystemConfig;

/**
 * 联通一期爱立信FTP参数解析。
 * 
 * @author ChenSijiang 2010-4-27
 */
public class GV1ASCII extends Parser {

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	@Override
	public boolean parseData() {
		logger.debug(collectObjInfo.getTaskID() + ":开始解析联通一期爱立信FTP参数：" + fileName);
		File f = new File(fileName);
		InputStream in = null;
		if (!f.exists()) {
			logger.error(collectObjInfo.getTaskID() + ":文件不存在：" + fileName);
			return false;
		}

		try {
			long taskID = collectObjInfo.getTaskID();
			EricssonV1CmParserImp p = new EricssonV1CmParserImp();
			p.parse(fileName, collectObjInfo.getDevInfo().getOmcID(), collectObjInfo.getLastCollectTime(), taskID);
			logger.debug(taskID + ":联通一期爱立信FTP参数解析完成：" + fileName);
			return true;
		} catch (Exception e) {
			logger.error(collectObjInfo.getTaskID() + ":解析联通一期爱立信FTP参数时异常：" + fileName, e);
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

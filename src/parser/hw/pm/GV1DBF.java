package parser.hw.pm;

import java.io.File;

import org.apache.log4j.Logger;

import collect.FTPConfig;

import parser.Parser;
import util.LogMgr;
import framework.SystemConfig;

public class GV1DBF extends Parser {

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	@Override
	public boolean parseData() {
		File f = new File(fileName);

		HWV3R8Parser parser = new HWV3R8Parser(collectObjInfo);
		logger.info(collectObjInfo.getTaskID() + "-开始解析dbf文件:" + fileName);
		boolean b = parser.parser(f);
		if (b) {
			logger.info(collectObjInfo.getTaskID() + "-dbf文件解析成功:" + fileName);
		} else {
			logger.info(collectObjInfo.getTaskID() + "-dbf文件解析失败:" + fileName);
		}
		FTPConfig cfg = FTPConfig.getFTPConfig(collectObjInfo.getTaskID());
		boolean isDel = SystemConfig.getInstance().isDeleteWhenOff();
		if (cfg != null)
			isDel = cfg.isDelete();
		if (isDel) {
			f.delete();
		}
		return b;
	}

}

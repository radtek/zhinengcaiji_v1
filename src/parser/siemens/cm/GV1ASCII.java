package parser.siemens.cm;

import java.io.File;

import collect.FTPConfig;

import parser.Parser;
import framework.SystemConfig;

/**
 * G网西门子参数，文件方式解析。
 * 
 * @author ChenSijiang 2010.07.09
 * @since 3.1
 */
public class GV1ASCII extends Parser {

	@Override
	public boolean parseData() throws Exception {
		SiemensCmParser parser = new SiemensCmParser();
		long taskId = collectObjInfo.getTaskID();
		boolean b = false;
		log.debug(taskId + "-开始解析:" + fileName);
		b = parser.parse(fileName, collectObjInfo);
		if (b) {
			log.debug(taskId + "-解析成功:" + fileName);
		} else {
			log.debug(taskId + "-解析失败:" + fileName);
		}
		FTPConfig cfg = FTPConfig.getFTPConfig(collectObjInfo.getTaskID());
		boolean isDel = SystemConfig.getInstance().isDeleteWhenOff();
		if (cfg != null)
			isDel = cfg.isDelete();
		if (isDel) {
			new File(fileName).delete();
		}
		return b;
	}

}

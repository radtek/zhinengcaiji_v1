package parser.lucent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

import collect.FTPConfig;

import parser.Parser;
import parser.lucent.evdo.EvdoParser;
import parser.lucent.evdo.EvdoParserCallback;
import parser.lucent.evdo.LucentEvdoParser;
import parser.lucent.evdo.LucentEvdoParserCallback;
import parser.lucent.evdo.LucentEvdoParserCallbackHCSFMS;
import parser.lucent.evdo.LucentEvdoParserHCSFMS;
import util.LogMgr;
import util.Util;
import datalog.DataLogInfo;
import framework.SystemConfig;

/**
 * 阿尔卡特-朗讯EVDO性能解析。
 * 
 * @author ChenSijiang 2010-4-26
 */
public class CV1ASCII extends Parser {

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	@Override
	public boolean parseData() {
		logger.debug(collectObjInfo.getTaskID() + ":开始解析朗讯EVDO性能：" + fileName);
		File f = new File(fileName);
		if (!f.exists()) {
			logger.error(collectObjInfo.getTaskID() + ":文件不存在：" + fileName);
			return false;
		}
		InputStream in = null;
		EvdoParser evdoParser = null;
		EvdoParserCallback callback = null;
		try {
			in = new FileInputStream(f);
			if (f.getName().contains("HCSFMS")) {
				evdoParser = new LucentEvdoParserHCSFMS(in);
				callback = new LucentEvdoParserCallbackHCSFMS();
			} else if (f.getName().contains("HDRFMS")) {
				evdoParser = new LucentEvdoParser(in);
				callback = new LucentEvdoParserCallback();
			} else {
				logger.error(collectObjInfo.getTaskID() + ":无法识别的文件:" + fileName);
			}
			evdoParser.parse(callback, String.valueOf(collectObjInfo.getDevInfo().getOmcID()), collectObjInfo.getLastCollectTime(), true,
					collectObjInfo.getTaskID());
			logger.debug(collectObjInfo.getTaskID() + ":朗讯EVDO性能解析完成：" + fileName);
			return true;
		} catch (Exception e) {
			logger.error(collectObjInfo.getTaskID() + ":解析朗讯性能时异常：" + fileName, e);
			return false;
		} finally {
			if (evdoParser != null) {
				evdoParser.dispose();
			}
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

	// add
	// 剩余数据
	String remainingData = "";

	// 计算采集的次数
	private int m_ParseTime = 0;

	public boolean buildAlData(char[] chData, int iLen, BufferedWriter bw) {
		boolean bReturn = true;

		remainingData += new String(chData, 0, iLen);

		String logStr = null;

		// 解析的次数
		if (++m_ParseTime % 100 == 0) {
			logStr = this + ": " + collectObjInfo.getDescribe() + " parse time:" + m_ParseTime;
			log.debug(logStr);
			collectObjInfo.log(DataLogInfo.STATUS_PARSE, logStr);
		}
		boolean bLastCharN = false;// 最后一个字符是\n
		if (remainingData.charAt(remainingData.length() - 1) == '\n')
			bLastCharN = true;

		// 分行
		String[] strzRowData = remainingData.split("\n");

		// 没有数据
		if (strzRowData.length == 0)
			return true;

		// 特殊标记表示达到最后一行
		int nRowCount = strzRowData.length - 1;
		remainingData = strzRowData[nRowCount];
		if (remainingData.equals("**FILEEND**"))
			remainingData = "";

		// 如果最后一个字符是\n 下次采集的时候,将是补上\n这个字符
		if (bLastCharN)
			remainingData += "\n";

		try {
			// 最后一行不解析,与下次数据一起解析
			for (int i = 0; i < nRowCount; ++i) {
				if (Util.isNull(strzRowData[i]))
					continue;
				bw.write(strzRowData[i] + "\n");
				bw.flush();
			}
		} catch (Exception e) {
			bReturn = false;
			logStr = this + ": Cause:";
			log.error(logStr, e);
			collectObjInfo.log(DataLogInfo.STATUS_PARSE, logStr, e);
		}

		return bReturn;
	}

}

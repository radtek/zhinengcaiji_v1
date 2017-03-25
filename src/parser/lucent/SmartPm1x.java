package parser.lucent;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import templet.lucent.SmartTemplet;
import util.DeCompression;
import util.LineReader;
import util.LogMgr;
import util.Util;

/**
 * CDMA阿朗1X性能，SMART数据，r36版本。
 * 
 * @author ChenSijiang 2012-7-5
 * @version 1.0.0 1.0.1 liangww 2012-07-17 增加initTemplets方法。templets只初始化一次<br>
 *          1.0.2 liangww 2012-07-24 增加对.smdump文件解压功能<br>
 */
public class SmartPm1x extends Parser {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	private String logKey;

	private List<SmartTemplet> templets = null;			// 模板list

	private List<String> srcFileNameList = null;		// 原始文件list

	/**
	 * 初始化initTemplets
	 * 
	 * @throws Exception
	 */
	private void initTemplets() throws Exception {
		//
		if (templets != null) {
			return;
		}
		String strStamptime = Util.getDateString(collectObjInfo.getLastCollectTime());
		logKey = "[" + collectObjInfo.getTaskID() + "][" + strStamptime + "]";

		templets = SmartTemplet.parseTemplet(collectObjInfo.getParseTmpRecord().getFileName());
		if (templets == null) {
			throw new Exception("模板读取失败。");
		} else {
			for (int i = 0; i < templets.size(); i++) {
				templets.get(i).setLogKey(logKey);
			}
		}
	}

	void decompress() {
		if (!fileName.endsWith(".smdump")) {
			// 添加到原始文件list
			srcFileNameList = new ArrayList<String>(1);
			srcFileNameList.add(fileName);
			return;
		}

		try {
			srcFileNameList = DeCompression.decompress(collectObjInfo.getTaskID(), collectObjInfo.getParseTemplet(), fileName,
					collectObjInfo.getLastCollectTime(), collectObjInfo.getPeriod(), false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error(logKey + "解压出现异常：" + fileName, e);
		}

	}

	@Override
	public boolean parseData() throws Exception {

		// 初始化templet
		initTemplets();
		// 解压文件
		decompress();

		if (srcFileNameList != null) {
			try {
				for (int i = 0; i < srcFileNameList.size(); i++) {
					parseFile(srcFileNameList.get(i));
				}
			} finally {
				for (int i = 0; i < srcFileNameList.size(); i++) {
					new File(srcFileNameList.get(i)).delete();
				}

				// 清除
				srcFileNameList.clear();
				srcFileNameList = null;
			}
		}

		return true;
	}

	boolean parseFile(String srcFileName) {
		File file = new File(srcFileName);
		InputStream in = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		LineReader lineReader = null;

		try {
			in = new FileInputStream(file);
			isr = new InputStreamReader(in);
			br = new BufferedReader(isr);
			lineReader = new LineReader(br);

			boolean bMove = false;
			while (lineReader.hasLine()) {
				for (int i = 0; i < templets.size(); i++) {
					bMove = true;
					SmartTemplet templet = templets.get(i);
					//
					if (templet.canParse(lineReader.getLine())) {
						templet.parse(lineReader, distribute);
						bMove = false;
						break;
					}
				}// for(int i = 0; i < templets.size(); i++)

				// 如果需要移动
				if (bMove) {
					lineReader.move();
				}
			}// while (lineReader.hasLine())
		} catch (Exception e) {
			log.error(logKey + "解析文件时异常：" + file, e);
			return false;
		} finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(isr);
			IOUtils.closeQuietly(in);
			this.distribute.commit();
		}
		return true;
	}

}

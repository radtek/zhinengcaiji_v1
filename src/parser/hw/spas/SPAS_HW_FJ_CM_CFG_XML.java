package parser.hw.spas;

import java.io.File;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.LogMgr;
import util.Util;

/**
 * 龙计划福建华为参数/配置，XML格式。
 * 
 * @author ChenSijiang 2012-6-20
 */
public class SPAS_HW_FJ_CM_CFG_XML extends Parser {

	private long taskid;

	private Timestamp stamptime;

	private String strStamptime;

	private String strCollecttime;

	private File xmlFile;

	private HW_XMLParser parser;

	private HW_XMLSqlldr sqlldr;

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	@Override
	public boolean parseData() throws Exception {
		taskid = collectObjInfo.getTaskID();
		stamptime = collectObjInfo.getLastCollectTime();
		strStamptime = Util.getDateString(stamptime);
		strCollecttime = Util.getDateString(new Date());
		xmlFile = new File(fileName);
		sqlldr = new HW_XMLSqlldr(collectObjInfo);

		log.debug("isSPAS=true");
		String nfileName = FilenameUtils.normalize(this.fileName);
		String zip = collectObjInfo.filenameMap.get(nfileName);
		if (zip == null || !zip.contains("_PARA_")) {
			log.warn(collectObjInfo.getTaskID() + " 文件" + nfileName + "，未找到对应的原始压缩包名。list=" + collectObjInfo.filenameMap);
			collectObjInfo.spasOmcId = String.valueOf(collectObjInfo.getDevInfo().getOmcID());
		} else {
			zip = FilenameUtils.getBaseName(zip);
			String[] sp = zip.split("_");
			collectObjInfo.spasOmcId = sp[5] + sp[6];
		}
		log.debug(taskid + " 开始解析华为参数/配置数据：" + xmlFile + "， 文件大小：" + xmlFile.length() + "字节");

		try {
			parser = new HW_XMLParser(xmlFile);
			HW_XML_MO_Entry entry = null;
			while ((entry = parser.parseNextMO()) != null) {
				if (entry.getClassName().equalsIgnoreCase("cdma1xch") || entry.getClassName().equalsIgnoreCase("cdmadoch")
						|| entry.getClassName().equalsIgnoreCase("cbsccbts") || entry.getClassName().equalsIgnoreCase("cbscg3sector")
						|| entry.getClassName().equalsIgnoreCase("cbscg3pilot")) {
					String tabName = "DS_CLT_CM_" + entry.getClassName() + "_HW";
					sqlldr.add(entry, tabName, strStamptime, strCollecttime);
				}

			}
			sqlldr.commit();
		} catch (Exception e) {
			log.error(taskid + " 解析华为参数过程中出错：" + xmlFile, e);
			return false;
		} finally {
			if (parser != null)
				parser.close();
		}

		return true;
	}

	public static void main(String[] args) throws Exception {
		SPAS_HW_FJ_CM_CFG_XML p = new SPAS_HW_FJ_CM_CFG_XML();
		CollectObjInfo task = new CollectObjInfo(123);
		task.setLastCollectTime(new Timestamp(0));
		p.collectObjInfo = task;
		DevInfo di = new DevInfo();
		di.setOmcID(1111);
		task.setDevInfo(di);
		p.fileName = "F:\\ftp_root\\home\\uway\\FJ\\CDMAExport_C_FJ_XM_BSS_1_134.134.12.226_2012061900.xml";
		p.parseData();
	}
}

package parser.hw.pm.w.xml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import parser.hw.pm.w.xml.Tools.MeasObjLdn;
import task.CollectObjInfo;
import task.DevInfo;
import util.LogMgr;
import util.Util;

public class WV1XML extends Parser {

	String rncName;

	String strStamptime;

	String strCollecttime;

	String omcId;

	String logKey;

	SqlldrTool sqlldr;

	static final Logger logger = LogMgr.getInstance().getSystemLogger();

	final Map<String, List<String>> TABLES_COLS = new HashMap<String, List<String>>();

	Map<String, String> map;

	private String vendor = "ZY0808";

	@Override
	public boolean parseData() throws Exception {

		if (strStamptime == null) {
			strStamptime = Util.getDateString(collectObjInfo.getLastCollectTime());
			strCollecttime = Util.getDateString(new Date());
			omcId = String.valueOf(collectObjInfo.getDevInfo().getOmcID());
			logKey = String.format("[%s][%s]", collectObjInfo.getTaskID(), strStamptime);
		}
		if (map == null) {
			map = Tools.readTemplet(collectObjInfo.getParseTmpID());
			if (map == null) {
				logger.error(logKey + "读取模板失败");
				return false;
			}
		}

		InputStream in = null;
		XMLStreamReader reader = null;
		String currTableName = null;
		String measInfoId = null;
		List<String> currMeasTypes = null;
		List<MeasObjLdn> currMeasObjLdn = null;
		List<String> currMeasResults = null;
		logger.debug(logKey + "解析文件---开始,fileName=" + fileName);
		try {
			in = new FileInputStream(fileName);
			reader = XMLInputFactory.newInstance().createXMLStreamReader(in);
			if (TABLES_COLS.size() == 0)
				Tools.loadTableCols(map.values(), TABLES_COLS);
			if (sqlldr == null) {
				sqlldr = new SqlldrTool(TABLES_COLS, collectObjInfo);
				sqlldr.init();
			}
			List<String> oneRowData = new ArrayList<String>();
			int type = -1;
			while (reader.hasNext()) {
				type = reader.next();
				String tagName = null;

				if (type == XMLStreamConstants.START_ELEMENT || type == XMLStreamConstants.END_ELEMENT)
					tagName = reader.getLocalName();

				if (tagName == null) {
					continue;
				}
				switch (type) {
					case XMLStreamConstants.START_ELEMENT :
						if (tagName.equalsIgnoreCase("measValue")) {
							if (currTableName != null) {
								currMeasObjLdn = Tools.parseMeasObjLdn(reader.getAttributeValue(null, "measObjLdn"));
							}
						} else if (tagName.equalsIgnoreCase("measResults")) {
							if (currTableName != null) {
								currMeasResults = Tools.parseMeasResults(reader.getElementText());
								setOneRowData(oneRowData, currMeasResults, currMeasTypes, currMeasObjLdn, currTableName);
								sqlldr.writeRow(oneRowData, currTableName);
							}
						} else if (tagName.equalsIgnoreCase("measTypes")) {
							if (currTableName != null) {
								currMeasTypes = Tools.parseMeasTypes(reader.getElementText());
								if (currMeasTypes == null)
									currTableName = null;
							}
						} else if (tagName.equalsIgnoreCase("measInfo")) {
							measInfoId = Util.nvl(reader.getAttributeValue(null, "measInfoId"), "");
							if (map.containsKey(measInfoId))
								currTableName = "CLT_PM_W_HW_X_" + measInfoId;
							else
								currTableName = null;

						} else if (tagName.equalsIgnoreCase("managedElement")) {
							rncName = Util.nvl(reader.getAttributeValue(null, "userLabel"), "");
						}
						break;
					default :
						break;
				}
			}

		} catch (Exception e) {
			logger.error(logKey + "解析时异常，measInfoId=" + measInfoId, e);
		} finally {
			IOUtils.closeQuietly(in);
			reader.close();
		}
		logger.debug(logKey + "解析文件---结束,fileName=" + fileName);
		return false;
	}

	void setOneRowData(List<String> oneRowData, List<String> currMeasResults, List<String> currMeasTypes, List<MeasObjLdn> currMeasObjLdn,
			String currTableName) {
		oneRowData.clear();
		if (currMeasResults == null)
			return;

		if (currMeasObjLdn != null) {
			for (MeasObjLdn m : currMeasObjLdn) {
				String mName = m.name.toUpperCase();
				if (Tools.ALIAS.containsKey(mName))
					mName = Tools.ALIAS.get(mName);
				currMeasResults.add(m.value);
				if (!currMeasTypes.contains(mName))
					currMeasTypes.add(mName);
			}
		}

		List<String> cols = TABLES_COLS.get(currTableName);
		oneRowData.add(omcId);
		oneRowData.add(strCollecttime);
		oneRowData.add(strStamptime);
		oneRowData.add(rncName);
		int size = cols.size() - 4;
		for (int i = 0; i < size; i++) {
			oneRowData.add(null);
		}

		for (int i = 0; i < currMeasTypes.size(); i++) {
			String type = currMeasTypes.get(i);
			if (Util.isOracleNumberString(type) != null) {
				type = "COUNTER_" + type;
			}
			String result = currMeasResults.get(i);
			int location = -1;
			for (int j = 0; j < cols.size(); j++) {
				String col = cols.get(j);
				if (col.equalsIgnoreCase(type)) {
					location = j;
				}
			}
			if (location > -1) {
				oneRowData.set(location, result);
			}
		}
	}

	public String getMessages() {
		return sqlldr.getMessages(vendor, rncName);
	}

	public boolean startSqlldr() {
		return sqlldr.runSqlldr();
	}

	public static void main(String[] args) {
		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setParseTmpID(201101121);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));

		WV1XML w = new WV1XML();
		// w.fileName =
		// "E:\\资料\\解析\\hw\\河南二期网优平台接口-华为\\河南华为性能\\node-b\\A20110117.2345+0800-0000+0800_default.ZZWN0945.xml";
		w.fileName = "E:\\uway\\bug\\igp1\\河北\\A20140917.1100+0800-1130+0800_SJZRNC95.xml";
		w.collectObjInfo = obj;
		try {
			w.parseData();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

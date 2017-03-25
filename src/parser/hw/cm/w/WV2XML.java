package parser.hw.cm.w;

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

import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.LogMgr;
import util.Util;

/**
 * 华为wcdma参数文件，XML方式。
 * 
 * @author ChenSijiang 2011-1-4 下午04:39:25
 */
public class WV2XML extends Parser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private String logKey;

	private String stamptime;

	private String collecttime;

	private String omcid;

	private String rncName;// RNC名，从第一个MO节点的<attr name="name"></attr>中取

	private SqlldrUtil sqlldr;

	private final Map<String, List<String>> TABLES_COLS = new HashMap<String, List<String>>();

	/**
	 * 构造方法。
	 */
	public WV2XML() {
		super();
	}

	@Override
	public boolean parseData() throws Exception {
		this.stamptime = Util.getDateString(collectObjInfo.getLastCollectTime());
		this.collecttime = Util.getDateString(new Date());
		this.omcid = String.valueOf(collectObjInfo.getDevInfo().getOmcID());
		this.logKey = String.format("[%s][%s]", collectObjInfo.getTaskID(), this.stamptime);
		Map<String, String> map = Tools.readTemplet(collectObjInfo.getParseTmpID());
		if (map == null)
			return false;
		if (map.size() == 0)
			return true;

		InputStream in = null;
		XMLStreamReader reader = null;

		boolean isMOStart = false;// 是否已经遇到MO节点了（非公共部分MO节点）
		boolean isFindTopMO = false; // 是否找到了第一个MO节点，即整个文件的公共部分。
		String currTableName = null;// 当前表名
		try {
			Tools.loadTableCols(map.values(), TABLES_COLS);
			sqlldr = new SqlldrUtil(TABLES_COLS, collectObjInfo);
			sqlldr.init();
			readyForSqlldr();
			List<String> rowData = new ArrayList<String>();
			in = new FileInputStream(fileName);
			reader = XMLInputFactory.newInstance().createXMLStreamReader(in);
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
						if (tagName.equalsIgnoreCase("MO")) {
							if (!isFindTopMO) {
								// 此处是找到了第一个MO节点
								isFindTopMO = true;
							} else {
								if (currTableName != null) // 得到了一行完整的记录
								{
									setRowDataValue(rowData, TABLES_COLS.get(currTableName), "OBJ_NAME", rncName);
									sqlldr.writeRow(rowData, currTableName);
								}
								String className = Util.nvl(reader.getAttributeValue(null, "className"), "").toUpperCase();
								currTableName = map.get(className); // 判断模板中是否配置了要采此className节点
								if (currTableName != null) {
									handRowData(rowData, currTableName);
									isMOStart = true;
								}
							}
						} else if (tagName.equalsIgnoreCase("attr")) {
							String attrNameVal = Util.nvl(reader.getAttributeValue(null, "name"), "").toUpperCase();// 取attr节点中，name属性的值。
							String attrContent = Util.nvl(reader.getElementText(), "");// 取attr节点中的文件内容。
							if (isFindTopMO && rncName == null && attrNameVal.equalsIgnoreCase("name")) {
								rncName = attrContent;
							} else if (isMOStart && currTableName != null) {
								if (TABLES_COLS.get(currTableName).contains(attrNameVal)) {
									setRowDataValue(rowData, TABLES_COLS.get(currTableName), attrNameVal, attrContent);
								}
							}
						}
						break;

					default :
						break;
				}
			}

			reader.close();
			in.close();
			sqlldr.runSqlldr();
			return true;
		} catch (Exception e) {
			logger.error(logKey + "解析文件时出错:" + fileName, e);
			return false;
		} finally {
			if (reader != null)
				reader.close();
			Util.closeCloseable(in);
		}
	}

	private void readyForSqlldr() {

	}

	// 填充一行数据
	private void handRowData(List<String> rowData, String tn) {
		rowData.clear();
		rowData.add(omcid);
		rowData.add(collecttime);
		rowData.add(stamptime);
		// rowData.add(rncName);
		List<String> cols = TABLES_COLS.get(tn);
		int size = cols.size() - 2;
		for (int i = 0; i < size; i++) {
			rowData.add(null);
		}
	}

	private void setRowDataValue(List<String> rowData, List<String> cols, String attrNameVal, String attrContent) {
		int location = -1;
		for (int i = 0; i < cols.size(); i++) {
			if (cols.get(i).equals(attrNameVal)) {
				location = i;
				break;
			}
		}
		if (location > 0) {
			rowData.set(location, attrContent);
		}
	}

	public static void main(String[] args) {
		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setParseTmpID(20110112);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));

		WV2XML w = new WV2XML();
		// w.fileName =
		// "D:\\chensj_20110107\\河南二期网优平台接口-华为\\河南华为参数\\基站\\CMExport_ZZWH1403_192.168.60.188_2011010323.xml";
		w.fileName = "E:\\出差\\广州联通_20110121\\河南二期网优平台接口-华为\\河南华为参数\\RNC\\CMExport_ZZR33(RNC10)_172.21.33.22_2011010323.xml";
		w.collectObjInfo = obj;
		try {
			w.parseData();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

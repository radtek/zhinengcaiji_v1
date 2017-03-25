package parser.hw.pm;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import store.AbstractStore;
import store.StoreFactory;
import task.CollectObjInfo;
import task.DevInfo;
import templet.GenericSectionHeadD;
import templet.Table;
import templet.hw.cdma.pm.CDMA_GD_HW_P;
import templet.hw.cdma.pm.CDMA_GD_HW_P.CDMA_GD_HW_P_Field;
import templet.hw.cdma.pm.CDMA_GD_HW_P.CDMA_GD_HW_P_Templet;
import util.LogMgr;
import util.Util;

/**
 * C+W测试中要解析入库的华为性能文件，XML格式，与北京的不同。 与北京电信的区别，主要是：北京一个文件有多个表的数据，这个只有一个，北京的counter名是数字，这个是英文名。
 * 
 * @author ChenSijiang 2012-9-11
 */
public class CDMA_GD_HW_Parser extends Parser {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	/* measResults中表示空值的字符。 */
	private static final String NULL_VALUE_SIGN = "NIL";

	private Timestamp stamptime;

	private String strStamptime;

	private long taskId;

	private String logKey = null;

	private CDMA_GD_HW_P parseTemplet;

	private GenericSectionHeadD distTemplet;

	private CDMA_GD_HW_P_Templet currTemplet;

	private Table currTable;

	private AbstractStore<?> store;

	private List<CDMA_GD_HW_P_Field> ldnList = new ArrayList<CDMA_GD_HW_P.CDMA_GD_HW_P_Field>();

	private List<CDMA_GD_HW_P_Field> valuesList = new ArrayList<CDMA_GD_HW_P.CDMA_GD_HW_P_Field>();

	private StringBuilder buff = new StringBuilder();

	@Override
	public boolean parseData() throws Exception {
		File file = new File(getFileName());
		if (!file.exists() || !file.isFile())
			throw new Exception("待解析的文件不存在：" + file);

		parseTemplet = (CDMA_GD_HW_P) this.getCollectObjInfo().getParseTemplet();
		distTemplet = (GenericSectionHeadD) this.getCollectObjInfo().getDistributeTemplet();

		this.stamptime = collectObjInfo.getLastCollectTime();
		this.strStamptime = Util.getDateString(this.stamptime);
		this.taskId = collectObjInfo.getTaskID();
		this.logKey = "[" + this.taskId + "][" + this.strStamptime + "]";

		InputStream in = null;
		XMLStreamReader reader = null;
		try {
			in = new FileInputStream(fileName);
			log.debug(logKey + "开始解析 - " + fileName);
			reader = XMLInputFactory.newInstance().createXMLStreamReader(in);
			int type = -1;
			String elementType = null;
			// List<CDMA_GD_HW_P_Field> heads = new
			// ArrayList<CDMA_GD_HW_P_Field>();//
			// 字段表头，即measTypes里以空格分隔的各个counter名。
			// List<String> values = new ArrayList<String>(); //
			// 从measResults中读取到的counter值列表。
			// Map<String, String> measValueAttrs = new HashMap<String,
			// String>();// measValue中的各属性。
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
						if (elementType == null) {
							if (tagName.equalsIgnoreCase("fileSender")) {
								elementType = reader.getAttributeValue(null, "elementType");
								this.parseTemplet.findTempletByElementType(elementType);
								this.currTemplet = this.parseTemplet.findTempletByElementType(elementType);
								if (this.currTemplet == null) {
									log.error(logKey + "未找到对应的模板，elementType=" + elementType);
									return false;
								}
								this.currTable = this.distTemplet.getTemplet(this.currTemplet.getId()).getTables().get(0);
								this.store = StoreFactory.getStore(this.getCollectObjInfo().getDisTmpID(), this.currTable,
										this.getCollectObjInfo(),dataTime);
								this.store.open();
								log.debug(logKey + "入库器已初始化，elementType=" + elementType);
							}
						} else if (this.valuesList.isEmpty()) {
							if (tagName.equalsIgnoreCase("measTypes")) {
								_parseMeasTypes(reader.getElementText());
							}
						} else {
							if (tagName.equalsIgnoreCase("measResults")) {
								for (CDMA_GD_HW_P_Field fx : this.valuesList) {
									fx.setValue(null);
								}
								_parseMeasResults(reader.getElementText());
								this.buff.setLength(0);
								for (int i = 0; i < this.valuesList.size(); i++) {
									this.buff.append(valuesList.get(i).getValue());
									this.buff.append(this.currTable.getSplitSign());
								}
								for (int i = 0; i < this.ldnList.size(); i++) {
									this.buff.append(ldnList.get(i).getValue());
									// if ( i < this.ldnList.size() - 1 )
									this.buff.append(this.currTable.getSplitSign());
								}
								this.store.write(this.buff.toString());
								this.buff.setLength(0);
							} else if (tagName.equalsIgnoreCase("measValue")) {
								this.ldnList.clear();
								_parserMeasObjLdn(reader.getAttributeValue(null, "measObjLdn"));
							}
						}
						break;
					case XMLStreamConstants.END_ELEMENT :

						break;
					default :
						break;
				}
			}
			log.debug(logKey + "解析结束：" + file);
			this.store.commit();
			log.debug(logKey + "入库结束。");
		} catch (Exception e) {
			log.error(logKey + "解析入库文件'" + file + "'时出错。", e);
		} finally {
			if (this.store != null)
				this.store.close();
			parseTemplet = null;
			distTemplet = null;
			currTemplet = null;
			currTable = null;
			store = null;
			this.valuesList.clear();
			this.ldnList.clear();
			try {
				if (reader != null)
					reader.close();
			} finally {
				IOUtils.closeQuietly(in);
			}

		}
		return false;
	}

	private void _parserMeasObjLdn(String measObjLdn) {
		if (Util.isNull(measObjLdn))
			return;
		String[] sp0 = measObjLdn.split(",");
		//int index = 0;
		for (String s0 : sp0) {
			if (Util.isNull(s0))
				continue;
			String[] sp1 = s0.split("=");
			String name = "";
			String value = "";
			if (sp1.length > 0)
				name = sp1[0].trim();
			if (sp1.length > 1)
				value = sp1[1].trim();
			if (Util.isNotNull(name)) {
				if (this.currTemplet.containsField(name)) {
					CDMA_GD_HW_P_Field f = this.currTemplet.getField(name);
					f.setValue(value);
					this.ldnList.add(f);
				}
			}
			//index++;
		}

	}

	private void _parseMeasResults(String measResults) {
		if (Util.isNull(measResults))
			return;
		String[] sp = measResults.split(" ");
		List<String> tmp = new ArrayList<String>();
		for (String s : sp) {
			if (Util.isNotNull(s)) {
				if (s.trim().equalsIgnoreCase(NULL_VALUE_SIGN))
					tmp.add("");
				else
					tmp.add(s.trim());
			}
		}

		for (int i = 0; i < tmp.size(); i++) {
			for (CDMA_GD_HW_P_Field f : this.valuesList) {
				if (i == f.getRealIndex())
					f.setValue(tmp.get(i));
			}
		}

	}

	private void _parseMeasTypes(String measTypes) {
		if (Util.isNull(measTypes))
			return;
		String[] sp = measTypes.split(" ");
		int index = 0;
		for (String s : sp) {
			if (Util.isNotNull(s)) {
				if (this.currTemplet.containsField(s.trim())) {
					CDMA_GD_HW_P_Field f = this.currTemplet.getField(s.trim());
					f.setRealIndex(index);
					this.valuesList.add(f);
				}
				index++;
			}
		}
	}

	public static void main(String[] args) {
		CollectObjInfo obj = new CollectObjInfo(112);
		obj.setLastCollectTime(new Timestamp(0));
		DevInfo di = new DevInfo();
		di.setOmcID(99);
		obj.setDevInfo(di);
		CDMA_GD_HW_Parser p = new CDMA_GD_HW_Parser();
		p.fileName = "C:\\Users\\ChenSijiang\\Desktop\\华为数据样本\\性能\\PM_201208280000-201208280100_5_1.xml";
		p.collectObjInfo = obj;
		try {
			p.parseData();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

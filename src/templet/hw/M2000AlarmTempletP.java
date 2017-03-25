package templet.hw;

import java.io.File;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import templet.AbstractTempletBase;
import util.Util;
import framework.SystemConfig;

/**
 * M2000AlarmTempletP
 * 
 * @author liuwx
 * @since 2.0
 */
public class M2000AlarmTempletP extends AbstractTempletBase {

	/**
	 * 字段域模板定义
	 */
	public class FieldTemplet {

		public int fieldIndex = 0;// 字段的索引

		public String strFieldMapping = "";// 字段隐射名

		public String strFieldName = "";// 字段名称

		public boolean isKey = false;// 是否属于关键字

		public String strKeyValue = "";// 关键值

		public boolean isDefault = false;// 是否存在默认值

		public String strDefaultValue = "";// 默认值

		public int dataType = 0;// 数据类型

		public String strDataTimeFormat = "";// 如果是时间类型的格式方式
	}

	/**
	 * 表模板定义
	 */
	public class TableTemplet {

		public int tableIndex;// 表的索引

		public String strTableName;// 表名

		// public String table_desc;
		public boolean isFillTitle = false;

		public Map<Integer, FieldTemplet> field = new HashMap<Integer, FieldTemplet>();
	}

	// 存储表模板信息
	public Map<Integer, TableTemplet> tableInfo = new HashMap<Integer, TableTemplet>();

	static Socket server;

	public static String ALARM_HANDSHAKE;// 告警握手报文

	public static String ALARM_NUM;// 设备告警流水号

	public static String NETWORK_NUM;// 网络流水号

	public static String OBJECT_IDCODE;// 对象标识

	public static String OBJECT_NAME;// 对象名称

	public static String OBJECT_TYPE;// 对象类型

	public static String NETWORK_IDCODE;// 网元标识

	public static String NETWORK_NAME;// 网元名称

	public static String NETWORK_TYPE;// 网元类型

	public static String ALARM_ID;// 告警ID

	public static String ALARM_CLASS;// 告警种类

	public static String ALARM_STATUS;// 告警状态

	public static String ALARM_TYPE_ID;// 告警类型ID

	public static String ALARM_TYPE;// 告警类型

	public static String BEGIN_TIME;// 发生时间

	public static String RESUME_DATE;// 恢复时间

	public static String CONFIRM_DATE;// 确认时间

	public static String PITCH_INFO;// 定位信息

	public static String OPERATOR;// 操作员

	//
	public static String ALARM_NAME;

	public static String ALARM_CLASS_ID;

	public static String ALARM_LEVEL_ID;

	public static String ALARM_LEVEL;

	public static String BEGIN_FLAG;

	public static String END_FLAG;

	public static String SPLIT = "\r\n";

	// private String tempPath = "a_clt_hw_socket_parse_config_dist.xml";

	/*
	 * 解析模板
	 */
	@Override
	public void parseTemp(String tempFile) throws Exception {
		if (tempFile == null || tempFile.trim().equals(""))
			return;

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = null;

		// 模板文件路径
		String TempletFilePath = SystemConfig.getInstance().getTempletPath();
		TempletFilePath = TempletFilePath + File.separatorChar + tempFile;
		File file1 = new File(TempletFilePath);
		doc = builder.parse(file1);

		/* 获取动用信息 ,公用Node节点信息 */
		NodeList flagList = doc.getElementsByTagName("FLAG");
		if (flagList != null) {
			if (doc.getElementsByTagName("BEGIN_FLAG").item(0).getFirstChild() == null)
				BEGIN_FLAG = "";
			else
				BEGIN_FLAG = doc.getElementsByTagName("BEGIN_FLAG").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("END_FLAG").item(0).getFirstChild() == null)
				END_FLAG = "";
			else
				END_FLAG = doc.getElementsByTagName("END_FLAG").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("SPLIT").item(0).getFirstChild() != null) {
				String s = doc.getElementsByTagName("SPLIT").item(0).getFirstChild().getNodeValue();
				if (Util.isNotNull(s))
					SPLIT = s;
			}

		}
		/* 获取动用信息 ,公用Node节点信息 */
		NodeList fieldFlagList = doc.getElementsByTagName("FIELDFLAG");
		if (fieldFlagList.getLength() >= 1) {
			// 告警握手报文
			if (doc.getElementsByTagName("ALARM_HANDSHAKE").item(0).getFirstChild() == null)
				ALARM_HANDSHAKE = "";
			else
				ALARM_HANDSHAKE = doc.getElementsByTagName("ALARM_HANDSHAKE").item(0).getFirstChild().getNodeValue();

			// 告警数据报文
			if (doc.getElementsByTagName("ALARM_NUM").item(0).getFirstChild() == null)
				ALARM_NUM = "";
			else
				ALARM_NUM = doc.getElementsByTagName("ALARM_NUM").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("NETWORK_NUM").item(0).getFirstChild() == null)
				NETWORK_NUM = "";
			else
				NETWORK_NUM = doc.getElementsByTagName("NETWORK_NUM").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("OBJECT_IDCODE").item(0).getFirstChild() == null)
				OBJECT_IDCODE = "";
			else
				OBJECT_IDCODE = doc.getElementsByTagName("OBJECT_IDCODE").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("OBJECT_NAME").item(0).getFirstChild() == null)
				OBJECT_NAME = "";
			else
				OBJECT_NAME = doc.getElementsByTagName("OBJECT_NAME").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("OBJECT_TYPE").item(0).getFirstChild() == null)
				OBJECT_TYPE = "";
			else
				OBJECT_TYPE = doc.getElementsByTagName("OBJECT_TYPE").item(0).getFirstChild().getNodeValue();

			//
			if (doc.getElementsByTagName("NETWORK_IDCODE").item(0).getFirstChild() == null)
				NETWORK_IDCODE = "";
			else
				NETWORK_IDCODE = doc.getElementsByTagName("NETWORK_IDCODE").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("NETWORK_NAME").item(0).getFirstChild() == null)
				NETWORK_NAME = "";
			else
				NETWORK_NAME = doc.getElementsByTagName("NETWORK_NAME").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("NETWORK_TYPE").item(0).getFirstChild() == null)
				NETWORK_TYPE = "";
			else
				NETWORK_TYPE = doc.getElementsByTagName("NETWORK_TYPE").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("ALARM_ID").item(0).getFirstChild() == null)
				ALARM_ID = "";
			else
				ALARM_ID = doc.getElementsByTagName("ALARM_ID").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("ALARM_CLASS").item(0).getFirstChild() == null)
				ALARM_CLASS = "";
			else
				ALARM_CLASS = doc.getElementsByTagName("ALARM_CLASS").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("ALARM_STATUS").item(0).getFirstChild() == null)
				ALARM_STATUS = "";
			else
				ALARM_STATUS = doc.getElementsByTagName("ALARM_STATUS").item(0).getFirstChild().getNodeValue();

			// 告警类型ID
			if (doc.getElementsByTagName("ALARM_TYPE_ID").item(0).getFirstChild() == null)
				ALARM_TYPE_ID = "";
			else
				ALARM_TYPE_ID = doc.getElementsByTagName("ALARM_TYPE_ID").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("ALARM_TYPE").item(0).getFirstChild() == null)
				ALARM_TYPE = "";
			else
				ALARM_TYPE = doc.getElementsByTagName("ALARM_TYPE").item(0).getFirstChild().getNodeValue();

			//
			if (doc.getElementsByTagName("ALARM_NAME").item(0).getFirstChild() == null)
				ALARM_NAME = "";
			else
				ALARM_NAME = doc.getElementsByTagName("ALARM_NAME").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("ALARM_CLASS_ID").item(0).getFirstChild() == null)
				ALARM_CLASS_ID = "";
			else
				ALARM_CLASS_ID = doc.getElementsByTagName("ALARM_CLASS_ID").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("ALARM_LEVEL_ID").item(0).getFirstChild() == null)
				ALARM_LEVEL_ID = "";
			else
				ALARM_LEVEL_ID = doc.getElementsByTagName("ALARM_LEVEL_ID").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("ALARM_LEVEL").item(0).getFirstChild() == null)
				ALARM_LEVEL = "";
			else
				ALARM_LEVEL = doc.getElementsByTagName("ALARM_LEVEL").item(0).getFirstChild().getNodeValue();

			//

			//
			if (doc.getElementsByTagName("BEGIN_TIME").item(0).getFirstChild() == null)
				BEGIN_TIME = "";
			else
				BEGIN_TIME = doc.getElementsByTagName("BEGIN_TIME").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("RESUME_DATE").item(0).getFirstChild() == null)
				RESUME_DATE = "";
			else
				RESUME_DATE = doc.getElementsByTagName("RESUME_DATE").item(0).getFirstChild().getNodeValue();
			//
			if (doc.getElementsByTagName("CONFIRM_DATE").item(0).getFirstChild() == null)
				CONFIRM_DATE = "";
			else
				CONFIRM_DATE = doc.getElementsByTagName("CONFIRM_DATE").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("PITCH_INFO").item(0).getFirstChild() == null)
				PITCH_INFO = "";
			else
				PITCH_INFO = doc.getElementsByTagName("PITCH_INFO").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("OPERATOR").item(0).getFirstChild() == null)
				OPERATOR = "";
			else
				OPERATOR = doc.getElementsByTagName("OPERATOR").item(0).getFirstChild().getNodeValue();
		}

		NodeList tableNodeInfo = doc.getElementsByTagName("DATATABLE");

		/* 读取表信息 */
		for (int i = 0; i < tableNodeInfo.getLength(); i++) {
			TableTemplet table = new TableTemplet();

			if (doc.getElementsByTagName("TABLEINDEX").item(i).getFirstChild() == null)
				table.tableIndex = 0;
			else
				table.tableIndex = Integer.parseInt(doc.getElementsByTagName("TABLEINDEX").item(i).getFirstChild().getNodeValue());

			if (doc.getElementsByTagName("TABLENAME").item(i).getFirstChild() == null)
				table.strTableName = "";
			else
				table.strTableName = doc.getElementsByTagName("TABLENAME").item(i).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("FILLTITLE") != null && doc.getElementsByTagName("FILLTITLE").item(i) != null
					&& doc.getElementsByTagName("FILLTITLE").item(i).getFirstChild() != null) {
				String strValue = doc.getElementsByTagName("FILLTITLE").item(i).getFirstChild().getNodeValue();
				if (Integer.parseInt(strValue) == 1)
					table.isFillTitle = true;
			}

			if (doc.getElementsByTagName("FIELDS").item(i) != null && doc.getElementsByTagName("FIELDS").item(i).getFirstChild() != null) {
				Node fieldnode = doc.getElementsByTagName("FIELDS").item(i);
				parseFieldInfo(table.field, fieldnode);
			}
			tableInfo.put(table.tableIndex, table);
		}
	}

	/**
	 * 解析Fields节点
	 * 
	 * @param TableInfo
	 * @param CurrentNode
	 */
	private void parseFieldInfo(Map<Integer, FieldTemplet> tableInfo, Node CurrentNode) {
		NodeList Ssn = CurrentNode.getChildNodes();
		// 节点的FIELDS下面是否存在子节点
		for (int nIndex = 0; nIndex < Ssn.getLength(); nIndex++) {
			Node tempnode = Ssn.item(nIndex);
			// 判断当前节点是否是 FIELDITEM 节点 判断依据根据节点类型与节点名称
			if (tempnode.getNodeType() == Node.ELEMENT_NODE && tempnode.getNodeName().toUpperCase().equals("FIELDITEM")) {
				NodeList childnodeList = tempnode.getChildNodes();
				if (childnodeList != null) {
					FieldTemplet field = new FieldTemplet();
					for (int i = 0; i < childnodeList.getLength(); i++) {
						Node childnode = childnodeList.item(i);
						if (childnode.getNodeType() == Node.ELEMENT_NODE) {
							String NodeName = childnode.getNodeName().toUpperCase();
							String strValue = getNodeValue(childnode);
							if (NodeName.equals("FIELDINDEX")) {
								if (strValue == null || strValue.equals(""))
									field.fieldIndex = 0;
								else
									field.fieldIndex = Integer.parseInt(strValue);
							} else if (NodeName.equals("FIELDMAPPING")) {
								field.strFieldMapping = strValue;
							} else if (NodeName.equals("FIELDNAME")) {
								field.strFieldName = strValue;
							} else if (NodeName.equals("ISKEY")) {
								if (strValue.trim().equals("1"))
									field.isKey = true;
							} else if (NodeName.equals("KEYVALUE")) {
								field.strKeyValue = strValue;
							} else if (NodeName.equals("ISDEFAULT")) {
								if (strValue.trim().equals("1"))
									field.isDefault = true;
							} else if (NodeName.equals("DEFAULTVALUE")) {
								field.strDefaultValue = strValue;
							} else if (NodeName.equals("DATATYPE")) {
								if (strValue == null || strValue.equals(""))
									field.dataType = 0;
								else
									field.dataType = Integer.parseInt(strValue);
							} else if (NodeName.equals("DATATIMEFORMAT")) {
								field.strDataTimeFormat = strValue;
							}
						}
					}
					tableInfo.put(field.fieldIndex, field);
				}
			}
		}
	}

}

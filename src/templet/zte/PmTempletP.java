package templet.zte;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import templet.AbstractTempletBase;
import util.LogMgr;
import util.Util;
import framework.SystemConfig;

/**
 * 中兴多表文件解析 2010-02-02
 * 
 * @author liuwx
 */
public class PmTempletP extends AbstractTempletBase {

	// private List<String> tableSignList;
	private String tableSignString;

	/**
	 * 模板头类定义
	 */
	public class HeaderTemplet {

		public String netype = ""; // 字段的名称

		public String nodeid;

		public String startTime;

		public String endTime;

		public String ver;
	}

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

		public String table_desc;

		// and on 2011-04-13
		public String fieldSplit;

		public String valueSplit;

		// end add

		public boolean isFillTitle = false;

		public Map<Integer, FieldTemplet> field = new HashMap<Integer, FieldTemplet>();
	}

	/* 入库信息 */
	public String strTmpName = null;// 模板名称

	public String strEdition = null;// 版本类型

	/* 数据库信息 */
	public String dbDriver = ""; // 连接数据库的驱动

	public String dbDriverUrl = "";// 数据库的连接信息

	public String dbDataBase = "";// 数据库名称

	public String dbUserName = "";// 用户名

	public String dbPassword = "";// 密码

	// 存储表模板信息
	public Map<Integer, TableTemplet> tableInfo = new HashMap<Integer, TableTemplet>();

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public PmTempletP() {
	}

	/*
	 * 解析模板
	 */
	public void parseTemp(String tempFile) {
		if (tempFile == null || tempFile.trim().equals(""))
			return;

		StringBuilder sb = null;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;
		try {
			builder = factory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			log.error(":解析配置文档出现异常，原因：", e);
			return;
		}
		Document doc = null;

		// 模板文件路径
		String TempletFilePath = SystemConfig.getInstance().getTempletPath();
		TempletFilePath = TempletFilePath + File.separatorChar + tempFile;

		File file1 = new File(TempletFilePath);

		try {
			doc = builder.parse(file1);
		} catch (SAXException e) {
			log.error(":解析配置文档出现异常，原因：", e);
			return;
		} catch (IOException e) {
			log.error(":解析配置文档出现IO异常，原因：", e);
			return;
		}

		/* 获取动用信息 ,公用Node节点信息 */
		NodeList publicNodeList = doc.getElementsByTagName("PUBLIC");
		if (publicNodeList.getLength() >= 1) {
			if (doc.getElementsByTagName("DRIVER").item(0).getFirstChild() == null)
				this.dbDriver = "";
			else
				this.dbDriver = doc.getElementsByTagName("DRIVER").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("DRIVERURL").item(0).getFirstChild() == null)
				this.dbDriverUrl = "";
			else
				this.dbDriverUrl = doc.getElementsByTagName("DRIVERURL").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("DATABASE").item(0).getFirstChild() == null)
				this.dbDataBase = "";
			else
				this.dbDataBase = doc.getElementsByTagName("DATABASE").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("USERNAME").item(0).getFirstChild() == null)
				this.dbUserName = "";
			else
				this.dbUserName = doc.getElementsByTagName("USERNAME").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("PASSWORD").item(0).getFirstChild() == null)
				this.dbPassword = "";
			else
				this.dbPassword = doc.getElementsByTagName("PASSWORD").item(0).getFirstChild().getNodeValue();
		}

		NodeList tableSignNodeList = doc.getElementsByTagName("TABLESIGN");
		if (tableSignNodeList != null) {
			NodeList subTableSignNodeList = doc.getElementsByTagName("SUBTABLESIGN");
			if (subTableSignNodeList != null) {
				// tableSignList = new ArrayList<String>();
				sb = new StringBuilder();
				for (int i = 0; i < subTableSignNodeList.getLength(); i++) {
					Node node = subTableSignNodeList.item(i);
					// tableSignList.add(this.getNodeValue(node));
					sb.append(this.getNodeValue(node));
				}
				tableSignString = sb.toString();
				sb.delete(0, sb.length());
			}
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

			if (doc.getElementsByTagName("FIELDSPLIT") != null && doc.getElementsByTagName("FIELDSPLIT").item(i) != null
					&& doc.getElementsByTagName("FIELDSPLIT").item(i).getFirstChild() != null) {
				String strValue = doc.getElementsByTagName("FIELDSPLIT").item(i).getFirstChild().getNodeValue();
				if (!Util.isNull(strValue))
					table.fieldSplit = strValue;
			}
			if (doc.getElementsByTagName("VALUESPLIT") != null && doc.getElementsByTagName("VALUESPLIT").item(i) != null
					&& doc.getElementsByTagName("VALUESPLIT").item(i).getFirstChild() != null) {
				String strValue = doc.getElementsByTagName("VALUESPLIT").item(i).getFirstChild().getNodeValue();
				if (!Util.isNull(strValue))
					table.valueSplit = strValue;
			}

			if (doc.getElementsByTagName("FILLTITLE") != null && doc.getElementsByTagName("FILLTITLE").item(i) != null
					&& doc.getElementsByTagName("FILLTITLE").item(i).getFirstChild() != null) {
				String strValue = doc.getElementsByTagName("FILLTITLE").item(i).getFirstChild().getNodeValue();
				if (Integer.parseInt(strValue) == 1)
					table.isFillTitle = true;
			}

			if (doc.getElementsByTagName("TABLEDESC") != null && doc.getElementsByTagName("TABLEDESC").item(i) != null
					&& doc.getElementsByTagName("TABLEDESC").item(i).getFirstChild() != null) {
				String strValue = doc.getElementsByTagName("TABLEDESC").item(i).getFirstChild().getNodeValue();
				table.table_desc = strValue;
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

	public String getTableSignString() {
		return tableSignString;
	}

}

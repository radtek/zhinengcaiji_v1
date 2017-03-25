package distributor;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import templet.AbstractTempletBase;
import framework.SystemConfig;

/**
 * 分发模板
 * 
 * @author IGP TDT
 * @since 1.0
 */
public class DistributeTemplet extends AbstractTempletBase {

	public class FieldTemplet {

		public int m_nFieldIndex = 0;// 字段的索引

		public String m_strFieldMapping = "";// 字段隐射名,用于动态入库

		public String m_strFieldName = "";// 字段名称

		public boolean m_bIsKey = false;// 是否属于关键字

		public String m_strKeyValue = "";// 关键值

		public boolean m_bIsDefault = false;// 是否存在默认值

		public String m_strDefaultValue = "";// 默认值

		public int m_nDataType = 0;// 数据类型

		public String m_strDataTimeFormat = "";// 如果是时间类型的格式方式

		public String rawName = ""; // add by chensj(20100507) 原始文件中的列名
	}

	public class TableTemplet {

		public int tableIndex;// 表的索引

		public String tableName;// 表名

		public boolean isFillTitle = false; // sqlldr 是否给文件首行写上字段名

		public Map<Integer, FieldTemplet> fields = new HashMap<Integer, FieldTemplet>();
	}

	/* 数据库信息 */
	public String dbDriver = ""; // 连接数据库的驱动

	public String dbUrl = "";// 数据库的连接信息

	public String dbDataBase = "";// 数据库名称

	public String dbUserName = "";// 用户名

	public String dbPwd = "";// 密码

	public int stockStyle = 0;// 入库方式 1:insert;2:sqlldr;3://sqlldr采用动态列导入

	public int onceStockCount = 0; // 一次入库记录条数

	public Map<Integer, TableTemplet> tableTemplets = new HashMap<Integer, TableTemplet>();

	// sqlldr 需要的参数,保存FileWriter句柄
	public Map<Integer, TableItem> tableItems = new HashMap<Integer, TableItem>();

	/**
	 * 解析模板
	 */
	public void parseTemp(String distFileName) throws Exception {
		if (distFileName == null || distFileName.trim().equals(""))
			return;

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = null;

		// 模板文件路径
		String templetFilePath = SystemConfig.getInstance().getTempletPath() + File.separatorChar + distFileName;
		doc = builder.parse(new File(templetFilePath));

		/* 获取动用信息 ,公用Node节点信息 */
		NodeList publicNodeList = doc.getElementsByTagName("PUBLIC");
		if (publicNodeList.getLength() >= 1) {
			if (doc.getElementsByTagName("DRIVER").item(0).getFirstChild() == null)
				this.dbDriver = "";
			else
				this.dbDriver = doc.getElementsByTagName("DRIVER").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("DRIVERURL").item(0).getFirstChild() == null)
				this.dbUrl = "";
			else
				this.dbUrl = doc.getElementsByTagName("DRIVERURL").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("DATABASE").item(0).getFirstChild() == null)
				this.dbDataBase = "";
			else
				this.dbDataBase = doc.getElementsByTagName("DATABASE").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("USERNAME").item(0).getFirstChild() == null)
				this.dbUserName = "";
			else
				this.dbUserName = doc.getElementsByTagName("USERNAME").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("PASSWORD").item(0).getFirstChild() == null)
				this.dbPwd = "";
			else
				this.dbPwd = doc.getElementsByTagName("PASSWORD").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("STOCKSTYLE").item(0).getFirstChild() == null)
				this.stockStyle = 0;
			else
				this.stockStyle = Integer.parseInt(doc.getElementsByTagName("STOCKSTYLE").item(0).getFirstChild().getNodeValue());

			if (doc.getElementsByTagName("ONCESTOCKCOUNT").item(0).getFirstChild() == null)
				this.onceStockCount = 0;
			else
				this.onceStockCount = Integer.parseInt(doc.getElementsByTagName("ONCESTOCKCOUNT").item(0).getFirstChild().getNodeValue());
		}

		/* 读取表信息 */
		NodeList tableNodeInfo = doc.getElementsByTagName("DATATABLE");
		for (int i = 0; i < tableNodeInfo.getLength(); i++) {
			TableTemplet table = new TableTemplet();

			if (doc.getElementsByTagName("TABLEINDEX").item(i).getFirstChild() == null)
				table.tableIndex = 0;
			else {
				try {
					table.tableIndex = Integer.parseInt(doc.getElementsByTagName("TABLEINDEX").item(i).getFirstChild().getNodeValue());
				} catch (Exception e) {
				}
			}
			if (doc.getElementsByTagName("TABLENAME").item(i).getFirstChild() == null)
				table.tableName = "";
			else
				table.tableName = doc.getElementsByTagName("TABLENAME").item(i).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("FILLTITLE") != null && doc.getElementsByTagName("FILLTITLE").item(i) != null
					&& doc.getElementsByTagName("FILLTITLE").item(i).getFirstChild() != null) {
				String strValue = doc.getElementsByTagName("FILLTITLE").item(i).getFirstChild().getNodeValue();
				if (Integer.parseInt(strValue) == 1)
					table.isFillTitle = true;
			}

			if (doc.getElementsByTagName("FIELDS").item(i) != null && doc.getElementsByTagName("FIELDS").item(i).getFirstChild() != null) {
				Node fieldsNode = doc.getElementsByTagName("FIELDS").item(i);
				parseFieldInfo(table.fields, fieldsNode);
			}
			tableTemplets.put(table.tableIndex, table);
		}
	}

	private void parseFieldInfo(Map<Integer, FieldTemplet> fields, Node fieldsNode) {
		NodeList allFileds = fieldsNode.getChildNodes();
		// 节点的FIELDS下面是否存在子节点
		for (int nIndex = 0; nIndex < allFileds.getLength(); nIndex++) {
			Node fieldNode = allFileds.item(nIndex);
			// 判断当前节点是否是 FIELDITEM 节点 判断依据根据节点类型与节点名称
			if (fieldNode.getNodeType() == Node.ELEMENT_NODE && fieldNode.getNodeName().toUpperCase().equals("FIELDITEM")) {
				NodeList childnodeList = fieldNode.getChildNodes();
				if (childnodeList != null) {
					FieldTemplet field = new FieldTemplet();
					for (int i = 0; i < childnodeList.getLength(); i++) {
						Node childnode = childnodeList.item(i);
						if (childnode.getNodeType() == Node.ELEMENT_NODE) {
							String NodeName = childnode.getNodeName().toUpperCase();
							String strValue = getNodeValue(childnode);
							if (NodeName.equals("FIELDINDEX")) {
								if (strValue == null || strValue.equals(""))
									field.m_nFieldIndex = 0;
								else
									field.m_nFieldIndex = Integer.parseInt(strValue);
							} else if (NodeName.equals("FIELDMAPPING")) {
								field.m_strFieldMapping = strValue;
							} else if (NodeName.equals("FIELDNAME")) {
								field.m_strFieldName = strValue;
							}
							// ------- add by chensj(20100507) 原始文件中的列名
							else if (NodeName.equals("RAWNAME")) {
								field.rawName = strValue;
							}
							// ----------------------------------------------
							else if (NodeName.equals("ISKEY")) {
								if (strValue.trim().equals("1"))
									field.m_bIsKey = true;
							} else if (NodeName.equals("KEYVALUE")) {
								field.m_strKeyValue = strValue;
							} else if (NodeName.equals("ISDEFAULT")) {
								if (strValue.trim().equals("1"))
									field.m_bIsDefault = true;
							} else if (NodeName.equals("DEFAULTVALUE")) {
								field.m_strDefaultValue = strValue;
							} else if (NodeName.equals("DATATYPE")) {
								if (strValue == null || strValue.equals(""))
									field.m_nDataType = 0;
								else
									field.m_nDataType = Integer.parseInt(strValue);
							} else if (NodeName.equals("DATATIMEFORMAT")) {
								field.m_strDataTimeFormat = strValue;
							}
						}
					}
					fields.put(field.m_nFieldIndex, field);
				}
			}
		}
	}

}

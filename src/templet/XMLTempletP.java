package templet;

import java.io.File;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import framework.SystemConfig;

/**
 * XML解析模板
 * 
 * @author IGP TDT
 * @since 1.0
 */
public class XMLTempletP extends AbstractTempletBase {

	public class FieldItem {

		public String m_strName; // 列名称，输出给分发的列名称

		public String m_strType; // 此列值的类型，string,date,无此项为string

		public String m_strDateFormat; // 日期类型的日期格式

		public int m_nFieldIndex; // 字段在分隔后字符串数组中的下标,对应Table中1，2生成列方法

		// 列来源类型,对应Table中3生成列方法
		// COLUMN_SRC_TYPE
		// 1-源于SRC_ELEMENT的属性
		// 2-源于下层ELEMENT的值
		// 3-源于匹配下层Element属性条件的Element值
		public int m_nSrcType;

		public String m_strInnerTagName; // 源于哪个下层Element，列来源类型2，3时有效

		public String m_strAttributeName; // 源于哪个属性，列来源1，3时有效

		public String m_strAttributeValue; // 要匹配的属性条件值
	}

	public class Table {

		public int m_nTableIndex; // 表序号，传递给分发

		public int m_nGenColumnType; // 生成列的方法,1-分隔符分隔内容，2-子Element内容排列，3-由列自身指定

		public int m_nSrcIDType; // 源标识的类型,1-Tag标识，2-属性标识

		public String m_strAttributeName; // 属性标识名称，属性标识时有效

		public String m_strAttributeValue; // 属性标识值，属性标识时有效

		public String m_strSubTagName; // 生成列方法2时，子Element的TagName

		public Vector<FieldItem> m_listFields = new Vector<FieldItem>();
	};

	public class ParseTag {

		public String m_strTagName; // 从哪种Element获取

		public Vector<Table> m_listTable = new Vector<Table>();
	};

	public Vector<ParseTag> m_listParseTag = new Vector<ParseTag>();// 需要解析的Tag

	public void parseTemp(String templetFileName) throws Exception {
		if (templetFileName == null || templetFileName.trim().equals(""))
			return;

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = null;

		// 模板文件路径
		String TempletFilePath = SystemConfig.getInstance().getTempletPath();
		TempletFilePath = TempletFilePath + File.separatorChar + templetFileName;
		File file = new File(TempletFilePath);
		doc = builder.parse(file);

		// 获取需要解析的Tag
		NodeList nl = doc.getElementsByTagName("PARSE_TAG");
		for (int i = 0; i < nl.getLength(); ++i) {
			ParseTag parseTag = new ParseTag();
			for (Node childNode = nl.item(i).getFirstChild(); childNode != null; childNode = childNode.getNextSibling()) {
				if (childNode.getNodeType() != Node.ELEMENT_NODE)
					continue;

				// SRC_TAGNAME
				if (childNode.getNodeName().toUpperCase().equals("SRC_TAGNAME")) {
					parseTag.m_strTagName = childNode.getFirstChild().getNodeValue();
					continue;
				}

				// table
				if (!childNode.getNodeName().toUpperCase().equals("TABLE")) {
					continue;
				}

				Table table = new Table();
				parseTag.m_listTable.add(table);
				fillTable(table, childNode);

			}// for <PARSETAG>.children

			this.m_listParseTag.add(parseTag);

		}// for <PARSETAG>
	}

	private void fillTable(Table table, Node tableNode) {
		NodeList childNodeList = tableNode.getChildNodes();

		try {
			for (int i = 0; i < childNodeList.getLength(); i++) {
				Node childNode = childNodeList.item(i);

				if (childNode.getNodeType() != Node.ELEMENT_NODE)
					continue;

				String strName = childNode.getNodeName();
				if (strName.equals("TABLE_INDEX")) {
					table.m_nTableIndex = Integer.parseInt(childNode.getFirstChild().getNodeValue());
				} else if (strName.equals("GEN_COLUMN_TYPE")) {
					table.m_nGenColumnType = Integer.parseInt(childNode.getFirstChild().getNodeValue());
				} else if (strName.equals("SRC_ID_TYPE")) {
					table.m_nSrcIDType = Integer.parseInt(childNode.getFirstChild().getNodeValue());
				} else if (strName.equals("SRC_ID_ATTRIB_NAME") && childNode.getFirstChild() != null) {
					table.m_strAttributeName = childNode.getFirstChild().getNodeValue();
				} else if (strName.equals("SRC_ID_ATTRIB_VALUE") && childNode.getFirstChild() != null) {
					table.m_strAttributeValue = childNode.getFirstChild().getNodeValue();
				} else if (strName.equals("SUB_ELEMENT_TAGNAME") && childNode.getFirstChild() != null) {
					table.m_strSubTagName = childNode.getFirstChild().getNodeValue();
				} else if (strName.equals("COLUMNS") && childNode.getFirstChild() != null) {
					fillColumns(table.m_listFields, childNode);
				}
			}
		} catch (Exception e) {
			log.error("fillTable异常", e);
		}
	}

	private void fillColumns(Vector<FieldItem> fields, Node columnsNode) {
		NodeList childNodeList = columnsNode.getChildNodes();

		try {
			for (int i = 0; i < childNodeList.getLength(); i++) {
				Node childNode = childNodeList.item(i);
				if (childNode.getNodeType() != Node.ELEMENT_NODE)
					continue;

				String strName = childNode.getNodeName();
				if (strName.equals("FIELDITEM")) {
					FieldItem fieldItem = new FieldItem();
					fields.add(fieldItem);

					NodeList fieldsList = childNode.getChildNodes();
					for (int j = 0; j < fieldsList.getLength(); j++) {
						Node fieldNode = fieldsList.item(j);

						if (fieldNode.getNodeType() != Node.ELEMENT_NODE)
							continue;

						String strFieldElementName = fieldNode.getNodeName();
						if (strFieldElementName.equals("FIELDNAME")) {
							fieldItem.m_strName = fieldNode.getFirstChild().getNodeValue();
						} else if (strFieldElementName.equals("COLUMN_SRC_TYPE")) {
							fieldItem.m_nSrcType = Integer.parseInt(fieldNode.getFirstChild().getNodeValue());
						} else if (strFieldElementName.equals("FIELDINDEX")) {
							fieldItem.m_nFieldIndex = Integer.parseInt(fieldNode.getFirstChild().getNodeValue());
						} else if (strFieldElementName.equals("FIELDTYPE")) {
							fieldItem.m_strType = fieldNode.getFirstChild().getNodeValue();
						} else if (strFieldElementName.equals("DATEFORMAT")) {
							fieldItem.m_strDateFormat = fieldNode.getFirstChild().getNodeValue();
						} else if (strFieldElementName.equals("INNER_ELEMENT_TAGNAME")) {
							fieldItem.m_strInnerTagName = fieldNode.getFirstChild().getNodeValue();
						} else if (strFieldElementName.equals("ELEMENT_ATTRIBUTE")) {
							fieldItem.m_strAttributeName = fieldNode.getFirstChild().getNodeValue();
						} else if (strFieldElementName.equals("ATTRIBUTE_VALUE")) {
							fieldItem.m_strAttributeValue = fieldNode.getFirstChild().getNodeValue();
						}

					}
				}
			}
		} catch (Exception e) {
			log.error("fillColumns异常", e);
		}
	}
}

package templet;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import framework.SystemConfig;

/**
 * 按行解析模板
 * 
 * @author IGP TDT
 * @since 1.0
 */
public class LineTempletP extends AbstractTempletBase {

	public class FieldTemplet {

		public int m_nFieldIndex;// 字段的索引

		public String m_strFieldName = ""; // 字段的名称

		public int m_nStartPos = 0; // 开始截取的位置

		public int m_nDataLength = 0; // 数据的长度

		public String m_type; // 数据的类型，已知类型 string,date，可以为null，表示字符串类型

		public String m_dateFormat; // 如果是date类型，此值有效
	}

	public class SubTemplet {

		public String m_strFileName = "";// 根据文件名来决定属于什么表

		public int m_nFileNameCompare = 0;// 文件名称的比较方式

		public String m_RawColumnList = "";// 原始列的字段信息

		public String m_ColumnListAppend = "";// 需要追加的字段

		public String m_strLineHeadSign = "";// 开始标志位

		public int m_nLineHeadType = 0; // 开始字符的类型

		public int m_nColumnCount = 0; // 每行的字段个数

		public String m_strFieldSplitSign = ""; // 字段之间的分隔符号

		public String m_strFieldUpSplitSign = ""; // 字段之间的特殊分割符号,默认为空，如果有首先以该字符作为分割符。

		public boolean m_bEscape = true; // 是否转义

		public String m_strNewFieldSplitSign = ""; // 新字段之间分割符号

		public int m_nParseType;// 解析类型 0 split 类型 1 按位截取

		public Map<Integer, FieldTemplet> m_Filed = new HashMap<Integer, FieldTemplet>();

		public String nvl = "0"; // 默认为0，没有该节点认为为0
	}

	public int nScanType = 0;// 扫描的类型

	public String BeginSign = "";// 开始标志

	public String EndSign = "";// 结束标志

	public Vector<SubTemplet> m_nTemplet = new Vector<SubTemplet>();

	public Vector<String> unReserved = new Vector<String>();// 不需要保留的字段

	// 有些模板需要只取其中的某些字段,如果为空的话,表示全部导入
	public Map<Integer, String> columnMapping = new HashMap<Integer, String>();

	/**
	 * 解析模板
	 */
	public void parseTemp(String tmpName) throws Exception {
		if (tmpName == null || tmpName.trim().equals(""))
			return;

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = null;
		// 模板文件路径
		String templetFilePath = null;
		templetFilePath = SystemConfig.getInstance().getTempletPath() + File.separatorChar + tmpName;
		File file = new File(templetFilePath);
		doc = builder.parse(file);

		NodeList pn = doc.getElementsByTagName("PUBLIC");
		if (pn.getLength() >= 1) {
			nScanType = Integer.parseInt(doc.getElementsByTagName("SCANTYPE").item(0).getFirstChild().getNodeValue());
		}
		// 获取非保留字段信息
		NodeList nl = doc.getElementsByTagName("UNSTR");
		for (int i = 0; i < nl.getLength(); i++) {
			String m_NodeValue = doc.getElementsByTagName("UNSTR").item(i).getFirstChild().getNodeValue();
			unReserved.add(m_NodeValue);
		}

		// 获取保留字段信息
		NodeList nl2 = doc.getElementsByTagName("RITEM");
		for (int i = 0; i < nl2.getLength(); i++) {
			SubTemplet SubTemp = new SubTemplet();
			String strFileName = "";
			if (doc.getElementsByTagName("FILENAME").item(i) != null && doc.getElementsByTagName("FILENAME").item(i).getFirstChild() != null)
				strFileName = doc.getElementsByTagName("FILENAME").item(i).getFirstChild().getNodeValue();

			int nFileNameCompare = 0;
			if (doc.getElementsByTagName("FILENAMECOMPARE").item(i) != null
					&& doc.getElementsByTagName("FILENAMECOMPARE").item(i).getFirstChild() != null)
				nFileNameCompare = Integer.parseInt(doc.getElementsByTagName("FILENAMECOMPARE").item(i).getFirstChild().getNodeValue());

			String strRawColumnList = "";// 原始列名
			if (doc.getElementsByTagName("COLUMNLISTSIGN").item(i) != null
					&& doc.getElementsByTagName("COLUMNLISTSIGN").item(i).getFirstChild() != null)
				strRawColumnList = doc.getElementsByTagName("COLUMNLISTSIGN").item(i).getFirstChild().getNodeValue();
			String strColumnsAppend = "";// 需要追加的列名
			if (doc.getElementsByTagName("APPENDCOLUMNLIST").item(i) != null
					&& doc.getElementsByTagName("APPENDCOLUMNLIST").item(i).getFirstChild() != null)
				strColumnsAppend = doc.getElementsByTagName("APPENDCOLUMNLIST").item(i).getFirstChild().getNodeValue();
			String LineHeadSign = "";
			if (doc.getElementsByTagName("LINEHEADSIGN").item(i) != null && doc.getElementsByTagName("LINEHEADSIGN").item(i).getFirstChild() != null)
				LineHeadSign = doc.getElementsByTagName("LINEHEADSIGN").item(i).getFirstChild().getNodeValue();
			int LineHeadType = Integer.parseInt(doc.getElementsByTagName("LINEHEADTYPE").item(i).getFirstChild().getNodeValue());
			int nParseType = Integer.parseInt(doc.getElementsByTagName("PARSETYPE").item(i).getFirstChild().getNodeValue());
			int m_nColumnCount = Integer.parseInt(doc.getElementsByTagName("COLUMNCOUNT").item(i).getFirstChild().getNodeValue());
			String m_FieldSplitSign = doc.getElementsByTagName("FIELDSPLITSIGN").item(i).getFirstChild().getNodeValue();
			String m_NewFieldSplitSign = doc.getElementsByTagName("NEWFIELDSPLITSIGN").item(i).getFirstChild().getNodeValue();
			String m_FieldUpSplitSign = "";
			// 是否有UP分隔符
			if (doc.getElementsByTagName("FIELDUPSPLITSIGN") != null && doc.getElementsByTagName("FIELDUPSPLITSIGN").item(i) != null
					&& doc.getElementsByTagName("FIELDUPSPLITSIGN").item(i).getFirstChild() != null) {
				m_FieldUpSplitSign = doc.getElementsByTagName("FIELDUPSPLITSIGN").item(i).getFirstChild().getNodeValue();
			}

			// LINEHEADSIGN 为空的时候会出错,为了避免出错,在LINEHEADSIGN设置为NULL表示为空

			// 是否转义
			if (doc.getElementsByTagName("ESCAPCHAR") != null && doc.getElementsByTagName("ESCAPCHAR").item(i) != null
					&& doc.getElementsByTagName("ESCAPCHAR").item(i).getFirstChild() != null) {
				String strEscape = doc.getElementsByTagName("ESCAPCHAR").item(i).getFirstChild().getNodeValue();
				if (strEscape != null && strEscape.equals("0"))
					SubTemp.m_bEscape = false;
			}

			// 是否有nvl节点,没有的时候默认为0
			String nvl = "0";
			if (doc.getElementsByTagName("nvl") != null && doc.getElementsByTagName("nvl").item(i) != null) {
				if (doc.getElementsByTagName("nvl").item(i).getFirstChild() == null)
					nvl = "";
				else
					nvl = doc.getElementsByTagName("nvl").item(i).getFirstChild().getNodeValue();
			}
			SubTemp.nvl = nvl;

			SubTemp.m_strFileName = strFileName;
			SubTemp.m_nFileNameCompare = nFileNameCompare;

			SubTemp.m_RawColumnList = strRawColumnList;
			SubTemp.m_ColumnListAppend = strColumnsAppend;
			if (LineHeadSign.equals("NULL"))
				SubTemp.m_strLineHeadSign = "";
			else
				SubTemp.m_strLineHeadSign = LineHeadSign;

			SubTemp.m_nLineHeadType = LineHeadType;
			SubTemp.m_nColumnCount = m_nColumnCount;
			SubTemp.m_strFieldSplitSign = m_FieldSplitSign;
			SubTemp.m_strFieldUpSplitSign = m_FieldUpSplitSign;
			SubTemp.m_strNewFieldSplitSign = m_NewFieldSplitSign;
			SubTemp.m_nParseType = nParseType;

			if (doc.getElementsByTagName("COLUMNS").item(i) != null && doc.getElementsByTagName("COLUMNS").item(i).getFirstChild() != null) {
				Node fieldnode = doc.getElementsByTagName("COLUMNS").item(i);
				parseFieldInfo(SubTemp.m_Filed, fieldnode);
			}

			m_nTemplet.add(SubTemp);
		}
	}

	private void parseFieldInfo(Map<Integer, FieldTemplet> tableInfo, Node currentNode) {
		NodeList Ssn = currentNode.getChildNodes();
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
									field.m_nFieldIndex = 0;
								else
									field.m_nFieldIndex = Integer.parseInt(strValue);
							}

							else if (NodeName.equals("FIELDNAME")) {
								field.m_strFieldName = strValue;
							} else if (NodeName.equals("STARTPOS")) {
								field.m_nStartPos = Integer.parseInt(strValue);
							} else if (NodeName.equals("DATALENGTH")) {
								field.m_nDataLength = Integer.parseInt(strValue);
							} else if (NodeName.equals("FIELDTYPE")) {
								field.m_type = strValue;
							} else if (NodeName.equals("DATEFORMAT")) {
								field.m_dateFormat = strValue;
							}
						}
					}
					tableInfo.put(field.m_nFieldIndex, field);
					columnMapping.put(field.m_nFieldIndex, field.m_strFieldName);
				}
			}
		}
	}

	public static void main(String[] args) {
		new LineTempletP().buildTmp(9);
	}

}

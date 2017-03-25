package parser;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import task.CollectObjInfo;
import templet.XMLTempletP;
import templet.XMLTempletP.FieldItem;
import templet.XMLTempletP.ParseTag;
import templet.XMLTempletP.Table;

public class XMLParser extends Parser {

	public XMLParser() {
	}

	public XMLParser(CollectObjInfo collectInfo) {
		super(collectInfo);
	}

	protected String normalizeString(String s) {
		s = s.trim();
		s = s.replaceAll(";", " ");
		s = s.replaceAll("\n", " ");
		return s;
	}

	@Override
	public boolean parseData() throws Exception {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;
		builder = factory.newDocumentBuilder();

		File file = new File(fileName);
		Document doc = builder.parse(file);

		// 解析模板
		XMLTempletP templet = (XMLTempletP) (collectObjInfo.getParseTemplet());
		for (ParseTag parseTag : templet.m_listParseTag) {
			NodeList nl = doc.getElementsByTagName(parseTag.m_strTagName);
			for (int i = 0; i < nl.getLength(); i++) {
				Node node = nl.item(i);
				// 遍历Table
				for (Table table : parseTag.m_listTable) {
					// 查看标识，是否使用此Node;
					switch (table.m_nSrcIDType) {
						case 1 : // Tag做标识,所有此Tag的都可以
							break;
						case 2 : // 属性做标识,只有与属性相同的可以
						{
							Node attrib = node.getAttributes().getNamedItem(table.m_strAttributeName);
							if (attrib == null)
								continue;
							String attribValue = attrib.getFirstChild().getNodeValue();
							if (!attribValue.equals(table.m_strAttributeValue))
								continue;
						}
							break;
						default :
							continue;
					}

					// TODO:附加列尚未实现

					// 处理此Table

					// 组成新行需要的字符串
					StringBuffer strNewRow = new StringBuffer();
					// 每行添加 OMCID 并添加新的分隔符号
					strNewRow.append(collectObjInfo.getDevInfo().getOmcID());
					strNewRow.append(";");
					// 添加当前时间 格式YYYY-MM-DD HH24:MI:SS 并添加新的分隔符号
					Date now = new Date();
					SimpleDateFormat spformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					String strTime = spformat.format(now);
					strNewRow.append(strTime + ";");

					strTime = spformat.format(collectObjInfo.getLastCollectTime());
					strNewRow.append(strTime + ";");

					try {
						switch (table.m_nGenColumnType) {
							case 1 :
								strNewRow = parseTableLines1(table, node, strNewRow);
								break;
							case 2 :
								strNewRow = parseTableLines2(table, node, strNewRow);
								break;
							case 3 :
								strNewRow = parseTableLines3(table, node, strNewRow);
								break;
							default :
								continue;
						}
					} catch (Exception e) {
						e.printStackTrace();
						continue;
					}

					distribute.DistributeData(strNewRow.toString().getBytes(), table.m_nTableIndex);
				}
			}
		}

		return true;
	}

	/**
	 * 解析出一个table的一行
	 * 
	 * @param table
	 * @param nl
	 * @return
	 */
	private StringBuffer parseTableLines1(Table table, Node node, StringBuffer strNewRow) {
		// 取得要解析的标记信息

		StringBuffer strLine = new StringBuffer();

		String strTemp = node.getFirstChild().getNodeValue();
		strLine.append(normalizeString(strTemp.trim()));

		/*
		 * //添加附加列 for(int k = 0; k < tagInfo.m_mapAddColInfo.size(); ++k ) { ParseTemplet_XML.AdditionalColumnInfo addColInfo =
		 * tagInfo.m_mapAddColInfo.get( Integer.valueOf(k) ); Node addCol = doc.getElementsByTagName( addColInfo.m_strColTag ).item(j); if(
		 * addColInfo.m_strColAttr == null || addColInfo.m_strColAttr.equals("") ) { //属性名为空，Tag的内容作为附加列的值 strNewRow.append(
		 * addCol.getFirstChild().getNodeValue() ); } else { NamedNodeMap attribs = addCol.getAttributes(); String strAttrib = attribs.getNamedItem(
		 * addColInfo.m_strColAttr ).getNodeValue(); if( addColInfo.m_strColName == null || addColInfo.m_strColName.equals("") ) { //列名为空，以属性值作为附加列的值
		 * strNewRow.append( strAttrib ); } else { //解析属性值 Properties props = new Properties(); try{ strAttrib = strAttrib.replace( ",", "\n" );
		 * props.load( new ByteArrayInputStream ( strAttrib.getBytes() ) ); } catch( Exception exp ) { exp.printStackTrace(); } strNewRow.append(
		 * props.getProperty( addColInfo.m_strColName ) ); } } strNewRow.append( ";" ); }
		 */

		// 提取所需的列
		if (table.m_listFields.size() != 0) {
			String[] fields = strLine.toString().split(";");

			for (FieldItem field : table.m_listFields) {
				if (field.m_nFieldIndex > 0 && field.m_nFieldIndex < fields.length) {
					strNewRow.append(fields[field.m_nFieldIndex] + ";");
				} else
					strNewRow.append(";");

			}
		}
		strNewRow.append("\n");
		return strNewRow;
	}

	private StringBuffer parseTableLines2(Table table, Node node, StringBuffer strNewRow) {
		// 取得要解析的标记信息

		StringBuffer strLine = new StringBuffer();

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {
			if (child.getNodeType() == Node.ELEMENT_NODE && child.getNodeName().equals(table.m_strSubTagName)) {
				String strValue = child.getFirstChild().getNodeValue();
				if (strValue == null) {
					for (Node child2 = child.getFirstChild(); child2 != null; child2 = child2.getNextSibling()) {
						if (child2.getNodeType() == Node.ELEMENT_NODE) {
							strValue = child2.getFirstChild().getNodeValue();
							break;
						}
					}
				}
				strLine.append(normalizeString(strValue));
				strLine.append(";");
			}
		}
		// 删除最后一个分号，为了统一格式
		strLine.deleteCharAt(strLine.length() - 1);

		/*
		 * //添加附加列 for(int k = 0; k < tagInfo.m_mapAddColInfo.size(); ++k ) { ParseTemplet_XML.AdditionalColumnInfo addColInfo =
		 * tagInfo.m_mapAddColInfo.get( Integer.valueOf(k) ); Node addCol = doc.getElementsByTagName( addColInfo.m_strColTag ).item(j); if(
		 * addColInfo.m_strColAttr == null || addColInfo.m_strColAttr.equals("") ) { //属性名为空，Tag的内容作为附加列的值 strNewRow.append(
		 * addCol.getFirstChild().getNodeValue() ); } else { NamedNodeMap attribs = addCol.getAttributes(); String strAttrib = attribs.getNamedItem(
		 * addColInfo.m_strColAttr ).getNodeValue(); if( addColInfo.m_strColName == null || addColInfo.m_strColName.equals("") ) { //列名为空，以属性值作为附加列的值
		 * strNewRow.append( strAttrib ); } else { //解析属性值 Properties props = new Properties(); try{ strAttrib = strAttrib.replace( ",", "\n" );
		 * props.load( new ByteArrayInputStream ( strAttrib.getBytes() ) ); } catch( Exception exp ) { exp.printStackTrace(); } strNewRow.append(
		 * props.getProperty( addColInfo.m_strColName ) ); } } strNewRow.append( ";" ); }
		 */
		// 提取所需的列
		if (table.m_listFields.size() != 0) {
			String[] fields = strLine.toString().split(";");

			for (FieldItem field : table.m_listFields) {
				if (field.m_nFieldIndex > 0 && field.m_nFieldIndex < fields.length) {
					strNewRow.append(fields[field.m_nFieldIndex] + ";");
				} else
					strNewRow.append(";");

			}
		}
		strNewRow.append("\n");
		return strNewRow;
	}

	private StringBuffer parseTableLines3(Table table, Node node, StringBuffer strNewRow) {
		for (FieldItem field : table.m_listFields) {
			switch (field.m_nSrcType) {
				case 1 : // 1-源于SRC_ELEMENT的属性
				{
					// 取得属性
					Node attrib = node.getAttributes().getNamedItem(field.m_strAttributeName);
					if (attrib == null) {
						strNewRow.append(";");
						continue;
					}
					String sValue = attrib.getFirstChild().getNodeName();
					strNewRow.append(normalizeString(sValue) + ";");
				}
					break;
				case 2 : // 2-源于下层ELEMENT的值
				{
					// 取得下层ELEMENT值
					NodeList childList = node.getChildNodes();
					for (int i = 0; i < childList.getLength(); i++) {
						Node child = childList.item(i);

						if (child.getNodeType() != Node.ELEMENT_NODE)
							continue;

						if (child.getNodeName().equals(field.m_strInnerTagName)) {
							Node firstChild = child.getFirstChild();
							if (firstChild != null) {
								String sValue = firstChild.getNodeValue();
								strNewRow.append(normalizeString(sValue));
							}
							break;
						}
					}
					strNewRow.append(";");
					continue;
				}
				case 3 : // 3-源于匹配下层Element属性条件的Element值
				{
					// 取得匹配下层Element属性条件的Element值
					NodeList childList = node.getChildNodes();
					for (int i = 0; i < childList.getLength(); i++) {
						Node child = childList.item(i);

						if (child.getNodeType() != Node.ELEMENT_NODE)
							continue;

						if (child.getNodeName().equals(field.m_strInnerTagName)) {
							// 找到了Element
							// 找属性
							NamedNodeMap attribMap = child.getAttributes();
							if (attribMap == null) {
								continue;
							}
							Node attrib = attribMap.getNamedItem(field.m_strAttributeName);
							if (attrib != null) {
								String attribValue = attrib.getFirstChild().getNodeValue();
								if (attribValue.equals(field.m_strAttributeValue)) {
									// 匹配上了
									String strValue = child.getFirstChild().getNodeValue();
									strNewRow.append(normalizeString(strValue));
									break;
								}
							}
							// 没有匹配
							continue;
						}
					}
					// 都没有匹配，此字段填空
					strNewRow.append(";");
					continue;
				}
				default :
					strNewRow.append(";");
					continue;
			}

		}
		strNewRow.append("\n");
		return strNewRow;
	}
}

package templet;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import framework.SystemConfig;

/**
 * 按段来分析的模板,SECTITEM为table
 * 
 * @author IGP TDT
 * @since 1.0
 */
public class Sect21TempletP extends AbstractTempletBase {

	// 定义段的字段类
	public class FieldTemplet {

		public int m_nFieldIndex;// 字段的索引号

		public String m_strFieldName;// 字段名称

		public int m_nParseType;// 解析的类型

		public int m_nStartPos; // 按位分析字段时，字段的开始位置

		public int m_nDataLength;// 解析的类型

		public String m_strHeadFieldSign;// 字段的开始标记

		public String m_strTailFieldSign;// 字段的开始标记

		public String m_strSubFieldRowSplitSign;// 子字段中行分隔标记

		public boolean m_bSubSectSplit;// 子段时候需要split.默认不需要

		public String m_strSubFieldColSplitSign;// 子字段中列分隔标记

		// 子字段所在列的索引与字段
		public Map<Integer, FieldTemplet> m_SubFieldTemplet = new HashMap<Integer, FieldTemplet>();
	}

	// 定义段类
	public class SectTemplet {

		public int m_nSectTypeIndex; // 段类型索引

		public int m_nSectKeySearchType;// 段关键字判断的类型

		public String m_strSectKeyWord;// 段的关键字

		public String m_strNewSplitSign;// 新的字段之间的分隔符号

		public Map<Integer, FieldTemplet> m_FieldTemplet = new HashMap<Integer, FieldTemplet>();
	}

	public int m_nSectScanType = 0;// 段的分隔类型

	public String m_strSectSplitSign = "";// 按照段来分隔标记

	public String m_strHeadSectSplitSign = "";// 开始分隔标记

	public String m_strTailSectSplitSign = "";// 结束分隔标记

	public String m_strAllNewSplitSign = ";"; //

	// 存储段的模板信息
	public Map<Integer, SectTemplet> m_SectTemplet = new HashMap<Integer, SectTemplet>();

	// 公共字段信息
	public Map<Integer, FieldTemplet> m_CommonTemplet = new HashMap<Integer, FieldTemplet>();

	public void parseTemp(String TempContent) throws Exception {
		if (TempContent == null || TempContent.trim().equals(""))
			return;

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = null;

		// 模板文件路径
		String TempletFilePath = SystemConfig.getInstance().getTempletPath();
		TempletFilePath = TempletFilePath + File.separatorChar + TempContent;
		File file = new File(TempletFilePath);
		doc = builder.parse(file);// builder.parse(new
		// ByteArrayInputStream(TempContent.getBytes()));

		NodeList pn = doc.getElementsByTagName("PUBLIC");
		if (pn.getLength() >= 1) {
			if (doc.getElementsByTagName("SECTTYPE").item(0).getFirstChild() == null)
				this.m_nSectScanType = 0;
			else
				this.m_nSectScanType = Integer.parseInt(doc.getElementsByTagName("SECTTYPE").item(0).getFirstChild().getNodeValue());
			if (doc.getElementsByTagName("SECTSIGN").item(0).getFirstChild() == null)
				this.m_strSectSplitSign = "";
			else
				this.m_strSectSplitSign = doc.getElementsByTagName("SECTSIGN").item(0).getFirstChild().getNodeValue();
			this.m_strSectSplitSign = WrapPromptChange(this.m_strSectSplitSign);

			if (doc.getElementsByTagName("HEADSIGN").item(0).getFirstChild() == null)
				this.m_strHeadSectSplitSign = "";
			else
				this.m_strHeadSectSplitSign = doc.getElementsByTagName("HEADSIGN").item(0).getFirstChild().getNodeValue();
			this.m_strHeadSectSplitSign = WrapPromptChange(this.m_strHeadSectSplitSign);

			if (doc.getElementsByTagName("TAILSIGN").item(0).getFirstChild() == null)
				this.m_strTailSectSplitSign = "";
			else
				this.m_strTailSectSplitSign = doc.getElementsByTagName("TAILSIGN").item(0).getFirstChild().getNodeValue();
			this.m_strTailSectSplitSign = WrapPromptChange(this.m_strTailSectSplitSign);

			if (doc.getElementsByTagName("ALL_NEWSPLITSIGN").item(0).getFirstChild() == null)
				this.m_strAllNewSplitSign = ";";
			else
				this.m_strAllNewSplitSign = doc.getElementsByTagName("ALL_NEWSPLITSIGN").item(0).getFirstChild().getNodeValue();

			if (doc.getElementsByTagName("COMMONFIELDLIST").item(0) != null) {
				Node fieldnode = doc.getElementsByTagName("COMMONFIELDLIST").item(0);
				ParseField(m_CommonTemplet, fieldnode);
			}
		}
		NodeList Sn = doc.getElementsByTagName("SECTITEM");

		for (int i = 0; i < Sn.getLength(); i++) {
			SectTemplet secttemp = new SectTemplet();
			if (doc.getElementsByTagName("SECTINDEX").item(i).getFirstChild() == null)
				secttemp.m_nSectTypeIndex = 0;
			else
				secttemp.m_nSectTypeIndex = Integer.parseInt(doc.getElementsByTagName("SECTINDEX").item(i).getFirstChild().getNodeValue());
			// );
			if (doc.getElementsByTagName("KEYSEARCHTYPE").item(i).getFirstChild() == null)
				secttemp.m_nSectKeySearchType = 0;
			else
				secttemp.m_nSectKeySearchType = Integer.parseInt(doc.getElementsByTagName("KEYSEARCHTYPE").item(i).getFirstChild().getNodeValue());
			// );
			if (doc.getElementsByTagName("KEYWORD").item(i).getFirstChild() == null)
				secttemp.m_strSectKeyWord = "";
			else
				secttemp.m_strSectKeyWord = WrapPromptChange(doc.getElementsByTagName("KEYWORD").item(i).getFirstChild().getNodeValue());
			secttemp.m_strSectKeyWord = WrapPromptChange(secttemp.m_strSectKeyWord);

			/*
			 * if(doc.getElementsByTagName("COMMONFIELDLIST").item(i)==null||doc. getElementsByTagName
			 * ("COMMONFIELDLIST").item(i).getFirstChild()==null) secttemp.m_strCommonFieldList =""; else secttemp.m_strCommonFieldList =
			 * WrapPromptChange(doc.getElementsByTagName ("COMMONFIELDLIST").item(i).getFirstChild().getNodeValue()); secttemp.m_strCommonFieldList =
			 * WrapPromptChange(secttemp.m_strCommonFieldList);
			 */

			// System.out.println("段：m_strSectKeyWord="+secttemp.m_strSectKeyWord
			// );
			if (doc.getElementsByTagName("NEWSPLITSIGN").item(i).getFirstChild() == null)
				secttemp.m_strNewSplitSign = ";";
			else
				secttemp.m_strNewSplitSign = WrapPromptChange(doc.getElementsByTagName("NEWSPLITSIGN").item(i).getFirstChild().getNodeValue());
			secttemp.m_strNewSplitSign = WrapPromptChange(secttemp.m_strNewSplitSign);
			// System.out.println("段：m_strNewSplitSign="+secttemp.m_strNewSplitSign
			// );
			if (doc.getElementsByTagName("FIELDS").item(i) != null) {
				Node fieldnode = doc.getElementsByTagName("FIELDS").item(i);
				m_SectTemplet.put(secttemp.m_nSectTypeIndex, secttemp);
				ParseField(secttemp.m_FieldTemplet, fieldnode);
			}
			m_SectTemplet.put(secttemp.m_nSectTypeIndex, secttemp);

		}

	}

	// 回调函数,根据当前节点CurrentNode,解析一个字段的信息
	public void ParseField(Map<Integer, FieldTemplet> mapfield, Node CurrentNode) {
		NodeList Ssn = CurrentNode.getChildNodes();
		// 节点的SubFields下面是否存在自节点
		for (int nIndex = 0; nIndex < Ssn.getLength(); nIndex++) {
			Node tempnode = Ssn.item(nIndex);
			// 判断当前节点是否是 FIELDITEM 节点 判断依据根据节点类型与节点名称
			if (tempnode.getNodeType() == Node.ELEMENT_NODE && tempnode.getNodeName().toUpperCase().equals("FIELDITEM")) {
				FieldTemplet fieldtemp = new FieldTemplet();
				NodeList childnodeList = tempnode.getChildNodes();
				if (childnodeList != null) {
					for (int i = 0; i < childnodeList.getLength(); i++) {
						Node childnode = childnodeList.item(i);

						if (childnode.getNodeType() == Node.ELEMENT_NODE) {
							// 节点名称
							String NodeName = childnode.getNodeName().toUpperCase();
							if (NodeName.equals("SUBFIELDS")) {
								if (existSubField(childnode))
									ParseField(fieldtemp.m_SubFieldTemplet, childnode);
							} else {
								// 获取节点的值
								String strValue = getNodeValue(childnode);
								if (NodeName.equals("FIELDINDEX")) {
									if (strValue.trim().equals(""))
										fieldtemp.m_nFieldIndex = -1;
									else
										fieldtemp.m_nFieldIndex = Integer.parseInt(strValue);
								} else if (NodeName.equals("FIELDNAME")) {
									fieldtemp.m_strFieldName = strValue;
								} else if (NodeName.equals("PARSETYPE")) {
									if (strValue.trim().equals(""))
										fieldtemp.m_nParseType = -1;
									else
										fieldtemp.m_nParseType = Integer.parseInt(strValue);
								} else if (NodeName.equals("STARTPOS")) {
									if (strValue.trim().equals(""))
										fieldtemp.m_nStartPos = -1;
									else
										fieldtemp.m_nStartPos = Integer.parseInt(strValue);
								} else if (NodeName.equals("DATALENGTH")) {
									if (strValue.trim().equals(""))
										fieldtemp.m_nDataLength = -1;
									else
										fieldtemp.m_nDataLength = Integer.parseInt(strValue);
								} else if (NodeName.equals("HEADSIGN")) {
									fieldtemp.m_strHeadFieldSign = WrapPromptChange(strValue);
								} else if (NodeName.equals("TAILSIGN")) {
									fieldtemp.m_strTailFieldSign = WrapPromptChange(strValue);
								} else if (NodeName.equals("ROWSPLITSIGN")) {
									fieldtemp.m_strSubFieldRowSplitSign = WrapPromptChange(strValue);
								} else if (NodeName.equals("SUBISSPLIT")) {
									if (strValue.trim().equals("1"))
										fieldtemp.m_bSubSectSplit = true;
									else
										fieldtemp.m_bSubSectSplit = false;
								} else if (NodeName.equals("COLSPLITSIGN")) {
									fieldtemp.m_strSubFieldColSplitSign = WrapPromptChange(strValue);
								}
							}
						}
					}
				}
				mapfield.put(fieldtemp.m_nFieldIndex, fieldtemp);
			}// if
		}// for nIndex
	}
}

package templet;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import parser.GenericSectionHeadParser;
import parser.MacroParser;
import parser.xparser.IllegalTagException;
import util.Util;
import exception.InvalidParameterValueException;
import framework.SystemConfig;

/**
 * 带表头的按段解析解析模板类
 * 
 * @author Litp
 * @since 3.1
 * @see GenericSectionHeadParser
 * @see GenericSectionHeadD
 */
public class GenericSectionHeadP extends AbstractTempletBase {

	/** <需要解析的文件(对应file属性),templet对象> */
	private Map<String, Templet> templets = new HashMap<String, Templet>();

	public Map<String, Templet> getTemplets() {
		return templets;
	}

	@Override
	public void parseTemp(String tempContent) throws Exception {
		if (Util.isNull(tempContent))
			return;

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();

		// 模板文件路径
		String templetFilePath = SystemConfig.getInstance().getTempletPath() + File.separatorChar + tempContent.trim();
		File file = new File(templetFilePath);
		Document doc = builder.parse(file);
		NodeList templetList = doc.getElementsByTagName("templet");
		int templetsSize = templetList.getLength();
		if (templetsSize <= 0)
			return;
		Templet temObj = null;
		// 遍历所有templet节点
		for (int i = 0; i < templetsSize; i++) {
			temObj = new Templet();
			Node templet = templetList.item(i);
			// templet节点id
			int id = Integer.parseInt(templet.getAttributes().getNamedItem("id").getNodeValue());
			if (id < 0)
				throw new InvalidParameterValueException("templet id = " + id);

			// templet节点的file属性
			Node fileAtrr = templet.getAttributes().getNamedItem("file");
			if (fileAtrr != null) {
				String fileName = fileAtrr.getNodeValue();
				if (Util.isNull(fileName))
					throw new InvalidParameterValueException("file属性值不能为空");
				temObj.setFileName(fileName);
			} else
				throw new IllegalTagException("缺少file属性");

			//在templet标签总增加type属性，来判断是否DT，如果是，做回填字段处理（GRID_ID、PIECE_ID等）
			Node type = templet.getAttributes().getNamedItem("type");
			if (type != null) {
				String value = type.getNodeValue();
				temObj.setType(value);
			}
			
			/* templet节点的skipLine属性，读取原始文件时，要忽略掉的行数，有些文件，第二行才是列头，第一行是不用的。 */
			Node attrSkip = templet.getAttributes().getNamedItem("skipLine");
			if (attrSkip != null) {
				String strSkip = attrSkip.getNodeValue();
				if (Util.isNotNull(strSkip)) {
					int skip = 0;
					try {
						skip = Integer.parseInt(strSkip.trim());
					} catch (NumberFormatException e) {
					}
					if (skip >= 0)
						temObj.setSkipLine(skip);
				}
			}

			/* 合并表头 */
			Node combineheader = templet.getAttributes().getNamedItem("combineflag");
			if (combineheader != null) {
				String combine = combineheader.getNodeValue();
				if (Util.isNotNull(combine)) {
					boolean b = false;
					try {
						if (combine.trim().equals("1"))
							b = true;
					} catch (NumberFormatException e) {
					}
					temObj.setCombineflag(b);
				}
			}

			/* 文件编码方式。 */
			Node attrEnc = templet.getAttributes().getNamedItem("encoding");
			if (attrEnc != null) {
				String strEnc = attrEnc.getNodeValue();
				if (Util.isNotNull(strEnc))
					temObj.setEncoding(strEnc.trim());
			}

			Map<Integer, DS> dsMap = new HashMap<Integer, DS>(); // 所有ds节点
			for (Node node = templet.getFirstChild(); node != null; node = node.getNextSibling()) {
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					String nodeName = node.getNodeName();
					if (nodeName.equalsIgnoreCase("public")) {
						Public publicObj = getPublic(node);
						temObj.setPublicElement(publicObj);
					} else if (nodeName.equalsIgnoreCase("ds")) {
						DS dsObj = getDS(node);
						dsMap.put(dsObj.getId(), dsObj);
					}
				}
			}
			temObj.setDsMap(dsMap);
			temObj.setId(id);
			templets.put(temObj.getFileName(), temObj);
		}
	}

	/**
	 * 给点public节点，转化成Public对象
	 * 
	 * @param publicNode
	 * @return
	 */
	private Public getPublic(Node publicNode) throws Exception {
		Public pub = new Public();
		for (Node node = publicNode.getFirstChild(); node != null; node = node.getNextSibling()) {
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				String nodeName = node.getNodeName();
				if (nodeName.equalsIgnoreCase("startSign")) {
					pub.setStartSign(node.getFirstChild().getNodeValue());
				} else if (nodeName.equalsIgnoreCase("endSign")) {
					pub.setEndSign(node.getFirstChild().getNodeValue());
				} else if (nodeName.equalsIgnoreCase("fields")) {
					Fields fields = getFields(node);
					pub.setFields(fields);
				}
			}
		}
		return pub;
	}

	/**
	 * 给定ds节点，转化成DS对象
	 * 
	 * @param dsNode
	 * @return
	 */
	private DS getDS(Node dsNode) throws Exception {
		DS ds = new DS();
		int dsId = Integer.parseInt(dsNode.getAttributes().getNamedItem("id").getNodeValue());
		if (dsId < 0)
			throw new InvalidParameterValueException("ds id = " + dsId);

		ds.setId(dsId);
		
		ds.setIndexDepend(false);
		if(dsNode.getAttributes().getNamedItem("fieldDepend") != null && 
				"index".equalsIgnoreCase(dsNode.getAttributes().getNamedItem("fieldDepend").getNodeValue())){
			ds.setIndexDepend(true);
		}

		for (Node node = dsNode.getFirstChild(); node != null; node = node.getNextSibling()) {
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				String nodeName = node.getNodeName();
				if (nodeName.equalsIgnoreCase("meta")) {
					ds.setMeta(getMeta(node));
				} else if (nodeName.equalsIgnoreCase("fields")) {
					Fields fields = getFields(node);
					ds.setFields(fields);

					Collection<Field> cFields = fields.getFields().values();
					if (!cFields.isEmpty()) {
						List<String> ro = new ArrayList<String>();
						for (Field field : cFields) {
							String occur = field.getOccur();
							if (occur != null && occur.equalsIgnoreCase("required")) {
								ro.add(field.getName());
							}
						}
						ds.setRequiredOccur(ro);
					}
				}

			}
		}
		return ds;
	}

	/**
	 * 给定meta节点，转化成Meta对象
	 * 
	 * @param metaNode
	 * @return
	 */
	private Meta getMeta(Node metaNode) {
		Meta meta = new Meta();
		for (Node node = metaNode.getFirstChild(); node != null; node = node.getNextSibling()) {
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				String nodeName = node.getNodeName();
				if (nodeName.equalsIgnoreCase("endSign")) {
					String endSign = node.getFirstChild().getNodeValue();
					meta.setEndSign(endSign);
				} else if (nodeName.equalsIgnoreCase("head")) {
					Node nss = node.getAttributes().getNamedItem("splitSign");
					if (nss != null) {
						String splitSign = nss.getNodeValue();
						meta.setHeadSplitSign(splitSign);
					} else {
						// log.warn("'meta'节点中未找到'splitSign'属性。");
					}
					Node nodeMulSign = node.getAttributes().getNamedItem("multiSplitSign");
					if (nodeMulSign != null) {
						String multiSplitSign = nodeMulSign.getNodeValue();
						if (multiSplitSign != null)
							meta.setMultiSplitSign(multiSplitSign);
					}
				} else if (nodeName.equalsIgnoreCase("startSign")) {
					String startSign = node.getFirstChild().getNodeValue();
					meta.setStartSign(startSign);
				}
			}
		}
		return meta;
	}

	/**
	 * 给定fields节点，转化成Fields对象
	 * 
	 * @param fieldsNode
	 * @return
	 */
	private Fields getFields(Node fieldsNode) throws Exception {
		Fields fields = new Fields();
		Node splitNode = fieldsNode.getAttributes().getNamedItem("splitSign");
		if (splitNode != null) {
			String splitSign = splitNode.getNodeValue();
			fields.setSplitSign(splitSign);
		}

		Node mSplitNode = fieldsNode.getAttributes().getNamedItem("multiSplitSign");
		if (mSplitNode != null) {
			String multiSplitSign = mSplitNode.getNodeValue();
			if (multiSplitSign != null)
				fields.setMultiSplitSign(multiSplitSign);
		}

		Map<Integer, Field> fieldsMap = fields.getFields();
		for (Node node = fieldsNode.getFirstChild(); node != null; node = node.getNextSibling()) {
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				if (node.getNodeName().equalsIgnoreCase("field")) {
					Field f = getField(node);
					fieldsMap.put(f.getIndex(), f);
				}
			}
		}
		return fields;
	}

	/**
	 * 给定field节点，转化成Field对象
	 * 
	 * @param fieldNode
	 * @return
	 */
	private Field getField(Node fieldNode) throws Exception {
		Field field = new Field();

		Node indexNode = fieldNode.getAttributes().getNamedItem("index");
		if (indexNode == null)
			throw new IllegalTagException("缺少index属性");

		int index = Integer.parseInt(indexNode.getNodeValue());
		if (index < 0)
			throw new InvalidParameterValueException("field index = " + index);

		field.setIndex(index);

		Node nameNode = fieldNode.getAttributes().getNamedItem("name");
		if (nameNode != null) {
			String name = nameNode.getNodeValue();
			field.setName(name);
		}
		Node occurNode = fieldNode.getAttributes().getNamedItem("occur");
		if (occurNode != null) {
			String occur = occurNode.getNodeValue();
			field.setOccur(occur);
		}

		// liangww add 2012-03-30 增加对macro的解析
		Node macroNode = fieldNode.getAttributes().getNamedItem("macro");
		if (macroNode != null) {
			String macro = macroNode.getNodeValue();
			field.setMacro(macro);
		}

		// yuy add 2013-07-30 增加对如这种时间格式“2013-7-25 6:55:00.0”的处理
		Node specialTimeNode = fieldNode.getAttributes().getNamedItem("specialTime");
		if (specialTimeNode != null) {
			String specialTime = specialTimeNode.getNodeValue();
			field.setSpecialTime(specialTime);
		}
		
		// 是否分拆字符串 by yuy added 2014-03-12
		Node isSplitNode = fieldNode.getAttributes().getNamedItem("isSplit");
		if (isSplitNode != null) {
			String isSplit = isSplitNode.getNodeValue();
			field.setIsSplit(isSplit);
		}
		
		// 分拆符 by yuy added 2014-03-12
		Node splitSignNode = fieldNode.getAttributes().getNamedItem("splitSign");
		if (splitSignNode != null) {
			String splitSign = splitSignNode.getNodeValue();
			field.setSplitSign(splitSign);
		}
		
		// 值索引，取哪个值 by yuy added 2014-03-12
		Node indexOfValueNode = fieldNode.getAttributes().getNamedItem("indexOfValue");
		if (indexOfValueNode != null) {
			String indexOfValue = indexOfValueNode.getNodeValue();
			field.setIndexOfValue(indexOfValue);
		}

		for (Node node = fieldNode.getFirstChild(); node != null; node = node.getNextSibling()) {
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				String nodeName = node.getNodeName();
				if (nodeName.equalsIgnoreCase("startSign")) {
					field.setStartSign(node.getFirstChild().getNodeValue());
				} else if (nodeName.equalsIgnoreCase("endSign")) {
					field.setEndSign(node.getFirstChild().getNodeValue());
				}
			}
		}

		return field;
	}

	public static void main(String[] args) {
		GenericSectionHeadP p = new GenericSectionHeadP();
		try {
			p.parseTemp("clt_js_bsa_parse.xml");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/** 对应模板中templet标签 */
	public class Templet {

		private int id;

		private String fileName;

		private Public publicElement;

		private int skipLine;

		private String encoding;
		
		private String type;//是否actix路测

		/** <ds的id,ds对象> */
		private Map<Integer, DS> dsMap = new TreeMap<Integer, DS>(new IDComparator());

		private boolean combineflag = false;

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public Public getPublicElement() {
			return publicElement;
		}

		public void setPublicElement(Public publicElement) {
			this.publicElement = publicElement;
		}

		public Map<Integer, DS> getDsMap() {
			return dsMap;
		}

		public void setDsMap(Map<Integer, DS> dsMap) {
			this.dsMap = dsMap;
		}

		public String getFileName() {
			return fileName;
		}

		public void setFileName(String fileName) {
			this.fileName = fileName;
		}

		public int getSkipLine() {
			return skipLine;
		}

		public void setSkipLine(int skipLine) {
			this.skipLine = skipLine;
		}

		public void setEncoding(String encoding) {
			this.encoding = encoding;
		}

		public String getEncoding() {
			return encoding;
		}

		public boolean isCombineflag() {
			return combineflag;
		}

		public void setCombineflag(boolean combineflag) {
			this.combineflag = combineflag;
		}
		
		public String getType() {
			return type;
		}
		
		public void setType(String type) {
			this.type = type;
		}

	}

	/** 对应模板中public标签 */
	public class Public {

		private String startSign;

		private String endSign;

		private Fields fields;

		public String getStartSign() {
			return startSign;
		}

		public void setStartSign(String startSign) {
			this.startSign = startSign;
		}

		public String getEndSign() {
			return endSign;
		}

		public void setEndSign(String endSign) {
			this.endSign = endSign;
		}

		public Fields getFields() {
			return fields;
		}

		public void setFields(Fields fields) {
			this.fields = fields;
		}
	}

	/** 对应模板中ds标签 */
	public class DS {

		private int id;

		private Meta meta;

		private Fields fields;

		private List<String> requiredOccur; // 必须出现字段的清单
		
		private boolean indexDepend; //如果为true，则通过index来定位数据位置。否则通过name来定位

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public Meta getMeta() {
			return meta;
		}

		public void setMeta(Meta meta) {
			this.meta = meta;
		}

		public Fields getFields() {
			return fields;
		}

		public void setFields(Fields fields) {
			this.fields = fields;
		}

		public List<String> getRequiredOccur() {
			return requiredOccur;
		}

		public void setRequiredOccur(List<String> requiredOccur) {
			this.requiredOccur = requiredOccur;
		}

		public boolean isIndexDepend() {
			return indexDepend;
		}

		public void setIndexDepend(boolean indexDepend) {
			this.indexDepend = indexDepend;
		}

	}

	/** 对应模板中meta标签 */
	public class Meta {

		private String startSign;

		private String endSign;

		private String headSplitSign;

		private String multiSplitSign;

		public String getStartSign() {
			return startSign;
		}

		public void setStartSign(String startSign) {
			this.startSign = startSign;
		}

		public String getEndSign() {
			return endSign;
		}

		public void setEndSign(String endSign) {
			this.endSign = endSign;
		}

		public String getHeadSplitSign() {
			return headSplitSign;
		}

		public void setHeadSplitSign(String headSplitSign) {
			this.headSplitSign = headSplitSign;
		}

		public String getMultiSplitSign() {
			return multiSplitSign;
		}

		public void setMultiSplitSign(String multiSplitSign) {
			this.multiSplitSign = multiSplitSign;
		}
	}

	/** 对应模板中fields标签 */
	public class Fields {

		/** <index,field对象> */
		private Map<Integer, Field> fields = new TreeMap<Integer, Field>(new IDComparator());

		private String splitSign; // 字段分隔符

		private String multiSplitSign;
		
		public Map<Integer, Field> getFields() {
			return fields;
		}

		public void setFields(Map<Integer, Field> fields) {
			this.fields = fields;
		}

		public String getSplitSign() {
			return splitSign;
		}

		public void setSplitSign(String splitSign) {
			this.splitSign = splitSign;
		}

		public void setMultiSplitSign(String multiSplitSign) {
			this.multiSplitSign = multiSplitSign;
		}

		public String getMultiSplitSign() {
			return multiSplitSign;
		}
	}

	/** 对应模板中field标签 */
	public class Field {

		private String name;

		private int index;

		private String occur; // 此字段出现情况,取值required为必须出现

		private String startSign;

		private String endSign;

		private String specialTime; //类似这种时间2013-01-12 12：22：23.012的处理，把点号后面的毫秒去点
		
		private String isSplit; //是否分拆
		
		private String splitSign; //依据什么字符进行分拆
		
		private String indexOfValue; //取值的索引

		private String value;

		private int indexInHead = -1; // 字段在表头的索引

		// liangww add 2012-03-30
		/** 宏，表示用于特殊处理，详细见{@link MacroParser} **/
		private String macro = null;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getIndex() {
			return index;
		}

		public void setIndex(int index) {
			this.index = index;
		}

		public String getStartSign() {
			return startSign;
		}

		public void setStartSign(String startSign) {
			this.startSign = startSign;
		}

		public String getEndSign() {
			return endSign;
		}

		public void setEndSign(String endSign) {
			this.endSign = endSign;
		}

		public String getSpecialTime() {
			return specialTime;
		}

		public void setSpecialTime(String specialTime) {
			this.specialTime = specialTime;
		}
		
		public String getIsSplit() {
			return isSplit;
		}

		public void setIsSplit(String isSplit) {
			this.isSplit = isSplit;
		}

		public String getSplitSign() {
			return splitSign;
		}

		public void setSplitSign(String splitSign) {
			this.splitSign = splitSign;
		}

		public String getIndexOfValue() {
			return indexOfValue;
		}

		public void setIndexOfValue(String indexOfValue) {
			this.indexOfValue = indexOfValue;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public String getOccur() {
			return occur;
		}

		public void setOccur(String occur) {
			this.occur = occur;
		}

		public int getIndexInHead() {
			return indexInHead;
		}

		public void setIndexInHead(int indexInHead) {
			this.indexInHead = indexInHead;
		}

		@Override
		public String toString() {
			return "Field [index=" + index + ", name=" + name + ", value=" + value + ", macro=" + macro + "]";
		}

		public String getMacro() {
			return macro;
		}

		public void setMacro(String macro) {
			this.macro = macro;
		}

	}
}

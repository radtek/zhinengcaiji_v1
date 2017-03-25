package templet.hw.cdma.dt;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import parser.xparser.IllegalTagException;
import templet.AbstractTempletBase;
import templet.IDComparator;
import util.Util;
import exception.InvalidParameterValueException;
import framework.SystemConfig;

/**
 * 华为CDMA网DT数据解析模板类
 * 
 * @author lijiayu
 * @date 2013-09-09
 */
public class DtCdmaTempletP extends AbstractTempletBase {

	/** <需要解析的文件,templet对象> */
	private Templet templet = new Templet();

	public Templet getTemplet() {
		return templet;
	}

	@Override
	public void parseTemp(String tempContent) throws Exception {
		DocumentBuilderFactory builderFactor = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = builderFactor.newDocumentBuilder();
		// 模板文件路径
		String templetFilePath = SystemConfig.getInstance().getTempletPath() + File.separatorChar + tempContent.trim();
		File file = new File(templetFilePath);
		Document doc = builder.parse(file);
		NodeList templist = doc.getElementsByTagName("templet");
		if (null == templist || templist.getLength() <= 0)
			return;
		// 遍历所有模板
		for (int i = 0; i < templist.getLength(); i++) {
			Node tempNode = templist.item(0);
			// 设置模板 节点encoding
			templet.setEncoding(getNodeStr(tempNode, "encoding"));
			// 设置模板 节点splitSign
			templet.setSplitSign(getNodeStrForSplitMark(tempNode, "splitSign"));
			// 设置模板 所有 define 节点
			setDefineMap(tempNode);
		}
	}

	private void setDefineMap(Node tempNode) throws Exception {
		for (Node node = tempNode.getFirstChild(); node != null; node = node.getNextSibling()) {
			if (Node.ELEMENT_NODE == node.getNodeType()) {
				if (node.getNodeName().equalsIgnoreCase("define")) {
					Define df = getDefine(node);
					templet.getDefineMap().put(df.getId(), df);
				}
			}
		}
	}

	/**
	 * 给定Define节点，转化成Define对象
	 * 
	 * @param DefineNode
	 * @return Define
	 */
	private Define getDefine(Node node) throws Exception {
		Define df = new Define();
		df.setId(getNodeId(node, "define"));
		df.setName(getNodeStr(node, "name"));
		// 遍历所有的define 标签 下在的 field 标签
		for (Node fieldNode = node.getFirstChild(); fieldNode != null; fieldNode = fieldNode.getNextSibling()) {
			if (Node.ELEMENT_NODE == fieldNode.getNodeType()) {
				if (fieldNode.getNodeName().equalsIgnoreCase("field")) {
					Field field = getField(fieldNode);
					df.fields.add(field);
				}
			}
		}
		return df;
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
		field.setName(getNodeStr(fieldNode, "name"));
		return field;
	}

	private int getNodeId(Node node, String methodName) throws InvalidParameterValueException {
		int id = Integer.parseInt(node.getAttributes().getNamedItem("id").getNodeValue());
		if (id < 0)
			throw new InvalidParameterValueException("DtCdmaTempletP-->" + methodName + " id = " + id);
		else
			return id;
	}

	private String getNodeStr(Node node, String namedItem) {
		Node atrr = node.getAttributes().getNamedItem(namedItem);
		if (atrr != null) {
			String str = atrr.getNodeValue();
			if (Util.isNotNull(str))
				return str;
		}
		return null;
	}

	private String getNodeStrForSplitMark(Node node, String namedItem) {
		Node atrr = node.getAttributes().getNamedItem(namedItem);
		if (atrr != null) {
			String str = atrr.getNodeValue();
			if (str.equals("\\t"))
				str = "\t";
			return str;
		}
		return null;
	}

	/** 对应模板中templet标签 */
	public class Templet {

		private String encoding;

		private String splitSign;

		/** <define的id,define对象> */
		private Map<Integer, Define> defineMap = new TreeMap<Integer, Define>(new IDComparator());

		public String getEncoding() {
			return encoding;
		}

		public void setEncoding(String encoding) {
			this.encoding = encoding;
		}

		public String getSplitSign() {
			return splitSign;
		}

		public void setSplitSign(String splitSign) {
			this.splitSign = splitSign;
		}

		public Map<Integer, Define> getDefineMap() {
			return defineMap;
		}

		public void setDefineMap(Map<Integer, Define> defineMap) {
			this.defineMap = defineMap;
		}
	}

	/**
	 * 对应模板中define标签
	 * 
	 * @author lijiayu @ 2013年9月11日
	 */
	public class Define {

		private int id;

		private String name;

		private int time;

		private int gpsLinkSeq;

		private List<Field> fields = new ArrayList<DtCdmaTempletP.Field>();

		public List<Field> getFields() {
			return fields;
		}

		public void setFields(List<Field> fields) {
			this.fields = fields;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getTime() {
			return time;
		}

		public void setTime(int time) {
			this.time = time;
		}

		public int getGpsLinkSeq() {
			return gpsLinkSeq;
		}

		public void setGpsLinkSeq(int gpsLinkSeq) {
			this.gpsLinkSeq = gpsLinkSeq;
		}
	}

	public class Field {

		private int index;

		private String name;

		private String value;

		public int getIndex() {
			return index;
		}

		public void setIndex(int index) {
			this.index = index;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}

	public static void main(String[] args) {
		DtCdmaTempletP templetP = new DtCdmaTempletP();
		Field field = templetP.new Field();
		System.out.println(field.getValue());
		int pn = Integer.parseInt(field.getValue() == null ? "0" : field.getValue());
		try {
			templetP.parseTemp("clt_c_hw_dt_parse.xml");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}

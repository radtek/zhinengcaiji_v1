package templet;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import parser.DBAutoParser;
import parser.xparser.IllegalTagException;
import util.Util;
import access.DBAutoAccessor;
import exception.InvalidParameterValueException;
import framework.SystemConfig;

/**
 * 数据库采集智能匹配模板
 * 
 * @author ltp Jul 1, 2010
 * @since 3.1
 * @see DBAutoAccessor
 * @see DBAutoParser
 */
public class DBAutoTempletP extends AbstractTempletBase {

	/** <表名,templet对象> */
	private Map<String, Templet> templets = new LinkedHashMap<String, Templet>();

	@Override
	public void parseTemp(String tempContent) throws Exception {
		if (Util.isNull(tempContent))
			return;

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();

		// 模板文件路径
		String templetFilePath = SystemConfig.getInstance().getTempletPath() + File.separatorChar + tempContent;
		File file = new File(templetFilePath);
		Document doc = builder.parse(file);
		NodeList templetList = doc.getElementsByTagName("templet");
		int templetsSize = templetList.getLength();
		if (templetsSize <= 0)
			return;
		Templet temObj = null;
		// 遍历所有templet节点
		for (int i = 0; i < templetsSize; i++) {
			Node templet = templetList.item(i);
			temObj = getTemplet(templet);
			templets.put(temObj.getTableName(), temObj);
		}
	}

	private Templet getTemplet(Node tNode) throws Exception {
		Templet t = new Templet();
		Node idAtrr = tNode.getAttributes().getNamedItem("id");
		Node tableAtrr = tNode.getAttributes().getNamedItem("table");
		Node useAtrr = tNode.getAttributes().getNamedItem("used");
		// 可以没有此节点
		Node condiAtrr = tNode.getAttributes().getNamedItem("condition");
		// 可以没有此节点
		Node occurAtrr = tNode.getAttributes().getNamedItem("occur");
		if (idAtrr == null || tableAtrr == null || useAtrr == null) {
			throw new IllegalTagException("templet缺少属性");
		}
		// templet节点id
		int id = Integer.parseInt(idAtrr.getNodeValue());
		if (id < 0)
			throw new InvalidParameterValueException("id = " + id);
		// templet节点table
		String tableName = tableAtrr.getNodeValue();
		if (Util.isNull(tableName))
			throw new InvalidParameterValueException("table属性值不能为空");
		// templet节点used
		int isUsed = Integer.parseInt(useAtrr.getNodeValue());
		if (isUsed != 0 && isUsed != 1)
			throw new InvalidParameterValueException("used属性值不正确，只能为0或1");
		// templet节点condition
		String condition = null;
		if (condiAtrr != null) {
			condition = condiAtrr.getNodeValue();
		}
		// templet节点occur
		int occur = 0;
		if (occurAtrr != null) {
			occur = Integer.parseInt(occurAtrr.getNodeValue());
			if (occur != 0 && occur != 1)
				throw new InvalidParameterValueException("occur属性值不正确，只能为0或1");
		}
		// sql查询语句
		String sql = null;
		Map<Integer, Field> fields = t.getFields();
		for (Node node = tNode.getFirstChild(); node != null; node = node.getNextSibling()) {
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				if (node.getNodeName().equalsIgnoreCase("field")) {
					Field f = getField(node);
					fields.put(f.getIndex(), f);
				} else if (node.getNodeName().equalsIgnoreCase("sql")) {
					sql = node.getFirstChild().getNodeValue();
				}
			}
		}
		t.setId(id);
		t.setCondition(condition);
		t.setSql(sql);
		t.setUsed(isUsed == 1);
		t.setOccur(occur == 1);
		t.setTableName(tableName);
		return t;
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
		Node nameNode = fieldNode.getAttributes().getNamedItem("name");
		Node occurNode = fieldNode.getAttributes().getNamedItem("occur");
		if (indexNode == null || nameNode == null)
			throw new IllegalTagException("field缺少属性");

		int index = Integer.parseInt(indexNode.getNodeValue());
		if (index < 0)
			throw new InvalidParameterValueException("field index = " + index);
		String name = nameNode.getNodeValue();
		if (Util.isNull(name))
			throw new InvalidParameterValueException("field name = " + name);
		if (occurNode != null)
			field.setOccur(occurNode.getNodeValue());
		field.setName(name);
		field.setIndex(index);
		return field;
	}

	/** 对应模板中templet标签 */
	public class Templet {

		private int id;

		private String tableName;

		private boolean isUsed;

		private boolean isOccur;

		private String sql;

		private String condition;

		private Map<Integer, Field> fields = new TreeMap<Integer, Field>(new IDComparator());

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}

		public boolean isUsed() {
			return isUsed;
		}

		public void setUsed(boolean isUsed) {
			this.isUsed = isUsed;
		}

		public Map<Integer, Field> getFields() {
			return fields;
		}

		public void setFields(Map<Integer, Field> fields) {
			this.fields = fields;
		}

		public String getCondition() {
			return condition;
		}

		public void setCondition(String condition) {
			this.condition = condition;
		}

		public String getSql() {
			return sql;
		}

		public void setSql(String sql) {
			this.sql = sql;
		}

		public boolean isOccur() {
			return isOccur;
		}

		public void setOccur(boolean isOccur) {
			this.isOccur = isOccur;
		}
	}

	/** 对应模板中field标签 */
	public class Field {

		private String name;

		private int index;

		private String occur; // 此字段出现情况,取值required为必须出现,不出现则不解析数据

		private int colType;// 表字段的类型，对应java.sql.Types

		private int indexInHead = -1; // 字段在表头的索引,从1开始

		public int getColType() {
			return colType;
		}

		public void setColType(int colType) {
			this.colType = colType;
		}

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

		public int getIndexInHead() {
			return indexInHead;
		}

		public void setIndexInHead(int indexInHead) {
			this.indexInHead = indexInHead;
		}

		public String getOccur() {
			return occur;
		}

		public void setOccur(String occur) {
			this.occur = occur;
		}
	}

	public Map<String, Templet> getTemplets() {
		return templets;
	}

	public void setTemplets(Map<String, Templet> templets) {
		this.templets = templets;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		DBAutoTempletP p = new DBAutoTempletP();
		try {
			p.parseTemp("dbauto_parse.xml");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

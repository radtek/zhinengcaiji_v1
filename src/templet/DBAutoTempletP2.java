package templet;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import parser.DBAutoParser2;
import parser.xparser.IllegalTagException;
import util.Util;
import exception.InvalidParameterValueException;
import framework.SystemConfig;

/**
 * 数据库解析模板类，无需配置字段，只有解析模板，无分发模板
 * 
 * @author liuwx 2010-7-21
 * @since 1.0
 * @see DBAutoParser2
 */
public class DBAutoTempletP2 extends AbstractTempletBase {

	/** <表名,templet对象> */
	private Map<String, Templet> templets = new LinkedHashMap<String, Templet>();

	/** Map<表名，Map<采集表字段,厂家字段名)>> */
	private Map<String, Map<String, String>> mappingfields = new HashMap<String, Map<String, String>>();

	private Map<String, Map<String, Object>> mappingspecial = new HashMap<String, Map<String, Object>>();

	private boolean special = false;

	public boolean getSpecial() {
		return special;
	}

	public void setSpecial(boolean special) {
		this.special = special;
	}

	public Map<String, Map<String, Object>> getMappingspecial() {
		return mappingspecial;
	}

	public void setMappingspecial(
			Map<String, Map<String, Object>> mappingspecial) {
		this.mappingspecial = mappingspecial;
	}

	@Override
	public void parseTemp(String tempContent) throws Exception {
		if (Util.isNull(tempContent))
			return;

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();

		// 模板文件路径
		String templetFilePath = SystemConfig.getInstance().getTempletPath()
				+ File.separatorChar + tempContent;
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
			templets.put(temObj.getFromTableName(), temObj);
		}
	}

	private Templet getTemplet(Node tNode) throws Exception {
		Templet t = new Templet();
		Node idAtrr = tNode.getAttributes().getNamedItem("id");
		Node tableAtrr = tNode.getAttributes().getNamedItem("fromTable");
		Node destableAtrr = tNode.getAttributes().getNamedItem("toTable");
		Node useAtrr = tNode.getAttributes().getNamedItem("used");

		Node occurAtrr = tNode.getAttributes().getNamedItem("occur");

		// 可以没有此节点
		Node condiAtrr = tNode.getAttributes().getNamedItem("condition");
		Node htmlFilterAtrr = tNode.getAttributes().getNamedItem(
				"htmlTagsFilterColumns");
		if (idAtrr == null || tableAtrr == null || destableAtrr == null
				|| useAtrr == null) {
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

		String destable = destableAtrr.getNodeValue();
		if (Util.isNull(destable))
			throw new InvalidParameterValueException("destable属性值不能为空");

		// templet节点used
		int isUsed = Integer.parseInt(useAtrr.getNodeValue());
		if (isUsed != 0 && isUsed != 1)
			throw new InvalidParameterValueException("used属性值不正确，只能为0或1");

		int occurAtrrValue = 0;
		// 仅当有occur属性，且值为1时，才赋1，其它情况均为0
		if (occurAtrr != null) {
			if (Util.isNotNull(occurAtrr.getNodeValue())) {
				occurAtrrValue = occurAtrr.getNodeValue().trim().equals("1") ? 1
						: 0;
			}
		}

		String condition = null;
		if (condiAtrr != null) {
			condition = condiAtrr.getNodeValue();
		}

		String sql = null;

		Map<String, String> defaultValueColumns = null;

		if (tNode.getFirstChild() != null) {
			Map<String, String> cmap = new HashMap<String, String>();

			Map<String, Object> srcp = new HashMap<String, Object>();

			for (Node node = tNode.getFirstChild(); node != null; node = node
					.getNextSibling()) {
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					if (node.getNodeName().equalsIgnoreCase("mapping")) {
						Node specialNode = node.getAttributes().getNamedItem(
								"special");
						if (null != specialNode
								&& specialNode.getNodeValue().trim()
										.equalsIgnoreCase("1")) {
							setSpecial(true);
							NodeList nList = node.getChildNodes();
							for (int i = 0; i < nList.getLength(); i++) {
								Node n = nList.item(i);
								if (n.getNodeType() == Node.ELEMENT_NODE) {
									if (n.getNodeName().equalsIgnoreCase(
											"column")) {
										Node splitNode = n.getAttributes()
												.getNamedItem("split");
										Node nameNode = n.getAttributes()
												.getNamedItem("src");
										if (null == splitNode
												|| splitNode.getNodeValue()
														.trim().length() <= 0) {
											Node mappingField = n
													.getAttributes()
													.getNamedItem("dest");
											String name = nameNode
													.getNodeValue();
											String mappingValue = mappingField
													.getNodeValue();
											if (Util.isNull(name))
												throw new InvalidParameterValueException(
														"src name = " + name);
											if (Util.isNull(mappingValue))
												throw new InvalidParameterValueException(
														"desc name = "
																+ mappingValue);

											srcp.put(
													mappingValue.toUpperCase(),
													name.toUpperCase());
										} else {
											srcp.put(nameNode.getNodeValue()
													+ "destnull", n);
										}
									}
								}
							}
						} else {
							NodeList nList = node.getChildNodes();
							for (int i = 0; i < nList.getLength(); i++) {
								Node n = nList.item(i);
								if (n.getNodeType() == Node.ELEMENT_NODE) {
									if (n.getNodeName().equalsIgnoreCase(
											"column")) {
										findMapping(cmap, n);
									}
								}
							}
						}

					} else if (node.getNodeName().equalsIgnoreCase("sql")) {
						sql = node.getTextContent();
					}
					// 提取默认值字段
					else if (node.getNodeName().equalsIgnoreCase(
							"defaultValueColumns")) {
						NodeList nList = node.getChildNodes();
						defaultValueColumns = new HashMap<String, String>();
						for (int i = 0; i < nList.getLength(); i++) {
							Node n = nList.item(i);
							if (n.getNodeType() == Node.ELEMENT_NODE) {
								if (n.getNodeName().equalsIgnoreCase("column")) {
									Node nameNode = n.getAttributes()
											.getNamedItem("name");
									Node defaultNode = n.getAttributes()
											.getNamedItem("default");
									if (nameNode != null) {
										String name = nameNode.getNodeValue();
										defaultValueColumns
												.put(name.toUpperCase(),
														(defaultNode == null || ""
																.equals(defaultNode
																		.getNodeValue())) ? "0"
																: defaultNode
																		.getNodeValue());
									}
								}
							}
						}
					}
				}
			}
			if (!special)
				mappingfields.put(destable, cmap);
			else
				mappingspecial.put(destable, srcp);
		}
		t.setId(id);
		t.setCondition(condition);
		t.setUsed(isUsed == 1);
		t.setSql(sql);
		t.setOccur(occurAtrrValue == 1);
		t.setFromTableName(tableName);
		t.setDestTableName(destable);
		t.setDefaultValueColumns(defaultValueColumns);
		t.setHtmlTagsFilterColumns(htmlFilterAtrr == null ? null
				: htmlFilterAtrr.getNodeValue());
		return t;
	}

	private void findMapping(Map<String, String> cmap, Node n)
			throws InvalidParameterValueException {
		Node nameNode = n.getAttributes().getNamedItem("src");
		Node mappingField = n.getAttributes().getNamedItem("dest");
		String name = nameNode.getNodeValue();
		String mappingValue = mappingField.getNodeValue();
		if (Util.isNull(name))
			throw new InvalidParameterValueException("src name = " + name);
		if (Util.isNull(mappingValue))
			throw new InvalidParameterValueException("desc name = "
					+ mappingValue);

		cmap.put(mappingValue, name);
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
		DBAutoTempletP2 p = new DBAutoTempletP2();
		try {
			p.parseTemp("db_r12_clt_cm_nk5.4_parse.xml");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/** 对应模板中templet标签 */
	public class Templet {

		private int id;

		private String fromTableName;

		private boolean isUsed;

		private String condition;

		private String toTableName;

		private String sql;

		private String htmlTagsFilterColumns;

		private Map<String, String> defaultValueColumns;

		private boolean occur;

		public Map<String, String> getDefaultValueColumns() {
			return defaultValueColumns;
		}

		public void setDefaultValueColumns(
				Map<String, String> defaultValueColumns) {
			this.defaultValueColumns = defaultValueColumns;
		}

		public boolean isOccur() {
			return occur;
		}

		public void setOccur(boolean occur) {
			this.occur = occur;
		}

		public String getSql() {
			return sql;
		}

		public void setSql(String sql) {
			this.sql = sql;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getFromTableName() {
			return fromTableName;
		}

		public void setFromTableName(String tbName) {
			this.fromTableName = tbName;
		}

		public boolean isUsed() {
			return isUsed;
		}

		public void setUsed(boolean isUsed) {
			this.isUsed = isUsed;
		}

		public String getCondition() {
			return condition;
		}

		public void setCondition(String condition) {
			this.condition = condition;
		}

		public String getHtmlTagsFilterColumns() {
			return htmlTagsFilterColumns;
		}

		public void setHtmlTagsFilterColumns(String htmlTagsFilterColumns) {
			this.htmlTagsFilterColumns = htmlTagsFilterColumns;
		}

		public String getDestTableName() {
			return toTableName;
		}

		public void setDestTableName(String tbName) {
			this.toTableName = tbName;
		}
	}

	public Map<String, Map<String, String>> getMappingfields() {
		return mappingfields;
	}

	public void setMappingfields(Map<String, Map<String, String>> mappingfields) {
		this.mappingfields = mappingfields;
	}

}

package templet;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import parser.GenericSectionHeadParser;
import templet.Table.Column;
import util.Util;
import distributor.DistributeTemplet;
import exception.InvalidParameterValueException;
import framework.SystemConfig;

/**
 * 带表头的按段解析方式对应的分发模板类
 * 
 * @author Litp
 * @since 3.1
 * @see GenericSectionHeadParser
 * @see GenericSectionHeadP
 */
public class GenericSectionHeadD extends DistributeTemplet {

	/** <模板编号,templet对象> */
	private Map<Integer, Templet> templets = new HashMap<Integer, Templet>();

	public Templet getTemplet(int id) {
		return this.templets.get(id);
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
		NodeList templetsList = doc.getElementsByTagName("templet");
		int templetsSize = templetsList.getLength();
		if (templetsSize <= 0)
			return;
		// 遍历所有templet节点
		for (int i = 0; i < templetsSize; i++) {
			Templet temObj = new Templet();
			Node templet = templetsList.item(i);
			// templet节点id
			int id = Integer.parseInt(templet.getAttributes().getNamedItem("id").getNodeValue());
			if (id < 0)
				throw new InvalidParameterValueException("templet id = " + id);
			temObj.setId(id);
			Map<Integer, Table> tables = temObj.getTables(); // 所有的表
			for (Node node = templet.getFirstChild(); node != null; node = node.getNextSibling()) {
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					String nodeName = node.getNodeName();
					if (nodeName.equalsIgnoreCase("table")) {
						Table table = getTable(node);
						tables.put(table.getId(), table);
					}
				}
			}
			templets.put(id, temObj);
		}
	}

	/**
	 * 给定table节点，转化成Table对象
	 * 
	 * @param tableNode
	 * @return
	 * @throws Exception
	 */
	private Table getTable(Node tableNode) throws Exception {
		Table table = new Table();
		int id = Integer.parseInt(tableNode.getAttributes().getNamedItem("id").getNodeValue());
		if (id < 0)
			throw new InvalidParameterValueException("table id = " + id);
		String name = tableNode.getAttributes().getNamedItem("name").getNodeValue();
		String splitSign = tableNode.getAttributes().getNamedItem("split").getNodeValue();
		
		//时间字段所在的列索引，要入为stamptime字段值
		Node indexOfTimeColumn = tableNode.getAttributes().getNamedItem("indexOfTimeColumn");
		if(indexOfTimeColumn != null){
			table.setIndexOfTimeColumn(indexOfTimeColumn.getNodeValue());
		}
		
		Map<Integer, Column> columns = table.getColumns();
		for (Node node = tableNode.getFirstChild(); node != null; node = node.getNextSibling()) {
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				if (node.getNodeName().equalsIgnoreCase("column")) {
					Column c = getColumn(node);
					columns.put(c.getIndex(), c);
				}
			}
		}
		table.setId(id);
		table.setName(name);
		table.setSplitSign(splitSign);
		return table;
	}

	/**
	 * 给定column节点，转化成Column对象
	 * 
	 * @param columnNode
	 * @return
	 * @throws Exception
	 */
	private Column getColumn(Node columnNode) throws Exception {
		Column c = new Column();
		String name = columnNode.getAttributes().getNamedItem("name").getNodeValue();
		int index = Integer.parseInt(columnNode.getAttributes().getNamedItem("index").getNodeValue());
		if (index < 0)
			throw new InvalidParameterValueException("column index = " + index);
		int type = -1;
		Node tNode = columnNode.getAttributes().getNamedItem("type");
		if (tNode != null) {
			type = Integer.parseInt(tNode.getNodeValue());
		}
		Node fNode = columnNode.getAttributes().getNamedItem("format");
		if (fNode != null)
			c.setFormat(fNode.getNodeValue());

		// add 2011-10-17
		Node node1 = columnNode.getAttributes().getNamedItem("ignore");
		if (node1 != null) {
			String bo = node1.getNodeValue().trim();
			c.setIgnore(Boolean.parseBoolean(bo));
		}

		c.setIndex(index);
		c.setName(name);
		c.setType(type);
		return c;
	}

	public static void main(String[] args) {
		GenericSectionHeadD d = new GenericSectionHeadD();
		try {
			d.parseTemp("bell_pm_alt_b10_gbl_dist.xml");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/** 对应模板中templet标签 */
	public class Templet {

		private int id; // 模板编号

		/** templet节点下所有的table <table的id,Table对象> */
		private Map<Integer, Table> tables = new HashMap<Integer, Table>();

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public Map<Integer, Table> getTables() {
			return tables;
		}

		public void setTables(Map<Integer, Table> tables) {
			this.tables = tables;
		}
	}

}

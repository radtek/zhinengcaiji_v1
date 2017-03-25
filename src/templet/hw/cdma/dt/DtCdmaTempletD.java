package templet.hw.cdma.dt;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import templet.Table;
import templet.Table.Column;
import util.Util;
import distributor.DistributeTemplet;
import exception.InvalidParameterValueException;
import framework.SystemConfig;

/**
 * 华为CDMA网DT数据分发模板类
 * 
 * @author Lijy
 * @date 2013-09-09
 */
public class DtCdmaTempletD extends DistributeTemplet {

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
		NodeList templetList = doc.getElementsByTagName("templet");
		if (templetList.getLength() <= 0)
			return;
		// 遍历所有模板
		for (int i = 0; i < templetList.getLength(); i++) {
			Node tempNode = templetList.item(i);
			// 设置模板节点id
			setId(tempNode);
			// 设置模板 节点table
			setTableMap(tempNode);
		}
	}

	protected void setId(Node tempNode) throws Exception {
		int id = getNodeId(tempNode, "Templet");
		templet.setId(id);
	}

	protected int getNodeId(Node node, String methodName) throws InvalidParameterValueException {
		int id = Integer.parseInt(node.getAttributes().getNamedItem("id").getNodeValue());
		if (id < 0)
			throw new InvalidParameterValueException("DtCdmaTempletD-->" + methodName + " id = " + id);
		else
			return id;
	}

	protected String getNodeStr(Node node, String namedItem) {
		Node atrr = node.getAttributes().getNamedItem(namedItem);
		if (atrr != null) {
			String str = atrr.getNodeValue();
			if (Util.isNotNull(str))
				return str;
		}
		return null;
	}

	protected void setTableMap(Node tempNode) throws Exception {
		for (Node node = tempNode.getFirstChild(); node != null; node = node.getNextSibling()) {
			if (Node.ELEMENT_NODE == node.getNodeType()) {
				if (node.getNodeName().equalsIgnoreCase("table")) {
					Table table = getTable(node);
					templet.getTables().put(table.getId(), table);
				}
			}
		}
	}

	/**
	 * 给定table节点，转化成Table对象
	 * 
	 * @param tableNode
	 * @return
	 * @throws Exception
	 */
	protected Table getTable(Node tableNode) throws Exception {
		Table table = new Table();
		int id = Integer.parseInt(tableNode.getAttributes().getNamedItem("id").getNodeValue());
		if (id < 0)
			throw new InvalidParameterValueException("table id = " + id);
		String name = getNodeStr(tableNode, "name");
		String splitSign = getNodeStr(tableNode, "split");

		Map<String, Column> columns = table.getColumnMap();
		for (Node node = tableNode.getFirstChild(); node != null; node = node.getNextSibling()) {
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				if (node.getNodeName().equalsIgnoreCase("column")) {
					Column c = getColumn(node);
					columns.put(c.getSrc().toUpperCase(), c);
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
	protected Column getColumn(Node columnNode) throws Exception {
		Column c = new Column();
		String name = columnNode.getAttributes().getNamedItem("name").getNodeValue();
		String src = columnNode.getAttributes().getNamedItem("src").getNodeValue();
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

		c.setSrc(src);
		c.setName(name);
		c.setType(type);
		return c;
	}

	public static void main(String[] args) {
		DtCdmaTempletD d = new DtCdmaTempletD();
		try {
			d.parseTemp("clt_c_hw_dt_disk.xml");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

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

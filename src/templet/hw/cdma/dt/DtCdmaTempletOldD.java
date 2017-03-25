package templet.hw.cdma.dt;

import java.util.Map;

import org.w3c.dom.Node;

import templet.Table;
import templet.Table.Column;
import exception.InvalidParameterValueException;

/**
 * 华为CDMA网DT数据分发模板类
 * 
 * @author Lijy
 * @date 2013-09-09
 */
public class DtCdmaTempletOldD extends DtCdmaTempletD {

	/**
	 * 给定table节点，转化成Table对象
	 * 
	 * @param tableNode
	 * @return
	 * @throws Exception
	 */
	@Override
	protected Table getTable(Node tableNode) throws Exception {
		Table table = new Table();
		int id = Integer.parseInt(tableNode.getAttributes().getNamedItem("id").getNodeValue());
		if (id < 0)
			throw new InvalidParameterValueException("table id = " + id);
		String name = getNodeStr(tableNode, "name");
		String splitSign = getNodeStr(tableNode, "split");

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
	@Override
	protected Column getColumn(Node columnNode) throws Exception {
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
	
}

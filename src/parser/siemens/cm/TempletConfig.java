package parser.siemens.cm;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;

import util.LogMgr;

class TempletConfig {

	public List<Table> tables = new ArrayList<Table>();

	/**
	 * 批量提交时的入库间隔条数，默认100条提交一次。
	 */
	public int interval = 100;

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	@SuppressWarnings("unchecked")
	public static TempletConfig getInstance(Document doc) {
		TempletConfig instance = new TempletConfig();

		try {
			Element root = doc.getRootElement();
			try {
				instance.interval = Integer.parseInt(root.element("public").element("interval").getText());
			} catch (Exception e) {
				logger.error("TempletConfig:public interval error." + e.getMessage());
			}
			List<Element> tbs = root.elements("table");
			for (Element tb : tbs) {
				Table table = null;
				String name = tb.elementText("name");
				List<String> startSign = new ArrayList<String>();
				List<Element> items = tb.element("startSign").elements();
				for (Element e : items) {
					startSign.add(e.getText());
				}
				String endSign = tb.elementText("endSign");
				String splitSign = tb.elementText("splitSign");
				String nameValueSplitSign = tb.elementText("nameValueSplitSign");
				String tableName = tb.elementText("tableName");
				String key = tb.elementText("key");
				List<Property> properties = new ArrayList<Property>();
				List<Element> ps = tb.element("properties").elements();
				for (Element p : ps) {
					Property property = null;
					String propertyName = p.elementText("propertyName");
					String columnName = p.elementText("columnName");
					int dataType = Integer.parseInt(p.elementText("dataType"));
					String dataFormat = p.elementText("dataFormat");
					property = new Property(propertyName, columnName, dataType, dataFormat);
					properties.add(property);
				}
				table = new Table(name, startSign, endSign, splitSign, nameValueSplitSign, key, tableName, properties);
				instance.tables.add(table);
			}
		} catch (Exception e) {
			logger.error("加载模板时异常", e);
			return null;
		}
		return instance;
	}

	public static class Table {

		public String name;

		public List<String> startSign;

		public String endSign;

		public String splitSign;

		public String nameValueSplitSign;

		public String key;

		public String tableName;

		public List<Property> properties;

		public Table(String name, List<String> startSign, String endSign, String splitSign, String nameValueSplitSign, String key, String tableName,
				List<Property> properties) {
			super();
			this.name = name;
			this.startSign = startSign;
			this.endSign = endSign;
			this.splitSign = splitSign;
			this.nameValueSplitSign = nameValueSplitSign;
			this.key = key;
			this.tableName = tableName;
			this.properties = properties;
		}

	}

	public static class Property {

		public String propertyName;

		public String columnName;

		public int dataType;

		public String dataFormat;

		public Property(String propertyName, String columnName, int dataType, String dataFormat) {
			super();
			this.propertyName = propertyName;
			this.columnName = columnName;
			this.dataType = dataType;
			this.dataFormat = dataFormat;
		}

	}
}

package templet.hw.cdma.mml;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import templet.AbstractTempletBase;
import util.LogMgr;
import framework.SystemConfig;

public class MmlCdmaTempletP extends AbstractTempletBase {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	public List<Template> templets;

	@Override
	public void parseTemp(String tempContent) throws Exception {
		// TODO Auto-generated method stub
		File file = new File(SystemConfig.getInstance().getTempletPath() + File.separator + tempContent);
		if (!file.exists() || !file.isFile())
			throw new Exception("模板文件（" + file + "）不存在。");

		logger.debug("开始解析模板文件:" + file.getAbsolutePath());
		templets = new ArrayList<Template>();
		FileInputStream fIns = new FileInputStream(file);
		Element rootEle = new SAXReader().read(fIns).getRootElement();
		List<Element> templist = rootEle.elements("template");
		Map<Integer, Event> eventMap = null;
		for (Element e : templist) {
			Template t = new Template();
			int id = Integer.parseInt(e.attributeValue("id"));
			List<Element> eventElementList = e.elements("event");
			Event event = null;
			List<Table> tableList = null;
			eventMap = new HashMap<Integer, Event>();
			for (Element element : eventElementList) {
				event = new Event();
				int eventId = Integer.parseInt(element.attributeValue("id"));

				Element commandTemplate = element.element("commandTemplate");
				if (commandTemplate == null || commandTemplate.getTextTrim().equals("")) {
					throw new NullPointerException("模板文件" + tempContent + "中commandTemplate属性缺少或者值为空");
				}
				event.commandTemplate = commandTemplate.getTextTrim();

				Element para = element.element("para");
				if (para != null) {
					event.para = para.getTextTrim();
				}

				Element commandExceple = element.element("commandExceple");
				if (commandExceple != null) {
					event.commandExceple = commandExceple.getTextTrim();
				}

				Element engineEventId = element.element("engineEventId");
				if (engineEventId != null && !engineEventId.getTextTrim().equals("")) {
					event.engineEventId = Integer.parseInt(engineEventId.getTextTrim());
				}

				Element startTime = element.element("startTime");
				if (startTime != null) {
					event.startTime = startTime.getTextTrim();
				}

				Element period = element.element("period");
				if (period == null || period.getTextTrim().equals("")) {
					throw new NullPointerException("模板文件" + tempContent + "中period属性缺少或者值为空");
				}
				event.period = Integer.parseInt(period.getTextTrim());

				Element arrangement = element.element("arrangement");
				if (arrangement == null || arrangement.getTextTrim().equals("")) {
					throw new NullPointerException("模板文件" + tempContent + "中arrangement属性缺少或者值为空");
				}
				event.arrangement = arrangement.getTextTrim();

				List<Element> eles = element.elements("table");
				tableList = new ArrayList<Table>();
				Table table = null;
				List<Column> columnList = null;
				for (Element el : eles) {
					table = new Table();
					int tableId = Integer.parseInt(el.attributeValue("id"));
					String tableName = el.attributeValue("name");
					if (tableName == null || tableName.equals("")) {
						throw new NullPointerException("模板文件" + tempContent + "中tableName属性缺少或者值为空");
					}
					table.tableName = tableName;

					String dataType = el.attributeValue("dataType");
					if (dataType == null || dataType.equals("")) {
						throw new NullPointerException("模板文件" + tempContent + "中dataType属性缺少或者值为空");
					}
					table.dataType = Integer.parseInt(dataType);

					List<Element> elems = el.elements("column");
					columnList = new ArrayList<Column>();
					Column column = null;
					for (Element ele : elems) {
						column = new Column();
						String columnName = ele.attributeValue("name");
						if (columnName == null || columnName.equals("")) {
							throw new NullPointerException("模板文件" + tempContent + "中columnName属性缺少或者值为空");
						}
						column.name = columnName;

						String from = ele.attributeValue("from");
						if (from == null || from.equals("")) {
							throw new NullPointerException("模板文件" + tempContent + "中from属性缺少或者值为空");
						}
						column.from = from;
						columnList.add(column);
					}
					table.tableId = tableId;
					table.columnList = columnList;
					tableList.add(table);
				}
				event.eventId = eventId;
				event.tableList = tableList;
				if (eventMap.get(eventId) == null)
					eventMap.put(eventId, event);
				else
					throw new Exception("模板文件" + tempContent + "中eventId = " + eventId + " 已存在");
			}
			t.templateId = id;
			t.eventMap = eventMap;
			templets.add(t);
		}
		logger.debug("解析模板文件结束:" + file);
	}

	public class Template {

		public Integer templateId;

		public Event event;

		public Map<Integer, Event> eventMap;
	}

	public class Event {

		public Integer eventId;

		public String commandTemplate;

		public String para;

		public String commandExceple;

		public Integer engineEventId;

		public String startTime;

		public Integer period;

		public String arrangement;

		public List<Table> tableList;
	}

	public class Table {

		public Integer tableId;

		public String tableName;

		public Integer dataType;

		public List<Column> columnList;
	}

	public class Column {

		public String name;

		public String from;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		MmlCdmaTempletP temp = new MmlCdmaTempletP();
		temp.parseTemp("CLT_C_HW_MML.xml");
		System.out.println(temp.templets);
	}

}

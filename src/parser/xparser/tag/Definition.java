package parser.xparser.tag;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import util.LogMgr;
import util.Util;

public class Definition extends Tag {

	private MappingElement mapping;

	private RuleElement[] ruleElements;

	private ProcessElement processElement;

	private CfgElement cfgElement;

	// <表名，表的所有列<列的信息>>
	private Map<String, List<PropertyElement>> scopeMap = new Hashtable<String, List<PropertyElement>>();

	private InputStream configInputStream;

	private Document doc;

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	public Definition(InputStream definition) throws Exception {
		super("definition");

		configInputStream = definition;
		try {
			doc = new SAXReader().read(configInputStream);
		} catch (DocumentException e) {
			logger.error("从指定的输入流读取模板时，发生异常", e);
			throw new DefinitionParseException(e);
		}
	}

	public Definition(String fileName) throws Exception {
		this(new FileInputStream(fileName));
	}

	public void load() throws Exception {
		try {
			if (doc.selectSingleNode("/definition") == null) {
				throw new DefinitionParseException("未发现根节点 definition");
			}
			loadCfg();
			loadMapping();
			loadRules();
			loadProcess();
		} catch (Exception e) {
			logger.error("解析配置时发生异常", e);
			throw e;
		}
	}

	public RuleElement[] getRuleElements() {
		return ruleElements;
	}

	public CfgElement getCfgElement() {
		return cfgElement;
	}

	public void setCfgElement(CfgElement cfgElement) {
		this.cfgElement = cfgElement;
	}

	private void loadCfg() throws Exception {
		Element cfg = (Element) doc.selectSingleNode("/definition/cfg");
		String charset = cfg.elementText("charset");
		String driver = cfg.elementText("driver");
		String url = cfg.elementText("url");
		String username = cfg.elementText("username");
		String password = cfg.elementText("password");
		String service = cfg.elementText("service");
		String sign = cfg.elementText("distribute-split-sign");
		String skip = cfg.elementText("skip");
		if (Util.isNull(skip)) {
			skip = "1";
		}
		int count = 100;
		try {
			count = cfg.elementText("backlog-count") != null ? Integer.parseInt(cfg.elementText("backlog-count")) : count;
		} catch (Exception e) {
			logger.error("指定的backlog-count无法转换为数字，将使用默认值100");
		}
		this.cfgElement = new CfgElement(charset, driver, url, username, password, service, sign, count, Integer.parseInt(skip));
	}

	@SuppressWarnings("unchecked")
	private void loadMapping() throws Exception {
		List<Element> properties = doc.selectNodes("/definition/mapping/properties");
		if (properties == null || properties.size() == 0) {
			throw new DefinitionParseException("没有properties或mapping节点");
		}
		Tag[] childs = new Tag[properties.size()];
		for (int i = 0; i < childs.length; i++) {
			Element e = properties.get(i);
			PropertiesElement pe = new PropertiesElement(e.attributeValue("table"));
			String autocreate = e.attributeValue("autocreatetable");
			if (autocreate != null && autocreate.equalsIgnoreCase("true")) {
				pe.setAutoCreateTable(true);
			}
			boolean store = e.attributeValue("store") != null ? Boolean.parseBoolean(e.attributeValue("store")) : true;
			pe.setStore(store);

			List<Element> proEles = e.elements("property");
			Tag[] propertyTags = new Tag[proEles.size()];
			for (int j = 0; j < propertyTags.length; j++) {
				Element el = proEles.get(j);
				String value = el.attributeValue("value");
				int id = Integer.parseInt(el.attributeValue("id"));
				String name = el.attributeValue("name");
				String column = el.attributeValue("column");
				String type = el.attributeValue("type") == null ? "" : el.attributeValue("type");
				String format = el.attributeValue("format") == null ? "" : el.attributeValue("format");
				String scope = el.attributeValue("scope");
				PropertyElement pye = new PropertyElement(id, name, column, type, format);
				pye.setTableName(e.attributeValue("table"));
				pye.setScope(scope);
				if (Util.isNotNull(value)) {
					pye.setVarFlag(true);
					pye.setVar(value);
				}
				propertyTags[j] = pye;

				if (scope != null) {
					List<PropertyElement> cols = null;
					if (scopeMap.containsKey(scope)) {
						cols = scopeMap.get(scope);
						if (cols == null) {
							cols = new ArrayList<PropertyElement>();
							scopeMap.put(scope, cols);
						}
					} else {
						cols = new ArrayList<PropertyElement>();
						scopeMap.put(scope, cols);
					}
					cols.add(pye);
				}
			}
			pe.setChild(propertyTags);

			childs[i] = pe;
		}
		MappingElement me = new MappingElement();
		me.setChild(childs);
		this.mapping = me;
	}

	@SuppressWarnings("unchecked")
	private void loadRules() throws Exception {
		List<Element> rules = doc.selectNodes("/definition/rule");
		ruleElements = new RuleElement[rules.size()];
		for (int i = 0; i < rules.size(); i++) {
			RuleElement re = new RuleElement(Integer.parseInt(rules.get(i).attributeValue("id")), rules.get(i).attributeValue("name"));
			re.setChild(loadChilds(rules.get(i)));

			ruleElements[i] = re;
		}
	}

	@SuppressWarnings("unchecked")
	private void loadProcess() throws Exception {
		Element process = (Element) doc.selectSingleNode("/definition/process");
		processElement = new ProcessElement();
		List<Element> owners = process.selectNodes("owner");
		List<Tag> tmp1 = new ArrayList<Tag>();
		for (Element e : owners) {
			OwnerElement oe = new OwnerElement(getRuleById(Integer.parseInt(e.attributeValue("rule-ref"))));
			List<Element> rs = e.selectNodes("record");
			List<Tag> tmp2 = new ArrayList<Tag>();
			for (Element ee : rs) {
				RecordElement re = new RecordElement(getRuleById(Integer.parseInt(ee.attributeValue("match-rule"))), getRuleById(Integer.parseInt(ee
						.attributeValue("dig-rule"))));
				re.setOwnerName(ee.attributeValue("owner"));
				re.setFile(ee.attributeValue("file"));
				tmp2.add(re);
			}
			oe.setChild(tmp2.toArray(new Tag[0]));
			tmp1.add(oe);
		}
		processElement.setChild(tmp1.toArray(new Tag[0]));
	}

	public ProcessElement getProcessElement() {
		return processElement;
	}

	public RuleElement getRuleById(int id) throws Exception {
		for (RuleElement re : ruleElements) {
			if (re == null) {
				Element e = (Element) doc.selectSingleNode("/definition/rule[@id=" + id + "]");
				if (e != null) {
					Tag[] childs = loadChilds(e);
					RuleElement r = null;
					int rid = e.attributeValue("id") == null ? 0 : Integer.parseInt(e.attributeValue("id"));
					String rname = e.attributeValue("name");
					r = new RuleElement(rid, rname);
					r.setChild(childs);
					return r;
				} else {
					throw new Exception("语法不正确，未到找指定的rule节点，id:" + id);
				}
			} else if (re.getId() == id) {
				return re;
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private Tag[] loadChilds(Element parentElement) throws Exception {
		Tag[] childTags = null;

		List<Element> childElements = parentElement.elements();
		if (childElements == null || childElements.size() == 0) {
			return null;
		}
		childTags = new Tag[childElements.size()];
		for (int i = 0; i < childTags.length; i++) {
			Element e = childElements.get(i);
			String tagName = e.getName();
			if (tagName.equals("split")) {
				String value = e.attributeValue("value");
				int times = e.attributeValue("times") != null ? Integer.parseInt(e.attributeValue("times")) : 0;
				SplitElement se = new SplitElement(value, times);
				childTags[i] = se;
				se.setChild(loadChilds(e));
			} else if (tagName.equals("field")) {
				int index = Integer.parseInt(e.attributeValue("index"));
				String ownerName = e.attributeValue("owner");
				PropertyElement pe = e.attributeValue("property") == null ? null : getPropertyById(Integer.parseInt(e.attributeValue("property")),
						ownerName);
				FieldElement fe = new FieldElement(index, pe, ownerName);
				childTags[i] = fe;
				fe.setChild(loadChilds(e));
			} else if (tagName.equals("trim-sign")) {
				String start = e.attributeValue("start");
				String end = e.attributeValue("end");
				TrimSignElement tse = new TrimSignElement(start, end);
				boolean isGreed = e.attributeValue("greed") == null ? false : Boolean.parseBoolean(e.attributeValue("greed"));
				tse.setGreedFlag(isGreed);
				childTags[i] = tse;
				tse.setChild(loadChilds(e));
			} else if (tagName.equals("trim-index")) {
				int start = Integer.parseInt(e.attributeValue("start"));
				int end = Integer.parseInt(e.attributeValue("end"));
				TrimIndexElement tie = new TrimIndexElement(start, end);
				childTags[i] = tie;
				tie.setChild(loadChilds(e));
			} else if (tagName.endsWith("switch")) {
				SwitchElement se = new SwitchElement();
				childTags[i] = se;
				se.setChild(loadChilds(e));
			} else if (tagName.endsWith("case")) {
				CaseElement ce = new CaseElement();
				ce.setReturnValue(e.attributeValue("return"));
				RuleElement ruleRef = null;
				int ruleRefId = e.attributeValue("return-ref") != null ? Integer.parseInt(e.attributeValue("return-ref")) : 0;
				if (ruleRefId > 0) {
					ruleRef = getRuleById(ruleRefId);
				}
				ce.setReturnRef(ruleRef);
				childTags[i] = ce;
				ce.setChild(loadChilds(e));
			} else if (tagName.equals("default")) {
				DefaultElement de = new DefaultElement();
				de.setReturnValue(e.attributeValue("return"));
				childTags[i] = de;
				de.setChild(loadChilds(e));
			} else if (tagName.equals("include")) {
				IncludeElement ie = new IncludeElement();
				ie.setValue(e.attributeValue("value"));
				childTags[i] = ie;
				ie.setChild(loadChilds(e));
			} else if (tagName.equals("area-exist")) {
				AreaExistElement aee = new AreaExistElement();
				aee.setStartSign(e.attributeValue("start-sign"));
				aee.setEndSign(e.attributeValue("end-sign"));
				childTags[i] = aee;
				aee.setChild(loadChilds(e));
			} else if (tagName.equals("const")) {
				ConstElement ce = new ConstElement();
				ce.setValue(e.attributeValue("value"));
				childTags[i] = ce;
				ce.setChild(loadChilds(e));
			} else if (tagName.equals("strcat")) {
				StrCatElement sce = new StrCatElement(e.attributeValue("mark"));
				childTags[i] = sce;
				sce.setChild(loadChilds(e));
			} else if (tagName.equals("list")) {
				ListElement le = new ListElement();
				childTags[i] = le;
				le.setChild(loadChilds(e));
			} else if (tagName.equals("item")) {
				ItemElement ie = new ItemElement(e.attributeValue("value"));
				childTags[i] = ie;
				ie.setChild(loadChilds(e));
			} else if (tagName.equals("raw")) {
				RawElement re = new RawElement();
				childTags[i] = re;
				re.setChild(loadChilds(e));
			} else {
				if (parentElement != null) {
					logger.error(parentElement.getPath() + "下未找到任何子节点");
				}
			}
		}

		return childTags;
	}

	public PropertyElement getPropertyById(int id, String tableName) {

		Tag[] tags = mapping.getChild();

		for (Tag tag : tags) {
			PropertiesElement pse = (PropertiesElement) tag;
			if (pse.getTable().equals(tableName)) {
				if (pse.getPropertyById(id) != null) {
					return pse.getPropertyById(id);
				}
			}
		}

		return null;
	}

	public PropertiesElement getPropertiesByTable(String table) {
		Tag[] tags = mapping.getChild();

		for (Tag tag : tags) {
			PropertiesElement pse = (PropertiesElement) tag;
			if (pse.getTable().equals(table)) {
				return pse;
			}
		}

		return null;
	}

	public MappingElement getMapping() {
		return mapping;
	}

	public void dispose() {
		if (configInputStream != null) {
			try {
				configInputStream.close();
			} catch (IOException e) {
			}
		}
	}

	public Map<String, List<PropertyElement>> getScopeMap() {
		return scopeMap;
	}

	@Override
	public Object apply(Object params) {
		// TODO Auto-generated method stub
		return null;
	}

	public static void main(String[] args) throws Exception {
		Definition def = new Definition("E:\\datacollector_path\\Templet\\zte_xparser_test.xml");
		def.load();
		System.out.println();

	}

}

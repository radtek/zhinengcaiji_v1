package framework;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import parser.Parser;
import templet.TempletBase;
import util.Param;
import access.AbstractAccessor;
import distributor.Distribute;
import formator.Formator;

/**
 * PBean管理器
 * <p>
 * 从pbean.xml文件中加载对应的处理bean
 * </p>
 * 
 * @author YangJian
 * @since 3.0 3.0.2 liangww 2012-07-17 增加parseFormatorBeans
 */
public class PBeanMgr {

	private PBeanContainer<AccessorBean> accessorBeans;

	private PBeanContainer<ParserBean> parserBeans;

	private PBeanContainer<TemplateBean> templateBeans;

	private PBeanContainer<DistributorBean> distributorBeans; // v3.1版本加入进来的

	private PBeanContainer<ParseFormatorBeans> parseFormatorBeans; // v3.2版本加入进来的

	private static final String configFilePath = "." + File.separator + "conf" + File.separator + "pbean.xml";

	private Document doc;

	private static PBeanMgr instance = null;

	private PBeanMgr() {
		super();
		accessorBeans = new PBeanContainer<AccessorBean>();
		parserBeans = new PBeanContainer<ParserBean>();
		templateBeans = new PBeanContainer<TemplateBean>();
		distributorBeans = new PBeanContainer<DistributorBean>();

		// liangww add 2012-07-17
		parseFormatorBeans = new PBeanContainer<ParseFormatorBeans>();

		init();
	}

	public static synchronized PBeanMgr getInstance() {
		if (instance == null) {
			instance = new PBeanMgr();
		}

		return instance;
	}

	private void init() {
		File f = new File(configFilePath);
		if (!f.exists() || !f.isFile())
			return;

		SAXReader reader = new SAXReader();
		try {
			doc = reader.read(new FileInputStream(configFilePath));
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		loadPBean("/pbeans/accessors/accessor", accessorBeans, AccessorBean.class);
		loadPBean("/pbeans/parsers/parser", parserBeans, ParserBean.class);
		loadPBean("/pbeans/distributors/distributor", distributorBeans, DistributorBean.class);
		loadPBean("/pbeans/templates/template", templateBeans, TemplateBean.class);

		// liangww add 2012-07-17
		loadPBean("/pbeans/parseFormators/parseFormator", parseFormatorBeans, ParseFormatorBeans.class);
	}

	@SuppressWarnings("unchecked")
	private <T extends PBean> void loadPBean(String PBeanXPath, PBeanContainer<T> container, Class<T> clazz) {
		List<Element> lst = doc.selectNodes(PBeanXPath);
		if (lst == null)
			return;

		for (Element e : lst) {
			T bean = null;
			try {
				bean = clazz.newInstance();
			} catch (Exception ex) {
				ex.printStackTrace();
			}

			if (bean == null)
				continue;

			boolean b = retrievePBean(e, bean);
			if (b)
				container.add(bean);
		}
	}

	/**
	 * 根据传入的节点获取PBean基类信息并填充至传入参数bean对象中
	 * 
	 * @param e
	 * @param bean
	 */
	@SuppressWarnings("unchecked")
	private <T extends PBean> boolean retrievePBean(Element e, T bean) {
		if (bean == null)
			return false;

		String strID = e.attributeValue("id");
		if (strID == null)
			return false;
		int id = Integer.parseInt(strID);
		if (id < 0)
			return false;
		bean.setId(id);

		Element eName = e.element("name");
		if (eName != null)
			bean.setName(eName.getText());

		Element eDes = e.element("des");
		if (eDes != null)
			bean.setDes(eDes.getText());

		Element eBean = e.element("bean");
		if (eBean == null)
			return false;
		String strBean = eBean.getText();
		if (strBean == null || strBean.trim().length() < 1)
			return false;
		bean.setBean(eBean.getText());

		Element eParams = e.element("params");
		if (eParams == null)
			return true;

		List<Element> eParamLst = eParams.elements("param");
		if (eParamLst == null)
			return true;

		for (Element eP : eParamLst) {
			if (eP == null)
				continue;

			Element ePName = eP.element("name");
			if (ePName == null)
				continue;
			String strPName = ePName.getText();

			Element ePValue = eP.element("value");
			if (ePValue == null)
				continue;
			String strPValue = ePValue.getText();

			Param p = new Param(strPName, strPValue);
			bean.addParam(p);
		}

		return true;
	}

	public String getAccessorBeanName(int id) {
		return accessorBeans.getBeanByID(id);
	}

	public AbstractAccessor getAccessorBean(int id) {
		String beanClass = getAccessorBeanName(id);
		if (beanClass == null)
			return null;

		return (AbstractAccessor) toObject(beanClass);
	}

	public String getParserBeanName(int id) {
		return parserBeans.getBeanByID(id);
	}

	public Parser getParserBean(int id) {
		String beanClass = getParserBeanName(id);
		if (beanClass == null)
			return null;

		return (Parser) toObject(beanClass);
	}

	public String getDistributorBeanName(int id) {
		return distributorBeans.getBeanByID(id);
	}

	public Distribute getDistributorBean(int id) {
		String beanClass = getDistributorBeanName(id);
		if (beanClass == null)
			return null;

		return (Distribute) toObject(beanClass);
	}

	public Formator getParseFormatorBean(int id) {
		String beanClass = this.parseFormatorBeans.getBeanByID(id);
		if (beanClass == null)
			return null;

		return (Formator) toObject(beanClass);
	}

	public String getTemplateBeanName(int id) {
		return templateBeans.getBeanByID(id);
	}

	public TempletBase getTemplateBean(int id) {
		String beanClass = getTemplateBeanName(id);
		if (beanClass == null)
			return null;

		return (TempletBase) toObject(beanClass);
	}

	private Object toObject(String className) {
		Object o = null;
		try {
			o = Class.forName(className).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return o;
	}

	// 单元测试
	public static void main(String[] args) {
		PBeanMgr.getInstance().getAccessorBean(5);
	}
}

// class PBeanMgr end.

class PBean {

	int id;

	String name;

	String des;

	String bean;

	Map<String, Param> params;

	public PBean() {
		super();
		params = new HashMap<String, Param>();
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

	public String getDes() {
		return des;
	}

	public void setDes(String des) {
		this.des = des;
	}

	public String getBean() {
		return bean;
	}

	public void setBean(String bean) {
		this.bean = bean;
	}

	public void addParam(Param p) {
		if (p == null || p.getName() == null)
			return;

		params.put(p.getName(), p);
	}
}

class AccessorBean extends PBean {

	public AccessorBean() {
		super();
	}
}

class ParserBean extends PBean {

	public ParserBean() {
		super();
	}
}

class TemplateBean extends PBean {

	public TemplateBean() {
		super();
	}
}

class DistributorBean extends PBean {

	public DistributorBean() {
		super();
	}
}

class ParseFormatorBeans extends PBean {

	public ParseFormatorBeans() {
		super();
	}
}

class PBeanContainer<T extends PBean> {

	Map<Integer, T> beans;

	public PBeanContainer() {
		super();
		beans = new HashMap<Integer, T>();
	}

	public String getBeanByID(int id) {
		String s = null;
		if (beans.containsKey(id)) {
			PBean bean = beans.get(id);
			if (bean != null)
				s = bean.getBean();
		}

		return s;
	}

	public void add(T bean) {
		beans.put(bean.getId(), bean);
	}
}

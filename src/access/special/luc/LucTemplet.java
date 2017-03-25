package access.special.luc;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import util.LogMgr;

public class LucTemplet {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	List<LucSubTemplet> templets;

	public LucTemplet(List<LucSubTemplet> templets) {
		super();
		this.templets = templets;
	}

	@Override
	public String toString() {
		return "LucTemplet [templets=" + templets + "]";
	}

	public LucSubTemplet findBySctLog(String sct, String log) {
		for (LucSubTemplet s : templets) {
			if (FilenameUtils.wildcardMatch(sct, s.sct) && FilenameUtils.wildcardMatch(log, s.log))
				return s;
		}
		return null;
	}

	public static LucTemplet parse(File xml) {
		if (xml == null || !xml.exists() || !xml.isFile()) {
			log.error("LUC模板不存在或非文件 - " + xml);
			return null;
		}

		Document doc = null;
		try {
			doc = new SAXReader().read(xml);
			Element root = doc.getRootElement();
			List<LucSubTemplet> subs = new ArrayList<LucSubTemplet>();
			LucTemplet tem = new LucTemplet(subs);
			List<Element> templetElements = root.elements("templet");
			for (Element templetElement : templetElements) {
				String id = templetElement.attributeValue("id");
				String table = templetElement.attributeValue("table");
				String sct = templetElement.attributeValue("sct");
				String log = templetElement.attributeValue("log");
				String sep = templetElement.attributeValue("separator");
				SortedMap<String, String> fileds = new TreeMap<String, String>();
				List<Element> filedEls = templetElement.elements("field");
				for (Element e : filedEls) {
					fileds.put(e.attributeValue("src").toLowerCase(), e.attributeValue("dest"));
				}
				tem.templets.add(new LucSubTemplet(id, table, sct, log, sep, fileds));
			}

			return tem;
		} catch (Exception e) {
			log.error("解析luc模板出现异常 - " + xml, e);
			return null;
		}

	}

	public static void main(String[] args) {
		LucTemplet tem = parse(new File("C:\\Users\\ChenSijiang\\Desktop\\luc.xml"));
		System.out.println(tem);
	}
}

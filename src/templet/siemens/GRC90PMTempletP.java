package templet.siemens;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import templet.AbstractTempletBase;
import framework.SystemConfig;

/**
 * 西门子RC90性能模板
 * 
 * @author litp
 * @since 3.0
 * @see GRC90ASCII
 */
public class GRC90PMTempletP extends AbstractTempletBase {

	private Map<String, List<String>> map = new HashMap<String, List<String>>();

	@Override
	public void parseTemp(String tempContent) throws Exception {
		String tmpFilePath = SystemConfig.getInstance().getTempletPath();
		tmpFilePath = tmpFilePath + File.separatorChar + tempContent;
		File f = new File(tmpFilePath);
		if (!f.exists()) {
			log.error("模板不存在" + tmpFilePath);
			return;
		}
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(f);
		NodeList nl = doc.getElementsByTagName("TABLE");
		for (int i = 0; i < nl.getLength(); i++) {
			Node node = nl.item(i);
			String table = node.getAttributes().getNamedItem("NAME").getNodeValue();
			List<String> list = new ArrayList<String>();
			NodeList countNodes = node.getChildNodes();
			for (int j = 0; j < countNodes.getLength(); j++) {
				Node count = countNodes.item(j);
				String text = count.getTextContent();
				if (!list.contains(text)) {
					list.add(text);
				}
			}
			map.put(table, list);
		}
	}

	/**
	 * 如果返回的table为空，那么此条数据就不需要解析
	 * 
	 * @param countKey
	 * @return
	 */
	public String getTableByCountKey(String countKey) {
		String table = null;
		Set<String> tables = map.keySet();
		for (String ta : tables) {
			List<String> list = map.get(ta);
			if (list.contains(countKey)) {
				table = ta;
				break;
			}
		}
		return table;
	}

	public static void main(String[] args) {
		GRC90PMTempletP p = new GRC90PMTempletP();
		try {
			p.parseTemp("clt_pm_sim_rc90_parse.xml");
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(p.getTableByCountKey("(5,0)"));
	}

}

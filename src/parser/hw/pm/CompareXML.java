package parser.hw.pm;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import util.Util;

public class CompareXML {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		File file1 = new File("C:\\Users\\ChensSijiang\\Desktop\\河北、北京对比\\河北.xml");
		File file2 = new File("C:\\Users\\ChensSijiang\\Desktop\\河北、北京对比\\北京.xml");
		PrintWriter out1 = new PrintWriter("C:\\Users\\ChensSijiang\\Desktop\\河北、北京对比\\河北统计.txt");
		PrintWriter out2 = new PrintWriter("C:\\Users\\ChensSijiang\\Desktop\\河北、北京对比\\北京统计.txt");
		Map<String, List<String>> map1 = new HashMap<String, List<String>>();
		Map<String, List<String>> map2 = new HashMap<String, List<String>>();
		List<String> keys1 = new ArrayList<String>();
		List<String> keys2 = new ArrayList<String>();
		loadMap(file1, map1);
		loadMap(file2, map2);
		sortKeys(map1, keys1);
		sortKeys(map2, keys2);
		out(out1, keys1, map1);
		out1.close();
		out(out2, keys2, map2);
		out2.close();
	}

	static final void out(PrintWriter out, List<String> keys, Map<String, List<String>> map) throws Exception {
		for (String key : keys) {
			out.println("[" + key + "]");
			List<String> types = map.get(key);
			Collections.sort(types);
			for (String type : types) {
				out.println(type);
			}
			out.println();
			out.flush();
		}
	}

	static final void sortKeys(Map<String, List<String>> map, List<String> keys) throws Exception {
		Iterator<String> it = map.keySet().iterator();
		while (it.hasNext()) {
			keys.add(it.next());
		}
		Collections.sort(keys);
	}

	static final void loadMap(File f, Map<String, List<String>> map) throws Exception {
		SAXReader sr = new SAXReader();
		Document doc = sr.read(f);
		List<Element> measInfoElements = doc.getRootElement().element("measData").elements("measInfo");
		for (Element measInfoElement : measInfoElements) {
			String measInfoId = measInfoElement.attributeValue("measInfoId");
			List<String> types = new ArrayList<String>();
			String[] strTypes = measInfoElement.elementTextTrim("measTypes").split(" ");
			for (String s : strTypes) {
				if (Util.isNotNull(s))
					types.add(s.trim());
			}
			map.put(measInfoId, types);
		}
	}
}

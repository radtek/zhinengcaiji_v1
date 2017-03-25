package parser.eric.pm;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * 统计性能文件字段及moid
 * 
 * @author ChenSijiang 20100417
 * 
 */
public class Test {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		SAXReader reader = new SAXReader();
		reader.setEntityResolver(new IgnoreDTDEntityResolver());
		Document doc = reader.read(new File(
		/* 从此处读取 */
		"f:\\ftp_root\\eric\\A20100302.1600+0800-1615+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=DGRNC01,MeContext=DGRNC01_statsfile.xml"));

		List<MoidInfo> moids = new ArrayList<MoidInfo>();

		List<Element> mdElements = doc.selectNodes("/mdc/md");
		for (int i = 0; i < mdElements.size(); i++) {
			MoidInfo mo = null;

			Element mdEl = mdElements.get(i);
			List<Element> mtEls = mdEl.selectNodes("mi/mt");
			List<Element> mvEls = mdEl.selectNodes("mi/mv");

			if (mvEls.size() > 0) {
				mo = new MoidInfo(mvEls.get(0).elementTextTrim("moid"));
				if (moids.contains(mo)) {
					mo = moids.remove(moids.indexOf(mo));
				} else {
					mo = new MoidInfo(mvEls.get(0).elementTextTrim("moid"));
				}
			} else {
				continue;
			}
			for (Element e : mtEls) {
				String prop = e.getTextTrim();
				if (!mo.props.contains(prop)) {
					mo.props.add(prop);
				}
			}
			moids.add(mo);
		}

		PrintWriter pw = new PrintWriter("d:\\moid.txt"/* 写至此处 */);
		for (MoidInfo mo : moids) {
			for (String s : mo.props) {
				pw.print(s);
				for (int i = 0; i < 55 - s.length(); i++) {
					pw.print(" ");
				}
				pw.print("[");
				pw.flush();
				for (int i = 0; i < mo.names.size(); i++) {
					pw.print(mo.names.get(i));
					if (i != mo.names.size() - 1) {
						pw.print("  ");
					}
				}
				pw.flush();
				pw.println("]");
			}

			pw.println();
			pw.flush();
		}

		pw.close();
	}
}

class MoidInfo {

	List<String> names = new ArrayList<String>();

	List<String> props = new ArrayList<String>();

	MoidInfo(String moid) {
		String[] items = moid.split(",");
		for (String s : items) {
			names.add(s.split("=")[0]);
		}
	}

	@Override
	public boolean equals(Object obj) {
		MoidInfo mo = (MoidInfo) obj;
		if (names.size() != mo.names.size()) {
			return false;
		}
		for (int i = 0; i < mo.names.size(); i++) {
			if (!names.get(i).equalsIgnoreCase(mo.names.get(i))) {
				return false;
			}
		}
		return true;
	}
}

class IgnoreDTDEntityResolver implements EntityResolver {

	@Override
	public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
		return new InputSource(new ByteArrayInputStream("<?xml version='1.0' encoding='UTF-8'?>".getBytes()));
	}

}

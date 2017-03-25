package parser.lucent;

import java.io.PrintWriter;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class CreateTable {

	public static void main(String[] args) throws Exception {
		PrintWriter pw = new PrintWriter("C:\\Users\\ChenSijiang\\Desktop\\tb.txt");
		Document doc = new SAXReader().read("E:\\datacollector_path\\templet\\clt_luc_1x_smart.xml");
		List<Element> templets = doc.getRootElement().elements("templet");
		for (Element templet : templets) {
			pw.println("create table " + templet.attributeValue("table"));
			pw.println("(");
			pw.println("OMCID NUMBER,");
			pw.println("COLLECTTIME DATE,");
			pw.println("STAMPTIME DATE,");
			pw.println("ECP VARCHAR2(50),");
			pw.println("DCS VARCHAR2(50),");
			pw.println("SID VARCHAR2(50),");
			pw.println("RELEASE VARCHAR2(50),");
			List<Element> fields = templet.elements("field");
			for (Element field : fields) {
				pw.println(field.attributeValue("col") + " NUMBER,");
			}
			pw.println(");");
			pw.println();
		}
		pw.close();
	}
}

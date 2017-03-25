package parser.siemens.cm;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

public class TempletUtil {

	static class Prop {

		String name;

		String type;

		public Prop(String name, String type) {
			super();
			this.name = name;
			this.type = type;
		}

	}

	static void makeTemplet() throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\ChenSijiang\\Desktop\\table.txt")));

		Document doc = DocumentHelper.createDocument();
		Element root = doc.addElement("templet");

		String line = null;
		String tn = null;
		List<Prop> props = new ArrayList<Prop>();
		while ((line = br.readLine()) != null) {
			line = line.replace(",", "").trim();
			if (line.equals("")) {
				continue;
			}
			if (tn == null) {
				tn = line;
			} else if (line.equals(");")) {
				Element eTable = root.addElement("table");
				Element eName = eTable.addElement("name");
				eName.setText(tn.replace("CLT_CM_", "").replace("_SIE_BR90", ""));
				Element eStartSign = eTable.addElement("startSign");
				Element eItem = eStartSign.addElement("item");
				eItem.setText("CREATE " + eName.getText() + ":");
				Element eSplitSign = eTable.addElement("splitSign");
				eSplitSign.setText(",");
				Element eNVS = eTable.addElement("nameValueSplitSign");
				eNVS.setText("=");
				Element eTN = eTable.addElement("tableName");
				eTN.setText(tn);
				Element eProps = eTable.addElement("properties");
				for (Prop p : props) {
					Element eProp = eProps.addElement("property");
					Element ePN = eProp.addElement("propertyName");
					ePN.setText(p.name);
					Element eCN = eProp.addElement("columnName");
					eCN.setText(p.name);
					String type = p.type;
					String dataType = null;
					String dataFormat = null;
					if (type.equals("number")) {
						dataType = "1";
						dataFormat = "";
					} else if (type.equals("date")) {
						dataType = "3";
						dataFormat = "yyyy-mm-dd hh24:mi:ss";
					} else if (type.equals("varchar2(255)")) {
						dataType = "2";
						dataFormat = "";
					}
					Element eDT = eProp.addElement("dataType");
					eDT.setText(dataType);
					Element eDF = eProp.addElement("dataFormat");
					eDF.setText(dataFormat);
				}
				tn = null;
				props.clear();
			} else {
				String[] items = line.split(" ");
				String name = items[0];
				String type = items[1];
				Prop p = new Prop(name, type);
				props.add(p);
			}
		}

		PrintWriter pw = new PrintWriter("d:\\a.xml");
		pw.print(doc.asXML());
		pw.flush();
		pw.close();
		br.close();
	}
}

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;

import util.Util;

public class MakeHW6000Templet
{
	public static void main(String[] args) throws Exception
	{
		Map<String, String> map = new HashMap<String, String>();
		map.put("sms_file_0", "TRAFFICA_SMS");
		map.put("rnc", "CLT_NE_W_RNC");
		map.put("utrancell", "CLT_NE_W_UTRANCELL");

		int index = 0;
		Document p = DocumentHelper.createDocument();
		Document d = DocumentHelper.createDocument();
		p.addElement("templets");
		d.addElement("templets");
		File dir = new File("C:\\Users\\ChenSijiang\\Desktop\\zhaowg\\");
		for (File xls : dir.listFiles())
		{
			String tableName;
			String name = xls.getName().replace(".csv", "").toLowerCase();

			tableName = map.get(name);
			if ( Util.isNull(tableName) )
				continue;

			List<String> head = new ArrayList<String>();
			FileInputStream fis = new FileInputStream(xls);
			LineIterator it = IOUtils.lineIterator(fis, "utf-8");
			String first = null;
			if ( it.hasNext() )
				first = it.nextLine();
			first = first.replace("\"", "");
			IOUtils.closeQuietly(fis);
			String[] sp = first.split(",");
			for (String xx : sp)
			{
				if ( Util.isNotNull(xx) )
				{
					head.add(xx.trim());
				}
			}

			Element e = p.getRootElement().addElement("templet").addAttribute("id", index
					+ "").addAttribute("file", "/ne/" + xls.getName());
			e = e.addElement("ds").addAttribute("id", "0");
			e.addElement("meta").addElement("head").addAttribute("splitSign", ",");
			e = e.addElement("fields").addAttribute("splitSign", ",");
			int i = 0;
			for (String s : head)
			{
				e.addElement("field").addAttribute("name", s).addAttribute("index", String.valueOf(i++));
			}
			i = 0;
			e = d.getRootElement().addElement("templet").addAttribute("id", index
					+ "");
			e = e.addElement("table").addAttribute("id", "0").addAttribute("name", tableName).addAttribute("split", "|");
			for (String s : head)
			{

				if ( s.equalsIgnoreCase("PERIOD_START_TIME")
						|| s.equalsIgnoreCase("LAST_MODIFIED")
						|| s.equalsIgnoreCase("TIME_STAMP")
						|| s.equalsIgnoreCase("STARTTIME")
						|| s.equalsIgnoreCase("ENDTIME")
						|| s.equalsIgnoreCase("RECTIME") )
				{
					e.addElement("column").addAttribute("name", s.toUpperCase()).addAttribute("index", String.valueOf(i++)).addAttribute("type", "3").addAttribute("format", "yyyy-mm-dd hh24:mi:ss");
				}
				else
				{
					e.addElement("column").addAttribute("name", s.toUpperCase()).addAttribute("index", String.valueOf(i++));
				}
			}
			index++;
		}
		XMLWriter w = new XMLWriter(new FileOutputStream("C:\\Users\\ChenSijiang\\Desktop\\p.xml"));
		w.write(p);
		w.flush();
		w.close();
		w = new XMLWriter(new FileOutputStream("C:\\Users\\ChenSijiang\\Desktop\\d.xml"));
		w.write(d);
		w.flush();
		w.close();
	}
}

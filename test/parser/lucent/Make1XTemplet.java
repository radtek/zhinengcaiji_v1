package parser.lucent;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import util.Util;
import util.opencsv.CSVReader;

public class Make1XTemplet
{
	public static void main(String[] args) throws Exception
	{
		Map<String, List<String[]>> map = new HashMap<String, List<String[]>>();
		CSVReader r = new CSVReader(new InputStreamReader(new FileInputStream("C:\\Users\\ChenSijiang\\Desktop\\aaa.csv")));
		PrintWriter pw = new PrintWriter("C:\\Users\\ChenSijiang\\Desktop\\hsmr_1x_parse.xml");
		pw.println("<?xml version=\"1.0\" encoding=\"gb2312\" ?>");
		pw.println("<templets>");
		r.readNext();
		String[] l = null;
		while ((l = r.readNext()) != null)
		{
			String rawTb = l[0].trim();
			String clt = l[2].trim();
			if ( Util.isNull(rawTb) )
				continue;
			if ( map.containsKey(clt) )
			{
				map.get(clt).add(l);
			}
			else
			{
				List<String[]> lst = new ArrayList<String[]>();
				lst.add(l);
				map.put(clt, lst);
			}

		}
		r.close();

		Iterator<String> it = map.keySet().iterator();
		while (it.hasNext())
		{
			String clt = it.next();
			List<String[]> lineLst = map.get(clt);
			pw.println("<templet table=\"" + clt + "\" headSign=\"HEAD"
					+ lineLst.get(0)[0] + " \" dataSign=\"" + lineLst.get(0)[0]
					+ " \" splitSign=\" \">");
			for (int i = 0; i < lineLst.size(); i++)
			{
				String[] line = lineLst.get(i);
				String rawField = line[1].trim();
				String cltField = line[3].trim();
				pw.println("<field raw=\"" + rawField + "\" col=\"" + cltField
						+ "\" />");
			}
			pw.println("</templet>");
		}
		pw.println("</templets>");
		pw.flush();
		pw.close();
	}
}

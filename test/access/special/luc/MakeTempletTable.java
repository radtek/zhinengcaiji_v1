package access.special.luc;

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

public class MakeTempletTable
{
	public static void main(String[] args) throws Exception
	{
		PrintWriter templet = new PrintWriter("C:\\Users\\ChenSijiang\\Desktop\\luc.xml");
		PrintWriter table = new PrintWriter("C:\\Users\\ChenSijiang\\Desktop\\tables.txt");
		Map<String, List<CSV>> map = new HashMap<String, List<CSV>>();

		CSVReader r = new CSVReader(new InputStreamReader(new FileInputStream("C:\\Users\\ChenSijiang\\Desktop\\luc.csv")));
		r.readNext();
		String[] line = null;

		while ((line = r.readNext()) != null)
		{
			CSV row = CSV.parse(line);
			if ( row != null )
			{
				if ( map.containsKey(row.clt) )
				{
					map.get(row.clt).add(row);
				}
				else
				{
					List<CSV> list = new ArrayList<CSV>();
					list.add(row);
					map.put(row.clt, list);
				}
			}
		}
		r.close();
		templet.println("<?xml version=\"1.0\" encoding=\"gb2312\" ?>");
		templet.println("<templets>");
		int count = 0;
		Iterator<String> it = map.keySet().iterator();
		while (it.hasNext())
		{
			String key = it.next();
			List<CSV> rows = map.get(key);
			templet.println("<templet id=\"" + (count++) + "\" table=\"" + key
					+ "\" sct=\"/home/*/" + rows.get(0).vendorName
					+ ".sct\" log=\"/home/*/" + rows.get(0).vendorName
					+ ".log\" separator=\";\">");
			table.println("create table " + key + " (");
			table.println("OMCID NUMBER,");
			table.println("COLLECTTIME DATE,");
			table.println("STAMPTIME DATE,");

			for (int i = 0; i < rows.size(); i++)
			{
				table.print(rows.get(i).dest + " VARCHAR2(100)");
				templet.println("<field src=\"" + rows.get(i).src
						+ "\" dest=\"" + rows.get(i).dest + "\" />");
				if ( i < rows.size() - 1 )
					table.println(",");
				else
					table.println();
			}
			table.println(");");
			table.flush();
			templet.flush();
			templet.println("</templet>");
		}

		templet.println("</templets>");
		templet.flush();
		templet.close();
	}

	private static class CSV
	{
		int id;
		String vendorName;
		String src;
		String dest;
		String type;
		int len;
		String clt;

		public CSV(int id, String vendorName, String src, String clt, String dest, String type, int len)
		{
			super();
			this.id = id;
			this.vendorName = vendorName;
			this.src = src;
			this.clt = clt;
			this.dest = dest;
			this.type = type;
			this.len = len;
		}

		public CSV(int id, String vendorName, String src, String dest, String type, int len, String clt)
		{
			super();
			this.id = id;
			this.vendorName = vendorName;
			this.src = src;
			this.dest = dest;
			this.type = type;
			this.len = len;
			this.clt = clt.toUpperCase();
		}

		public static CSV parse(String[] line)
		{
			if ( Util.isNull(line[2]) )
				return null;
			return new CSV(Integer.parseInt(line[0]), line[1].trim(), line[2].trim(), line[3].trim(), line[4].trim(), line[5].trim(), Integer.parseInt(line[6]));
		}
	}
}

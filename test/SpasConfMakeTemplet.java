import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

public class SpasConfMakeTemplet
{
	public static void main(String[] args) throws Exception
	{
		File dir = new File("F:\\ftp_root\\GD\\CONF\\GD_CONF_UWAY_0_0_ALL_20120208000000\\");
		PrintWriter p = new PrintWriter("C:\\Users\\ChenSijiang\\Desktop\\spas_file_conf_parse.xml");
		PrintWriter d = new PrintWriter("C:\\Users\\ChenSijiang\\Desktop\\spas_file_conf_dist.xml");
		PrintWriter t = new PrintWriter("C:\\Users\\ChenSijiang\\Desktop\\t.txt");
		p.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		p.println("<templets>");
		d.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		d.println("<templets>");
		File[] fs = dir.listFiles();
		int i = -1;
		// BJ_CONF_BSC_20120110.csv
		for (File f : fs)
		{
			i++;
			InputStream in = new FileInputStream(f);
			LineIterator it = IOUtils.lineIterator(in, null);
			String sp[] = it.nextLine().split(",");
			in.close();
			p.println("<templet id=\""
					+ i
					+ "\" file=\""
					+ f.getName().replace("BJ_", "*_").replace("_20120110", "_%%Y%%M%%D")
					+ "\">");
			p.println("<ds id=\"0\">");
			p.println("<meta>");
			p.println("<head splitSign=\",\"/>");
			p.println("</meta>");
			p.println("<fields splitSign=\",\">");
			d.println("<templet id=\"" + i + "\">");
			d.println("<table id=\"0\" name=\""
					+ f.getName().replace("BJ_", "DS_CLT_").replace("_20120110.csv", "")
					+ "\" split=\"|\">");
			t.println("create table "
					+ f.getName().replace("BJ_", "DS_CLT_").replace("_20120110.csv", "")
					+ " (");
			t.println("omcid number,");
			t.println("collecttime date,");
			t.println("stamptime date,");
			int j = -1;
			for (String s : sp)
			{
				j++;
				p.println("<field name=\"" + s + "\" index=\"" + j + "\"/>");
				if ( s.equals("时间戳") )
				{
					d.println("<column name=\""
							+ "col_"
							+ j
							+ "\" index=\""
							+ j
							+ "\" type=\"3\" format=\"yyyy-mm-dd hh24:mi:ss\"/>");
					t.println("col_" + j + " date,");
				}
				else
				{
					d.println("<column name=\"" + "col_" + j + "\" index=\""
							+ j + "\"/>");
					t.println("col_" + j + " varchar2(100),");
				}
			}
			p.println("</fields>");
			p.println("</ds>");
			p.println("</templet>");
			d.println("</table>");
			d.println("</templet>");
			t.println(");");
			j = -1;
			for (String s : sp)
			{
				j++;
				t.println("comment on column "
						+ f.getName().replace("BJ_", "DS_CLT_").replace("_20120110.csv", "")
						+ ".col_" + j + " is '" + s + "';");
			}
		}

		p.println("</templets>");
		d.println("</templets>");
		p.flush();
		p.close();
		d.flush();
		d.close();
		t.close();
	}
}

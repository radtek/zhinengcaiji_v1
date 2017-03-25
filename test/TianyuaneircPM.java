import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import util.CommonDB;
import util.DbPool;
import util.Util;

public class TianyuaneircPM
{
	public static void main(String[] args) throws Exception
	{

		Map<String, List<String>> map = parseHead();

		Connection con = DbPool.getConn();

		File dir = new File("C:\\Users\\ChenSijiang\\Desktop\\20111213\\10_00\\");
		File[] fs = dir.listFiles();
		for (File f : fs)
		{
			String head = f.getName().substring(0, f.getName().indexOf("-"));
			String table = "CLT_PM_ERI_R12_" + head;
			if ( !CommonDB.tableExists(con, table) )
			{
				System.out.println("del " + f + " " + f.delete());
				continue;
			}
			List<String> tbCols = CommonDB.loadCols(table);
			List<String> tyCols = map.get(head);
			for (String c : tyCols)
			{
				if ( !tbCols.contains(c) )
				{
					try
					{
						CommonDB.executeUpdate("alter table " + table + " add "
								+ c + " varchar2(200)");
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
			}

			StringWriter sw = new StringWriter();
			InputStream in = new FileInputStream(f);
			IOUtils.copy(in, sw);
			in.close();
			RandomAccessFile raf = new RandomAccessFile(f, "rw");

			for (int i = 0; i < tyCols.size(); i++)
			{
				raf.write(tyCols.get(i).getBytes());
				if ( i < tyCols.size() - 1 )
					raf.write("\t".getBytes());
			}
			raf.write("\n".getBytes());
			raf.write(sw.toString().getBytes());
			raf.close();
		}
		changename();
	}

	static Map<String, List<String>> parseHead() throws Exception
	{
		Map<String, List<String>> map = new HashMap<String, List<String>>();

		LineIterator it = IOUtils.lineIterator(new FileInputStream("C:\\Users\\ChenSijiang\\Desktop\\表头(1).txt"), null);
		String head = null;
		while (it.hasNext())
		{
			String line = it.nextLine();
			if ( Util.isNull(line) )
				continue;
			if ( line.startsWith("[") )
			{
				head = line.replace("[", "").replace("]", "");
			}
			else if ( head != null )
			{
				String field = line.split(",")[0];
				if ( map.containsKey(head) )
				{
					map.get(head).add(field);
				}
				else
				{
					List<String> list = new ArrayList<String>();
					list.add(field);
					map.put(head, list);
				}
			}
		}
		it.close();
		return map;
	}

	static void changename()
	{
		File dir = new File("C:\\Users\\ChenSijiang\\Desktop\\20111213\\");
		for (File d : dir.listFiles())
		{
			for (File f : d.listFiles())
			{
				String head = f.getName().substring(0, f.getName().indexOf("-"));
				f.renameTo(new File(f.getParent(), head + ".csv"));
			}
		}
	}
}

package access.special.luc;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import util.opencsv.CSVWriter;

public class MakeCSV
{
	public static void main(String[] args) throws Exception
	{
		int index = 0;
		PrintWriter pw = new PrintWriter("C:\\Users\\ChenSijiang\\Desktop\\luc.csv");
		CSVWriter w = new CSVWriter(pw);
		w.writeNext(new String[] { "TABLE_ID", "厂家表名", "厂家字段", "采集表名", "采集字段", "采集字段类型", "采集字段长度" });
		File dir = new File("C:\\Users\\ChenSijiang\\Desktop\\20111219\\");
		File[] files = dir.listFiles();
		for (File sct : files)
		{
			if ( !sct.getName().endsWith(".sct") )
				continue;
			int tableId = index++;
			String lucTb = sct.getName().replace(".sct", "");
			String cltName = "CLT_CM_LUCNT_" + lucTb.toUpperCase();
			w.writeNext(new String[] { String.valueOf(tableId), lucTb, "", "", "OMCID", "NUMBER", "22" });
			w.writeNext(new String[] { String.valueOf(tableId), lucTb, "", "", "COLLECTTIME", "DATE", "7" });
			w.writeNext(new String[] { String.valueOf(tableId), lucTb, "", "", "STAMPTIME", "DATE", "7" });
			Map<Integer, String> map = LucTelnetCollect.parseSCT(sct);
			List<String> sortFileds = new ArrayList<String>();
			for (int i = 0; i < map.size(); i++)
			{
				sortFileds.add(map.get(i));
			}
			for (String f : sortFileds)
			{
				String col = f.replace(".", "_").replace("[", "").replace("]", "").toUpperCase();
				if ( col.length() > 30 )
				{
					col = col.substring(col.indexOf("_") + 1);
				}
				if ( col.equals("LEVEL") )
					col = "LEVEL_COL";
				if ( col.equals("CAST") )
					col = "LEVEL_CAST";
				w.writeNext(new String[] { String.valueOf(tableId), lucTb, f, cltName, col, "VARCHAR2", "100" });
			}
			w.flush();
		}
		w.flush();
		w.close();
	}
}

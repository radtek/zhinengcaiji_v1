import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import util.CommonDB;
import util.Util;

public class AddHWCol
{
	private InputStream in = new FileInputStream("");

	public AddHWCol() throws FileNotFoundException
	{
	}

	public AddHWCol(int a) throws FileNotFoundException
	{

	}

	public static void main(String[] args) throws Exception
	{
		System.out.println(InetAddress.getAllByName("WIN-KV7179a"));

		Map<String, String> map = new HashMap<String, String>();
		map.put("tbl_result_1275071419_3", "clt_pm_hw_v9r11_1275071419");
		map.put("tbl_result_1275071420_3", "clt_pm_hw_v9r11_1275071420");
		map.put("tbl_result_1275071421_3", "clt_pm_hw_v9r11_1275071421");
		map.put("tbl_result_1275071423_3", "clt_pm_hw_v9r11_1275071423");
		map.put("tbl_result_1275071424_3", "clt_pm_hw_v9r11_1275071424");
		map.put("tbl_result_1275071425_3", "clt_pm_hw_v9r11_1275071425");
		map.put("tbl_result_1275071426_3", "clt_pm_hw_v9r11_1275071426");
		map.put("tbl_result_1275071427_3", "clt_pm_hw_v9r11_1275071427");
		map.put("tbl_result_1275071428_3", "clt_pm_hw_v9r11_1275071428");
		map.put("tbl_result_1275071429_3", "clt_pm_hw_v9r11_1275071429");
		map.put("tbl_result_1275071430_3", "clt_pm_hw_v9r11_1275071430");
		map.put("tbl_result_1275071435_3", "clt_pm_hw_v9r11_1275071435");
		map.put("tbl_result_1275071617_3", "clt_pm_hw_v9r11_1275071617");
		map.put("tbl_result_1275071618_3", "clt_pm_hw_v9r11_1275071618");
		map.put("tbl_result_1275071817_3", "clt_pm_hw_v9r11_1275071817");
		map.put("tbl_result_1275071821_3", "clt_pm_hw_v9r11_1275071821");
		map.put("tbl_result_1275071822_3", "clt_pm_hw_v9r11_1275071822");
		map.put("tbl_result_1275071823_3", "clt_pm_hw_v9r11_1275071823");
		map.put("tbl_result_1275071824_3", "clt_pm_hw_v9r11_1275071824");
		map.put("tbl_result_1275072521_3", "clt_pm_hw_v9r11_1275072521");
		map.put("tbl_result_1275072522_3", "clt_pm_hw_v9r11_1275072522");
		map.put("tbl_result_1275072523_3", "clt_pm_hw_v9r11_1275072523");
		map.put("tbl_result_1275072524_3", "clt_pm_hw_v9r11_1275072524");
		map.put("tbl_result_1275072531_3", "clt_pm_hw_v9r11_1275072531");
		map.put("tbl_result_1275072532_3", "clt_pm_hw_v9r11_1275072532");
		map.put("tbl_result_1275072617_3", "clt_pm_hw_v9r11_1275072617");
		map.put("tbl_result_1275072618_3", "clt_pm_hw_v9r11_1275072618");
		map.put("tbl_result_1275071418_3", "clt_pm_hw_v9r11_1275071418");
		map.put("tbl_result_1275071422_3", "clt_pm_hw_v9r11_1275071422");
		map.put("tbl_result_1275071432_3", "clt_pm_hw_v9r11_1275071432");
		map.put("tbl_result_1275071433_3", "clt_pm_hw_v9r11_1275071433");
		map.put("tbl_result_1275071437_3", "clt_pm_hw_v9r11_1275071437");
		map.put("tbl_result_1275071438_3", "clt_pm_hw_v9r11_1275071438");
		map.put("tbl_result_1275071441_3", "clt_pm_hw_v9r11_1275071441");
		map.put("tbl_result_1275071217_3", "clt_pm_hw_v9r11_1275071217");
		map.put("tbl_result_1275071626_3", "clt_pm_hw_v9r11_1275071626");
		map.put("tbl_result_1275072525_3", "clt_pm_hw_v9r11_1275072525");
		map.put("tbl_result_1275072526_3", "clt_pm_hw_v9r11_1275072526");
		map.put("tbl_result_1275072527_3", "clt_pm_hw_v9r11_1275072527");
		map.put("tbl_result_1275072528_3", "clt_pm_hw_v9r11_1275072528");
		map.put("tbl_result_1275072717_3", "clt_pm_hw_v9r11_1275072717");
		map.put("tbl_result_1275072017_3", "clt_pm_hw_v9r11_1275072017");
		map.put("tbl_result_1275072018_3", "clt_pm_hw_v9r11_1275072018");
		map.put("tbl_result_1275072051_3", "clt_pm_hw_v9r11_1275072051");
		map.put("tbl_result_1275072519_3", "clt_pm_hw_v9r11_1275072519");
		map.put("tbl_result_1275068417_3", "clt_pm_hw_v9r11_1275068417");
		map.put("tbl_objectinstance", "clt_pm_hw_v9r11_tblobjins");

		File dir = new File("C:\\Users\\ChenSijiang\\Desktop\\HW_NPM\\");
		for (File xls : dir.listFiles())
		{
			String tableName;
			String name = xls.getName().replace(".csv", "").toLowerCase();
			if ( map.containsKey(name) )
			{
				name = map.get(name);
				System.out.println(name);
			}
			else
			{
				System.err.println(name);
				continue;
			}

			tableName = name;

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

			for (String col : head)
			{
				if ( col.contains("COUNTER_") )
				{
					try
					{
						CommonDB.executeUpdate("alter table " + tableName
								+ " add " + col + " number");
					}
					catch (Exception e)
					{
						System.out.println(e.getMessage());
					}
				}
			}
		}
	}
}

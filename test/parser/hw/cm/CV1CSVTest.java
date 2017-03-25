package parser.hw.cm;

import java.sql.Timestamp;

import task.CollectObjInfo;
import task.DevInfo;

public class CV1CSVTest
{
	public static void main(String[] args)
	{

		CV1CSV csv = new CV1CSV();
		String filePath = "F:\\liang\\tmp\\0604-0609\\bj\\参数原始文件\\CDMAExport_BSC10_136.37.76.211_2012060602.csv";
		csv.setFileName(filePath);
		CollectObjInfo info = new CollectObjInfo(1);
		DevInfo dev = new DevInfo();
		info.setDevInfo(dev);
		dev.setOmcID(1234);
		info.setLastCollectTime(new Timestamp(11));
		csv.setCollectObjInfo(info);
		try
		{
			csv.parse();
			// System.out.println("xxx end");
			// csv.setFileName("C:\\Users\\ChenSijiang\\Desktop\\aaa.csv");
			// csv.parse();
			// System.out.println("aaa end");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

}

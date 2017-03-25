package util;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import task.CollectObjInfo;
import task.DevInfo;

public class ExcelToCsvUtil2Test
{
	public static void main(String[] args) throws Exception
	{
		String excelFile = "F:\\liang\\tmp\\0924-0929\\xls\\2012年中秋福利名单（异地）.xls";
//		excelFile = "F:\\liang\\tmp\\0924-0929\\xls\\2012年中秋福利名单（异地）.xls";
		CollectObjInfo taskInfo = new CollectObjInfo(1122);
		DevInfo d = new DevInfo();
		d.setOmcID(99);
		taskInfo.setDevInfo(d);
		taskInfo.setLastCollectTime(new Timestamp(new Date().getTime()));
		ExcelToCsvUtil2 excelToCsvUtil = new ExcelToCsvUtil2(excelFile, taskInfo);
		
		List<String> list = excelToCsvUtil.toCsv();
		System.out.println(list);
	}
}

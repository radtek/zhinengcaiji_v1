package parser.others.gpslog;

import java.sql.BatchUpdateException;
import java.sql.Timestamp;
import java.util.Date;

import task.CollectObjInfo;
import task.DevInfo;

public class CV1BinTest
{
	public static void main(String[] args) throws Exception
	{
		Date date = new Date();

		String fileName = "F:\\liang\\tmp\\0423-0429\\igp_js_gps\\new\\LSL_JS_20120422_0000.bin";
		
		fileName = "F:\\liang\\tmp\\0604-0609\\lsl_20120602_10.bin";
		fileName = "F:\\liang\\tmp\\0604-0609\\LSL_JS_20120602_0400.bin";
		
		
		CollectObjInfo obj = new CollectObjInfo(755123);
		obj.setLastCollectTime(new Timestamp(System.currentTimeMillis()));
		CV1Bin pe = new CV1Bin(obj);
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));
		pe.setCollectObjInfo(obj);
		pe.parseData(fileName);

		// String gistime = "1573,398588.560";
		// // 158492049360
		System.out.println(System.currentTimeMillis() - date.getTime());
	}
}

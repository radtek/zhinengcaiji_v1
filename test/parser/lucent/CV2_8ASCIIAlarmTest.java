package parser.lucent;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import task.CollectObjInfo;
import task.DevInfo;

public class CV2_8ASCIIAlarmTest
{
	public static void main(String[] args) throws Exception
	{
		String path = "F:\\liang\\tmp\\0604-0609\\120511.APX";
		path = "F:\\liang\\tmp\\0604-0609\\120530\\120530.APX";
		
//		path = "F:\\liang\\tmp\\0618-0622\\rop_20120606";
		File file = new File(path);
		String[] paths = new String[]{path};
		
		if(file.isDirectory())
		{
			File[] files = file.listFiles();
			paths = new String[files.length];
			for(int i=0; i < paths.length; i++)
			{
				paths[i] = files[i].getAbsolutePath();
			}
		}
		
		for(int i=0; i < paths.length; i++)
		{
			InputStream in = new FileInputStream(paths[i]);
			CV2_8ASCIIAlarmParser parser = new CV2_8ASCIIAlarmParser();
			
			parser.setCollectObjInfo(getCollectObjInfo());
			
			parser.parse(in, null);
			parser.sqlldr();
			System.out.println(parser.alarmList.size());
		}
	}

	
	public static CollectObjInfo getCollectObjInfo()
	{
		CollectObjInfo obj = new CollectObjInfo(755123);
		obj.setLastCollectTime(new Timestamp(System.currentTimeMillis()));
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));
		
		return obj;
	}
	
	
}

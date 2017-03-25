package parser.zte.am;

import java.sql.Timestamp;

import framework.ConstDef;

import task.CollectObjInfo;
import task.DevInfo;


/**
 * 
 * @author  liangww
 * @version 1.0
 * @create  2012-7-24 下午03:01:21
 */
public class WV1CorbaSdAmParserTest
{

	public static void main(String[] args) throws Exception
	{
		
		CollectObjInfo collectObjInfo = new CollectObjInfo(111);
		//山东现场测试代码
		DevInfo devInfo = new DevInfo();
		devInfo.setIP("134.33.19.229");
		devInfo.setHostUser("nmsftpuser");
		devInfo.setHostPwd("ZXemsFtp123");
		collectObjInfo.setDevInfo(devInfo);
		collectObjInfo.setDevPort(21111);
		
		collectObjInfo.setCollectPath("/ior/*EPIRPImpl;");
		//30天前的
		collectObjInfo.setLastCollectTime(new Timestamp( System.currentTimeMillis() + 1000 * 60 * 60 * 24 * 30 ));
		
		collectObjInfo.setPeriod(ConstDef.COLLECT_PERIOD_DAY);
		
		WV1CorbaSdAmParser amParser = new WV1CorbaSdAmParser();
		
		
		amParser.setCollectObjInfo(collectObjInfo);
		
		amParser.parseData();
		
		
		
	}
	
	
	
}

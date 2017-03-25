package parser.eric.am;

import java.sql.Timestamp;

import task.CollectObjInfo;
import task.DevInfo;
import framework.ConstDef;

/**
 * 山东爱立信 w网  corba 测试类
 * @author  liangww
 * @version 1.0
 * @create  2012-8-6 上午11:07:38
 */
public class WV1CorbaSdAmParserTest
{
	
	public static void main(String[] args) throws Exception
	{
		CollectObjInfo collectObjInfo = new CollectObjInfo(111);
		//山东现场测试代码
		DevInfo devInfo = new DevInfo();
		devInfo.setIP("134.33.19.11");
		devInfo.setHostUser("nbi");
		devInfo.setHostPwd("nbi");
		collectObjInfo.setDevInfo(devInfo);
		collectObjInfo.setDevPort(21);
		
		collectObjInfo.setCollectPath("/opt/nbi/config/epirp.ior;");
		//30天前的
		collectObjInfo.setLastCollectTime(new Timestamp( System.currentTimeMillis() + 1000 * 60 * 60 * 24 * 30 ));
		
		collectObjInfo.setPeriod(ConstDef.COLLECT_PERIOD_DAY);
		
		WV1CorbaSdAmParser amParser = new WV1CorbaSdAmParser();
		
		
		amParser.setCollectObjInfo(collectObjInfo);
		
		amParser.parseData();
		
		
	}

}

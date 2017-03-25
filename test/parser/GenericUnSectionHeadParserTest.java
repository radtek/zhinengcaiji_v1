package parser;

import parser.lucent.CV2_8ASCIIAlarmTest;
import task.CollectObjInfo;
import templet.GenericSectionHeadD;
import templet.GenericSectionHeadP;
import distributor.GenericSectionHeadDistributor;

public class GenericUnSectionHeadParserTest
{
	public static void main(String[] args) throws Exception
	{
		CollectObjInfo collectObjInfo = CV2_8ASCIIAlarmTest.getCollectObjInfo();

		
		GenericSectionHeadP p = new GenericSectionHeadP();
		p.tmpFileName  = "clt_aaa_stat_parse.xml";	
		p.parseTemp(p.tmpFileName );
		collectObjInfo.setParseTemplet(p);
		
		//只用修改这，测试前需要启动gpfdist
		//目前是执行命令gpfdist -d F:/liang/tmp/gpfdist  -p 8081
		
		
		GenericSectionHeadD distributeTemplet = new GenericSectionHeadD();
		distributeTemplet.tmpType = 6002;
		distributeTemplet.tmpFileName = "clt_aaa_stat_dist.xml";	
		distributeTemplet.parseTemp(distributeTemplet.tmpFileName);
		collectObjInfo.setDistributeTemplet(distributeTemplet);
		
		GenericUnSectionHeadParser  parser = new GenericUnSectionHeadParser(collectObjInfo);
		
		GenericSectionHeadDistributor distributor = new GenericSectionHeadDistributor(collectObjInfo);
		distributor.init(collectObjInfo);
		
		
		String fileName = "F:\\ftp\\aaa\\AAA_10_20120911_0630_4310.TXT";
		parser.setDsConfigName("AAA_*_%%Y%%M%%D_*.TXT");
		parser.setFileName(fileName);
		parser.setDistribute(distributor);
		
		parser.parseData();
		
		
		
	}

}

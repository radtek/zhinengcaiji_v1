package parser;

import java.sql.Timestamp;
import java.util.Date;

import task.CollectObjInfo;
import task.DevInfo;
import templet.GenericSectionHeadD;
import templet.GenericSectionHeadP;
import templet.TempletBase;
import templet.TempletRecord;

public class GenericSectionHeadParserTest
{
	public static void main(String[] args)
	{
		CollectObjInfo obj = new CollectObjInfo(999);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));
		obj.setDevInfo(new DevInfo());

		TempletBase pTemp = new GenericSectionHeadP();
//		pTemp.buildTmp(407003);
		TempletRecord pTempletRecord = new TempletRecord();
		pTempletRecord.setFileName("fl_clt_pm_alt_b11_parse.xml");
		pTempletRecord.setType(6001);
		pTemp.buildTmp(pTempletRecord);
		obj.setParseTemplet(pTemp);

		GenericSectionHeadD dTemp = new GenericSectionHeadD();
//		dTemp.buildTmp(407004);
		TempletRecord dTempletRecord = new TempletRecord();
		dTempletRecord.setFileName("fl_clt_pm_alt_b11_dist.xml");
		dTempletRecord.setType(6001);
		dTemp.buildTmp(dTempletRecord);
		obj.setDistributeTemplet(dTemp);

		GenericSectionHeadParser parser = new GenericSectionHeadParser(obj);
		parser.fileName = "F:\\liang\\tmp\\1015-1019\\新建文件夹\\R11000018.275";
		// 它只是和templet模板中的file属性相等即可
		parser.setDsConfigName("/APME/OBSYNT/*/%%Y%%M%%D/R110000(%%H+1).%%DayOfYear");

		parser.parseData();
		System.out.println("end...");
//		System.out.println((int) '\r');
		
		
		
	}

}

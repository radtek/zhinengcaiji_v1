package parser.lucent;

import java.sql.Timestamp;

import distributor.Distribute;
import distributor.SmartDistributor;

import framework.Factory;

import task.CollectObjInfo;
import task.DevInfo;
import templet.TempletRecord;
import util.Util;

public class SmartPm1xTest
{
	
	public static void main(String[] args) throws Exception
	{
		CollectObjInfo task = new CollectObjInfo(999);
		DevInfo di = new DevInfo();
		di.setOmcID(1);
		task.setDevInfo(di);
		task.setParseTmpID(12070901);
		task.setLastCollectTime(new Timestamp(Util.getDate1("2012-06-18 21:00:00").getTime()));
		
		//设置解析配置
		TempletRecord parserRecord = new TempletRecord();
		parserRecord.setFileName("luc_pm_smt_parse.xml");
		task.setParseTmpRecord(parserRecord);
		
		//设置分发配置
		TempletRecord disRecord = new TempletRecord();
		disRecord.setType(6002);
		disRecord.setFileName("luc_pm_smt_dist.xml");
		task.setDistributeTemplet( Factory.createTemplet(disRecord));
		
		
		SmartPm1x parser = new SmartPm1x();
		parser.setDistribute(new SmartDistributor(task));
		
		parser.setCollectObjInfo(task);
		parser.setFileName("F:\\liang\\tmp\\0702-0705\\新建文件夹\\12061821");
		parser.parseData();
	}
}

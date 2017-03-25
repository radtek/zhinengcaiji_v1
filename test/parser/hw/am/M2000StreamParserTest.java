package parser.hw.am;

import java.io.FileInputStream;
import java.io.InputStream;

import parser.lucent.CV2_8ASCIIAlarmTest;

public class M2000StreamParserTest
{

	public static void main(String[] args) throws Exception
	{
		
		InputStream in = new FileInputStream("F:\\liang\\tmp\\corba山东硬件告警\\山东告警原始数据\\华为3G.txt");
		
		M2000StreamParser parser = new M2000StreamParser();
		
		parser.setCollectObjInfo(CV2_8ASCIIAlarmTest.getCollectObjInfo());
		parser.parse(in, null);
		
		
		
	}
	
	
}

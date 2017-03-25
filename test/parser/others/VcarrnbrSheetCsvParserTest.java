package parser.others;

import parser.lucent.CV2_8ASCIIAlarmTest;

public class VcarrnbrSheetCsvParserTest
{
	public static void main(String[] args) throws Exception
	{
		VcarrnbrSheetCsvParser parser = new VcarrnbrSheetCsvParser();
		
		parser.setCollectObjInfo(CV2_8ASCIIAlarmTest.getCollectObjInfo());
		
		
		String strFileName	= "F:\\liang\\tmp\\1010-1012\\HB_VERGEORDER_20120701_BJ_HB201206010001_2.csv";
		parser.setFileName(strFileName);
		
		parser.parseData();
		
	}

}

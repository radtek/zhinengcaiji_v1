package parser.hw.mml;

import java.sql.Timestamp;
import java.util.Date;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.Util;

/**
 * 此类专用于华为的慢慢来数据，此种数据不需要模板(解析模板和分发模板),直接在数据库中创建表并插入数据
 * 
 * @author ltp Feb 23, 2010
 * @since 1.0
 */
public class WV1ASCII extends Parser {

	// private HwMmlBProcessor processor = null;// 处理类

	public WV1ASCII() {
	}

	public WV1ASCII(CollectObjInfo TaskInfo) {
		super(TaskInfo);
	}

	@Override
	public boolean parseData() {
		MML_Sqlldr processor = new MML_Sqlldr(getCollectObjInfo(), getFileName());
		String parseFileName = getFileName();

		log.info("----- 开始采集华为参数数据------" + parseFileName);
		boolean b = processor.parse();
		if (b) {
			log.info("---------采集华为参数数据成功！-------" + parseFileName);
			return true;
		} else {
			log.error("-------采集华为参数数据失败！---------" + parseFileName);
			return false;
		}

	}

	public static void main(String[] args) throws Exception {
		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(820);
		obj.setDevInfo(dev);
//		Date date = new Date();
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2011-01-20 00:00:00").getTime()));
		WV1ASCII xml = new WV1ASCII();
		xml.collectObjInfo = obj;
		// lucent\\UTRAN-SNAP20100801020001.xml
		// nodeb.xml
		// rnc.xml
		xml.setFileName("C:\\Users\\ChenSijiang\\Desktop\\CFGMML-RNC136-20110406080011.txt");
		xml.parseData();
		// System.out.println("Class3CellReconfParams".toUpperCase());
	}

}

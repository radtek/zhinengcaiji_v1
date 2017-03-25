package parser.webservice.js.cu;

import java.net.URL;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import parser.Parser;
import util.LogMgr;
import util.Util;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 江苏联通告警采集，通过webservice接口。为了“客服支撑系统”而开发的。
 * 
 * @author ChenSijiang 2012-8-1
 */
public class JS_CU_AlarmParser extends Parser {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	/* 调用webservice方法的周期（毫秒）。 */
	private static final int CALL_PERIOD = 20 * 1000;

	private long taskId;

	private String url;

	private GetAlarmsSoap12Stub stub;

	@Override
	public boolean parseData() throws Exception {
		this.taskId = super.getCollectObjInfo().getTaskID();
		this.url = super.getCollectObjInfo().getCollectPath();
		if (this.url != null)
			this.url = this.url.split(";")[0].trim();
		log.debug(taskId + " 江苏联通WebService告警采集开始，URL：" + this.url);
		if (Util.isNull(this.url)) {
			log.error(taskId + " 江苏联通WebService告警采集失败，因为URL为空，请确定任务的collect_path中是否配置了WebService的URL.");
			return false;
		}

		for (;;) {
			boolean bCallOk = false;
			this.initStub();
			if (this.stub != null)
				bCallOk = this.callWebMethod();

			/* web方法调用失败，将stub设为空，下次循环重新连接。 */
			if (!bCallOk)
				this.stub = null;

			try {
				Thread.sleep(CALL_PERIOD);
			} catch (InterruptedException e) {
				log.warn(taskId + " WebService告警采集任务被中断。");
				break;
			}
		}

		return true;
	}

	protected void initStub() {
		if (this.stub != null)
			return;
		try {
			this.stub = new GetAlarmsSoap12Stub(new URL(this.url), null);
		} catch (Exception e) {
			log.error(taskId + " 初始化WebService失败。", e);
			this.stub = null;
		}
	}

	protected boolean callWebMethod() {
		this.callGetNewAlarms();
		this.callGetClearedAlerms();
		return true;
	}

	protected boolean callGetNewAlarms() {
		try {
			String alarmContent = this.stub.getNewAlarms();
		} catch (RemoteException e) {
			log.error(taskId + " getNewAlarms()调用失败。", e);
			return false;
		}
		return true;
	}

	protected boolean callGetClearedAlerms() {
		try {
			String clearContent = this.stub.getClearedAlerms();
		} catch (RemoteException e) {
			log.error(taskId + " getClearedAlerms()调用失败。", e);
			return false;
		}
		return true;
	}

	public static void main(String[] args) throws Exception {
		String content_1 = "[{\"CITY_NAME\":\"盐城市\",\"RNC_NAME\":\"盐城市_RNC5\",\"EVENT_TIME\":\"2012-8-1 17:10:42\",\"CANCEL_TIME\":\"\",\"SITE_ABC\":\"2\",\"SITE_DN\":\"4000:Root=3G,SubNetwork=NANJING,SubNetwork=UTRAN,ManagedElement=BTSEquipment-PWi-jingudasha,NodeBFunction=0\"},{\"CITY_NAME\":\"盐城市\",\"RNC_NAME\":\"盐城市_RNC5\",\"EVENT_TIME\":\"2012-8-1 17:10:42\",\"CANCEL_TIME\":\"\",\"SITE_ABC\":\"2\",\"SITE_DN\":\"4000:Root=3G,SubNetwork=NANJING,SubNetwork=UTRAN,ManagedElement=BTSEquipment-PWi-jingudasha,NodeBFunction=0\"},{\"CITY_NAME\":\"盐城市\",\"RNC_NAME\":\"盐城市_RNC5\",\"EVENT_TIME\":\"2012-8-1 17:10:42\",\"CANCEL_TIME\":\"\",\"SITE_ABC\":\"2\",\"SITE_DN\":\"4000:Root=3G,SubNetwork=NANJING,SubNetwork=UTRAN,ManagedElement=BTSEquipment-PWi-jingudasha,NodeBFunction=0\"},{\"CITY_NAME\":\"盐城市\",\"RNC_NAME\":\"盐城市_RNC5\",\"EVENT_TIME\":\"2012-8-1 17:10:42\",\"CANCEL_TIME\":\"\",\"SITE_ABC\":\"2\",\"SITE_DN\":\"4000:Root=3G,SubNetwork=NANJING,SubNetwork=UTRAN,ManagedElement=BTSEquipment-PWi-jingudasha,NodeBFunction=0\"},{\"CITY_NAME\":\"连云港市\",\"RNC_NAME\":\"连云港市_RNC2\",\"EVENT_TIME\":\"2012-8-1 17:10:24\",\"CANCEL_TIME\":\"\",\"SITE_ABC\":\"2\",\"SITE_DN\":\"4000:Root=3G,SubNetwork=NANJING,SubNetwork=UTRAN,ManagedElement=BTSEquipment-W-xinbaxiaodangcun,NodeBFunction=0\"},{\"CITY_NAME\":\"盐城市\",\"RNC_NAME\":\"盐城市_RNC5\",\"EVENT_TIME\":\"2012-8-1 17:10:42\",\"CANCEL_TIME\":\"\",\"SITE_ABC\":\"2\",\"SITE_DN\":\"4000:Root=3G,SubNetwork=NANJING,SubNetwork=UTRAN,ManagedElement=BTSEquipment-PWi-jingudasha,NodeBFunction=0\"}]";
		String content_2 = "{\"CITY_NAME\":\"盐城市\",\"RNC_NAME\":\"盐城市_RNC5\",\"EVENT_TIME\":\"2012-8-1 17:10:42\",\"CANCEL_TIME\":\"\",\"SITE_ABC\":\"2\",\"SITE_DN\":\"4000:Root=3G,SubNetwork=NANJING,SubNetwork=UTRAN,ManagedElement=BTSEquipment-PWi-jingudasha,NodeBFunction=0\"}";

		ObjectMapper m = new ObjectMapper();
		AlarmBean[] beans = m.readValue(content_1, AlarmBean[].class);
		for (AlarmBean bean : beans) {
			System.out.println(bean);
		}

	}
}

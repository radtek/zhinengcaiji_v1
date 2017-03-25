package parser.lucent;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parser.AbstractStreamParser;
import store.SqlldrStore;
import store.SqlldrStoreParam;
import templet.Table;
import templet.Table.Column;
import util.Util;
import framework.ConstDef;

/**
 * 阿尔卡特-朗讯 告警数据解析。v2.8 见文档CDMA_V2.8_关于阿朗硬件告警采集与处理需求说明_PD205_FV1.2.docx CV1ASCIIAlarm
 * 
 * @author liangww 2012-6-2
 * @version 1.0 1.0.1 liangww 2012-06-14 删除getStrFromLast，getStr方法<br>
 */
public class CV2_8ASCIIAlarmParser extends AbstractStreamParser {

	private StringBuilder buf = new StringBuilder();

	List<Alarm> alarmList = new ArrayList<Alarm>();

	@Override
	public void parse(InputStream in, OutputStream out) throws IOException {
		// TODO Auto-generated method stub
		char[] data = new char[1024];
		Reader reader = new InputStreamReader(in);
		int len = -1;
		int index = -1;
		final String end = "";

		while ((len = reader.read(data)) != -1) {
			buf.append(new String(data, 0, len));
			while (true) {
				index = buf.indexOf(end);
				if (index == -1) {
					break;
				}

				try {
					parse(buf.substring(0, index));
				} catch (Exception e) {
					log.warn("", e);
				} finally {
					buf.delete(0, index + end.length());
				}// finally
			}// while(true)
		}// while((len = reader.read(data)) != -1)
	}

	@Override
	public boolean parseData() throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	public void parse(String record) throws Exception {
		Alarm alarm = new Alarm();

		// 先去掉第一行换行
		if (record.charAt(0) == '\n') {
			record = record.substring(1);
		} else if ("\r\n".equals(record.substring(0, 2))) {
			record = record.substring(2);
		}

		// REPT CELL HEH
		if (record.indexOf("REPT:CELL") != -1 && record.indexOf("HEH") != -1) {
			alarm.LEVEL_C = record.substring(0, 2);
			alarm.MSG_TYPE = "REPT";
			alarm.BTS_ID = Integer.valueOf(Util.getStr(record, "REPT:CELL ", " "));
			alarm.CDMA_ID = Util.getStr(record, "REPT:CELL ", " ", " HEH");
			alarm.CELL_ID = getCellId(alarm.CDMA_ID);// getStr(record, "CBR ", ",");
			alarm.SUPPRESSED_MSG_COUNT = Integer.valueOf(Util.getStr(record, "SUPPRESSED MSGS: ", "\n").trim());
			alarm.ERROR_TYPE = Util.getStr(record, "ERROR TYPE: ", "\n");
			alarm.SUC_OR_FAIL_COUNT = getSucOrFailCount(record);
			String beginStr = (alarm.SUC_OR_FAIL_COUNT != null) ? (alarm.SUC_OR_FAIL_COUNT) : alarm.ERROR_TYPE;

			String statusType = Util.getStr(record, beginStr, "\n", "\n");
			alarm.STATUS_TYPE = Util.getStr(statusType, "", ":");
			if (alarm.STATUS_TYPE != null) {
				alarm.STATUS_TYPE += ":";		//
				alarm.ADD_INFO = statusType.substring(alarm.STATUS_TYPE.length());		//
			} else {
				alarm.STATUS_TYPE = statusType;
			}

			alarm.ENTIRE_MSG = record;
			alarm.START_TIME = Util.getStrFromLast(record, " #", "", "\n");	//
			alarm.SN = Util.getStrFromLast(record, "\n", "", " #");	// //
		}
		// OP:CELL
		else if (record.indexOf("OP:CELL") != -1 && (record.indexOf(" OOS,") != -1 || record.indexOf(" ACTIVE") != -1)) {
			alarm.LEVEL_C = record.substring(0, 2);
			alarm.MSG_TYPE = "OP";
			alarm.BTS_ID = Integer.valueOf(Util.getStr(record, "OP:CELL ", " "));
			alarm.CDMA_ID = getOpCmda(record, alarm.BTS_ID + " ");
			//
			alarm.CELL_ID = getCellId(alarm.CDMA_ID);// getStr(record, "RRH ", ",");
			alarm.STATUS_TYPE = record.indexOf(" OOS") == -1 ? "OOS" : "ACTIVE";

			alarm.ENTIRE_MSG = record;

			alarm.START_TIME = Util.getStrFromLast(record, " #", "", "\n");	//
			alarm.SN = Util.getStrFromLast(record, "\n", "", " #");	// //

		}

		if (alarm.isVaild()) {
			alarmList.add(alarm);
			// 一天大概158282的量，一次1k个差不多了
			if (alarmList.size() >= 1000) {
				sqlldr();
			}
		}
	}

	public void sqlldr() throws Exception {
		if (alarmList.size() == 0) {
			return;
		}

		Timestamp timestamp = new Timestamp(System.currentTimeMillis());

		Table tableD = new Table();
		tableD.setName("CLT_AM_ALARM_LUC");
		Map<Integer, Column> columns = new HashMap<Integer, Table.Column>();
		columns.put(1, new Column("START_TIME", 1, ConstDef.COLLECT_FIELD_DATATYPE_DATATIME, "mm/dd/yy hh24:mi:ss"));
		columns.put(2, new Column("SN", 2, 2, "4000"));
		columns.put(3, new Column("LEVEL_C", 3, 2, "4000"));
		columns.put(4, new Column("MSG_TYPE", 4, 2, "4000"));
		columns.put(5, new Column("BTS_ID", 5, 1, ""));		// int
		columns.put(6, new Column("CELL_ID", 6, 1, ""));
		columns.put(7, new Column("CDMA_ID", 7, 2, "4000"));
		columns.put(8, new Column("SUPPRESSED_MSG_COUNT", 8, 1, ""));
		columns.put(9, new Column("ERROR_TYPE", 9, 2, "4000"));
		columns.put(10, new Column("SUC_OR_FAIL_COUNT", 10, 2, "4000"));		// int
		columns.put(11, new Column("STATUS_TYPE", 11, 2, "4000"));
		columns.put(12, new Column("ADD_INFO", 12, 2, "4000"));
		columns.put(13, new Column("ENTIRE_MSG ", 13, ConstDef.COLLECT_FIELD_SPECIAL_FORMAT, "CHAR(4000) \"REPLACE(:ENTIRE_MSG,'\\\\n',CHR(10))\""));

		tableD.setColumns(columns);
		SqlldrStore sqlldrStore = null;
		try {
			sqlldrStore = new SqlldrStore(new SqlldrStoreParam(1, tableD));
			sqlldrStore.setCollectInfo(collectObjInfo);
			sqlldrStore.setTaskID(this.collectObjInfo.getTaskID());
			sqlldrStore.setDataTime(timestamp);
			sqlldrStore.setOmcID(this.collectObjInfo.getDevInfo().getOmcID());
			sqlldrStore.open();

			StringBuilder buf = new StringBuilder();
			for (int i = 0; i < alarmList.size(); i++) {
				Alarm alarm = alarmList.get(i);
				buf.append(alarm.START_TIME).append(tableD.getSplitSign());
				buf.append(alarm.SN).append(tableD.getSplitSign());
				buf.append(alarm.LEVEL_C).append(tableD.getSplitSign());
				buf.append(alarm.MSG_TYPE).append(tableD.getSplitSign());
				buf.append(alarm.BTS_ID).append(tableD.getSplitSign());
				buf.append(alarm.CELL_ID).append(tableD.getSplitSign());
				buf.append(alarm.CDMA_ID).append(tableD.getSplitSign());
				buf.append(alarm.SUPPRESSED_MSG_COUNT).append(tableD.getSplitSign());
				buf.append(alarm.ERROR_TYPE).append(tableD.getSplitSign());
				buf.append(alarm.SUC_OR_FAIL_COUNT).append(tableD.getSplitSign());
				buf.append(alarm.STATUS_TYPE).append(tableD.getSplitSign());
				buf.append(alarm.ADD_INFO).append(tableD.getSplitSign());
				buf.append(alarm.ENTIRE_MSG.replace("\n", "\\n")).append(tableD.getSplitSign());

				sqlldrStore.write(buf.toString());
				buf.setLength(0);
			}
			sqlldrStore.flush();
			sqlldrStore.commit();

		} finally {
			sqlldrStore.close();
			sqlldrStore = null;
			alarmList.clear();
		}
	}

	static String getSucOrFailCount(String record) {
		String[] array = {"SUCCESS COUNT = ", "FAIL COUNT = "};
		String result = null;

		for (int i = 0; i < array.length; i++) {
			result = Util.getStr(record, array[i], "\n");
			if (result != null) {
				return array[i] + result;
			}
		}

		return null;
	}

	static String getCellId(String cmdaId) {
		String[] array = {"CBR ", "RRH "};
		String result = null;

		for (int i = 0; i < array.length; i++) {
			result = Util.getStr(cmdaId, array[i], ",");
			if (result != null) {
				return result;
			}
		}

		return null;
	}

	static String getOpCmda(String record, String beginStr) {
		String[] array = {" OOS", " ACTIVE"};
		String result = null;
		record = Util.getStr(record, "", "\n");

		for (int i = 0; i < array.length; i++) {
			result = Util.getStr(record, beginStr, array[i]);
			if (result != null) {
				return result;
			}
		}

		return null;
	}

	public static class Alarm {

		String START_TIME;

		String SN;

		String LEVEL_C;

		String MSG_TYPE;

		int BTS_ID;

		String CDMA_ID;

		String CELL_ID;

		int SUPPRESSED_MSG_COUNT;

		String ERROR_TYPE;

		String SUC_OR_FAIL_COUNT;

		String STATUS_TYPE;

		String ADD_INFO;

		String ENTIRE_MSG;

		static String format(String str) {
			// 去掉之前的空格
			return str != null ? str.trim() : "";
		}

		/**
		 * 格式化
		 * 
		 * @return
		 */
		public boolean format() {
			// 去掉之前的空格
			START_TIME = format(START_TIME);
			SN = format(SN);
			MSG_TYPE = format(MSG_TYPE);
			CDMA_ID = format(CDMA_ID);
			CELL_ID = format(CELL_ID);
			ERROR_TYPE = format(ERROR_TYPE);
			SUC_OR_FAIL_COUNT = format(SUC_OR_FAIL_COUNT);
			STATUS_TYPE = format(STATUS_TYPE);
			ADD_INFO = format(ADD_INFO);
			ENTIRE_MSG = format(ENTIRE_MSG);

			return true;
		}

		/**
		 * 是否有效
		 * 
		 * @return
		 */
		public boolean isVaild() {
			if (LEVEL_C == null || SN == null) {
				return false;
			}

			format();

			// op type 不是info级别的
			if (MSG_TYPE.equals("OP") && !"  ".equals(LEVEL_C)) {
				return false;
			}

			return true;
		}
	}

	@Override
	public void dispose() {
		// TODO Auto-generated method stub
		try {
			sqlldr();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.debug(collectObjInfo.getTaskID() + "-sqlldr出异常", e);
		}
	}

}

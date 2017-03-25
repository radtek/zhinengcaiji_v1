package parser.corba.nms;

import java.io.UnsupportedEncodingException;
import java.sql.Date;

import org.apache.log4j.Logger;
import org.omg.CORBA.BooleanHolder;
import org.omg.CORBA.IntHolder;
import org.omg.CosNotification.EventBatchHolder;
import org.omg.CosNotification.Property;
import org.omg.CosNotification.StructuredEvent;

import parser.Parser;
import parser.corba.nms.util.CorbaCollectUtil;
import util.LogMgr;
import util.Util;
import AlarmIRPConstDefs.DNOpt;
import AlarmIRPSystem.AlarmIRP;
import AlarmIRPSystem.AlarmInformationIterator;
import AlarmIRPSystem.AlarmInformationIteratorHolder;
import EPIRPSystem.EPIRP;
import ManagedGenericIRPConstDefs.StringOpt;
import NotificationIRPNotifications.NonFilterableEventBody;

/**
 * 解析网管corba接口告警数据。
 * 
 * @author ChenSijiang 2012-6-8
 */
public class NMSCorbaParser extends Parser {

	protected static final Logger log = LogMgr.getInstance().getSystemLogger();

	/* 使用AlarmInformationIterator获取告警时，每次取多少条。 */
	private static final short ALARM_COUNT_OF_PER_ITER = 100;

	/* 上级网管标识。 */
	private static final String MANAGER_IDENTIFIER = "uway_nms_corba_collector";

	protected String logKey;

	@Override
	public boolean parseData() throws Exception {
		logKey = "[" + collectObjInfo.getTaskID() + "][" + Util.getDateString(collectObjInfo.getLastCollectTime()) + "]";

		long periodTime = (collectObjInfo.getPeriodTime());
		Date stamptime = new Date(collectObjInfo.getLastCollectTime().getTime());
		Date nextStamptime = new Date(collectObjInfo.getLastCollectTime().getTime() + periodTime);

		EPIRP epirp = null;
		AlarmIRP alarmIRP = null;
		String epirpIOR = null;
		try {
			epirpIOR = new IorFinder().find(collectObjInfo);
		} catch (IorNotFoundException e) {
			log.error(logKey + "ior获取失败。", e);
			return false;
		}
		log.debug(logKey + "EPIRP对象的ior获取成功 - " + epirpIOR);
		try {
			epirp = CorbaCollectUtil.creatEPIRP(epirpIOR);
		} catch (Exception e) {
			log.error(logKey + "EPIRP对象获取失败。", e);
			return false;
		}
		try {
			alarmIRP = CorbaCollectUtil.findAlarmIRP(epirp, MANAGER_IDENTIFIER);
		} catch (Exception e) {
			log.error(logKey + "AlarmIRp对象获取失败。", e);
			return false;
		}

		log.debug(logKey + "AlarmIRP获取成功 - " + alarmIRP);

		StringOpt filter = new StringOpt();
		String condition = String.format("($b.time >= %s and $b.time < %s)", CorbaCollectUtil.dateToUtct(stamptime).time,
				CorbaCollectUtil.dateToUtct(nextStamptime).time);
		filter.value(condition);
		log.debug(logKey + "get_alarm_list过滤条件： " + condition);

		AlarmInformationIterator alarmInformationIterator = null;;
		try {
			/* 获取告警条数。 */
			IntHolder criticalCount = new IntHolder();
			IntHolder majorCount = new IntHolder();
			IntHolder minorCount = new IntHolder();
			IntHolder warningCount = new IntHolder();
			IntHolder indeterminateCount = new IntHolder();
			IntHolder clearedCount = new IntHolder();
			alarmIRP.get_alarm_count(filter, criticalCount, majorCount, minorCount, warningCount, indeterminateCount, clearedCount);
			int totalAlarmCount = criticalCount.value + majorCount.value + minorCount.value + warningCount.value + indeterminateCount.value
					+ clearedCount.value;
			log.debug(logKey + "通过get_alarm_count获取到告警总条数：" + totalAlarmCount + "，其中，critical：" + criticalCount.value + "，major：" + majorCount.value
					+ "，minor：" + minorCount.value + "，warning：" + warningCount.value + "，indeterminate：" + indeterminateCount.value + "，cleared："
					+ clearedCount.value);

			DNOpt baseObject = new DNOpt();
			BooleanHolder bListFlag = new BooleanHolder();
			AlarmInformationIteratorHolder alarmItHolder = new AlarmInformationIteratorHolder();
			StructuredEvent[] events = alarmIRP.get_alarm_list(filter, baseObject, bListFlag, alarmItHolder);
			log.debug(logKey + "get_alarm_list调用成功，bListFlag=" + bListFlag.value);
			// BooleanHolder被放置的值为true时，表示告警数据放在返回值里，
			// 否则放在AlarmInformationIteratorHolder里。
			if (bListFlag.value) {
				log.debug(logKey + "通过get_alarm_list返回列表，接收到了" + events.length + "条告警。");
				for (StructuredEvent event : events) {
					handleOneAlarm(event);
				}
			} else {
				EventBatchHolder alarmInfos = new EventBatchHolder();
				alarmInformationIterator = alarmItHolder.value;
				log.debug(logKey + "获取到的AlarmInformationIterator为：" + alarmInformationIterator);
				/* 统计已收到的告警条数。 */
				int receivedCount = 0;
				while (receivedCount < totalAlarmCount) {
					alarmInformationIterator.next_alarm_informations(ALARM_COUNT_OF_PER_ITER, alarmInfos);
					log.debug(logKey + "本次通过AlarmInformationIterator.next_alarm_informations接收到了" + alarmInfos.value.length + "条告警，共有"
							+ totalAlarmCount + "条需要接收。");
					for (StructuredEvent event : alarmInfos.value) {
						receivedCount++;
						handleOneAlarm(event);
					}
					log.debug(logKey + "通过AlarmInformationIterator.next_alarm_informations实际接收到了" + receivedCount + "条告警，应接收" + totalAlarmCount
							+ "条告警。");
				}
			}
		} catch (Exception e) {
			log.error(logKey + "获取告警出错（" + CorbaCollectUtil.extraReasonFromCorbaException(e) + "）。", e);
		} finally {
			log.debug(logKey + "告警处理完成。");
			/* 必须调用destroy()方法，否则下次再获取它，会报错。 */
			if (alarmInformationIterator != null)
				alarmInformationIterator.destroy();
			log.debug(logKey + "已销毁AlarmInformationIterator对象。");
			/* 释放AlarmIRP对象。 */
			CorbaCollectUtil.releaseIRP(epirp, MANAGER_IDENTIFIER, alarmIRP);
			log.debug(logKey + "已释放AlarmIRP对象。");

			// liangww add 2012-08-03
			dispose();
		}
		return true;
	}

	/**
	 * 在这里处理一条告警数据。
	 */
	protected void handleOneAlarm(StructuredEvent alarm) throws Exception {
		String encode = collectObjInfo.getDevInfo().getEncode();
		if (Util.isNull(encode))
			encode = "gbk";
		log.debug("");
		log.debug("*********************one alarm information*****************************");
		log.debug("==============fixed_header==============");
		log.debug("event_name=" + alarm.header.fixed_header.event_name);
		log.debug("domain_name=" + alarm.header.fixed_header.event_type.domain_name);
		log.debug("type_name=" + alarm.header.fixed_header.event_type.type_name);
		for (Property data : alarm.header.variable_header) {
			Object val = CorbaCollectUtil.parseAnyVal(data.value);
			log.debug("[head]name=" + data.name + "            value=" + val);
		}
		log.debug("==============remainder_of_body==============");
		NonFilterableEventBody body = CorbaCollectUtil.parseNonFilterableEventBody(alarm.remainder_of_body);
		log.debug("filterable_body=" + CorbaCollectUtil.parseAnyVal(body.remainder_of_non_filterable_body));
		for (Property data : body.name_value_pairs) {
			Object val = CorbaCollectUtil.parseAnyVal(data.value);
			log.debug("[name_value_pairs]name=" + data.name + "            value=" + new String(val.toString().getBytes("iso_8859-1"), encode));
		}

		log.debug("==============structured events==============");
		Property[] datas = alarm.filterable_data;
		for (Property data : datas) {
			Object val = CorbaCollectUtil.parseAnyVal(data.value);
			log.debug("name=" + data.name + "            value=" + val);

		}
	}

	public static String tranform(String str, String encode) throws Exception {
		if (str == null) {
			return null;
		}

		if (encode == null) {
			encode = "GBK";
		}

		return new String(str.toString().getBytes("iso_8859-1"), encode);
	}

	/**
	 * 销毁
	 */
	public void dispose() {

	}
}

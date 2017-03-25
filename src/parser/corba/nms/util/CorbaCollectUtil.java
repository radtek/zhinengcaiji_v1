package parser.corba.nms.util;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.omg.CORBA.Any;
import org.omg.CORBA.StringHolder;
import org.omg.CORBA.TCKind;
import org.omg.CosNotification.Property;
import org.omg.TimeBase.UtcT;
import org.omg.TimeBase.UtcTHelper;

import util.LogMgr;
import util.Util;
import AlarmIRPConstDefs.AttributeChangeSetHelper;
import AlarmIRPConstDefs.AttributeSetHelper;
import AlarmIRPConstDefs.AttributeValue;
import AlarmIRPConstDefs.AttributeValueChange;
import AlarmIRPConstDefs.Comment;
import AlarmIRPConstDefs.CommentSetHelper;
import AlarmIRPConstDefs.CorrelatedNotification;
import AlarmIRPConstDefs.CorrelatedNotificationSetHelper;
import AlarmIRPConstDefs.TrendIndication;
import AlarmIRPConstDefs.TrendIndicationHelper;
import AlarmIRPSystem.AlarmIRP;
import AlarmIRPSystem.AlarmIRPHelper;
import EPIRPConstDefs.IRPElement;
import EPIRPConstDefs.SupportedIRP;
import EPIRPConstDefs.SupportedIRPListHolder;
import EPIRPSystem.EPIRP;
import EPIRPSystem.EPIRPHelper;
import NotificationIRPNotifications.NonFilterableEventBody;
import NotificationIRPNotifications.NonFilterableEventBodyHelper;
import NotificationIRPSystem.NotificationIRP;
import NotificationIRPSystem.NotificationIRPHelper;

/**
 * 网管corba接口工具类。
 * 
 * @author ChenSijiang 2012-6-8
 */
public class CorbaCollectUtil {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	private static final long UNIX_TO_UTC_OFFSET_SECS = 12219292800L;

	/** 告警信息中的value都是org.omg.CORBA.Any类型，这里把它根据实际类型转成String. */
	public static String parseAnyVal(Any any) {
		int kindVal = any.type().kind().value();
		Object val = null;
		switch (kindVal) {
			case TCKind._tk_string :
				val = any.extract_string();
				break;
			case TCKind._tk_float :
				val = any.extract_float();
				break;
			case TCKind._tk_short :
				val = any.extract_short();
				break;
			case TCKind._tk_long :
				val = any.extract_long();
				break;
			case TCKind._tk_struct :
				/* 实际发现数据中的struct类型，都是UtcT这个结构。 */
				UtcT utct = UtcTHelper.extract(any);
				val = "inacchi=" + utct.inacchi + ", inacclo=" + utct.inacclo + ", tdf=" + utct.tdf + ", time=" + utct.time + "("
						+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(utctToDate(utct)) + ")";
				break;
			case TCKind._tk_enum :
				TrendIndication ti = TrendIndicationHelper.extract(any);
				val = ti.value();
				break;
			case TCKind._tk_boolean :
				val = any.extract_boolean();
				break;
			case TCKind._tk_alias :
				String id = "";
				try {
					id = any.type().id().toLowerCase();
				} catch (Exception e) {
				}
				StringBuilder sb = new StringBuilder();
				sb.append("[");
				AttributeValueChange[] chSet = null;
				Comment[] comments = null;
				CorrelatedNotification[] cn = null;
				AttributeValue[] av = null;
				if (id.contains("attributechangeset"))
					chSet = AttributeChangeSetHelper.extract(any);
				else if (id.contains("commentset"))
					comments = CommentSetHelper.extract(any);
				else if (id.contains("correlatednotificationset"))
					cn = CorrelatedNotificationSetHelper.extract(any);
				else if (id.contains("attributeset"))
					av = AttributeSetHelper.extract(any);
				else
					sb.append("~" + id + "~");
				if (chSet != null) {
					for (int i = 0; i < chSet.length; i++) {
						AttributeValueChange v = chSet[i];
						sb.append("(name=").append(v.attribute_name).append(", old_value=").append(v.old_value).append(", newValue=")
								.append(v.new_value).append(")");
						if (i < chSet.length - 1)
							sb.append(", ");
					}
				} else if (comments != null) {
					for (int i = 0; i < comments.length; i++) {
						Comment v = comments[i];
						sb.append("(system_id=").append(v.system_id).append(", comment_text=").append(v.comment_text).append(", user_id=")
								.append(v.user_id).append(", comment_time=").append(utctToDate(v.comment_time)).append(")");
						if (i < comments.length - 1)
							sb.append(", ");
					}
				} else if (cn != null) {
					for (int i = 0; i < cn.length; i++) {
						CorrelatedNotification v = cn[i];
						sb.append("(source=").append(v.source).append(", notif_id_set=").append(printIntArray(v.notif_id_set)).append(")");
						if (i < cn.length - 1)
							sb.append(", ");
					}
				} else if (av != null) {
					for (int i = 0; i < av.length; i++) {
						AttributeValue v = av[i];
						sb.append("(attribute_name=").append(v.attribute_name).append(", value=").append(parseAnyVal(v.value)).append(")");
						if (i < av.length - 1)
							sb.append(", ");
					}
				}
				sb.append("]");
				val = sb.toString();
				break;
			case TCKind._tk_null :
				val = "null";
				break;
			default :
				val = "??????";
				break;
		}
		return val.toString();
	}

	/** remainder_of_body中的struct类型，是NonFilterableEventBody结构。 */
	public static NonFilterableEventBody parseNonFilterableEventBody(Any any) {
		int kindVal = any.type().kind().value();
		switch (kindVal) {
			case TCKind._tk_struct :
				NonFilterableEventBody body = NonFilterableEventBodyHelper.extract(any);
				return body;
			default :
				return null;
		}
	}

	/** utct结构转成date的方法。 */
	public static Date utctToDate(UtcT utct) {
		long sin = utct.time - (UNIX_TO_UTC_OFFSET_SECS * 10000000);
		long mills = sin / 10000;
		return new Date(mills);
	}

	/** date结构转成utct的方法。 */
	public static UtcT dateToUtct(Date date) {
		long mills = date.getTime();
		long sin = mills * 10000;
		return new UtcT(sin + (UNIX_TO_UTC_OFFSET_SECS * 10000000), 0, (short) 0, (short) 0);
	}

	/**
	 * 根据EPIRP对象的IOR字符串，创建EPIRP对象。注意要处理运行时异常。
	 */
	public static EPIRP creatEPIRP(String epirpIOR) {
		org.omg.CORBA.Object objRef = makeORB().string_to_object(epirpIOR);
		EPIRP epIRP = EPIRPHelper.narrow(objRef);
		return epIRP;
	}

	/**
	 * 根据AlarmIRP对象的IOR字符串，创建AlarmIRP对象。注意要处理运行时异常。
	 */
	public static AlarmIRP createAlarmIRP(String alarmIrpIOR) {
		org.omg.CORBA.Object objRef = makeORB().string_to_object(alarmIrpIOR);
		AlarmIRP alarmIRP = AlarmIRPHelper.narrow(objRef);
		return alarmIRP;
	}

	/**
	 * 尝试从EPIRP对象中找出NotifcationIRP对象。
	 * 
	 * @param epirp
	 *            EPIRP对象。
	 * @param managerIdentifier
	 *            调用者标识，可随便取一个，但不能为空。
	 * @return NotificationIRP对象。
	 * @throws Exception
	 *             获取失败。
	 */
	public static NotificationIRP findNotifcationIRP(final EPIRP epirp, final String managerIdentifier) throws Exception {
		if (epirp == null)
			throw new Exception("EPIRP对象为null.");
		if (managerIdentifier == null || managerIdentifier.trim().length() == 0)
			throw new Exception("managerIdentifier为空：" + managerIdentifier);

		NotificationIRP notifyIRP = null;

		/* 以下是获取NotifcationIRP的必要参数。 */
		String systemDN = null;
		String managerIdentifierStr = managerIdentifier;
		String irpId = null;
		String notifyIOR = null;
		String version = "";

		try {
			/* 获取所有支持的IRP对象描述。 */
			SupportedIRPListHolder supportedIRPListHolder = new SupportedIRPListHolder();
			epirp.get_irp_outline(version, supportedIRPListHolder);
			SupportedIRP[] allSupportedIRP = supportedIRPListHolder.value;
			if (allSupportedIRP == null || allSupportedIRP.length == 0)
				throw new Exception("未能获取到IRP对象信息列表：" + allSupportedIRP);

			/* 遍历IRP对象信息列表，找出用于通知的IRP对象信息。 */
			for (SupportedIRP irpInfo : allSupportedIRP) {
				/* SupportedIRP对象中的成员，有一个为空，就不可用。 */
				if (irpInfo == null || irpInfo.system_dn == null || irpInfo.irp_list == null || irpInfo.irp_list.length == 0)
					continue;
				/* 每个SupportedIRP下，又有多个IRPElement，找出NotifcationIRP对象信息是哪个。 */
				for (IRPElement irpElement : irpInfo.irp_list) {
					/* irp_id是获取NotifcationIRP时必须的参数，如果为空，就不可用。 */
					if (irpElement == null || irpElement.irp_id == null)
						continue;
					if (irpElement.irp_versions != null && irpElement.irp_versions.length > 0 && irpElement.irp_versions[0] != null
							&& irpElement.irp_versions[0].toLowerCase().contains("notification")) {
						irpId = irpElement.irp_id;
						systemDN = irpInfo.system_dn;
						break;
					}
				}
			}
			if (systemDN == null || irpId == null)
				throw new Exception("未能找到SystemDN和IrpId，SystemDN=" + systemDN + "，IrpId=" + irpId);

			/* 获取NotifcationIRP的ior字符串。 */
			StringHolder notifyIorHolder = new StringHolder();
			epirp.get_irp_reference(managerIdentifierStr, systemDN, irpId, notifyIorHolder);
			notifyIOR = notifyIorHolder.value;
			if (notifyIOR == null || notifyIOR.trim().length() == 0)
				throw new Exception("NotifcationIRP的ior字符串为空：" + notifyIOR);

			/* 获取到NotificationIRP对象。 */
			notifyIRP = NotificationIRPHelper.narrow(makeORB().string_to_object(notifyIOR));

			return notifyIRP;
		} catch (Exception e) {
			String detailReson = extraReasonFromCorbaException(e);
			if (detailReson == null)
				throw e;
			throw new Exception(detailReson, e);
		}
	}

	/**
	 * 尝试从EPIRP对象中找出AlarmIRP对象。
	 * 
	 * @param epirp
	 *            EPIRP对象。
	 * @param managerIdentifier
	 *            调用者标识，可随便取一个，但不能为空。
	 * @return AlarmIRP对象。
	 * @throws Exception
	 *             获取失败。
	 */
	public static AlarmIRP findAlarmIRP(final EPIRP epirp, final String managerIdentifier) throws Exception {
		if (epirp == null)
			throw new Exception("EPIRP对象为null.");
		if (managerIdentifier == null || managerIdentifier.trim().length() == 0)
			throw new Exception("managerIdentifier为空：" + managerIdentifier);

		AlarmIRP alarmIRP = null;

		/* 以下是获取AlarmIRP的必要参数。 */
		String systemDN = null;
		String managerIdentifierStr = managerIdentifier;
		String irpId = null;
		String alarmIOR = null;
		String version = "";

		try {
			/* 获取所有支持的IRP对象描述。 */
			SupportedIRPListHolder supportedIRPListHolder = new SupportedIRPListHolder();
			epirp.get_irp_outline(version, supportedIRPListHolder);
			SupportedIRP[] allSupportedIRP = supportedIRPListHolder.value;
			if (allSupportedIRP == null || allSupportedIRP.length == 0)
				throw new Exception("未能获取到IRP对象信息列表：" + allSupportedIRP);

			/* 遍历IRP对象信息列表，找出用于告警的IRP对象信息。 */
			for (SupportedIRP irpInfo : allSupportedIRP) {
				/* SupportedIRP对象中的成员，有一个为空，就不可用。 */
				if (irpInfo == null || irpInfo.system_dn == null || irpInfo.irp_list == null || irpInfo.irp_list.length == 0)
					continue;
				/* 每个SupportedIRP下，又有多个IRPElement，找出告警对象信息是哪个。 */
				for (IRPElement irpElement : irpInfo.irp_list) {
					/* irp_id是获取AlarmIRP时必须的参数，如果为空，就不可用。 */
					if (irpElement == null || irpElement.irp_id == null)
						continue;
					if (irpElement.irp_versions != null && irpElement.irp_versions.length > 0 && irpElement.irp_versions[0] != null
							&& irpElement.irp_versions[0].toLowerCase().contains("alarm")) {
						irpId = irpElement.irp_id;
						systemDN = irpInfo.system_dn;
						break;
					}
				}
			}
			if (systemDN == null || irpId == null)
				throw new Exception("未能找到SystemDN和IrpId，SystemDN=" + systemDN + "，IrpId=" + irpId);

			/* 获取AlarmIRP的ior字符串。 */
			StringHolder alarmIorHolder = new StringHolder();
			epirp.get_irp_reference(managerIdentifierStr, systemDN, irpId, alarmIorHolder);
			alarmIOR = alarmIorHolder.value;
			if (alarmIOR == null || alarmIOR.trim().length() == 0)
				throw new Exception("AlarmIRP的ior字符串为空：" + alarmIOR);

			log.debug("本次查找到的AlarmIRP字符串为：" + alarmIOR + "，EPIRP为：" + epirp);

			/* 获取到AlarmIRP对象。 */
			alarmIRP = AlarmIRPHelper.narrow(makeORB().string_to_object(alarmIOR));

			return alarmIRP;
		} catch (Exception e) {
			String detailReson = extraReasonFromCorbaException(e);
			if (detailReson == null)
				throw e;
			throw new Exception(detailReson, e);
		}
	}

	/** 释放一个从EPIRP获取到的其它IRP对象，比如AlarmIRP. */
	public static void releaseIRP(EPIRP epirp, String managerIdentifier, org.omg.CORBA.Object obj) {
		try {
			if (epirp != null && obj != null)
				epirp.release_irp_reference(managerIdentifier, CorbaCollectUtil.makeORB().object_to_string(obj));
		} catch (Exception e) {
			// 异常不处理，即使处理也没有意义。
		}
	}

	/**
	 * 提取错误原因，比如GetAlarmList异常有个reason字段，InvalidParameter有个parameter字段， 但它们是不会在异常堆栈出现的，这里把它提取出来。
	 */
	public static String extraReasonFromCorbaException(Exception ex) {
		if (ex == null)
			return null;
		try {
			Field field = ex.getClass().getDeclaredField("reason");
			return "[Reason=" + field.get(ex).toString() + "]";
		} catch (Exception e) {
		}
		try {
			Field field = ex.getClass().getDeclaredField("parameter");
			return "[Error Parameter=" + field.get(ex).toString() + "]";
		} catch (Exception e) {
		}
		return null;
	}

	/* 创建ORB，指定使用jacorb的实现。 */
	private static org.omg.CORBA.ORB makeORB() {
		Properties prop = new Properties();
		prop.put("org.omg.CORBA.ORBClass", "org.jacorb.orb.ORB");
		return org.jacorb.orb.ORB.init((String[]) null, prop);
	}

	private static String printIntArray(int[] intArray) {
		if (intArray == null)
			return "";
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < intArray.length; i++) {
			sb.append(intArray[i]);
			if (i < intArray.length - 1)
				sb.append(", ");
		}
		return sb.toString();
	}

	/**
	 * 从properties中查找与key相等等property,并返回其value的string
	 * 
	 * @param properties
	 * @param key
	 * @return
	 */
	public static String findValue(Property[] properties, String key) {
		for (int i = 0; i < properties.length; i++) {
			Property property = properties[i];
			if (key.equals(property.name)) {
				return parseAnyVal(property.value);
			}
		}

		return null;
	}

	public static void main(String[] args) throws Exception {
		Date date = Util.getDate1("2012-06-14 03:00:00");
		System.out.println(dateToUtct(date).time);
		date = Util.getDate1("2012-06-19 03:00:00");
		System.out.println(dateToUtct(date).time);
	}
}

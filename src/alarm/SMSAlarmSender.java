package alarm;

/**
 * 以短信方式发送告警
 * 
 * @author YangJian
 * @since 3.0
 */
public class SMSAlarmSender implements AlarmSender {

	public SMSAlarmSender() {
		super();
	}

	@Override
	public byte send(Alarm alarm) {
		return 0;
	}

}

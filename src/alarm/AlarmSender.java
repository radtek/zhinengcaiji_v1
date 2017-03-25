package alarm;

/**
 * 告警发送接口
 * 
 * @author YangJian
 * @since 3.0
 */
public interface AlarmSender {

	/**
	 * 发送告警
	 * 
	 * @param alarm
	 * @return 0为成功，-1为失败
	 */
	public byte send(Alarm alarm);
}

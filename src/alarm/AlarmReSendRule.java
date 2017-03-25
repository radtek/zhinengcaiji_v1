package alarm;

/**
 * 告警重发规则(当告警发送失败时需要进行重发)
 * 
 * @author ltp Apr 20, 2010
 * @since 3.0
 */
public class AlarmReSendRule {

	private int maxReSendTimes; // 最大重发次数（当发送失败时）

	private int timeout; // 单位为分钟

	public AlarmReSendRule() {
		super();
	}

	public AlarmReSendRule(int maxReSendTimes, int timeout) {
		super();
		this.maxReSendTimes = maxReSendTimes;
		this.timeout = timeout;
	}

	public int getMaxReSendTimes() {
		return maxReSendTimes;
	}

	public void setMaxReSendTimes(int maxReSendTimes) {
		this.maxReSendTimes = maxReSendTimes;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

}

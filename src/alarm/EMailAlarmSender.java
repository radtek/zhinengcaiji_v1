package alarm;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;

import org.apache.log4j.Logger;

import util.Email;
import util.LogMgr;
import framework.SystemConfig;

/**
 * 以邮件方式发送告警
 * 
 * @author YangJian
 * @since 3.0
 */
public class EMailAlarmSender implements AlarmSender {

	private Logger log = LogMgr.getInstance().getSystemLogger();

	private String content = "告警描述：\t\t任务号:%s\t\t错误源:%s\t\t错误描述:%s\t\t发生时间:%s\t\t告警级别:%s\t\t错误码:%s";

	public EMailAlarmSender() {
		super();
	}

	@Override
	public byte send(Alarm alarm) {
		byte result = -1;
		if (alarm != null) {
			Email mail = new Email();
			SystemConfig config = SystemConfig.getInstance();
			if (config == null) {
				return result;
			}
			try {
				// 收件箱地址
				String[] to = config.getMailTO();
				// 收件箱地址
				String[] cc = config.getMailCC();
				// 收件箱地址
				String[] bcc = config.getMailBCC();
				// SMTP 服务器
				String host = config.getMailSMTPHost();
				// 发件箱地址
				String account = config.getMailAccount();
				// 发件箱密码
				String password = config.getMailPassword();
				// 发送主题
				String subjcet = alarm.getTitle();
				// 发关内容
				String newCnt = String.format(content, alarm.getTaskID(), alarm.getSource(), alarm.getDescription(), alarm.getOccuredTime(),
						alarm.getAlarmLevel(), alarm.getErrorCode());
				//
				mail.setAddress(to, Email.TO);
				mail.setAddress(cc, Email.CC);
				mail.setAddress(bcc, Email.BCC);
				mail.setSMTPHost(host, account, password);
				mail.setFromAddress(account);
				mail.setSubject(subjcet);
				mail.setHtmlBody(newCnt);
				mail.sendBatch();
				result = 0;
			} catch (AddressException e) {
				log.error("邮件地址异常！", e);
				result = -1;
			} catch (MessagingException e) {
				log.error("邮件异常！", e);
				result = -1;
			}
		}

		return result;
	}

	public static void main(String[] args) {
		EMailAlarmSender e = new EMailAlarmSender();
		e.send(new Alarm());
	}

}

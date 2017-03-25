package console;

import java.util.Date;
import java.util.Properties;

import util.Util;
import cn.uway.console.command.CommandAction;
import cn.uway.console.io.CommandIO;
import db.dao.UserDAO;
import db.pojo.User;

public class LoginAction implements CommandAction {

	@Override
	public boolean handleCommand(String[] args, CommandIO io) throws Exception {
		io.setPrefix("  >");
		io.setLeftPadding("   ");
		boolean loginFlag = false;
		int count = 0;
		while (!loginFlag && count++ < 3) {
			String u = io.readLine("login:");
			String p = io.readLine("password:", false);
			User user = new User();
			user.setUserName(u);
			user.setUserPwd(p);
			loginFlag = new UserDAO().checkAccount(user);
			if (loginFlag) {
				Date loginTime = new Date();
				String hostName = Util.getHostName();
				Properties props = System.getProperties();
				String osName = props.getProperty("os.name");
				String osVersion = props.getProperty("os.version");

				io.println("Hello " + u + ".   " + osName + " " + osVersion + "   " + hostName + "   " + Util.getDateString(loginTime));
				return true;
			} else {
				io.println("登录失败");
			}
		}
		return false;
	}

}

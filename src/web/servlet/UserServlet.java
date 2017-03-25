package web.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import util.Util;

import db.dao.UserDAO;
import db.pojo.ActionResult;
import db.pojo.User;
import framework.IGPError;

/**
 * UserServlet
 * 
 * @author yuanxf
 * @since 1.0 2010-6-7
 */
public class UserServlet extends BasicServlet<UserDAO> {

	private static final long serialVersionUID = 1L;

	public ActionResult query(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL("user.jsp");
		result.setData(dao.list());
		return result;

	}

	public ActionResult update(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		String strID = req.getParameter("id");
		result.setForwardURL("userUpdate.jsp");
		result.setData(dao.getById(Integer.parseInt(strID)));
		return result;
	}

	public ActionResult updateResult(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");
		ActionResult result = new ActionResult();
		String userid = req.getParameter("tag");
		String username = req.getParameter("username");
		String pwd = req.getParameter("pwd");
		String group = req.getParameter("group");
		User u = new User();
		u.setId(Integer.parseInt(userid));
		u.setUserName(username);
		u.setUserPwd(pwd);
		u.setGroupID(Integer.parseInt(group));

		result.setForwardURL(forwardURL);
		result.setReturnURL(returnURL);
		result.setData(dao.update(u));;
		return result;
	}

	public ActionResult add(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");
		ActionResult result = new ActionResult();
		String userName = req.getParameter("username");
		String userPwd = req.getParameter("pwd");
		String groupId = req.getParameter("group");
		User user = new User();
		user.setUserName(userName);
		user.setUserPwd(userPwd);
		user.setGroupID(Integer.parseInt(groupId));

		boolean bool = dao.validate(user);
		if (!bool) {
			int num = dao.add(user);
			if (num >= 1) {
				result.setError(new IGPError("", "ok", "", ""));
			} else {
				result.setError(new IGPError("", "失败", "", ""));
			}
		} else {
			result.setError(new IGPError("", "用户名已存在", "", ""));
		}
		result.setForwardURL(forwardURL);
		result.setReturnURL(returnURL);
		return result;

	}

	public ActionResult modifyUserPwd(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");

		String oldPwd = req.getParameter("oldpwd");
		String newPwd = req.getParameter("pwd");
		String userId = (String) req.getSession().getAttribute("userId");
		// System.out.println("========="+userId);
		User oldUser = dao.getById(Integer.parseInt(userId));
		if (oldUser.getUserPwd().equals(Util.toMD5(oldPwd))) {
			boolean bool = dao.modifyPwd(Integer.parseInt(userId), newPwd);
			if (bool) {
				result.setError(new IGPError("0005", "密码修改成功", "OK", "OK"));
			} else {
				result.setError(new IGPError("0005", "密码修改失败", "内部原因", "联系开发"));
			}
		} else {
			result.setError(new IGPError("0005", "密码错误", "两次密码输入不一致", "请核对密码"));
		}
		result.setForwardURL(forwardURL);
		result.setReturnURL(returnURL);
		return result;

	}

}

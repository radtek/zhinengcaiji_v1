package web.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import db.dao.UserDAO;
import db.pojo.User;

public class AuthorizationServlet extends HttpServlet {

	private static final long serialVersionUID = -3695263751195704882L;

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String exit = req.getParameter("exit");
		if (exit == null)// 登录
		{
			String userName = req.getParameter("userName");
			String password = req.getParameter("password");
			String remember = req.getParameter("remember");
			boolean isRemember = remember != null && remember.equals("on");

			User u = new User();
			u.setUserName(userName);
			u.setUserPwd(password);
			boolean b = new UserDAO().checkAccount(u);

			if (b) {
				req.getSession().setAttribute("login", true);
				long userId = new UserDAO().getByName(userName).getId();
				req.getSession().setAttribute("userId", String.valueOf(userId));
				if (isRemember) {
					Cookie cookie = new Cookie("igpcookie", "igpcookie");
					cookie.setMaxAge(60 * 60 * 24 * 30);
					resp.addCookie(cookie);
				}
				resp.sendRedirect("/page/main.html");
			} else {
				resp.sendRedirect("/error.html");
			}
		} else
		// 注销
		{
			req.getSession().removeAttribute("login");
			req.getSession().invalidate();
			resp.setContentType("text/html;charset=utf-8");
			resp.getWriter().println("<script>window.parent.location.replace('/');</script>");
			resp.getWriter().flush();
		}
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		doPost(req, resp);
	}

}

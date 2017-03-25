package web.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class CookieServlet extends HttpServlet {

	private static final long serialVersionUID = -3759915471593562139L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		Cookie[] cs = req.getCookies();
		boolean flag = false;
		if (cs != null) {
			for (Cookie c : cs) {
				if (c != null) {
					if (c.getName().equals("igpcookie")) {
						flag = true;
						req.getSession().setAttribute("login", true);
						req.getRequestDispatcher("/page/main.html").forward(req, resp);
					}
				}
			}
		}
		if (!flag) {
			req.getRequestDispatcher("/index.jsp").forward(req, resp);
		}
	}
}

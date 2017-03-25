package web.servlet;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class AuthorizationFilter implements Filter {

	@Override
	public void destroy() {
	}

	@Override
	public void init(FilterConfig arg0) throws ServletException {
	}

	@Override
	public void doFilter(ServletRequest req, ServletResponse resp, FilterChain fc) throws IOException, ServletException {
		HttpServletRequest httpServletRequest = (HttpServletRequest) req;
		HttpServletResponse httpServletResponse = (HttpServletResponse) resp;
		HttpSession session = httpServletRequest.getSession();
		if (session.getAttribute("login") != null) {
			fc.doFilter(req, resp);
		} else {
			String script = "<script>if(window.parent!=null){ window.parent.location."
					+ "replace('/');}else{ window.location.replace('/');}</script>";
			httpServletResponse.getWriter().println(script);
			httpServletResponse.getWriter().flush();
		}
	}

}

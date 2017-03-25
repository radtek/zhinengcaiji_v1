package web.servlet;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import util.LogMgr;
import util.Util;
import db.dao.DAO;
import db.pojo.ActionResult;
import framework.IGPError;

/**
 * servlet基类
 * <p>
 * 开发规范:<br>
 * 1.前台页面传入的表示操作类型的参数其名必须为action,如果没有则默认为查询操作,子类可覆写此默认行为;<br>
 * 2.操作结果统一通过request中名为result(为ActionResult实例)的属性获取;<br>
 * 3.如果需要用到操作结果提示页面,那统一使用result.jsp页面，此时必须指定ActionResult中的returnURL属性值;<br>
 * 4.各业务方法返回前必须填写result中forwardURL属性值;<br>
 * 5.必须使用IGPError对象来封装操作错误或者异常信息;<br>
 * 6.编号前后台传递的时候统一使用 id;<br>
 * 7.自己添加的业务方法必须按照诸如 public ActionResult yourMethod(HttpServletRequest req,HttpServletResponse resp) throws ServletException,IOException
 * 形式进行，唯一需要修改的就是yourMethod,切记方法访问修饰符必须是public;
 * </p>
 * 
 * @author IGP TDT
 * @since 1.0
 */
public abstract class BasicServlet<T extends DAO<?>> extends HttpServlet {

	private static final long serialVersionUID = 5131523935580673298L;

	protected Logger logger = LogMgr.getInstance().getSystemLogger();

	protected T dao;

	public static final String ACTION = "action"; // 操作类型名字

	public static final String RESULTNAME = "result"; // 操作结果名字

	protected static final String DEFAULT_FORWARD_URL = "/page/result.jsp"; // 默认forward页面

	protected static final String DEFAULT_RETURN_URL = "javascript:history.back(-1);"; // 默认return页面

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		doPost(req, resp);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		if (dao == null) {
			// 如果dao没有创建，则自动创建；也就是说不需要人工创建dao
			ParameterizedType pt = (ParameterizedType) this.getClass().getGenericSuperclass();
			Class<T> clazz = (Class<T>) pt.getActualTypeArguments()[0];
			try {
				dao = (T) clazz.newInstance();
			} catch (Exception e) {
				logger.error("内部错误,原因:", e);
			}
		}

		req.setCharacterEncoding("utf-8");
		resp.setCharacterEncoding("utf-8");
		// resp.setContentType("text/html;charset=utf-8");

		ActionResult result = null;
		String action = req.getParameter("action"); // 操作类型

		// 根据传入的方法名调用相应的方法,默认为defaultAction操作
		try {
			if (Util.isNull(action)) {
				throw new NoSuchMethodException();
			}

			Method method = this.getClass().getMethod(action, HttpServletRequest.class, HttpServletResponse.class);
			method.setAccessible(true);
			result = (ActionResult) method.invoke(this, req, resp);
		} catch (NoSuchMethodException e) {
			result = defaultAction(req, resp);
		} catch (Exception e) {
			logger.error("内部错误,原因:", e);
		}

		if (result == null) {
			result = new ActionResult(new IGPError("0001", "内部错误", "内部原因", "联系开发人员"), DEFAULT_FORWARD_URL, DEFAULT_RETURN_URL, null);
		}

		req.setAttribute(RESULTNAME, result);
		String fURL = result.getForwardURL();
		if (Util.isNotNull(fURL)) {
			req.getRequestDispatcher(fURL).forward(req, resp);
		}
	}

	public ActionResult add(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		return null;
	}

	/**
	 * 读取指定编号的记录
	 * 
	 * @param req
	 * @param resp
	 * @return
	 * @throws ServletException
	 * @throws IOException
	 */
	public ActionResult get(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		String strID = req.getParameter("id");
		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");

		int id = -1;
		try {
			id = Integer.parseInt(strID.trim());
		} catch (NumberFormatException e) {
		}

		if (Util.isNull(forwardURL) || Util.isNull(returnURL) || id < 0) {
			result.setError(new IGPError());
			result.setData(null);
			result.setForwardURL(DEFAULT_FORWARD_URL);
			result.setReturnURL(DEFAULT_RETURN_URL);
		} else {
			result.setError(new IGPError());
			result.setData(dao.getById(id));
			result.setForwardURL(forwardURL);
			result.setReturnURL(returnURL);
		}

		return result;
	}

	/**
	 * 删除单条记录
	 * 
	 * @param req
	 * @param resp
	 * @return
	 * @throws ServletException
	 * @throws IOException
	 */
	public ActionResult del(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		String strID = req.getParameter("id");
		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");

		int id = -1;
		try {
			id = Integer.parseInt(strID.trim());
		} catch (NumberFormatException e) {
		}

		// 如果不填写forwardURL则为默认页面
		if (Util.isNull(forwardURL))
			forwardURL = DEFAULT_FORWARD_URL;

		if (Util.isNull(returnURL) || id < 0) {
			result.setError(new IGPError());
			result.setData(null);
			result.setReturnURL(DEFAULT_RETURN_URL);
		} else {
			dao.delete(id);

			result.setError(new IGPError());
			result.setData(null);
			result.setReturnURL(returnURL);
		}
		result.setForwardURL(forwardURL);

		return result;
	}

	public ActionResult update(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		return null;
	}

	/**
	 * 查询记录,默认实现为查询所有记录
	 * 
	 * @param req
	 * @param resp
	 * @return
	 * @throws ServletException
	 * @throws IOException
	 */
	public ActionResult query(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");

		if (Util.isNull(forwardURL) || Util.isNull(returnURL)) {
			result.setError(new IGPError());
			result.setData(null);
			result.setForwardURL(DEFAULT_FORWARD_URL);
			result.setReturnURL(DEFAULT_RETURN_URL);
		} else {
			result.setError(new IGPError());
			result.setData(dao.list());
			result.setForwardURL(forwardURL);
			result.setReturnURL(returnURL);
		}

		return result;
	}

	/**
	 * 默认行为,这里默认为查询操作
	 */
	protected ActionResult defaultAction(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		return query(req, resp);
	}
}

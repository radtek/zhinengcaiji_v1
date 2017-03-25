package web.servlet;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import task.CollectObjInfo;
import task.TaskMgr;
import util.CommonDB;
import util.LogMgr;
import util.Util;
import db.dao.TaskDAO;
import db.pojo.Task;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 公共小操作Servlet,仅仅适应Ajax请求操作
 * <p>
 * 所有业务方法都只返回String类型
 * </p>
 * 
 * @author YangJian
 * @since 1.0
 */
public class ToysServlet extends HttpServlet {

	private static final long serialVersionUID = 4402519123126734714L;

	protected Logger logger = LogMgr.getInstance().getSystemLogger();

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		doPost(req, resp);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		Object returnObj = new String("request error");

		req.setCharacterEncoding("utf-8");
		resp.setCharacterEncoding("utf-8");
		resp.setContentType("text/html;charset=utf-8");

		String action = req.getParameter("action"); // 操作类型

		// 根据传入的方法名调用相应的方法
		try {
			if (Util.isNull(action)) {
				throw new NoSuchMethodException();
			}

			Method method = this.getClass().getMethod(action, HttpServletRequest.class, HttpServletResponse.class);
			method.setAccessible(true);
			returnObj = method.invoke(this, req, resp);
		} catch (NoSuchMethodException e) {
		} catch (Exception e) {
			logger.error("内部错误,原因:", e);
		}

		if (returnObj == null) {
			returnObj = new String("");
		}

		resp.getWriter().write(returnObj.toString());
	}

	/**
	 * 数据源表达式匹配测试
	 * 
	 * @param req
	 * @param resp
	 * @return
	 */
	public String dsExprTest(HttpServletRequest req, HttpServletResponse resp) {
		String expr = req.getParameter("expr");
		String strTime = req.getParameter("time");

		String result = null;
		try {
			Date date = Util.getDate1(strTime);
			Timestamp tsp = new Timestamp(date.getTime());
			result = ConstDef.ParseFilePath(expr, tsp);
		} catch (ParseException pe) {
			result = "测试失败,原因:时间格式错误.";
		} catch (Exception e) {
			result = "测试失败,原因:" + e.getMessage();
		}

		return result;
	}

	public String getTaskFilePath(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String filePath = null;
		int id = -1;
		try {
			id = Integer.parseInt(req.getParameter("taskID").trim());
			Task task = new TaskDAO().getById(id);
			filePath = task.getCollectPath();
		} catch (NumberFormatException e) {
			filePath = "taskID为非数字！";
		}

		return filePath;
	}

	public String getTasks(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		StringBuilder s = new StringBuilder("[");
		List<Task> list = new TaskDAO().list();
		if (list != null && !list.isEmpty()) {
			for (Task task : list) {
				s.append("{\"des\":\"").append(task.getTaskDescribe()).append("\", \"taskId\":").append(task.getTaskId()).append("},");
			}
			s.deleteCharAt(s.length() - 1).append("]");
		}

		return s.toString();
	}

	public String getActiveTasks(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		List<CollectObjInfo> tasks = TaskMgr.getInstance().list();
		if (tasks.size() == 0)
			return "";
		String collector = Util.getHostName(); // 本地计算机名
		int pId = SystemConfig.getInstance().getMRProcessId();
		if (pId != 0)
			collector += "@" + pId;
		StringBuilder s = new StringBuilder("[");
		long now = System.currentTimeMillis();
		for (CollectObjInfo task : tasks) {
			s.append("{\"taskId\":\"");
			if (CommonDB.isReAdoptObj(task)) {
				s.append(task.getKeyID() - 10000000);
			} else {
				s.append(task.getTaskID());
			}
			String cost = "";
			long fast = now - task.startTime.getTime();
			// 小于一分钟之内使用秒为单位
			if (fast < (1000 * 60))
				cost = Math.round(fast / 1000) + " 秒";
			// 大于一分钟的使用分钟作为单位
			else {
				cost = Math.round(fast / (1000 * 60)) + " 分钟";
			}
			s.append("\",\"des\":\"").append(task.getDescribe());
			s.append("\",\"lastct\":\"").append(task.getLastCollectTime());
			s.append("\",\"coltime\":\"").append(cost);
			s.append("\",\"collector\":\"").append(collector).append("\"},");
		}

		s.deleteCharAt(s.length() - 1).append("]");
		return s.toString();
	}

	public static void main(String[] args) {
		StringBuilder s = new StringBuilder();
		s.append("agsafsa'");

		// s.deleteCharAt(s.length() - 1).append("[");
		System.out.println(s.toString());
	}
}

package web.servlet;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import util.Util;
import db.dao.PageQueryResult;
import db.dao.RTaskDAO;
import db.dao.TaskDAO;
import db.pojo.ActionResult;
import db.pojo.RTask;
import db.pojo.Task;
import framework.IGPError;

/**
 * 补采任务Servlet
 * 
 * @author litp
 * @since 1.0
 */
public class RTaskServlet extends BasicServlet<RTaskDAO> {

	private static final long serialVersionUID = 1L;

	private static final int DEFAULT_PAGE_SIZE = 10; // 默认PageSize大小

	public ActionResult getAllTasks(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");

		// 如果不填写forwardURL则为默认页面
		if (Util.isNull(forwardURL))
			forwardURL = DEFAULT_FORWARD_URL;
		if (Util.isNull(returnURL))
			returnURL = DEFAULT_RETURN_URL;

		List<Task> tasks = new TaskDAO().list();
		req.setAttribute("hostName", Util.getHostName());
		result.setError(new IGPError());
		result.setData(tasks);
		result.setReturnURL(returnURL);
		result.setForwardURL(forwardURL);
		return result;

	}

	@Override
	public ActionResult query(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");
		int currentPage = Integer.parseInt(req.getParameter("page").trim());
		String sqlCondition = req.getParameter("sqlCondition").trim();
		String pageSize = req.getParameter("pageSize");

		// 如果不填写forwardURL则为默认页面
		if (Util.isNull(forwardURL))
			forwardURL = DEFAULT_FORWARD_URL;
		if (Util.isNull(returnURL))
			returnURL = DEFAULT_RETURN_URL;

		PageQueryResult<RTask> qr = null;
		int intSize = DEFAULT_PAGE_SIZE;
		if (pageSize != null && !pageSize.trim().equals("")) {
			intSize = Integer.parseInt(pageSize.trim());
			intSize = intSize <= 0 ? DEFAULT_PAGE_SIZE : intSize;
		}
		Map<String, String> conditions = null;
		if (!sqlCondition.equals("")) {
			qr = dao.pageQuery(intSize, currentPage, sqlCondition);
			req.setAttribute("sqlCondition", sqlCondition);
		} else {
			conditions = toMap(req, resp);
			qr = dao.pageQuery(intSize, currentPage, conditions);
		}
		req.setAttribute("hostName", Util.getHostName());
		// 条件回显
		setBack(req, conditions);
		// 采集任务信息
		List<Task> list = new TaskDAO().list();
		req.setAttribute("tasks", list);
		// 补采任务信息
		result.setError(new IGPError());
		result.setData(qr);
		result.setReturnURL(returnURL);
		result.setForwardURL(forwardURL);
		return result;
	}

	@Override
	public ActionResult update(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		boolean flag = false;

		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");

		// 如果不填写forwardURL则为默认页面
		if (Util.isNull(forwardURL))
			forwardURL = DEFAULT_FORWARD_URL;
		if (Util.isNull(returnURL))
			returnURL = DEFAULT_RETURN_URL;
		String data = null;
		RTask r = toRTask(req);
		flag = dao.update(r);
		if (flag) {
			data = "更新成功!";
		} else {
			data = "更新失败!";
		}
		result.setError(new IGPError());
		result.setData(data);
		result.setReturnURL(returnURL);
		result.setForwardURL(forwardURL);

		return result;
	}

	public ActionResult delMore(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		String data = null;

		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");

		// 如果不填写forwardURL则为默认页面
		if (Util.isNull(forwardURL))
			forwardURL = DEFAULT_FORWARD_URL;
		if (Util.isNull(returnURL))
			returnURL = DEFAULT_RETURN_URL;

		String ids = req.getParameter("ids").trim();
		// 如果delMore不为空，那么就为删除多条，否则删除单条
		String[] idArray = ids.split(";");
		if (((RTaskDAO) dao).delMore(idArray) > 0) {
			data = "批量删除成功!";
		} else {
			data = "批量删除失败!";
		}
		result.setError(new IGPError());
		result.setData(data);
		result.setReturnURL(returnURL);
		result.setForwardURL(forwardURL);
		return result;
	}

	@Override
	public ActionResult add(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		int id = 0;

		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");

		// 如果不填写forwardURL则为默认页面
		if (Util.isNull(forwardURL))
			forwardURL = DEFAULT_FORWARD_URL;
		if (Util.isNull(returnURL))
			returnURL = DEFAULT_RETURN_URL;
		String data = null;
		RTask r = toRTask(req);
		// 只有在此任务是在使用状态时，才可以向数据库中添加
		Task t = new TaskDAO().getById(r.getTaskID());
		if (t == null) {
			data = "操作失败,原因:对应的正常任务(" + r.getTaskID() + ")不存在";
		} else {
			if (t.getIsUsed() == 1) {
				id = dao.add(r);
				if (id > 0) {
					data = "添加成功!id:" + id;
				} else {
					data = "添加失败!";
				}
			} else {
				data = "由于此任务不是在作用状态，所以不能添加!";
			}
		}

		//
		result.setError(new IGPError());
		result.setData(data);
		result.setReturnURL(returnURL);
		result.setForwardURL(forwardURL);
		return result;
	}

	private void setBack(HttpServletRequest req, Map<String, String> condition) throws ServletException, IOException {
		if (condition != null && !condition.isEmpty()) {
			Set<String> columns = condition.keySet();
			for (String col : columns) {
				req.setAttribute(col, condition.get(col));
			}
		}
	}

	private Map<String, String> toMap(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		Map<String, String> conditions = new HashMap<String, String>(8);
		String id = req.getParameter("qId").trim();
		String tId = req.getParameter("taskId").trim();
		String cn = req.getParameter("collector_name").trim();
		String rt = req.getParameter("readoptType").trim();
		String cs = req.getParameter("collectStatus").trim();
		String ct = req.getParameter("collectTime").trim();
		String cp = req.getParameter("collect_period").trim();
		String ctp = req.getParameter("collect_type").trim();
		if (!id.equals("")) {
			conditions.put("id", id);
		}
		if (!tId.equals("")) {
			conditions.put("taskId", tId);
		}
		if (!cn.equals("")) {
			conditions.put("collector_name", cn);
		}
		if (!rt.equals("")) {
			conditions.put("readoptType", rt);
		}
		if (!cs.equals("")) {
			conditions.put("collectStatus", cs);
		}
		if (!ct.equals("")) {
			conditions.put("collectTime", ct);
		}
		if (!cp.equals("")) {
			conditions.put("collect_period", cp);
		}
		if (!ctp.equals("")) {
			conditions.put("collect_type", ctp);
		}

		return conditions;
	}

	private RTask toRTask(HttpServletRequest req) {
		RTask r = new RTask();
		r.setId(toInt(req.getParameter("id")));
		r.setTaskID(toInt(req.getParameter("taskID")));
		r.setFilePath(req.getParameter("filePath"));
		r.setCollectTime(req.getParameter("collectTime"));
		r.setStampTime(req.getParameter("stampTime"));
		r.setCollectorName(req.getParameter("collectorName"));
		r.setReadoptType(toInt(req.getParameter("readoptType")));
		r.setCollectStatus(toInt(req.getParameter("collectStatus")));
		r.setCause(req.getParameter("cause"));
		return r;
	}

	private int toInt(String ident) {
		int id = 0;
		if (ident != null) {
			ident = ident.trim();
			id = ident.equals("") ? 0 : Integer.parseInt(ident);
		}
		return id;
	}

}

package web.servlet;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import util.Util;
import db.dao.DeviceDAO;
import db.dao.PageQueryResult;
import db.dao.RTaskDAO;
import db.dao.TaskDAO;
import db.dao.TempletDAO;
import db.pojo.ActionResult;
import db.pojo.CollectPeriod;
import db.pojo.CollectType;
import db.pojo.Task;
import framework.IGPError;
import framework.PBeanMgr;

public class TaskServlet extends BasicServlet<TaskDAO> {

	private static final long serialVersionUID = -6779662662425661647L;

	private static final int PAGE_SIZE = 15;

	public ActionResult list(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		int currentPage = Integer.parseInt(req.getParameter("currentPage"));
		PageQueryResult<Task> datas = null;

		Task t = req.getSession().getAttribute("taskCondition") == null ? null : (Task) req.getSession().getAttribute("taskCondition");

		String flag = req.getParameter("clearCondition");
		if (flag.equals("0")) {
			t = null;
			req.getSession().removeAttribute("taskCondition");
		}
		datas = dao.advQuery(t, PAGE_SIZE, currentPage);

		return new ActionResult(new IGPError(), "/page/task.jsp", "/page/task.jsp", datas);
	}

	public ActionResult showDetail(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		int taskId = Integer.parseInt(req.getParameter("taskId"));
		Task t = dao.getById(taskId);
		return new ActionResult(new IGPError(), "/page/taskDetail.jsp", "/page/task.jsp", t);
	}

	public ActionResult modif(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		int taskId = Integer.parseInt(req.getParameter("taskId"));
		Task t = dao.getById(taskId);
		return new ActionResult(new IGPError(), "/page/taskModif.jsp", "/page/task.jsp", t);
	}

	public ActionResult saveModif(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		Task task = buildTask(req);
		boolean b = dao.update(task);
		return new ActionResult(new IGPError(), "/page/taskResult.jsp", "/page/task.jsp", b ? "修改成功" : "修改失败");
	}

	public ActionResult delTask(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String delFlag = req.getParameter("delFlag");
		List<Integer> taskIds = new ArrayList<Integer>();
		String[] items = delFlag.split(",");
		for (String s : items) {
			if (Util.isNotNull(s)) {
				taskIds.add(Integer.parseInt(s));
			}
		}
		List<Integer> fails = new ArrayList<Integer>();
		for (int taskId : taskIds) {
			if (!dao.delete(taskId)) {
				fails.add(taskId);
			}
		}
		String strResult = null;
		if (fails.size() == 0) {
			strResult = "全部删除成功";
		} else if (fails.size() == taskIds.size()) {
			strResult = "全部删除失败";
		} else {
			strResult = "部分删除成功<br />以下任务删除失败：<br />";
			for (int i = 0; i < fails.size(); i++) {
				strResult += fails.get(i);
				if (i < fails.size() - 1) {
					strResult += "，";
				}
			}
		}
		return new ActionResult(new IGPError(), "/page/taskResult.jsp", "/page/task.jsp", strResult);
	}

	public ActionResult toAdd(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		int taskId = Integer.parseInt(req.getParameter("taskId"));
		Task t = dao.getById(taskId);
		return new ActionResult(new IGPError(), "/page/addTask.jsp", "/page/task.jsp", t);
	}

	public ActionResult addTask(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		Task task = buildTask(req);
		boolean b = dao.add(task) == 1;

		return new ActionResult(new IGPError(), "/page/taskResult.jsp", "/page/task.jsp", b ? "添加成功" : "添加失败");
	}

	@Override
	public ActionResult query(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		Task t = buildTask(req);
		req.getSession().setAttribute("taskCondition", t);
		int currentPage = Integer.parseInt(req.getParameter("currentPage"));
		PageQueryResult<Task> datas = dao.advQuery(t, PAGE_SIZE, currentPage);
		return new ActionResult(new IGPError(), "/page/task.jsp", "/page/task.jsp", datas);
	}

	public ActionResult findDevice(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		int devId = Integer.parseInt(req.getParameter("devId"));
		boolean b = new DeviceDAO().getById(devId) != null;
		String s = b ? "1" : "0";
		resp.getWriter().print(s);
		resp.getWriter().flush();
		return new ActionResult(new IGPError(), null, null, null);
	}

	public ActionResult findTask(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		int taskId = Integer.parseInt(req.getParameter("taskId"));
		boolean b = new TaskDAO().getById(taskId) != null;
		b = new RTaskDAO().getById(taskId - 10000000) != null;
		String s = b ? "1" : "0";
		resp.getWriter().print(s);
		resp.getWriter().flush();
		return new ActionResult(new IGPError(), null, null, null);
	}

	public ActionResult findTemplet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		int tid = Integer.parseInt(req.getParameter("tid"));
		boolean b = new TempletDAO().getById(tid) != null;
		String s = b ? "1" : "0";
		resp.getWriter().print(s);
		resp.getWriter().flush();
		return new ActionResult(new IGPError(), null, null, null);
	}

	public ActionResult findParser(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		int pid = Integer.parseInt(req.getParameter("pid"));
		boolean b = PBeanMgr.getInstance().getParserBean(pid) != null;
		String s = b ? "1" : "0";
		resp.getWriter().print(s);
		resp.getWriter().flush();
		return new ActionResult(new IGPError(), null, null, null);
	}

	public ActionResult findDistributor(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		int did = Integer.parseInt(req.getParameter("did"));
		boolean b = PBeanMgr.getInstance().getTemplateBean(did) != null;
		String s = b ? "1" : "0";
		resp.getWriter().print(s);
		resp.getWriter().flush();
		return new ActionResult(new IGPError(), null, null, null);
	}

	private Task buildTask(HttpServletRequest req) {
		String strID = req.getParameter("taskId");
		if (Util.isNull(strID))
			strID = "-1";
		strID = strID.trim();
		int taskId = Integer.parseInt(strID);
		String taskDescribe = req.getParameter("taskDescribe") == null ? "" : req.getParameter("taskDescribe");
		int devId = Util.isNull(req.getParameter("devId")) ? -1 : Integer.parseInt(req.getParameter("devId"));
		int devPort = Util.isNull(req.getParameter("devPort")) ? -1 : Integer.parseInt(req.getParameter("devPort"));
		int proxyDevId = Util.isNull(req.getParameter("proxyDevId")) ? -1 : Integer.parseInt(req.getParameter("proxyDevId"));
		int proxyDevPort = Util.isNull(req.getParameter("proxyDevPort")) ? -1 : Integer.parseInt(req.getParameter("proxyDevPort"));
		int collectType = Util.isNull(req.getParameter("collectType")) ? -1 : Integer.parseInt(req.getParameter("collectType"));
		int collectPeriod = Util.isNull(req.getParameter("collectPeriod")) ? -1 : Integer.parseInt(req.getParameter("collectPeriod"));
		int collectTimeout = Util.isNull(req.getParameter("collectTimeout")) ? -1 : Integer.parseInt(req.getParameter("collectTimeout"));
		int collectTime = Util.isNull(req.getParameter("collectTime")) ? -1 : Integer.parseInt(req.getParameter("collectTime"));
		String collectPath = req.getParameter("collectPath") == null ? "" : req.getParameter("collectPath");
		int shellTimeout = Util.isNull(req.getParameter("shellTimeout")) ? -1 : Integer.parseInt(req.getParameter("shellTimeout"));
		int parseTmpId = Util.isNull(req.getParameter("parseTmpId")) ? -1 : Integer.parseInt(req.getParameter("parseTmpId"));
		int distributeTmpId = Util.isNull(req.getParameter("distributeTmpId")) ? -1 : Integer.parseInt(req.getParameter("distributeTmpId"));
		Timestamp sucDataTime = null;
		try {
			sucDataTime = Util.isNull(req.getParameter("sucDataTime")) ? null : new Timestamp(Util.getDate1(req.getParameter("sucDataTime"))
					.getTime());
		} catch (ParseException e) {
			logger.error("日期转换错误:" + req.getParameter("sucDataTime"), e);
		}
		int sucDataPos = Util.isNull(req.getParameter("sucDataPos")) ? -1 : Integer.parseInt(req.getParameter("sucDataPos"));
		int isUsed = Util.isNull(req.getParameter("isUsed")) ? -1 : Integer.parseInt(req.getParameter("isUsed"));
		int isUpdate = Util.isNull(req.getParameter("isUpdate")) ? -1 : Integer.parseInt(req.getParameter("isUpdate"));
		int maxCltTime = Util.isNull(req.getParameter("maxCltTime")) ? -1 : Integer.parseInt(req.getParameter("maxCltTime"));
		String shellCmdPrepare = req.getParameter("shellCmdPrepare") == null ? "" : req.getParameter("shellCmdPrepare");
		String shellCmdFinish = req.getParameter("shellCmdFinish") == null ? "" : req.getParameter("shellCmdFinish");
		int collectTimepos = Util.isNull(req.getParameter("collectTimepos")) ? -1 : Integer.parseInt(req.getParameter("collectTimepos"));
		String dbDriver = req.getParameter("dbDriver") == null ? "" : req.getParameter("dbDriver");
		String dbUrl = req.getParameter("dbUrl") == null ? "" : req.getParameter("dbUrl");
		int threadSleepTime = Util.isNull(req.getParameter("threadSleepTime")) ? -1 : Integer.parseInt(req.getParameter("threadSleepTime"));
		int blockTime = Util.isNull(req.getParameter("blockTime")) ? -1 : Integer.parseInt(req.getParameter("blockTime"));
		String collectorName = req.getParameter("collectorName") == null ? "" : req.getParameter("collectorName");
		int paramRecord = Util.isNull(req.getParameter("paramRecord")) ? -1 : Integer.parseInt(req.getParameter("paramRecord"));
		int groupId = Util.isNull(req.getParameter("groupId")) ? -1 : Integer.parseInt(req.getParameter("groupId"));
		Timestamp endDataTime = null;
		try {
			endDataTime = Util.isNull(req.getParameter("endDataTime")) ? null : new Timestamp(Util.getDate1(req.getParameter("endDataTime"))
					.getTime());
		} catch (ParseException e) {
			logger.error("日期转换错误:" + req.getParameter("endDataTime"), e);
		}
		int parserId = Util.isNull(req.getParameter("parserId")) ? -1 : Integer.parseInt(req.getParameter("parserId"));
		int distributorId = Util.isNull(req.getParameter("distributorId")) ? -1 : Integer.parseInt(req.getParameter("distributorId"));
		int redoTimeOffset = Util.isNull(req.getParameter("redoTimeOffset")) ? -1 : Integer.parseInt(req.getParameter("redoTimeOffset"));

		Task task = new Task(taskId, taskDescribe, devId, devPort, proxyDevId, proxyDevPort, CollectType.create(collectType),
				CollectPeriod.create(collectPeriod), collectTimeout, collectTime, collectPath, shellTimeout, parseTmpId, distributeTmpId,
				sucDataTime, sucDataPos, isUsed, isUpdate, maxCltTime, shellCmdPrepare, shellCmdFinish, collectTimepos, dbDriver, dbUrl,
				threadSleepTime, blockTime, collectorName, paramRecord, groupId, endDataTime, parserId, distributorId, redoTimeOffset);
		return task;
	}
}

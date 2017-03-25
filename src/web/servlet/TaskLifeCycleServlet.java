package web.servlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import task.TaskMgr;
import util.Util;
import db.dao.RTaskDAO;
import db.dao.TaskDAO;
import db.pojo.RTask;
import db.pojo.RtaskLifeCycleInfo;
import db.pojo.Task;
import db.pojo.TaskLifeCycleInfo;
import framework.SystemConfig;

/**
 * 任务生命周期监控。
 * 
 * @author ChenSijiang 2010-10-21
 * @since 1.1
 */
public class TaskLifeCycleServlet extends HttpServlet {

	private static final long serialVersionUID = 4101415589854100206L;

	private TaskMgr mgr = TaskMgr.getInstance();

	private TaskDAO taskDao = new TaskDAO();

	private RTaskDAO rtaskDao = new RTaskDAO();

	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		// 有showList参数，仅用于在首页显示正在运行的任务，然后不做其它事，直接return
		String showList = req.getParameter("showList");
		if (Util.isNotNull(showList)) {
			showList(req, resp);
			return;
		}

		// 查询几天的补采
		String strDay = req.getParameter("day");
		int day = 2;
		if (Util.isNotNull(strDay)) {
			try {
				day = Integer.parseInt(strDay.trim());
			} catch (Exception e) {
			}
		}

		// 要查询的任务ID
		String strTaskId = req.getParameter("taskId");
		Task task = null;
		String msg = null;
		int taskId = -1;
		boolean isFail = false;
		if (Util.isNull(strTaskId)) {
			isFail = true;
		} else {
			try {
				taskId = Integer.parseInt(strTaskId);
			} catch (Exception e) {
			}
			if (taskId == -1) {
				isFail = true;
			} else {
				task = taskDao.getById(taskId);
				if (task == null) {
					isFail = true;
				}
			}
		}
		if (isFail) {
			msg = "您输入的任务不存在";
			req.setAttribute("msg", msg);
		} else {

			// 处理正常任务
			TaskLifeCycleInfo info = new TaskLifeCycleInfo();
			CollectObjInfo cltInfo = mgr.getTask(taskId);
			if (cltInfo == null) {
				cltInfo = new CollectObjInfo(taskId);
				cltInfo.setLastCollectTime(task.getSucDataTime());
				cltInfo.setPeriod(task.getCollectPeriod().getValue());
			}
			String cost;
			String dataTime;
			if (cltInfo.startTime == null) {
				cost = "未运行";
				dataTime = Util.getDateString(cltInfo.getLastCollectTime());
			} else {

				dataTime = Util.getDateString(cltInfo.getLastCollectTime());
				info.setDataTime(dataTime);
				long fast = new Date().getTime() - cltInfo.startTime.getTime();
				if (fast < (1000 * 60))
					cost = Math.round(fast / 1000) + " 秒";
				else {
					cost = Math.round(fast / (1000 * 60)) + " 分钟";
				}
			}
			info.setCostTime(cost);
			info.setTaskId(taskId);
			info.setDataTime(dataTime);
			info.setTaskDescribe(task.getTaskDescribe());
			req.setAttribute("task", info);

			// 处理补采任务
			List<RTask> rtasks = getRTasks(taskId, day);
			if (rtasks.size() > 0) {
				for (RTask r : rtasks) {
					RtaskLifeCycleInfo rLife = new RtaskLifeCycleInfo(r);
					RegatherObjInfo rInfo = null;
					if (mgr.getObjByID(r.getId() + 10000000) != null) {
						rInfo = (RegatherObjInfo) mgr.getObjByID(r.getId() + 10000000);
					}
					if (rInfo != null) {
						long fast = new Date().getTime() - rInfo.startTime.getTime();
						if (fast < (1000 * 60))
							cost = Math.round(fast / 1000) + " 秒";
						else {
							cost = Math.round(fast / (1000 * 60)) + " 分钟";
						}
						rLife.setCostTime(cost);
					} else {
						rLife.setCostTime("未运行");
					}
					info.getReclts().add(rLife);
				}
			}
		}

		req.setAttribute("day", day);
		req.setAttribute("taskId", taskId);
		req.getRequestDispatcher("/page/taskLifeCycle.jsp").forward(req, resp);
	}

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		doPost(req, resp);
	}

	/**
	 * 在首页显示列表
	 */
	private void showList(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		List<CollectObjInfo> lst = TaskMgr.getInstance().list();
		List<Task> tasks = new ArrayList<Task>();
		List<RTask> rtasks = new ArrayList<RTask>();
		for (CollectObjInfo obj : lst) {
			long taskId = obj.getTaskID();
			String time = Util.getDateString(obj.getLastCollectTime());
			String cost;
			long now = System.currentTimeMillis();
			long fast = now - obj.startTime.getTime();
			// 小于一分钟之内使用秒为单位
			if (fast < (1000 * 60))
				cost = Math.round(fast / 1000) + " 秒";
			// 大于一分钟的使用分钟作为单位
			else {
				cost = Math.round(fast / (1000 * 60)) + " 分钟";
			}
			if (obj instanceof RegatherObjInfo) {
				RTask r = new RTask();
				r.setTaskID(taskId);
				RegatherObjInfo roi = (RegatherObjInfo) obj;
				r.setId(roi.getKeyID() - 10000000);
				r.setCollectTime(time);
				r.setCollectorName(roi.getDescribe());// 这里用collectorName存放任务描述
				r.setCause(cost); // 这里用cause存放运行时间
				rtasks.add(r);
			} else {
				TaskLifeCycleInfo t = new TaskLifeCycleInfo();
				t.setTaskId(taskId);
				t.setDataTime(time);
				t.setTaskDescribe(obj.getDescribe());
				t.setCostTime(cost);
				tasks.add(t);
			}
		}
		req.setAttribute("hasTasks", tasks.size() > 0 ? true : null);
		req.setAttribute("hasRtasks", rtasks.size() > 0 ? true : null);
		req.setAttribute("tasks", tasks);
		req.setAttribute("rtasks", rtasks);
		req.getRequestDispatcher("/page/mainFrm.jsp").forward(req, resp);
	}

	/**
	 * 根据正常任务ID，列出其补采任务（2天之内的，以stamptime排升序）
	 * 
	 * @param taskId
	 *            正常任务的id
	 * @param day
	 *            查询几天内的补采 ，默认2天，传入参数小于1时，为2天。
	 * @return
	 */
	private List<RTask> getRTasks(int taskId, int day) {
		String localHostName = Util.getHostName(); // 本地计算机名

		if (SystemConfig.getInstance().getMRProcessId() != 0) {
			localHostName += "@" + SystemConfig.getInstance().getMRProcessId();
		}
		int limit = day < 1 ? 2 : day;
		String sql = "select * from igp_conf_rtask where collector_name='" + localHostName + "'  and  stamptime>sysdate-" + limit + " and taskid="
				+ taskId + " order by stamptime";
		List<RTask> rtasks = rtaskDao.query(sql);
		return rtasks;
	}

	public static void main(String[] args) {
	}
}

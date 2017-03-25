<%@ page contentType="text/html; charset=utf-8" language="java"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
		<title>main</title>
		<script type="text/javascript" src="/js/jquery.js"></script>
		<script type="text/javascript">
	$(document).ready(function() {
		$("#taskId").focus();
	});
	function trimString(text) {
		return (text || "").replace(/^\s+|\s+$/g, "");
	}
	function checkForm() {
		var taskid = $("#taskId").val();
		taskid = trimString(taskid);
		if (taskid == "") {
			$("#taskId").select();
			$("#taskId").focus();
			return false;
		}
		if (isNaN(taskid)) {
			$("#taskId").select();
			$("#taskId").focus();
			return false;
		}
		if (taskid.indexOf(".") >= 0) {
			$("#taskId").select();
			$("#taskId").focus();
			return false;
		}

		return true;
	}
	function doRefresh() {
		window.location.reload(true);
	}
</script>
		<link href="/css/igp.css" rel="stylesheet" type="text/css" />
		<style type="text/css">
table {
	margin-left: 200px;
	margin-top: 10px;
}

tr {
	BORDER-BOTTOM: #d4d4d4 1px dashed;
	height: 2px;
}

a:link {
	color: #0066cc;
}

a:visited {
	color: #0066cc;
}

a:active {
	color: green;
}
</style>
	</head>

	<body>
		<button style="margin-left: 200px;" onclick=
	doRefresh();
>
			刷新
		</button>
		<table>
			<tr>
				<td colspan="4">
					<b>正在运行的任务</b>
				</td>
			</tr>
			<tr>
				<td width="100">
					任务ID
				</td>
				<td width="300">
					任务描述
				</td>
				<td width="160">
					采集的时间点
				</td>
				<td width="100">
					运行时间
				</td>
				<td width="100">
					&nbsp;
				</td>
			</tr>
			<c:choose>
				<c:when test="${requestScope.hasTasks == null }">
					<tr>
						<td colspan="4" align="center">
							<font color="red">没有正在运行的任务</font>
						</td>
					</tr>
				</c:when>
				<c:otherwise>
					<c:forEach items="${requestScope.tasks}" var="x">
						<tr>
							<td>
								${x.taskId }
							</td>
							<td>
								${x.taskDescribe }
							</td>
							<td>
								${x.dataTime }
							</td>
							<td>
								${x.costTime }
							</td>
							<td>
								<a href="/page/task/taskLifeCycleServlet?taskId=${x.taskId }">监控</a>
							</td>
						</tr>
					</c:forEach>
				</c:otherwise>
			</c:choose>
		</table>
		<br />
		<br />
		<table>
			<tr>
				<td colspan="5">
					<b>正在运行的补采任务</b>
				</td>
			</tr>
			<tr>
				<td width="100">
					ID
				</td>
				<td width="100">
					任务ID
				</td>
				<td width="200">
					任务描述
				</td>
				<td width="160">
					采集的时间点
				</td>
				<td width="100">
					运行时间
				</td>
				<td width="100">
					&nbsp;
				</td>
			</tr>
			<c:choose>
				<c:when test="${requestScope.hasRtasks == null }">
					<tr>
						<td colspan="4" align="center">
							<font color="red">没有正在运行的补采任务</font>
						</td>
					</tr>
				</c:when>
				<c:otherwise>
					<c:forEach items="${requestScope.rtasks}" var="x">
						<tr>
							<td>
								${x.id }
							</td>
							<td>
								${x.taskID }
							</td>
							<td>
								${x.collectorName }
							</td>
							<td>
								${x.collectTime }
							</td>
							<td>
								${x.cause }
							</td>
							<td>
								<a href="/page/task/taskLifeCycleServlet?taskId=${x.taskID }">监控</a>
							</td>
						</tr>
					</c:forEach>
				</c:otherwise>
			</c:choose>
		</table>
		<br />
		<br />
		<form action="/page/task/taskLifeCycleServlet"
			onsubmit="return checkForm()">
			<table>
				<tr style="BORDER-BOTTOM: #d4d4d4 1px none;">
					<td>
						任务ID:
						<input id="taskId" name="taskId" />
					</td>
				</tr>
				<tr style="BORDER-BOTTOM: #d4d4d4 1px none;">
					<td>
						<input type="submit" value="监控此任务" />
					</td>
				</tr>
			</table>
		</form>
	</body>
</html>

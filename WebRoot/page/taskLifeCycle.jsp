<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
		<link href="/css/igp.css" rel="stylesheet" type="text/css" />
		<style type="text/css">
table {
	margin-left: 100px;
	margin-top: 10px;
}

input {
	BORDER-BOTTOM: #000000 1px solid;
	border-top: none;
	border-left: none;
	border-right: none;
	text-align: center;
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
		<script type="text/javascript">
	function showPath(s) {
		String.prototype.replaceAll = function(regText, replaceText) {
			var raRegExp = new RegExp(regText, "g");
			return this.replace(raRegExp, replaceText);
		};
		alert(s.replaceAll(";", ";\n"));
	}
</script>
		<script type="text/javascript" src="/js/jquery.js"></script>
		<script type="text/javascript" src="/js/taskLifeCycle.js"></script>

		<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
		<title>采集任务生命周期</title>
	</head>
	<body>
		<div style="margin-left: 20px; margin-top: 10px;">
			<a href="javascript:" style="margin-left: 100px;" id="btnBack">
				返回 </a>
			<a href="javascript:" style="margin-left: 20px;" id="btnRefresh">
				刷新 </a>
			<input type="hidden" id="taskId" value="${requestScope.taskId}" />
		</div>

		<div style="margin-left: 20px; margin-top: 10px;">
			<c:choose>
				<c:when test="${requestScope.msg != null}">
					<font style="margin-left: 100px;" color="red">
						${requestScope.msg}</font>
				</c:when>
				<c:otherwise>
					<table width="1020">
						<tr>
							<td width="100">
								任务编号
							</td>
							<td width="200">
								任务描述
							</td>
							<td width="160">
								时间点
							</td>
							<td>
								已运行时间
							</td>
						</tr>
						<tr style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;">
							<td>
								${requestScope.task.taskId }
							</td>
							<td>
								${requestScope.task.taskDescribe }
							</td>
							<td>
								${requestScope.task.dataTime }
							</td>
							<td>
								${requestScope.task.costTime }
							</td>
						</tr>
					</table>
					<br />
					<br />
					<span style="margin-left: 100px; margin-top: 10px;"> <b><font
							size="+2">补采任务信息</font> </b>&nbsp;&nbsp;(查询 <input id="txtDay"
							value="${requestScope.day }" maxlength="5" size="2" /> 天内的补采信息)
						<a href="javascript:" id="btnQuery"> 查询 </a> </span>
					<c:choose>
						<c:when test="${requestScope.task.recltsCount==0}">
							<div style="margin-left: 200px; margin-top: 10px;">
								<font color="red"> 无补采任务</font>
							</div>
						</c:when>
						<c:otherwise>
							<table width="1020" id="runningTasks">
								<tr>
									<td colspan="8">
										<b>正在运行的任务</b>
										<a href="javascript:" style="margin-left: 10px;"
											id="showRunning">隐藏 </a>
									</td>
								</tr>
								<tr style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;">
									<td width="80">
										ID
									</td>
									<td width="200">
										补采路径
									</td>
									<td width="150">
										补采时间点
									</td>
									<td width="150">
										时间戳
									</td>
									<td width="80">
										补采类型
									</td>
									<td width="80">
										运行时间
									</td>
									<td width="80">
										补采次数
									</td>
									<td>
										补采原因
									</td>
								</tr>
								<c:forEach items="${requestScope.task.reclts  }" var="x">
									<c:if test="${x.rcltTimes !='未运行' }">
										<tr>
											<td>
												${x.id }
											</td>
											<td title="${x.filePath}">
												<a href="javascript:" onclick="showPath('${x.filePath}')">${x.shortPath
													}</a>
											</td>
											<td>
												${x.collectTime }
											</td>
											<td>
												${x.stampTime }
											</td>
											<td>
												${x.recltType }
											</td>
											<td>
												${x.costTime }
											</td>
											<td>
												${x.rcltTimes }
											</td>
											<td title="${x.cause }">
												${x.cause }
											</td>
										</tr>
									</c:if>
								</c:forEach>
							</table>
							<br />
							<br />
							<table width="1020" id="awaitingTasks">
								<tr>
									<td colspan="6">
										<b>待运行的任务</b><a href="javascript:" style="margin-left: 10px;"
											id="showAwaiting"> 隐藏 </a>
									</td>
								</tr>
								<tr>
									<td width="80">
										ID
									</td>
									<td width="200">
										补采路径
									</td>
									<td width="150">
										补采时间点
									</td>
									<td width="150">
										时间戳
									</td>
									<td width="150">
										预计开始时间
									</td>
									<td width="80">
										补采类型
									</td>
									<td>
										补采原因
									</td>
								</tr>
								<c:forEach items="${requestScope.task.reclts  }" var="x">
									<c:if test="${x.rcltTimes =='未运行' && x.recltStatus == '待运行' }">
										<tr>
											<td>
												${x.id }
											</td>
											<td title="${x.filePath}">
												<a href="javascript:" onclick="showPath('${x.filePath}')">${x.shortPath
													}</a>
											</td>
											<td>
												${x.collectTime }
											</td>
											<td>
												${x.stampTime }
											</td>
											<td>
												${x.startTime }
											</td>
											<td>
												${x.recltType }
											</td>
											<td title="${x.cause }">
												${x.cause }
											</td>
										</tr>
									</c:if>
								</c:forEach>
							</table>
							<br />
							<br />
							<table width="1020" id="finishedTasks">
								<tr>
									<td colspan="6">
										<b>已完成的任务</b><a href="javascript:" style="margin-left: 10px;"
											id="showFinished">显示 </a>
									</td>
								</tr>
								<tr>
									<td width="80">
										ID
									</td>
									<td width="200">
										补采路径
									</td>
									<td width="150">
										补采时间点
									</td>
									<td width="150">
										时间戳
									</td>
									<td width="80">
										补采类型
									</td>
									<td>
										补采原因
									</td>
								</tr>
								<c:forEach items="${requestScope.task.reclts  }" var="x">
									<c:if test="${x.rcltTimes =='未运行' && x.recltStatus == '已完成' }">
										<tr>
											<td>
												${x.id }
											</td>
											<td title="${x.filePath}">
												<a href="javascript:" onclick="showPath('${x.filePath}')">${x.shortPath
													}</a>
											</td>
											<td>
												${x.collectTime }
											</td>
											<td>
												${x.stampTime }
											</td>
											<td>
												${x.recltType }
											</td>
											<td title="${x.cause }">
												${x.cause }
											</td>
										</tr>
									</c:if>
								</c:forEach>
							</table>
						</c:otherwise>
					</c:choose>
				</c:otherwise>
			</c:choose>
		</div>


	</body>
</html>
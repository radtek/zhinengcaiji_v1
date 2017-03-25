<%@ page language="java" pageEncoding="utf-8"%>
<%@page import="util.Util"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<html>
	<head>
		<title>任务列表</title>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
		<link href="/css/igp.css" rel="stylesheet" type="text/css" />
		<script type="text/javascript" src="/js/jquery.js"></script>
		<script type="text/javascript" src="/js/task.js"></script>
	</head>
	<body>
		<form method="post" action="/page/task" name="form" id="taskForm">
			<input type="hidden" value="" name="action" id="action" />
			<input type="hidden" value="${requestScope.result.data.currentPage }"
				name="currentPage" id="currentPage" />
			<input type="hidden" value="${requestScope.result.data.pageCount }"
				name="pageCount" id="pageCount" />
			<input type="hidden" value="" id="taskId" name="taskId" />
			<input type="hidden" value="" id="delFlag" name="delFlag" />
			<input type="hidden" value="<%=Util.getHostName()%>"
				id="collector_name" />
			<input type="hidden" value="1" id="clearCondition"
				name="clearCondition" />
			<table width="100%" border="0">
				<tr>
					<th width="81%" scope="col" height="10"></th>
					<th width="1%" rowspan="2" scope="col"
						style="BORDER-RIGHT: #d4d4d4 1px dashed;"></th>
					<th width="18%" scope="col"></th>
				</tr>
				<tr>
					<td valign="top">

						<!--list start-->
						<table width="100%" style="margin-left: 10px;">
							<tr class="thead-left">
								<td colspan="10">
									<span class="currentTitle">任务管理</span>
								</td>
							</tr>
							<tr class="thead-left">
								<td colspan="10"
									style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;"></td>
							</tr>
							<tr class="thead-left">
								<td colspan="10">
									<button type="button" id="local">
										本机任务
									</button>
									<button type="button" id="usedTaskBtn"
										style="margin-left: 10px;">
										有效任务
									</button>
								</td>
							</tr>
							<tr class="thead-left">
								<td width="3%">
									<input type="checkbox" name="all" id="selAll">
								</td>
								<td width="9%">
									任务编号
								</td>
								<td width="16%">
									描述
								</td>
								<td width="15%">
									采集类型
								</td>
								<td width="10%">
									周期
								</td>
								<td width="14%">
									当前时间点
								</td>
								<td width="15%">
									结束时间点
								</td>
								<td width="9%">
									是否启用
								</td>
								<td width="9%" colspan="2">
									操作
								</td>
							</tr>
							<tr>
								<td colspan="9"
									style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;"></td>
							</tr>
							<c:forEach items="${requestScope.result.data.datas}" var="x">
								<tr id="${x.taskId }">
									<td>
										<input type="checkbox" name="single" class="cheSin">
									</td>
									<td>
										${x.taskId }
									</td>
									<td>
										${x.taskDescribe }
									</td>
									<td>
										${x.collectType.name }
									</td>
									<td>
										${x.collectPeriod.name }
									</td>
									<td>
										${x.sucDataTime }
									</td>
									<td>
										${x.endDataTime }
									</td>
									<td>
										${x.isUsed }
									</td>
									<td>
										<a href="/page/task?action=showDetail&taskId=${x.taskId }"
											class="uptSigle" name="showDetail">详细</a>
										<a href="/page/task?action=modif&taskId=${x.taskId }"
											class="uptSigle">修改</a>
									</td>
								</tr>
							</c:forEach>
							<!-- 
							<tr>
								<td>共计${sessionScope.size }条</td>
								
							</tr> -->
							<tr>
								<td align="right" colspan="9">
									<a href="#" id="firstPage">&lt;&lt;首页</a>&nbsp;&nbsp;&nbsp;
									<a href="#" id="prePage">&lt;&lt;上一页</a>&nbsp;&nbsp;&nbsp;
									<a href="#" id="nextPage">下一页&gt;&gt;</a>&nbsp;&nbsp;&nbsp;
									<a href="#" id="lastPage">末页&gt;&gt;</a>&nbsp;&nbsp;&nbsp;
									当前第${requestScope.result.data.currentPage
									}页/共${requestScope.result.data.pageCount
									}页&nbsp;&nbsp;&nbsp;&nbsp;到第
									<input type="text" size="2" maxlength="5" id="inputPage" />
									页
									<a href="#" id="go">go</a>&nbsp;&nbsp;
									<input id="addTask" value="添加" type="button" />
									<input id="delTask" value="删除" type="button" />
								</td>
							</tr>
						</table>
						<!--list end-->
					</td>
					<td valign="top" style="padding-left: 10px;">
						<table width="100%" border="0">
							<tr valign="top">
								<td>
									<input type="text" name="queryVal" id="queryValue">
								</td>
							</tr>
							<tr>
								<td>
									<select id="queryType">
										<option value="0" selected="selected">
											请选择
										</option>
										<option value="taskId">
											任务号
										</option>
										<option value="collectorName">
											采集机名称
										</option>
										<option value="taskDescribe">
											任务描述
										</option>
										<option value="isUsed">
											状态
										</option>
									</select>
								</td>
							</tr>
							<tr valign="top">
								<td>
									<button type="button" id="query">
										查询
									</button>
									<button type="button" id="advQuery">
										高级查询
									</button>
									<br />
									<br />
									任务号:
									<br />
									<input id="taskLifeCycleTaskId" />
									<br />
									<button id="taskLifeCycleQuery">
										查询生命周期
									</button>
								</td>
							</tr>
						</table>
						<table width="100%" border="0" style="margin-top: 30px;">
							<tr>
								<td>

								</td>
							</tr>
						</table>
					</td>
				</tr>
			</table>
		</form>
	</body>
</html>

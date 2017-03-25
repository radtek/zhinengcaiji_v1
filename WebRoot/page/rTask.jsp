<%@ page contentType="text/html; charset=utf-8" language="java"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions"%>
<html>
	<head>
		<title>补采任务管理</title>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
		<link href="/css/igp.css" rel="stylesheet" type="text/css" />
		<link href="/css/rtask.css" rel="stylesheet" type="text/css"/>
		<script type="text/javascript" src="/js/jquery.js"></script>
		<script language="javascript" charset="gb2312" type="text/javascript"
			src="/js/DateTimeCalendar.js"></script>
		<script type="text/javascript" charset="gb2312" src="/js/rtask.js"></script>
	</head>
	<body>
		<form method="post" action="/RTaskServlet" name="form">
			<input type="hidden" id="hostName" value="${requestScope.hostName }">
			<input type="hidden" name="action" id="action">
			<input type="hidden" name="forwardURL" id="forwardURL">
			<input type="hidden" name="returnURL" id="returnURL">
			<input type="hidden" name="id" id="id">
			<input type="hidden" name="ids" id="ids">
			<input type="hidden" name="page" id="page">
			<!--多个id -->
			<table width="100%" border="0">
				<tr>
					<th width="77%" scope="col">&nbsp;
					</th>
					<th width="3%" rowspan="2" scope="col"
						style="BORDER-RIGHT: #d4d4d4 1px dashed;">&nbsp;
					</th>
					<th width="20%" scope="col">&nbsp;
					</th>
				</tr>
				<tr>
					<td valign="top">
						<!--list start-->
						<table width="100%" style="margin-left: 10px;" id="mainTab">
							<tr>
								<td colspan="11">
									<span class="currentTitle">补采任务管理</span>
								</td>
							</tr>
							<tr>
								<td colspan="11"
									style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;"></td>
							</tr>
							<tr>
								<td colspan="11">
									<button type="button" id="local">
										本机任务
									</button>
									<button type="button" id="newRtask" style="margin-left: 10px;">
										新任务
									</button>
									<button type="button" id="hisRtask" style="margin-left: 10px;">
										历史任务
									</button>
									<button type="button" id="invaliRtask"
										style="margin-left: 10px;">
										失效任务
									</button>
								</td>
							</tr>
							<tr>
								<td colspan="2">
								</td>
								<td>
								</td>
								<td>
								</td>
								<td>
								</td>
								<td>
								</td>
								<td colspan="5">
								</td>
							</tr>
							<tr class="thead-left">
								<td colspan="11" valign="top">
									<table width="100%" style="margin-left: 0px;" id="mainTab">
										<tr>
											<td colspan="3">
											</td>
											<td width="10%">
											</td>
											<td width="12%">
											</td>
											<td width="13%">
											</td>
											<td width="13%">
											</td>
											<td colspan="4">
											</td>
										</tr>
										<tr class="thead-left">
											<td width="2%">
												<input type="checkbox" name="all" id="selAll">
											</td>
											<td width="7%">
												ID
											</td>
											<td width="7%">
												任务号
											</td>
											<td width="8%">
												采集路径
											</td>
											<td width="12%">
												采集时间
											</td>
											<td width="12%">
												记录入库时间
											</td>
											<td width="12%">
												机器名
											</td>
											<td width="5%">
												优先级
											</td>
											<td width="5%">
												状态
											</td>
											<td width="12%">
												预计开始时间
											</td>
											<td width="8%">
												补采原因
											</td>
											<td>
												<a href="#" id="delAll">全删</a>
											</td>
										</tr>
										<tr>
											<td colspan="11"
												style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;"></td>
										</tr>
										<c:forEach items="${requestScope.result.data.datas}"
											var="rTask">
											<c:if test="${rTask.collectStatus eq 0}">
												<c:set var="bgcol" value="" />
											</c:if>
											<c:if test="${rTask.collectStatus eq 3}">
												<c:set var="bgcol" value="#DEF8F5" />
											</c:if>
											<c:if test="${rTask.collectStatus eq -1}">
												<c:set var="bgcol" value="#EEEEEE" />
											</c:if>
											<tr bgcolor="${bgcol}">
												<td>
													<input type="checkbox" name="single" class="cheSin">
												</td>
												<td>
													${rTask.id }
												</td>
												<td>
													${rTask.taskID }
												</td>
												<td>
													${fn:substring(rTask.filePath, 0, 6)}
													<div align="center" class="showDiv">
														<div style="height: 15; width: 100%;" align="right">
															<a href="#"><img src="../images/close.jpg" /> </a>
														</div>
														<div style="width: 100%;" align="left">
															<textarea rows="20" cols="75" readonly="readonly"
																style="width: 100%">${rTask.filePath }</textarea>
														</div>
													</div>
													<a href="#" class="showFilepath">...</a>
												</td>
												<td>
													${rTask.collectTime }
												</td>
												<td>
													${rTask.stampTime }
												</td>
												<td>
													${rTask.collectorName }
												</td>
												<td>
													${rTask.readoptType }
												</td>
												<td>
													<c:if test="${rTask.collectStatus eq 0 }">
														<font color="green">新增</font>
													</c:if>
													<c:if test="${rTask.collectStatus eq 3 }">
														<font color="blue">历史</font>
													</c:if>
													<c:if test="${rTask.collectStatus eq -1 }">
														<font color="red">失效</font>
													</c:if>
												</td>
												<td>
													${rTask.preStartTime }
												</td>
												<td>
													${fn:substring(rTask.cause, 0, 6)}
													<div align="center" class="showDiv">
														<div style="height: 15; width: 100%;" align="right">
															<a href="#"><img src="../images/close.jpg" /> </a>
														</div>
														<div style="width: 100%;" align="left">
															<textarea rows="20" cols="75" readonly="readonly"
																style="width: 100%">${rTask.cause }</textarea>
														</div>
													</div>
													<a href="#" class="showFilepath">...</a>
												</td>
												<td>
													<c:if test="${rTask.collectStatus eq 0 }">
														<a href="#" class="uptSingle">修改</a>
													</c:if>
													<c:if
														test="${rTask.collectStatus eq 3 || rTask.collectStatus eq -1}">
														<a href="#" class="actSingle">激活</a>
													</c:if>
													<a href="#" class="delSingle">删除</a>
												</td>
											</tr>
										</c:forEach>
										<tr>
											<td colspan="11"
												style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;"></td>
										</tr>
										<tr>
											<td colspan="11" align="left">
												${requestScope.result.data.pageInfo}
											</td>
										</tr>
									</table>
								</td>
							</tr>
						</table>
						<!--list end-->
					</td>
					<td style="padding-left: 10px;">
						<table width="100%" border="0">
							<tr valign="top">
								<td align="right">
									<label>
										ID
									</label>
								</td>
								<td align="left" colspan="3">

									<input name="qId" type="text" id="qId"
										onKeyPress="return event.keyCode>=48&&event.keyCode<=57"
										value="${requestScope.qId }" size="15" maxlength="10" />
								</td>
							</tr>
							<tr valign="top">
								<td align="right">
									<label>
										任务号
									</label>
								</td>
								<td align="left" colspan="3">
									<select id="taskId" name="taskId">
										<option value="">
											请选择任务号
										</option>
										<c:forEach var="task" items="${requestScope.tasks}">
											<option title="${task.taskDescribe }" value="${task.taskId }"
												${requestScope.taskId eq task.taskId ? "selected='selected'" : "" }>
												${fn:substring(task.taskDescribe,0,5)}--${task.taskId }
											</option>
										</c:forEach>
									</select>
								</td>
							</tr>
							<tr valign="top">
								<td align="right">
									<label>
										采集机名称
									</label>
								</td>
								<td align="left" colspan="3">
									<input name="collector_name" type="text" id="collector_name"
										value="${requestScope.collector_name }" size="18"
										maxlength="50" />
								</td>
							</tr>
							<tr valign="top">
								<td align="right">
									<label>
										优先级
									</label>
								</td>
								<td align="left" colspan="3">
									<input name="readoptType" type="text" id="readoptType"
										onKeyPress="return event.keyCode>=48&&event.keyCode<=57"
										value="${requestScope.readoptType }" size="15" maxlength="10" />
								</td>
							</tr>
							<tr valign="top">
								<td align="right">
									<label>
										采集时间
									</label>
								</td>
								<td align="left" colspan="3">
									<input name="collectTime" type="text" id="collectTime"
										onfocus="calendar();" value="${requestScope.collectTime }"
										size="18" />
								</td>
							</tr>
							<tr valign="top">
								<td align="right">
									<label>
										状态
									</label>
								</td>
								<td align="left" colspan="3">

									<select id="collectStatus" name="collectStatus">
										<option value="" selected="selected">
											请选择状态
										</option>
										<option value="0" ${requestScope.collectStatus
											eq 0 ? "selected='selected'" : "" }>
											新增任务
										</option>
										<option value="3" ${requestScope.collectStatus
											eq 3 ? "selected='selected'" : "" }>
											历史任务
										</option>
										<option value="-1" ${requestScope.collectStatus
											eq -1 ? "selected='selected'" : "" } >
											失效任务
										</option>
									</select>
								</td>
							</tr>
							<tr valign="top">
								<td align="right">
									<label>
										采集类型
									</label>
								</td>
								<td align="left" colspan="3">

									<select id="collect_type" name="collect_type">
										<option value="" selected="selected">
											请选择类型
										</option>
										<option value="1" ${requestScope.collect_type
											eq 1 ? "selected='selected'" : "" }>
											TELNET
										</option>
										
										<option value="2" ${requestScope.collect_type
											eq 2 ? "selected='selected'" : "" }>
											TCP
										</option>
										<option value="3" ${requestScope.collect_type
											eq 3 ? "selected='selected'" : "" } >
											FTP
										</option>
										<option value="4" ${requestScope.collect_type
											eq 4 ? "selected='selected'" : "" } >
											LOCALFILE
										</option>
										<option value="5" ${requestScope.collect_type
											eq 5 ? "selected='selected'" : "" } >
											DB
										</option>
										<option value="6" ${requestScope.collect_type
											eq 6 ? "selected='selected'" : "" } >
											DBAuto
										</option>
										<option value="60" ${requestScope.collect_type
											eq 60 ? "selected='selected'" : "" } >
											DBAuto2
										</option>
										<option value="8" ${requestScope.collect_type
											eq 8 ? "selected='selected'" : "" } >
											M2000
										</option>
									</select>
								</td>
							</tr>
							<tr valign="top">
								<td align="right">
									<label>
										采集周期
									</label>
								</td>
								<td align="left" colspan="3">

									<select id="collect_period" name="collect_period">
										<option value="" selected="selected">
											请选择周期
										</option>
										<option value="1" ${requestScope.collect_period
											eq 1 ? "selected='selected'" : "" }>
											一直采集
										</option>
										<option value="2" ${requestScope.collect_period
											eq 2 ? "selected='selected'" : "" }>
											按天采集
										</option>
										<option value="3" ${requestScope.collect_period
											eq 3 ? "selected='selected'" : "" }>
											按小时采集
										</option>
										<option value="4" ${requestScope.collect_period
											eq 4 ? "selected='selected'" : "" }>
											按半小时采集
										</option>
										<option value="5" ${requestScope.collect_period
											eq 5 ? "selected='selected'" : "" }>
											按一刻钟采集
										</option>
										<option value="6" ${requestScope.collect_period
											eq 6 ? "selected='selected'" : "" }>
											按4小时采集
										</option>
										<option value="7" ${requestScope.collect_period
											eq 7 ? "selected='selected'" : "" }>
											按5分钟采集
										</option>
										<option value="8" ${requestScope.collect_period
											eq 8 ? "selected='selected'" : "" }>
											按12小时采集
										</option>
										
									</select>
								</td>
							</tr>
							<tr valign="top">
								<td align="right">
									<label>
										sql语句查询：
									</label>
								</td>
								<td align="left" colspan="3">
									<select id="sqlSelect">
										<option selected="selected" value="">
											常用语句
										</option>
										<option value="select * from igp_conf_rtask"
											${requestScope.sqlCondition eq
												"select * from
											igp_conf_rtask" ? "selected='selected'" : "" }>
											查询1
										</option>
										<option value="select * from igp_conf_rtask"
											${requestScope.sqlCondition eq
												"select * from
											igp_conf_rtask" ? "selected='selected'" : "" }>
											查询2
										</option>
										<option value="select * from igp_conf_rtask"
											${requestScope.sqlCondition eq
												"select * from
											igp_conf_rtask" ? "selected='selected'" : "" }>
											查询3
										</option>
									</select>
								</td>
							</tr>
							<tr valign="top">
								<td align="left" colspan="4">
									<textarea rows="5" cols="22" name="sqlCondition"
										id="sqlCondition">${requestScope.sqlCondition }</textarea>
								</td>
							</tr>
							<tr valign="top">
								<td colspan="2" align="right">
									<button type="button" id="query">
										查询
									</button>
								</td>
								<td colspan="2" align="center">
									<button type="button" id="reset">
										重置
									</button>
								</td>
							</tr>
						</table>
						<table width="100%" border="0" style="margin-top: 30px;">
							<tr>
								<td>
									<span class="navLink"> <a href="#" title="点击添加补采任务"
										id="add">&gt;&gt; 添加补采任务</a> </span>
								</td>
							</tr>
						</table>
					</td>
				</tr>
			</table>
		</form>
	</body>
</html>

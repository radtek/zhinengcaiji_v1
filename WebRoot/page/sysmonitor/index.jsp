<%@ page contentType="text/html; charset=utf-8" language="java"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions"%>
<html>
	<head>
		<title>系统监视</title>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
		<link href="/css/igp.css" rel="stylesheet" type="text/css" />
		<script type="text/javascript" src="../../js/jquery.js"></script>
		<script type="text/javascript" language="javascript">
	 	$(document).ready(function(){
	 		function getActiveTasks(){
	 			$("#tasksTab").find("tr").remove("tr[title!=original]");
	 			$.getJSON("/ToysServlet",{'action':"getActiveTasks"},function(json){
	 				if(json!=null&&json!=""){
						$.each(json, function(i,item){
							var str = "<tr><td>"+item.taskId+"</td><td>"+item.des+"</td><td>"+item.lastct+"</td>";
							str+="<td>"+item.coltime+"</td><td>"+item.collector+"</td></tr>"
							$("#lastTr").before(str);
						});
					}
				});
	 		};
			setInterval(getActiveTasks,2000);
	 	});
 </script>
	<body>
		<table width="100%" border="0">
			<tr>
				<th width="65%" scope="col">&nbsp;
					
				</th>
				<th width="3%" rowspan="2" scope="col"
					style="BORDER-RIGHT: #d4d4d4 1px dashed;">&nbsp;
					
				</th>
				<th width="32%" scope="col">&nbsp;
					
				</th>
			</tr>
			<tr>
				<td valign="top">
					<!--left start-->
					<table width="100%" style="margin-left: 10px;" id="mainTab">
						<tr>
							<td colspan="11">
								<span class="currentTitle">正在运行的任务</span>
							</td>
						</tr>
						<tr>
							<td colspan="11"
								style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;"></td>
						</tr>
						<tr class="thead-left">
							<td colspan="11" valign="top">
								<table width="100%" style="margin-left: 0px;" id="tasksTab">
									<tr class="thead-left" title="original" >
										<td width="10%">
											任务号
										</td>
										<td width="25%">
											任务描述
										</td>
										<td width="20%">
											采集时间点
										</td>
										<td width="20%">
											耗时
										</td>
										<td width="20%">
											机器名
										</td>
									</tr>
									<tr title="original">
										<td colspan="10"
											style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;"></td>
									</tr>
									<tr id="lastTr" title="original">
										<td colspan="10"
											style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;"></td>
									</tr>
								</table>
							</td>
						</tr>
					</table>

					<!--left end-->
				</td>
				<td valign="top" style="padding-left: 10px;"><table width="100%" border="0" style="margin-top: 20px;">
			  <tr>
							<td>
								<span class="navLink"><a href="" title="点击进行系统备份">>>
										告警浏览 </a> </span>
							</td>
						</tr>
					</table>
					<table width="100%" border="0" style="margin-top: 20px;">
						<tr>
							<td>
								<span class="navLink"><a href="summaryInterface.jsp"
									title="点击浏览汇总接口表">>> 汇总接口 </a> </span>
							</td>
						</tr>
					</table>
					<table width="100%" border="0" style="margin-top: 20px;">
						<tr>
							<td>
								<span class="navLink"><a href="env.jsp"
									title="点击浏览环境监视信息">>> 环境信息 </a> </span>
							</td>
						</tr>
					</table>
					<table width="100%" border="0" style="margin-top: 20px;">
						<tr>
							<td>
								<span class="navLink"><a href="collectLog.do"
									title="点击浏览系统日志信息">>> 采集日志 </a> </span>
							</td>
						</tr>
					</table>
				</td>
			</tr>
		</table>
	</body>
</html>

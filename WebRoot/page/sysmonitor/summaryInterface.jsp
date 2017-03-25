<%@ page language="java" pageEncoding="utf-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<html>
	<head>
		<META content=no-cache http-equiv=Pragma>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
		<title>汇总接口</title>
		<link href="/css/igp.css" rel="stylesheet" type="text/css" />
		<script language="javascript" src="/js/jquery.js"></script>
		<script language="javascript" src="/js/sysmonitor/summaryInterface.js"></script>
		<style type="text/css">
.searchBox {
	float: left;
	margin-bottom: 5px;
	color: #090;
	font-weight: bold;
}

.thead-left {
	
}

.thead-left TD {
	text-align: left;
}

.currentTitle {
	font-size: 18px;
	font-weight: bold;
}
</style>

	</head>

	<body>
		<form action="/page/sysmonitor/sum" method="post" id="sumForm">
			<input name="action" id="action" value="query" type="hidden" />
			<input name="currentPage" id="currentPage"
				value="${requestScope.result.data.currentPage }" type="hidden" />
			<input name="pageCount" id="pageCount"
				value="${requestScope.result.data.pageCount }" type="hidden" />
			<table width="100%" border="0">
				<tr>
					<th width="78%" scope="col">
						&nbsp;
					</th>
					<th width="2%" rowspan="2" scope="col"
						style="BORDER-RIGHT: #d4d4d4 1px dashed;">
						&nbsp;
					</th>
					<th width="20%" scope="col">
						&nbsp;
					</th>
				</tr>
				<tr>
					<td valign="top">
						<!--list start-->
						<table width="800" style="margin-left: 20px;">
							<tr class="thead-left">
								<td colspan="7" height="60">
									<span class="currentTitle">汇总接口表"LOG_CLT_INSERT" <font
										color="#999999">(只读)</font> </span>
								</td>
							</tr>
							<tr class="thead-left">
								<td>
									任务编号
								</td>
								<td>
									OMC ID
								</td>
								<td>
									clt表名
								</td>
								<td>
									采集数据时间
								</td>
								<td>
									记录入库时间
								</td>
								<td>
									记录数
								</td>
								<td>
									汇总状态
								</td>
							</tr>
							<c:forEach items="${requestScope.result.data.datas }" var="pojo">
								<tr>
									<td>
										${pojo.taskID }
									</td>
									<td>
										${pojo.omcID }
									</td>
									<td>
										${pojo.tbName }
									</td>
									<td>
										${pojo.stampTime }
									</td>
									<td>
										${pojo.count }
									</td>
									<td>
										${pojo.VSysDate }
									</td>
									<td>
										${pojo.calFlag }
									</td>
								</tr>
							</c:forEach>
							<tr>
								<td colspan="7"
									style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;">
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
								</td>
							</tr>

						</table>
						<!--list end-->
					</td>
					<td style="padding-left: 10px;" valign="top">
						<table width="100%" border="0" style="margin-top: 10px;">
							<tr>
								<td>
									<span class="navLink"><a href="index.jsp"
										title="点击返系统监视主界面"> &gt;&gt;返回 </a> </span>
								</td>
							</tr>
						</table>
						<table width="100%" border="0">
							<tr>
								<th scope="col">
									<input type="text" id="selectKeyWord" name="selectKeyWord" class="searchBox" />
								</th>
							</tr>
							<tr>
								<td>
									<select name="selectType" id="selectType">
										<option value="">
											请选择
										</option>
										<option value="omcid">
											OMC ID
										</option>
										<option value="tbName">
											clt表名
										</option>
										<option value="stamptime">
											采集数据时间
										</option>
										<option value="count">
											记录数
										</option>
										<option value="calFlag">
											汇总状态
										</option>
									</select>
									&nbsp;
									<input type="button" id="query" value="查询" />
								</td>
							</tr>
							<tr>
								<td></td>
							</tr>
						</table>
					</td>
				</tr>
			</table>
		</form>
	</body>
</html>
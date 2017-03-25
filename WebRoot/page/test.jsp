<%@ page language="java" pageEncoding="utf-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<html>
	<head>
		<title>My JSP 'addRTask.jsp' starting page</title>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
		<link href="/css/igp.css" rel="stylesheet" type="text/css" />
		<script type="text/javascript" src="../js/jquery.js"></script>
		<script type="text/javascript" language="javascript">
		$(function(){
			
			 //查询任务的路径
			 $("#getTaskFilepath").click(function(){
			 	var taskId = $("form select").val();
			 	
			 	if(taskId=="任务名--任务号"){
			 		alert("请选择任务！");
			 		return;
			 	}
			 	$.post("/RTaskServlet",{executeType:"getTaskFilepath",taskID:taskId},function(data){
			 		alert("data load:"+data);
			 		$("#filePath").val(data);
			 	});
			 });
		});
	</script>
	</head>
	<body>
		<form name="formAdd" action="/RTaskServlet" method="post">
			
			<div class="int">
				<label>
					采集路径
				</label>
				<a href="#" id="getTaskFilepath">&nbsp; &nbsp; &nbsp;查看此任务采集路径</a>
			</div>
			<div class="int">
				<label></label>
				<textarea rows="20" cols="100" id="filePath" name="filePath">${requestScope.filePath }</textarea>
			</div>
			
		</form>
	</body>
</html>

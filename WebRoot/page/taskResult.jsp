<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
		<title>任务操作结果</title>
		<link rel="stylesheet" href="../css/igp.css" type="text/css"></link>
	</head>
	<body>
		<center>
			<br />
			<br />
			<br />
			<br />
			<br />
			${requestScope.result.data }
			<br />
			<a href="${requestScope.result.returnURL }">返回</a>
		</center>
	</body>
</html>
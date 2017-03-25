<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
    	<META content=no-cache http-equiv=Pragma>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
		<title>操作结果</title>
		<link rel="stylesheet" href="/css/igp.css" type="text/css"></link>
	</head>
	<body>
        <table width="300" align="center" style="margin-top:30px;">
        	<tr><td>操作结果</td></tr>
            <tr><td>${requestScope.result.data}</td></tr>
            <tr><td>${requestScope.result.error.code}:${requestScope.result.error.des}</td></tr>
            <tr><td>${requestScope.result.error.cause}</td></tr>
            <tr><td>${requestScope.result.error.action}</td></tr>
            <tr><td><a href="${requestScope.result.returnURL}">返回</a></td></tr>
        </table>
	</body>
</html>
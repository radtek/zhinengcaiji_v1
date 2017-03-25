<%@ page contentType="text/html; charset=utf-8" language="java" import="java.util.*" errorPage="" %>
<%
	/**
	  *缺陷反馈
	  *Author:YangJian
	  *Since: 1.0
	  *2010-6-2
	**/
%>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>缺陷辅助信息收集及反馈</title>
<link href="/css/igp.css" rel="stylesheet" type="text/css" />
<script language="javascript" src="/js/jquery.js" ></script>
<style type="text/css">
<!--
.thead-left{
	
}
.thead-left TD{
	text-align:left;
}
.currentTitle{
	font-size:18px;
	font-weight:bold;
}
-->
</style>
</head>

<body>
<table width="100%" border="0">
  <tr>
    <th width="65%" scope="col">&nbsp;</th>
    <th width="3%" rowspan="2" scope="col" style="BORDER-RIGHT: #d4d4d4 1px dashed;">&nbsp;</th>
    <th width="32%" scope="col">&nbsp;</th>
  </tr>
  <tr>
    <td valign="top">
    <!--list start-->
    <table width="450" style="margin-left:100px;">
    <tr class="thead-left" >
		<td height="60"><span class="currentTitle">缺陷辅助信息收集及反馈</span></td>
	</tr>
	<form action="bug.do">

		<tr class="thead-left" >
	      <!--		
			<td>编号</td>
			</tr>
		 -->  
	    <tr><td style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;"></td></tr>	
	
	    <tr>
	        <td><font size="3">收集采集日志(点击直接下载)</font></td>
	        <td><input type="submit" name="collect" value="采集日志"></td>
	    </tr>
    </form>	 	
</table>
    <!--list end-->
    </td>
    <td style="padding-left:10px;"  valign="top">
      <table width="100%" border="0" style="margin-top:10px;">
        <tr>
          <td><span class="navLink"><a href="index.jsp" title="点击返回系统管理主界面">>> 返回 </a></span></td>
        </tr>
    </table>

    </td>
  </tr>
</table>
</body>
</html>
<%@ page contentType="text/html; charset=utf-8" language="java" import="java.util.*,java.io.File" errorPage="" %>
<%@page import="util.Util"%>
<%
	/**
	  *环境信息
	  *Author:YangJian
	  *Since: 1.0
	  *2010-6-2
	**/
%>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>环境信息</title>
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
		<td height="60"><span class="currentTitle">环境信息一览</span></td>
	</tr>
    
	<tr class="thead-left" ><td><b>计算机名: <%=Util.getHostName()%></b></td></tr>
    <tr><td style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;"></td></tr>		
    <tr><td>操作系统: <%=System.getProperties().get("os.name")%> &nbsp;&nbsp;架构: <%=System.getProperties().get("os.arch")%> &nbsp;&nbsp;版本: <%=System.getProperties().get("os.version")%></td></tr>
    <tr><td height="10"></td></tr>
    
    <tr class="thead-left" ><td><b>磁盘使用情况</b></td></tr>
    <tr><td style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;"></td></tr> 	
    <%
		String color = "#6699FF";
		try
		{
			File[] roots = File.listRoots();
			for (File f : roots)
			{
				float total = f.getTotalSpace();
				float remain = f.getFreeSpace();
				float width = ((total - remain) / total) * 300;
				if (width >= 270)
					color = "#ff0000";
				else
				    color = "#6699FF";
				out.println("<tr><td>" + f.getPath() + "</td></tr>");
				out.println("<tr><td><table style='BORDER: #999999 1px solid; height:10px;' width=300><tr><td width=" + width + " bgcolor=\"" + color + "\"></td><td></td></tr></table></td></tr>");
				out.println("<tr><td>" + (remain / (1024*1024*1024)) + " GB可用,共 " + (total / (1024*1024*1024)) + " GB</td></tr>");
				out.println("<tr><td></td></tr>");
			}
		}
		catch (Exception e)
		{
		}
	%>
    <tr><td height="10"></td></tr>
    
    <tr class="thead-left" ><td><b>虚拟机内存使用情况</b></td></tr>
    <tr><td style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;"></td></tr> 	
    <%
		float maxMemory = Runtime.getRuntime().maxMemory() / (1024*1024);
		float totalMemory = Runtime.getRuntime().totalMemory() / (1024 * 1024);
		float freeMemory = Runtime.getRuntime().freeMemory() / (1024 * 1024);
		float usedMemory = totalMemory - freeMemory;
		freeMemory = maxMemory - usedMemory;
		float len = (usedMemory / maxMemory)*300;
		if (len >= 270)
			color = "#ff0000";
		else
			color = "#6699FF";
		out.println("<tr><td height=30><table style='BORDER: #999999 1px solid; height:10px;' width=300><tr><td width=" + len + " bgcolor=\"" + color + "\"></td><td></td></tr></table></td></tr>");
	%>
    <tr><td>已使用: <%=usedMemory%>M &nbsp;&nbsp;剩余: <%=freeMemory%>M &nbsp;&nbsp;最大内存:<%=maxMemory%>M &nbsp;&nbsp;</td></tr>
    <tr><td></td></tr>
    <tr><td height="10"></td></tr>
    
    <tr class="thead-left" ><td><b>用户: <%=System.getProperties().get("user.name")%></b></td></tr>
    <tr><td style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;"></td></tr> 	
    <tr><td>当前目录: <%=System.getProperties().get("user.dir")%></td></tr>
    <tr><td>用户主目录: <%=System.getProperties().get("user.home")%></td></tr>
    <tr><td height="10"></td></tr>
    
     <tr class="thead-left" ><td><b>JAVA</b></td></tr>
    <tr><td style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;"></td></tr>
    <tr><td>版本: <%=System.getProperties().get("java.version")%> &nbsp;&nbsp;厂商: <%=System.getProperties().get("java.vendor")%></td></tr>
    <tr><td>根目录: <%=System.getProperties().get("java.home")%></td></tr>
    <tr><td>类路径: <br><%=System.getProperties().get("java.class.path")%></td></tr>
    <tr><td height="10"></td></tr>
    
</table>
    <!--list end-->
    </td>
    <td style="padding-left:10px;"  valign="top">
      <table width="100%" border="0" style="margin-top:10px;">
        <tr>
          <td><span class="navLink"><a href="index.jsp" title="点击返回系统监视主界面">>> 返回 </a></span></td>
        </tr>
    </table>

    </td>
  </tr>
</table>
</body>
</html>
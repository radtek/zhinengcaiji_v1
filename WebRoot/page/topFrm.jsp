<%@ page contentType="text/html; charset=utf-8" language="java" %>
<%@page import="util.Util"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<title>Top</title>
<link href="/css/igp.css" rel="stylesheet" type="text/css" />
<script language="javascript" src="/js/jquery.js" ></script>
</head>

<body>
<table width="100%" border="0" height="95">
  <tr>
    <th width="16%" rowspan="3" align="left" scope="col"><img src="/images/igp_logo.jpg" alt="IGP" title="IGP(智能采集平台)" /></th>
    <th width="57%" scope="col">&nbsp;</th>
    <th width="24%" scope="col">&nbsp;</th>
    <th width="3%" scope="col">&nbsp;</th>
  </tr>
  <tr>
    <td style="BORDER-BOTTOM: #d4d4d4 1px dashed;">
	<div class="top-nav-topFrm">
    	<DIV class=top-nav-items>
                <UL>
                <LI class=on><A href="mainFrm.jsp" target="mainFrame">首页</A> </LI>
                <LI><A href="task.jsp" target="mainFrame">任务管理</A> </LI>
                <LI><A href="rTask.jsp" target="mainFrame">补采管理</A> </LI>
                <LI><A href="template.do" target="mainFrame">模板管理</A> </LI>
                <LI><A href="device.do" target="mainFrame">设备管理</A> </LI>
                <LI><A href="sysmonitor/" target="mainFrame">系统监视</A> </LI>
                <LI><A href="sysmgr/" target="mainFrame">系统管理</A> </LI>
                </UL>
        </DIV>
    </div>
    </td>
    <td align="center"><DIV class=top-nav-info>[<%=Util.getHostName()%>] <A href="/auth?exit=exit" rel=nofollow>退出</A></DIV></td>
    <td>&nbsp;</td>
  </tr>
  <tr>
    <td>&nbsp;</td>
    <td>&nbsp;</td>
    <td>&nbsp;</td>
  </tr>
</table>
</body>
</html>

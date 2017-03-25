<%@ page language="java" import="java.util.*" pageEncoding="utf-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
  <head>  
    <title>系统管理</title>
  <style type="text/css">
<!--
.addDeviceTitle {
	font-size: 15px;
	font-weight: bold;
	float: left;
}
-->
  </style>
  </head>
  <link href="/css/igp.css" rel="stylesheet" type="text/css" />
<script language="javascript" src="/js/jquery.js" ></script>
  <style type="text/css">
<!--
.searchBox {
	float: left;
	margin-bottom:5px;
	color:#090;
	font-weight: bold;
}
.thead-left{
	
}
.thead-left TD{
	text-align:left;
}
-->
</style>


 <body>
    <table width="100%" border="0">
  <tr>
    <th width="65%" scope="col">&nbsp;</th>
    <th width="3%" rowspan="2" scope="col" style="BORDER-RIGHT: #d4d4d4 1px dashed;">&nbsp;</th>
    <th width="32%" scope="col">&nbsp;</th>
  </tr>
  <tr>
    <td valign="top">
    <!--left start-->

   
    <!--left end-->
    </td>
    <td valign="top" style="padding-left:10px;">
      <table width="100%" border="0" style="margin-top:10px;">
        <tr>
          <td><span class="navLink"><a href="user.do" title="点击进行系统用户管理">>> 用户管理 </a></span></td>
        </tr>
    </table>
      <table width="100%" border="0" style="margin-top:20px;">
        <tr>
          <td><span class="navLink"><a href="/sysMgrServlet?action=show&forwardURL=page/sysmgr/setting.jsp&returnURL=javascript:history.back();" title="点击进行系统参数设置">>> 参数设置 </a></span></td>
        </tr>
    </table>
    <table width="100%" border="0" style="margin-top:20px;">
        <tr>
          <td><span class="navLink"><a href="vendor.jsp" title="点击查看厂商列表">>> 厂商列表 </a></span></td>
        </tr>
    </table>
    <table width="100%" border="0" style="margin-top:20px;">
        <tr>
          <td><span class="navLink"><a href="backup.jsp" title="点击进行系统备份">>> 系统备份 </a></span></td>
        </tr>
    </table>
    <table width="100%" border="0" style="margin-top:20px;">
        <tr>
          <td><span class="navLink"><a href="tarBug.jsp" title="把系统定位信息反馈给支撑人员">>> 缺陷反馈 </a></span></td>
        </tr>
    </table>
    </td>
  </tr>
</table>
  </body>
</html>

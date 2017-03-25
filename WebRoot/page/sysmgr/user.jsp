<%@ page contentType="text/html; charset=utf-8" language="java" import="java.util.*" errorPage="" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@page import="db.pojo.User,db.pojo.UserGroup,db.dao.UserDAO"%>
<%
	/**
	  *用户管理
	  *Author:YangJian
	  *Since: 1.0
	  *2010-6-1
	**/
%>
<script type="text/javascript">
        function getdelete(){
              var bool= confirm( "确定要删除吗？"); 
             return bool;
        }

</script>
<%
	UserDAO dao = new UserDAO();
	List<User> users = dao.list();
%>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>用户管理</title>
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
    <table width="550" style="margin-left:100px;">
    <tr class="thead-left" >
		<td colspan="4" height="60"><span class="currentTitle">用户管理</span></td>
	</tr>
	<tr class="thead-left" >
		<td width="13%">编号</td>
		<td width="23%">用户名</td>
		<td width="34%">所属分组</td>
        <td width="30%">操作</td>
	</tr>
    <tr><td colspan="4" style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;"></td></tr>		 

     <c:set var="count" value="0" />
      <c:forEach items="${requestScope.result.data}" var="x">
      <c:if test="${x!=null}" >
      	<tr  height=25 style="cursor:pointer;"  onMouseOver="JavaScript:this.style.background='#ffdfc1'" onmouseout="JavaScript:this.style.background='#ffffff'"> 
            <td>${x.id}</td>
            <td>${x.userName}</td>
            <td>${x.group.name}</td>
             <c:if test="${x.userName == 'igp'}" >
                <td>默认分组,不能操作</td>
             </c:if>
             <c:if test="${x.userName != 'igp'}" >
                <td><a href='user.do?id=${x.id}&action=update'>修改</a>  <a href='user.do?id=${x.id}&action=del&forwardURL=../result.jsp&returnURL=user.do' onclick='return getdelete()'>删除</a></td>
             </c:if>
	   </tr> 
       <c:set var="count" value="${pageScope.count+1}" />
       </c:if>
      </c:forEach>
      
      <tr><td colspan=10 style="BORDER-TOP: #d4d4d4 1px dashed; height:2px;">共 <font color='#0000ff'>${pageScope.count}</font> 条</td></tr>
</table>
    <!--list end-->
    </td>
    <td style="padding-left:10px;"  valign="top">
      <table width="100%" border="0" style="margin-top:10px;">
        <tr>
          <td><span class="navLink"><a href="index.jsp" title="点击返回系统管理主界面">>> 返回 </a></span></td>
        </tr>
    </table>
    
    <table width="100%" border="0" style="margin-top:40px;">
        <tr>
          <td><span class="navLink"><a href="userGroup.do?action=userAdd" title="点击进行添加用户操作">>> 添加用户 </a></span></td>
        </tr>
    </table>
    
    <table width="100%" border="0" style="margin-top:10px;">
        <tr>
          <td><span class="navLink"><a href="modifyPwd.jsp" title="点击修改我的密码">>> 密码修改 </a></span></td>
        </tr>
    </table>
    
    <table width="100%" border="0" style="margin-top:10px;">
        <tr>
          <td><span class="navLink"><a href="userGroup.do" title="点击进入用户分组管理">>> 分组管理 </a></span></td>
        </tr>
    </table>
    </td>
  </tr>
</table>
</body>
</html>
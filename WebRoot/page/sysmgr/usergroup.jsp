<%@ page contentType="text/html; charset=utf-8" language="java" import="java.util.*" errorPage="" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@page import="db.pojo.User,db.pojo.UserGroup,db.dao.UserGroupDAO"%>
<%
	/**
	  *用户分组管理
	  *Author:YangJian
	  *Since: 1.0
	  *2010-6-2
	**/
%>
<%
	UserGroupDAO dao = new UserGroupDAO();
	List<UserGroup> userGroups = dao.list();
%>
<script type="text/javascript">
        function getdelete(){
              var bool= confirm( "确定要删除吗？"); 
             return bool;
        }

</script>

<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>用户分组管理</title>
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
    <table width="630" style="margin-left:100px;">
    <tr class="thead-left" >
		<td colspan="5" height="60"><span class="currentTitle">用户分组管理</span></td>
	</tr>
	<tr class="thead-left" >
		<td width="6%">编号</td>
		<td width="12%">分组名称</td>
        <td width="26%">权限</td>
		<td width="40%">描述</td>
        <td width="16%">操作</td>
	</tr>
    <tr><td colspan="5" style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;"></td></tr>	

	 <c:set var="count" value="0" />
      <c:forEach items="${requestScope.result.data}" var="x">
      <c:if test="${x!=null}" >
      	<tr  height=25 style="cursor:pointer;"  onMouseOver="JavaScript:this.style.background='#ffdfc1'" onmouseout="JavaScript:this.style.background='#ffffff'"> 
            <td>${x.id}</td>
            <td>${x.name}</td>
            <td>${x.ids}</td>
            <td>${x.note}</td>
             <c:if test="${x.id==1}" >
                <td>默认分组,不能操作</td>
             </c:if>
             <c:if test="${x.id!=1}" >
                <td><a href='userGroup.do?id=${x.id}&action=update'>修改</a>  <a href='userGroup.do?id=${x.id}&action=del&forwardURL=../result.jsp&returnURL=userGroup.do' onclick='return getdelete()'>删除</a></td>
             </c:if>            
	   </tr> 
	   
	     <tr> 
	      <c:forEach items="${x.users}" var="u">
	            <c:if test="${u!=null}" >
	              <c:set var="uname" value="${u.userName}" />
	               <td> ${uname}</td> 	
	            </c:if>	           
	      </c:forEach>
	      </tr>	     
	       
       <c:set var="count" value="${pageScope.count+1}" />
       </c:if>
      </c:forEach>
      
      <tr><td colspan=10 style="BORDER-TOP: #d4d4d4 1px dashed; height:2px;">共 <font color='#0000ff'>${pageScope.count}</font> 组</td></tr>
		
</table>
    <!--list end-->
    </td>
    <td style="padding-left:10px;"  valign="top">
      <table width="100%" border="0" style="margin-top:10px;">
        <tr>
          <td><span class="navLink"><a href="user.do" title="点击返回用户管理界面">>> 返回 [用户管理] </a></span></td>
        </tr>
    </table>
    
    <table width="100%" border="0" style="margin-top:40px;">
        <tr>
          <td><span class="navLink"><a href="usergroupAdd.jsp" title="点击进行添加用户分组操作">>> 添加分组 </a></span></td>
        </tr>
    </table>

    </td>
  </tr>
</table>
</body>
</html>
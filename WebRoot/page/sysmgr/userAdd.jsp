<%@ page contentType="text/html; charset=utf-8" language="java" import="java.util.*" errorPage="" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%
	/**
	  *添加新用户
	  *Author:YangJian
	  *Since: 1.0
	  *2010-6-1
	**/
%>

<script type="text/javascript">
    function getValidate(){
       var username = document.getElementById("username").value;
       var pwd = document.getElementById("pwd").value;
       if(username ==""){
                alert("用户名不能为空.");
                return false;
        }
        if(pwd ==""){
                alert("密码不能为空.");
                return false;
        }
       return true;    
    }

</script>

<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>添加新用户</title>
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
<SCRIPT type=text/javascript>
$(document).ready(function(){    
		if($("#username").val()){
            $("#pwd").focus();
        } else {
            $("#username").focus();
        }}
);
    
</SCRIPT>
<table width="100%" border="0">
  <tr>
    <th width="65%" scope="col">&nbsp;</th>
    <th width="3%" rowspan="2" scope="col" style="BORDER-RIGHT: #d4d4d4 1px dashed;">&nbsp;</th>
    <th width="32%" scope="col">&nbsp;</th>
  </tr>
  <tr>
    <td valign="top">
    
    <!--form start-->
    <form name="form1" method="post" action="user.do?action=add&forwardURL=../result.jsp&returnURL=user.do">
    <table width="550" style="margin-left:100px;">
    <tr class="thead-left" >
		<td height="60"><span class="currentTitle">添加新用户</span></td>
	</tr>
	<tr class="thead-left" >
		<td>用户名称:</td>
		</tr> 
    <tr class="thead-left" >
      <td>
        <label>
          <input type="text" name="username" id="username"  maxlength="20" tabindex="1">
          </label>        <font color="#999999">长度不能超过 20 位,必填项</font></td>
      </tr>
    <tr class="thead-left" >
      <td height="10"></td>
    </tr>
    <tr class="thead-left" >
		<td>用户密码:</td>
		</tr>
    <tr class="thead-left" >
      <td>
        <label>
          <input type="password" name="pwd" id="pwd"  maxlength="8" tabindex="2">
          </label>
      &nbsp;<font color="#999999">长度不能超过 8 位,必填项</font></td>
      </tr>
    <tr class="thead-left" >
      <td height="10"></td>
    </tr>
    <tr class="thead-left" >
		<td>所属分组:</td>
	</tr>
    <tr class="thead-left" >
      <td>
        <label>
          <select name="group" id="group" tabindex="3" style="width:130;">
            <option value="">请选择分组</option>
           
              <c:forEach items="${requestScope.result.data}" var="g">
                  <c:if test="${g!=null}">
                      <option value="${g.id}"> ${g.name}</option>
                  </c:if>
               </c:forEach>
            </select>
          </label>&nbsp;<font color="#999999">必选项</font>
      </td>
      </tr>
    <tr class="thead-left" >
      <td height="70">
      <label>
        <input type="submit" name="submit" id="submit" value="增加" onClick="return getValidate()">
      </label>
      </td>
     </tr>
	</table>
    </form>
    <!--form end-->
    
    </td>
    <td style="padding-left:10px;"  valign="top">
      <table width="100%" border="0" style="margin-top:10px;">
        <tr>
          <td><span class="navLink"><a href="user.do" title="点击返用户管理主界面">>> 返回 </a></span></td>
        </tr>
    </table>
    
    <table width="100%" border="0" style="margin-top:40px;">
        <tr>
          <td><span class="navLink"><a href="usergroup.jsp" title="点击进入用户分组管理">>> 分组管理 </a></span></td>
        </tr>
    </table>
    </td>
  </tr>
</table>
</body>
</html>
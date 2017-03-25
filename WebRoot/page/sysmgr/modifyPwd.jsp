<%@ page contentType="text/html; charset=utf-8" language="java" import="java.util.*" errorPage="" %>
<%
	/**
	  *密码修改
	  *Author:YangJian
	  *Since: 1.0
	  *2010-6-2
	**/
%>


<script type="text/javascript">
    function getValidate(){
        var bool= confirm( "确定提交修改吗？");
			  if (!bool){
			  	return false;
			  }
       var oldpwd = document.getElementById("oldpwd").value;
       var pwd = document.getElementById("pwd").value;
       var confirmpwd = document.getElementById("confirmpwd").value;
       if(oldpwd ==""){
                alert("旧密码不能为空.");
                return false;
        }
        if(pwd ==""){
                alert("新密码不能为空.");
                return false;
        }
        if(confirmpwd ==""){
                alert("确认密码不能为空.");
                return false;
        }
        if(pwd != confirmpwd){
           alert("新密码两次输入不一致.");
           return false;
        }
       return true;    
    }

</script>

<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>修改我的密码</title>
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
            $("#oldpwd").focus();
        }
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
    <form name="form1" method="post" action="user.do?action=modifyUserPwd&forwardURL=../result.jsp&returnURL=user.do">
    <table width="550" style="margin-left:100px;">
    <tr class="thead-left" >
		<td height="60"><span class="currentTitle">修改我的密码</span></td>
	</tr>
	<tr class="thead-left" >
		<td>旧密码:</td>
		</tr> 
    <tr class="thead-left" >
      <td>
          <label>
             <input type="text" name="oldpwd" id="oldpwd" tabindex="1">
          </label>
       </td>
      </tr>
    <tr class="thead-left" >
      <td height="10"></td>
    </tr>
    <tr class="thead-left" >
		<td>新密码:</td>
		</tr>
    <tr class="thead-left" >
      <td>
        <label>
          <input type="password" name="pwd" id="pwd" tabindex="2">
          </label>
      &nbsp;<font color="#999999">长度不能超过 8 位,必填项</font></td>
      </tr>
    <tr class="thead-left" >
      <td height="10"></td>
    </tr>
    <tr class="thead-left" >
		<td>请再输入一次新密码:</td>
	</tr>
    <tr class="thead-left" >
      <td>
        <label>
          <input type="password" name="confirmpwd" id="confirmpwd" tabindex="3">
          </label>&nbsp;<font color="#999999">和新密码一致,必填项</font>
      </td>
      </tr>
    <tr class="thead-left" >
      <td height="70">
      <label>
        <input type="submit" name="submit" id="submit" value="修改" onClick="return getValidate()">
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
    </td>
  </tr>
</table>
</body>
</html>
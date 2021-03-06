<%@ page contentType="text/html; charset=utf-8" language="java" errorPage="" %>
<%
	/**
	  *添加新用户
	  *Author:yuanxf
	  *Since: 1.0
	  *2010-6-8
	**/
%>

<script type="text/javascript">
    function getValidate(){
       var groupId = document.getElementById("groupId").value;
       var groupName = document.getElementById("groupName").value;
       var ids = document.getElementById("ids").value;
       var note = document.getElementById("note").value;
       if(groupId ==""){
                alert("分组编号不能为空.");
                return false;
        }
        if(groupName ==""){
                alert("分组名称不能为空.");
                return false;
        }
       if(ids ==""){
                alert("分组权限不能为空.");
                return false;
        }
        if(note ==""){
                alert("分组描述不能为空.");
                return false;
        }
       return true;    
    }

</script>

<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>添加新用户组</title>
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
	
            $("#groupId").focus();
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
    <form name="form1" method="post" action="userGroup.do?action=add&forwardURL=../result.jsp&returnURL=userGroup.do">
    <table width="550" style="margin-left:100px;">
    <tr class="thead-left" >
		<td height="60"><span class="currentTitle">添加新用户组</span></td>
	</tr>
	<tr class="thead-left" >
		<td>分组编号:</td>
		</tr> 
    <tr class="thead-left" >
      <td>
        <label>
          <input type="text" name="groupId" id="groupId" maxlength="20" onkeypress="return event.keyCode>=48&&event.keyCode<=57" ondragenter="return false" onpaste="return !clipboardData.getData('text').match(/\D/)" tabindex="1">
          </label>        <font color="#999999">长度不能超过 20 位,必填项</font></td>
      </tr>
    <tr class="thead-left" >
      <td height="10"></td>
    </tr>
    <tr class="thead-left" >
		<td>分组名称:</td>
		</tr>
    <tr class="thead-left" >
      <td>
        <label>
          <input type="text" name="groupName" id="groupName" maxlength="25"  tabindex="2">
          </label>
      &nbsp;<font color="#999999">长度不能超过 25 位,必填项</font></td>
      </tr>

    <tr class="thead-left" >
      <td height="10"></td>
    </tr>
    <tr class="thead-left" >
		<td>分组权限:</td>
		</tr>
    <tr class="thead-left" >
      <td>
        <label>
          <input type="text" name="ids" id="ids" maxlength="256" tabindex="2">
          </label>
      &nbsp;<font color="#999999">长度不能超过 256 位,必填项</font></td>
      </tr>

    <tr class="thead-left" >
      <td height="10"></td>
    </tr>
    <tr class="thead-left" >
		<td>分组描述:</td>
		</tr>
    <tr class="thead-left" >
      <td>
        <textarea name="note" cols="30" maxlength="500" rows="5"></textarea>
      &nbsp;<font color="#999999">长度不能超过 500 位,必填项</font></td>
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
          <td><span class="navLink"><a href="user.do" title="点击返用户管理主界面">>> 返回 [用户管理]</a></span></td>
        </tr>
    </table>
    
    <table width="100%" border="0" style="margin-top:40px;">
        <tr>
          <td><span class="navLink"><a href="userGroup.do" title="点击进入用户分组管理">>> 返回  分组管理 </a></span></td>
        </tr>
    </table>
    </td>
  </tr>
</table>
</body>
</html>
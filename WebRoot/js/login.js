$(document).ready(function() {
	$("#loginForm").submit(function() {
		if ($("#userName").val() == "") {
			alert("\u8bf7\u8f93\u5165\u7528\u6237\u540d");
			$("#userName").focus();
			return false;
		}
		return true;
	});
});
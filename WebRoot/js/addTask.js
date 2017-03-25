$(document).ready(function() {
	$("#back").click(function() {
		var url = $("#returnUrl").val();
		window.location.replace(url == "" ? "/page/task.jsp" : url);
	});
	$("#save").click(function() {
		if (!checkTask(true)) {
			return;
		}
		$("#modifForm").submit();
	});
	var ct = $("#collectType").val();
	if (ct != "") {
		$("select[name=collectType]").find("option").each(function() {
			if (parseInt($(this).val()) == parseInt(ct)) {
				$(this).attr("selected", true);
			}
		});
	}
	var cp = $("#collectPeriod").val();
	if (cp != "") {
		$("select[name=collectPeriod]").find("option").each(function() {
			if (parseInt($(this).val()) == parseInt(cp)) {
				$(this).attr("selected", true);
			}
		});
	}
});
/**
 * 验证任务表单。
 * 
 * @param checkTaskId
 *            是否要验证任务号
 * @return 是否通过验证
 */
function checkTask(checkTaskId) {
	var val = "";
	var input = null;
	// taskId
	if (checkTaskId) {
		val = $("#taskId").val();
		input = $("#taskId");
		if (val == "") {
			alert("\u4efb\u52a1\u53f7\u4e0d\u53ef\u4e3a\u7a7a");
			input.focus();
			return false;
		}
		if (isNaN(val)) {
			alert("\u4efb\u52a1\u53f7\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 1) {
			alert("\u4efb\u52a1\u53f7\u5fc5\u987b\u662f\u6b63\u6574\u6570");
			input.focus();
			return false;
		}
		var hasTask = $.ajax( {
			url : "/page/task?action=findTask&taskId=" + val,
			cache : false,
			async : false
		}).responseText;
		if (hasTask == "1") {
			alert("\u60a8\u8f93\u5165\u7684\u4efb\u52a1\u53f7\u5df2\u5b58\u5728");
			input.focus();
			return false;
		}
	}

	// devId
	val = $("#devId").val();
	input = $("#devId");
	if (val == "") {
		alert("\u8bbe\u5907\u53f7\u4e0d\u53ef\u4e3a\u7a7a");
		input.focus();
		return false;
	}
	if (isNaN(val)) {
		alert("\u8bbe\u5907\u53f7\u5fc5\u987b\u662f\u6570\u5b57");
		input.focus();
		return false;
	}
	if (val.indexOf(".", 0) > 0 || val < 1) {
		alert("\u8bbe\u5907\u53f7\u5fc5\u987b\u662f\u6b63\u6574\u6570");
		input.focus();
		return false;
	}
	var hasDev = $.ajax( {
		url : "/page/task?action=findDevice&devId=" + val,
		cache : false,
		async : false
	}).responseText;
	if (hasDev == "0") {
		alert("\u60a8\u8f93\u5165\u7684\u8bbe\u5907\u53f7\u4e0d\u5b58\u5728");
		input.focus();
		return false;
	}

	// devPort
	val = $("#devPort").val();
	input = $("#devPort");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u8bbe\u5907\u7aef\u53e3\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 1) {
			alert("\u8bbe\u5907\u7aef\u53e3\u5fc5\u987b\u662f\u6b63\u6574\u6570");
			input.focus();
			return false;
		}
		if (val < 1 || val > 65536) {
			alert("\u7aef\u53e3\u53f7\u4e0d\u80fd\u5927\u4e8e65536");
			input.focus();
			return false;
		}
	}

	// proxyDevId
	val = $("#proxyDevId").val();
	input = $("#proxyDevId");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u4ee3\u7406\u8bbe\u5907\u53f7\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 1) {
			alert("\u4ee3\u7406\u8bbe\u5907\u53f7\u5fc5\u987b\u662f\u6b63\u6574\u6570");
			input.focus();
			return false;
		}
		var hasProxyDev = $.ajax( {
			url : "/page/task?action=findDevice&devId=" + val,
			cache : false,
			async : false
		}).responseText;
		if (hasProxyDev == "0") {
			alert("\u4ee3\u7406\u60a8\u8f93\u5165\u7684\u8bbe\u5907\u53f7\u4e0d\u5b58\u5728");
			input.focus();
			return false;
		}
	}

	// proxyDevPort
	val = $("#proxyDevPort").val();
	input = $("#proxyDevPort");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u4ee3\u7406\u8bbe\u5907\u7aef\u53e3\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 1) {
			alert("\u4ee3\u7406\u8bbe\u5907\u7aef\u53e3\u5fc5\u987b\u662f\u6b63\u6574\u6570");
			input.focus();
			return false;
		}
		if (val < 1 || val > 65536) {
			alert("\u4ee3\u7406\u7aef\u53e3\u53f7\u4e0d\u80fd\u5927\u4e8e65536");
			input.focus();
			return false;
		}
	}

	// collectTimeout
	val = $("#collectTimeout").val();
	input = $("#collectTimeout");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u8d85\u65f6\u65f6\u95f4\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 0) {
			alert("\u8d85\u65f6\u65f6\u95f4\u5fc5\u987b\u662f\u5927\u4e8e0\u7684\u6574\u6570");
			input.focus();
			return false;
		}
	}

	// collectTime
	val = $("#collectTime").val();
	input = $("#collectTime");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u91c7\u96c6\u65f6\u95f4\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 0) {
			alert("\u91c7\u96c6\u65f6\u95f4\u5fc5\u987b\u662f\u5927\u4e8e0\u7684\u6574\u6570");
			input.focus();
			return false;
		}
		if (val < 0 || val > 59) {
			alert("\u91c7\u96c6\u65f6\u95f4\u53ea\u80fd\u57280-59\u4e4b\u95f4");
			input.focus();
			return false;
		}
	}

	// shellTimeout
	val = $("#shellTimeout").val();
	input = $("#shellTimeout");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u547d\u4ee4\u8d85\u65f6\u65f6\u95f4\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 0) {
			alert("\u547d\u4ee4\u8d85\u65f6\u65f6\u95f4\u5fc5\u987b\u662f\u5927\u4e8e0\u7684\u6574\u6570");
			input.focus();
			return false;
		}
	}

	// parseTmpId
	val = $("#parseTmpId").val();
	input = $("#parseTmpId");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u89e3\u6790\u6a21\u677f\u53f7\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 0) {
			alert("\u89e3\u6790\u6a21\u677f\u53f7\u5fc5\u987b\u662f\u5927\u4e8e0\u7684\u6574\u6570");
			input.focus();
			return false;
		}
		var hasParse = $.ajax( {
			url : "/page/task?action=findTemplet&tid=" + val,
			async : false,
			cache : false
		}).responseText;
		if (hasParse == "0") {
			alert("\u60a8\u8f93\u5165\u7684\u6a21\u677f\u4e0d\u5b58\u5728");
			input.focus();
			return false;
		}
	}

	// distributeTmpId
	val = $("#distributeTmpId").val();
	input = $("#distributeTmpId");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u5206\u53d1\u6a21\u677f\u53f7\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 0) {
			alert("\u5206\u53d1\u6a21\u677f\u53f7\u5fc5\u987b\u662f\u5927\u4e8e0\u7684\u6574\u6570");
			input.focus();
			return false;
		}
		var hasParse = $.ajax( {
			url : "/page/task?action=findTemplet&tid=" + val,
			async : false,
			cache : false
		}).responseText;
		if (hasParse == "0") {
			alert("\u5206\u53d1\u5165\u7684\u6a21\u677f\u4e0d\u5b58\u5728");
			input.focus();
			return false;
		}
	}

	// sucDataTime
	val = $("#sucDataTime").val();
	input = $("#sucDataTime");
	if (val == "") {
		alert("\u5f53\u524d\u65f6\u95f4\u70b9\u4e0d\u80fd\u4e3a\u7a7a");
		// input.focus();
		return false;
	}

	// sucDataPos
	val = $("#sucDataPos").val();
	input = $("#sucDataPos");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u5f53\u524d\u6570\u636e\u4f4d\u7f6e\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
	}

	// isUsed
	val = $("#isUsed").val();
	input = $("#isUsed");
	if (val != "") {
		if (val != "1" && val != "0") {
			alert("\u201c\u662f\u5426\u542f\u7528\u201d\u53ea\u80fd\u586b0\u62161\uff0c0\u4e3a\u4e0d\u542f\u7528\uff0c1\u4e3a\u542f\u7528");
			input.focus();
			return false;
		}
	}

	// isUpdate
	val = $("#isUpdate").val();
	input = $("#isUpdate");
	if (val != "") {
		if (val != "1" && val != "0") {
			alert("\u201c\u662f\u5426\u66f4\u65b0\u201d\u53ea\u80fd\u586b0\u62161\uff0c0\u4e3a\u4e0d\u66f4\u65b0\uff0c1\u4e3a\u66f4\u65b0");
			input.focus();
			return false;
		}
	}

	// maxCltTime
	val = $("#maxCltTime").val();
	input = $("#maxCltTime");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u91cd\u91c7\u6b21\u6570\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 0) {
			alert("\u91cd\u91c7\u6b21\u6570\u5fc5\u987b\u662f\u5927\u4e8e0\u7684\u6574\u6570");
			input.focus();
			return false;
		}
	}

	// collectTimepos
	val = $("#collectTimepos").val();
	input = $("#collectTimepos");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u91c7\u96c6\u65f6\u95f4\u504f\u79fb\u91cf\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0) {
			alert("\u91c7\u4fe1\u65f6\u95f4\u504f\u79fb\u91cf\u5fc5\u987b\u662f\u6574\u6570");
			input.focus();
			return false;
		}
	}

	// threadSleepTime
	val = $("#threadSleepTime").val();
	input = $("#threadSleepTime");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u7ebf\u7a0b\u4f11\u7720\u65f6\u95f4\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 0) {
			alert("\u7ebf\u7a0b\u4f11\u7720\u65f6\u95f4\u5fc5\u987b\u5927\u4e8e0\u7684\u6574\u6570");
			input.focus();
			return false;
		}
	}

	// blockTime
	val = $("#blockTime").val();
	input = $("#blockTime");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u8d85\u65f6\u65f6\u95f4\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 0) {
			alert("\u8d85\u65f6\u65f6\u95f4\u5fc5\u987b\u5927\u4e8e0\u7684\u6574\u6570");
			input.focus();
			return false;
		}
	}

	// paramRecord
	val = $("#paramRecord").val();
	input = $("#paramRecord");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u8bb0\u5f55\u6570\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 0) {
			alert("\u8bb0\u5f55\u6570\u5fc5\u987b\u5927\u4e8e0\u7684\u6574\u6570");
			input.focus();
			return false;
		}
	}

	// groupId
	val = $("#groupId").val();
	input = $("#groupId");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u7ec4\u7f16\u53f7\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 0) {
			alert("\u7ec4\u7f16\u53f7\u5fc5\u987b\u5927\u4e8e0\u7684\u6574\u6570");
			input.focus();
			return false;
		}
	}

	// parserId
	val = $("#parserId").val();
	input = $("#parserId");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u89e3\u6790\u5668\u7f16\u53f7\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		var hasParser = $.ajax( {
			url : "/page/task?action=findParser&pid=" + val,
			cache : false,
			async : false
		}).responseText;
		if (hasParser == "0") {
			alert("\u60a8\u8f93\u5165\u7684\u89e3\u6790\u5668\u7f16\u53f7\u4e0d\u5b58\u5728");
			input.focus();
			return false;
		}
	}

	// distributorId
	val = $("#distributorId").val();
	input = $("#distributorId");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u5206\u53d1\u5668\u7f16\u53f7\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		var hasDist = $.ajax( {
			url : "/page/task?action=findDistributor&did=" + val,
			cache : false,
			async : false
		}).responseText;
		if (hasDist == "0") {
			alert("\u60a8\u8f93\u5165\u7684\u5206\u53d1\u5668\u7f16\u53f7\u4e0d\u5b58\u5728");
			input.focus();
			return false;
		}
	}

	// redoTimeOffset
	val = $("#redoTimeOffset").val();
	input = $("#redoTimeOffset");
	if (val != "") {
		if (isNaN(val)) {
			alert("\u8865\u91c7\u65f6\u95f4\u504f\u79fb\u91cf\u5fc5\u987b\u662f\u6570\u5b57");
			input.focus();
			return false;
		}
		if (val.indexOf(".", 0) > 0 || val < 0) {
			alert("\u8865\u91c7\u65f6\u95f4\u504f\u79fb\u91cf\u5fc5\u987b\u5927\u4e8e0\u7684\u6574\u6570");
			input.focus();
			return false;
		}
	}

	return true;
}
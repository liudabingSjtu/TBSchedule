<%@page import="com.taobao.pamirs.schedule.ConsoleManager"%>
<%@page import="com.taobao.pamirs.schedule.ScheduleTaskType"%>
<%@page import="java.util.List"%>
<%@ page contentType="text/html; charset=GB2312" %>
<%
	if(ConsoleManager.isInitial() == false){
		response.sendRedirect("config.jsp");
	}
%>

<%
	String isManager = request.getParameter("manager");
%>
<html>
<head>
<title>
Schedule调度管理
</title>
<STYLE type=text/css>

TH{height:20px;color:#5371BA;font-weight:bold;font-size:12px;text-align:center;border:#8CB2E3 solid;border-width:0 1 1 0;background-color:#E4EFF1;}
TD{background-color: ;border:#8CB2E3 1px solid;border-width:0 1 1 0;font-size:12px;}
table{border-collapse:collapse}
</STYLE>

</head>
<body style="font-size:12px;">
<table id="list" border="1" >
<thead>
     <tr>
     	<th>序号</th>
     	<%
     		if ("true".equals(isManager)) {
     	%>
     	<th >管理</th>
		<%
			}
		%>
     	<th>任务类型</th>
     	<th>任务处理Bean</th>
     	<th>任务状态</th>
     	<th>心跳频率(秒)</th>
     	<th>死亡间隔(秒)</th>
     	<th>线程数</th>
     	<th>每次获取数据量</th>
     	<th>每次执行数量</th>
     	<th>没有数据时休眠时长(秒)</th>
     	<th>处理模式</th>
     	<th>每次处理完数据后休眠时间(秒)</th>
    	<th>清除处理域信息时长(小时)</th>
     	<th>执行开始时间</th>
     	<th>执行结束时间</th>
     	<th>自定义参数</th>
     	<th>任务项</th>
     </tr>
     </thead>
     <tbody>
<%
	List<ScheduleTaskType> taskTypes = ConsoleManager.getScheduleDataManager().getAllTaskTypeBaseInfo();
	String taskItems = "";
	for (int i = 0; i < taskTypes.size(); i++) {
		String pauseOrResumeAction = "pauseTaskType";
		String pauseOrResumeActionName = "暂停";
		String stsName = "正常";
		if (ScheduleTaskType.STS_PAUSE.equals(taskTypes.get(i).getSts())) {
			pauseOrResumeAction = "resumeTaskType";
			pauseOrResumeActionName = "恢复";
			stsName = "暂停";
		}
		taskItems = "";
		String[] strs = taskTypes.get(i).getTaskItems() ;
		if (strs != null) {
			for (int j = 0; j < strs.length; j++) {
				if (j > 0) {
					taskItems = taskItems + ",";
				}
				taskItems = taskItems + strs[j];
			}
		}
%>
     <tr onclick="openDetail(this,'<%=taskTypes.get(i).getBaseTaskType()%>')">
     	<td><%=(i + 1)%></td>
     	<%
     		if ("true".equals(isManager)) {
     	%>
     	<td width="120" align="center">
     		<a target="taskDetail" href="taskTypeEdit.jsp?taskType=<%=taskTypes.get(i).getBaseTaskType()%>"  style="color:#0000CD">编辑</a>
     		<a target="taskDetail" href="taskTypeDeal.jsp?action=clearTaskType&taskType=<%=taskTypes.get(i).getBaseTaskType()%>"  style="color:#0000CD">清理</a>
     		<a target="taskDetail" onclick="deleteTaskType('<%=taskTypes.get(i).getBaseTaskType()%>');" href="taskTypeDeal.jsp?action=deleteTaskType&taskType=<%=taskTypes.get(i).getBaseTaskType()%>" style="color:#0000CD">删除</a>
     		<a target="taskDetail" href="taskTypeDeal.jsp?action=<%=pauseOrResumeAction%>&taskType=<%=taskTypes.get(i).getBaseTaskType()%>" style="color:#0000CD"><%=pauseOrResumeActionName%></a>
     	</td>
		<%
			}
		%>
     	<td><%=taskTypes.get(i).getBaseTaskType()%></td>
     	<td><%=taskTypes.get(i).getDealBeanName()%></td>
     	<td><%=stsName%></td>
     	<td><%=taskTypes.get(i).getHeartBeatRate() / 1000.0%></td>
     	<td><%=taskTypes.get(i).getJudgeDeadInterval() / 1000.0%></td>
     	<td><%=taskTypes.get(i).getThreadNumber()%></td>
     	<td><%=taskTypes.get(i).getFetchDataNumber()%></td>
     	<td><%=taskTypes.get(i).getExecuteNumber()%></td>
     	<td><%=taskTypes.get(i).getSleepTimeNoData() == 0 ? "--"
						: taskTypes.get(i).getSleepTimeNoData() / 1000.0%></td>
     	<td><%=taskTypes.get(i).getProcessorType()%></td>
     	<td><%=taskTypes.get(i).getSleepTimeInterval() == 0 ? "--"
						: taskTypes.get(i).getSleepTimeInterval() / 1000.0%></td>
    	<td><%=taskTypes.get(i).getExpireOwnSignInterval()%></td>
     	<td><%=taskTypes.get(i).getPermitRunStartTime() == null ? "--"
						: taskTypes.get(i).getPermitRunStartTime()%></td>
     	<td><%=taskTypes.get(i).getPermitRunEndTime() == null ? "--"
						: taskTypes.get(i).getPermitRunEndTime()%></td>   
		<td><%=taskTypes.get(i).getTaskParameter() == null ? "--"
						: taskTypes.get(i).getTaskParameter()%></td>   
		<td><%=taskItems%></td>
     </tr>
<%
	}
%>
</tbody>
</table>
<br/>

<%
	if ("true".equals(isManager)) {
%>
<a target="taskDetail" href="taskTypeEdit.jsp?taskType=-1"  style="color:#0000CD">创建新任务...</a>
<%
	}
%>
运行期信息：<br/>
<iframe  name="taskDetail"  height="80%" width="100%"></iframe>
</body>
</html>
<script>
var oldSelectRow = null;
function openDetail(obj,baseTaskType){
	if(oldSelectRow != null){
		oldSelectRow.bgColor="";
	}
	obj.bgColor="#FFD700";
	oldSelectRow = obj;
    document.all("taskDetail").src = "taskTypeInfo.jsp?baseTaskType=" + baseTaskType;
}

if(list.rows.length >1){
	list.rows[1].click();
}

function deleteTaskType(baseTaskType){
	//return window.confirm("请确认所有的调度器都已经停止，否则会导致调度器异常！");
		
}
</script>
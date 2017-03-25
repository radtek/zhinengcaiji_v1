CREATE TABLE  "CLT_AM_EVENT_HW" 
   (	"OMCID" NUMBER, 
	"COLLECTTIME" DATE, 
	"STAMPTIME" DATE, 
	"NETWORK_ID" NUMBER, 
	"PRODUCT_NAME" VARCHAR2(200), 
	"NETWORK_TYPE" VARCHAR2(200), 
	"ALARM_SOURCE" VARCHAR2(200), 
	"DEVICE_ALARM_ID" VARCHAR2(200), 
	"ALARM_NAME" VARCHAR2(200), 
	"ALARM_TYPE" VARCHAR2(200), 
	"ALARM_LEVEL" VARCHAR2(200), 
	"ALARM_STATUS" VARCHAR2(200), 
	"HAPPEN_TIME" VARCHAR2(200), 
	"AFFIRM_TIME" VARCHAR2(200), 
	"AFFIRM_WORKER" VARCHAR2(200), 
	"LOCATION_INFO" VARCHAR2(1000), 
	"LINK_FLAG" VARCHAR2(200), 
	"LINK_NAME" VARCHAR2(200), 
	"LINK_TYPE" VARCHAR2(200), 
	"ALARM_FLAG" VARCHAR2(200), 
	"ALARM_ID" VARCHAR2(200), 
	"OBJECT_TYPE" VARCHAR2(200), 
	"BIZ_FLAG" VARCHAR2(200), 
	"ADD_INFO" VARCHAR2(200)
   ) ;
   
   
   CREATE TABLE  "CLT_AM_CURRENT_HISTORY_HW" 
   (	"OMCID" NUMBER, 
	"COLLECTTIME" DATE, 
	"STAMPTIME" DATE, 
	"NETWORK_ID" NUMBER, 
	"PRODUCT_NAME" VARCHAR2(200), 
	"NETWORK_TYPE" VARCHAR2(200), 
	"ALARM_SOURCE" VARCHAR2(200), 
	"DEVICE_ALARM_ID" VARCHAR2(200), 
	"ALARM_NAME" VARCHAR2(200), 
	"ALARM_TYPE" VARCHAR2(200), 
	"ALARM_LEVEL" VARCHAR2(200), 
	"ALARM_STATUS" VARCHAR2(200), 
	"HAPPEN_TIME" VARCHAR2(200), 
	"AFFIRM_TIME" VARCHAR2(200), 
	"CLEAR_TIME" VARCHAR2(200), 
	"AFFIRM_WORKER" VARCHAR2(200), 
	"CLEAR_WORKER" VARCHAR2(200), 
	"LOCATION_INFO" VARCHAR2(1000), 
	"LINK_FLAG" VARCHAR2(200), 
	"LINK_NAME" VARCHAR2(200), 
	"LINK_TYPE" VARCHAR2(200), 
	"ALARM_FLAG" VARCHAR2(200), 
	"ALARM_ID" VARCHAR2(200), 
	"OBJECT_TYPE" VARCHAR2(200), 
	"CLEAR_TYPE1" VARCHAR2(200), 
	"CLEAR_TYPE2" VARCHAR2(200), 
	"BIZ_FLAG" VARCHAR2(200), 
	"ADD_INFO" VARCHAR2(200)
   ) ;
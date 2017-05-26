import org.apache.spark.sql._
import org.apache.spark.sql.types._
import sqlContext.implicits._


//read the file to rdd and do necessary transform
val rdd=sc.textFile("file:/root/test/WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log.gz").map(_.split(" \"|\" \"|\" ")).map(x => x(0).split(" ") ++ Array(x(1)) ++ Array(x(2)) ++ x(3).split(" "))

//specify the schema of each log line
val schema = StructType("timestamp elb client_port backend_port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes request user_agent ssl_cipher ssl_protocol time".split(" ").map(x => StructField(x,StringType,true)))

//transfer the rdd to dataframe 
val df = sqlContext.createDataFrame(rdd.map(s=>Row(s(0), s(1), s(2), s(3), s(4), s(5), s(6), s(7), s(8), s(9), s(10), s(11), s(12), s(13), s(14), s(0).substring(0,20).replace("T", " "))), schema)


//set inactive threshold in seconds for sessionize: e.g: 15min
val thrs = 15*60; 


// convert the original dataframe to sessionized one. 
// 
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lag
import org.apache.spark.sql.functions.lead
import org.apache.spark.sql.functions.cume_dist

// w is the window for partition by client_port
// ws is the window for partition of acumulative sum of session_id
val w = Window.partitionBy("client_port").orderBy("timestamp")
val ws = Window.partitionBy("client_port").orderBy("timestamp").rowsBetween(Long.MinValue, 0)

// solution of question 1) Sessionize the web log by IP
//
//
// df1 is the sessionized dataframe, with column as ("client_port", "timestamp","rn", "time", "prev_time","new_session", "session_id","page_url")
// "client_port" 	: the client ip and port
// "timestamp" 		: original timestamp
// "rn" 			: rank number(number of request) of this ip
// "time" 			: access time in seconds of the ip of the request
// "prev_time"		: previous access time in seconds of the same ip
// "new_session"	: is it considered a new session, 1 : yes ; 0 : no. the threshold is set by "thrs" in front
// "session_id"		: the session number of the same ip 
// "page_url"		: the page url that is requested in the entry

val df1 = df.withColumn("rn", rank.over(w)).withColumn("time", unix_timestamp($"time")).withColumn("prev_time", lag("time", 1).over(w)).withColumn("new_session", when($"time"-$"prev_time"<thrs,0).otherwise(1)).withColumn("session_id",sum($"new_session").over(ws)).withColumn("page_url", split($"request"," ").getItem(1)).select("client_port", "timestamp","rn", "time", "prev_time","new_session", "session_id","page_url")

//count all hits in each ip for each session
val res1 = df1.groupBy("client_port","session_id").count()
//res1.show()


// solution of question 2) Determine the average session time
//
// 
val wst = Window.partitionBy("client_port", "session_id").orderBy("timestamp").rowsBetween(Long.MinValue,Long.MaxValue)
val df2 = df1.withColumn("session_time", max("time").over(wst)-min("time").over(wst))

//explaination: notice that if a client ip request a url for once and stay inactive for the next 15min, it's not a perfect approache to say that this session has session_time = 0 seconds. However, it's acceptable since there is no process pressure on the server side. 


// calculate the average session time for all session. 
val res2 = df2.filter($"new_session"===1).agg(avg($"session_time"))
// res2.show() 
// result: 14.487 seconds 

// Solution of question 3) Determine unique URL visits per session
import org.apache.spark.sql.functions.countDistinct

val res3 = df2.groupBy("client_port","session_id").agg(countDistinct("page_url"))


// Solution of question 4) Find the most engaged users, ie the IPs with the longest session times

// with longest total session time.
val df3 = df2.filter($"new_session"===1).groupBy("client_port").agg(expr("sum(session_time) as total_session_time"))
val df4 = df3.agg(expr("max(total_session_time) as max_session_time"))
val res4= df3.join(df4,df3("total_session_time")===df4("max_session_time"))
//res4.show()
//result: client_port: 54.169.191.85:15462; total_session_time: 3182


//with longest single session time:
val df5 = df2.filter($"new_session"===1).agg(expr("max(session_time) as max_single_session_time"))
val res5 = df2.filter($"new_session"===1).join(df5, df2("session_time")===df5("max_single_session_time"))
//res5.show()
//result : client_port: 203.191.34.178:10400; session_time: 2066

//with maximum request (might be able to find spider):
val df6 = df2.groupBy("client_port").agg(expr("count(page_url) as request_cnt"))
val df7 = df6.agg(expr("max(request_cnt) as max_request_cnt"))
val res6 = df6.join(df7, df6("request_cnt")===df7("max_request_cnt"))
//res6.show()
//result: client_port: 112.196.25.164:55986; request_cnt: 1946
//ps: for this ip, there were 1946 request in 1 session which last 299 second, probably it use some spider to scrap data from the web.




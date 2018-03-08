package writeCSVFile;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import dbConnection.MysqlKonaConnector;
import dbConnection.MysqlSSHConnector;


public class GetData {
	
	
	public static void getDataFromOpenEdx(String studyName) throws Exception {
		
		
		Connection conn = null;
		PreparedStatement ps = null;
		ArrayList<String[]> strArrays = new ArrayList<String[]>();
		Connection konaConnection = null;
		try {
			conn = MysqlSSHConnector.getConnection();
			
//			String sql = "select s.id as anony_table_id, s.anonymous_user_id, s.course_id, s.user_id, t.module_type, t.module_id, t.course_id, "
//					+ "t.created, t.modified, t.student_id, p.state, p.student_module_id  from (edxapp.student_anonymoususerid s join "
//					+ "edxapp.courseware_studentmodule t on s.user_id=t.student_id) right join "
//					+ "edxapp_csmh.coursewarehistoryextended_studentmodulehistoryextended p on p.student_module_id=t.id "
//					+ "group by p.id order by t.created";
			String sql = "SELECT * FROM edxapp_csmh.coursewarehistoryextended_studentmodulehistoryextended";
			ps = conn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			konaConnection = MysqlKonaConnector.getConnection();
			
			while(rs.next()) {
				
				String[] strArray = new String[42];
				
				String state = rs.getString("state") + "";

				// try to get json format data: for state column
				JSONParser parser = new JSONParser();
				JSONObject jobj = null;
				try {
					jobj = (JSONObject) parser.parse(state);
				} catch(Exception e) {
					continue;
				}
				
				JSONObject stateObj = null;
				if(state.indexOf("question_details") == -1) {
					continue;
				}
				
				if(jobj.containsKey("question_details") || state.indexOf("question_details") != -1) {
					if(jobj.get("question_details") != null) {
						stateObj = (JSONObject) parser.parse(jobj.get("question_details").toString());
						System.out.println(stateObj.toString());
						
						// data format:
						/*
						 * {  
							   "question_details":{  
							      "timestamp":"2017-12-22 18:34:43",
							      "session id": "5-sdfsdsdfs-201801041514"
							      "time zone":"US/Central",  -- 3
							      "student response type":"N/A",  --4
							      "student response subtype":"N/A",  --5
							      "tutor response type":"N/A",  --6
							      "tutor response subtype":"N/A",  --7
							      "level":"N/A",  --8
							      "problem name":"N/A",  --9
							      "problem view":"N/A",  --10
							      "problem start time":"N/A",  --11
							      "step name":"N/A",  --12
							      "attempt at step":"N/A",  --13
							      "outcome":"N/A",  --14
							      "selection":"N/A",  --15
							      "action":"N/A",  --16
							      "input":"N/A",  --17
							      "feedback text":"N/A",  --18
							      "feedback classification":"N/A",  --19
							      "help level":"N/A",  --20
							      "total number hints":"N/A",  --21
							      "condition name":"Condition_1",  --22
							      "condition type":"N/A",  --23
							      "kc":"N/A",  --24
							      "kc category":"N/A",  -25
							      "school":"TAMU",  --26
							      "class":"IEC-LAB",  --27
							      "cf_course":"course-v1:University+CS101+2015_T1",  --28
							      "cf_section":"Motion and Speed",  --29
							      "cf_subsection":"Motion",  --30
							      "cf_unit":"Motion",  --31
							      "cf_user_runtime_id":"128",  --32
							      "cf_student_pastel_id":"AGEDE",  --33
							      "cf_question":"N/A",  --34
							      "cf_choices":"N/A",  --35
							      "cf_video_url":"N/A",  --36
							      "cf_video_position":"N/A",  --37
							      "cf_page_id":"09d66db2dd1b44cead0ab1c258af3aa9",  --38
							      "cf_unit_id":"8b9b6ca01d6340c892e76ced728d9439",  --39
							      "cf_action":"page loaded",  --40
							      "cf_result":"N/A"  --41
							   }
							}
						 * 
						 * */
						// 2. session id
						strArray[1] = (String) stateObj.get("session id");
						
						// 3. Timestamp 
						String dateStr = (String) stateObj.get("timestamp");
						if(dateStr == null) {
							continue;
						}
						Date date = new Date();  
				        //注意format的格式要与日期String的格式相匹配  
				        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
				        date = sdf.parse(dateStr); 
				        strArray[2] = dateStr.toString();
				        
						// 4. Time Zone 
						strArray[3] = (String) stateObj.get("time zone");
						
						// 5. Student Response Type
						strArray[4] = (String) stateObj.get("student response type");
						
						// 6. Student Response SubType
						strArray[5] = (String) stateObj.get("student response subtype");
						
						// 7. Tutor Response Type
						strArray[6] = (String) stateObj.get("tutor response type");
						
						// 8. Tutor Response Subtype
						strArray[7] = (String) stateObj.get("tutor response subtype");
						
						// 9. Level()
						strArray[8] = (String) stateObj.get("level");
						
						// 10. Problem Name
						strArray[9] = (String) stateObj.get("problem name");
						
						// 11. Problem View
						strArray[10] = (String) stateObj.get("problem view");
						
						// 12. Step Name
						strArray[11] = (String) stateObj.get("step name");  
						
						// 13. Attempt At Step
						strArray[12] = (String) stateObj.get("attempt at step");
						
						// 14. outcome
						strArray[13] = (String) stateObj.get("outcome");
						
						// 15. Selection
						strArray[14] = (String) stateObj.get("selection");  
						
						//16. Action
						strArray[15] = (String) stateObj.get("action");  
						
						// 17. input
						strArray[16] = (String) stateObj.get("input"); 
						
						//18. Feedback Text
						strArray[17] = (String) stateObj.get("feedback text"); 
						
						// 19. Feedback Classification
						strArray[18] = (String) stateObj.get("feedback classification"); 
						
						// 20. Help Level
						strArray[19] = (String) stateObj.get("help level");  
						
						// 21. Total Num Hints
						strArray[20] = (String) stateObj.get("total number hints");  
						
						// 22. condition name
						strArray[21] = (String) stateObj.get("condition name");
						
						// 23. Condition Type
						strArray[22] = (String) stateObj.get("condition type"); 
						
						// 24. KC ()
						strArray[23] = (String) stateObj.get("kc");  
						
						// 25. KC Category()
						strArray[24] = (String) stateObj.get("kc category");  
						
						// 26. School
						strArray[25] = (String) stateObj.get("school"); 
						
						// 27. Class
						strArray[26] = (String) stateObj.get("class");  // Class  
						
						// 29. cf_course
						strArray[28] = (String) stateObj.get("cf_course");
						
						//30. cf_section
						strArray[29] = (String) stateObj.get("cf_section");
						
						//31. cf_subsection
						strArray[30] = (String) stateObj.get("cf_subsection"); // Course
						
						//32. cf_unit
						strArray[31] = (String) stateObj.get("cf_unit");
						
						//32. cf_user_runtime_id
						strArray[32] = (String) stateObj.get("cf_user_runtime_id");
						
						//33. cf_student_pastel_id
						strArray[33] = (String) stateObj.get("cf_student_pastel_id");
						
						//34. cf_video_url
						strArray[34] = (String) stateObj.get("cf_video_url");
						
						//35. cf_video_position
						strArray[35] = (String) stateObj.get("cf_video_position");
						
						//36. cf_unit_id
						strArray[36] = (String) stateObj.get("cf_page_id");
						
						//39. cf_unit_id
						//strArray[39] = (String) stateObj.get("cf_unit_id");
						
						//37. cf_action
						strArray[37] = (String) stateObj.get("cf_action");
						
						//38. cf_result
						strArray[38] = (String) stateObj.get("cf_result");
						
						//39. cf_question
						strArray[39] = (String) stateObj.get("cf_question");
						
						//40. cf_choices
						strArray[40] = (String) stateObj.get("cf_choices");
						
						//41. cf_sql_number
						if(stateObj.get("cf_seq_number") != null) {
							strArray[41] = (String) stateObj.get("cf_seq_number");
						} else {
							strArray[41] = "Not has seq number yet";
						}
						
						//0. Anony Student id
						String sql1 = "SELECT anonymous_user_id FROM edxapp.student_anonymoususerid where user_id =? and course_id = ?";
						PreparedStatement ps1 = conn.prepareStatement(sql1);
						ps1.setString(1, strArray[32]);
						System.out.println("User id is " + strArray[32]);
						System.out.println("Course id is " + strArray[28]);
						ps1.setString(2, strArray[28]);
						ResultSet rs1 = ps1.executeQuery();
						if(rs1.next()) {
							// 28. cf_study
							strArray[0] = rs1.getString(1);
						}
						
						
						
						String sqlForStudy = "SELECT study_name from studymanagement.study where study_key=(SELECT study_key from studymanagement.study_condition_mapping where condition_key = (SELECT condition_key from studymanagement.conditions where condition_name=?) order by study_key desc limit 1); ";
						PreparedStatement ps2 = konaConnection.prepareStatement(sqlForStudy);
						ps2.setString(1, strArray[21]);
						ResultSet rs2 = ps2.executeQuery();
						if(rs2.next()) {
							strArray[27] = rs2.getString(1);
						} else {
							strArray[27] = "N/A";
						}
						
						//if(!studyName.equals(strArray[27])) {
						//	continue;
						//}
						strArrays.add(strArray);
						
					} 
					
				} else {
					// doesn't contain any useful information, just a default XBlock log record:
					// data like these:
					// {"input_state":{"e8a56ca705584c4794658295f63eedb5_2_1":{}},"seed":1}
					System.out.println("This data doesn't contain any useful information.");
				}
					
			}  
			
			//System.out.println(strArrays.size());
			// write all the data to csv file
			CsvFileGenerater.csvFileGenerate(strArrays);

		
			
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			MysqlSSHConnector.closeConnection(conn, ps);
			
		} finally {
			MysqlSSHConnector.closeConnection(conn, ps);
		}
		
	}
	
	
	
	public static void main(String[] args) throws Exception {
		System.out.println("Start to read the data from tracking log database");
		GetData.getDataFromOpenEdx("2017_Spring_Study");
		
		
	}
}

package dbClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DbConnection {
	private static final Logger log= LogManager.getLogger();
	static Connection dbConn;
	static String faculties[]={"Engineering",
								"Philosophy",
								"Law and administration",
								"Languages"};
	
	static String studentsNameArray[]={"John Smith",
										"Rebecca Milson",
										"George Heartbreaker",
										"Deepika Chopra"};
	
	static String studentsSex[]={"male","female","male","female"};
	static int studentsAge[]={23,27,19,25};
	static int studentsLvl[]={2,3,1,3};

	static String classesName[]={"Introduction to labour law",
			"Graph algorithms","Existentialism in 20th century",
			"English grammar","From Plato to Kant"};
	static int classesFKeys[]={102,100,101,103,101};
	
	static int enrollmentStudentsFk[]={1,1,1,1,2,2,4,4,4};
	static int enrollmentClassesFk[]={1000,1002,1003,1004,1002,1003,1000,1002,1003};
	
	
	

	public static void main(String[] args) throws SQLException {
        try {
			Class.forName("org.hsqldb.jdbcDriver");
			dbConn = DriverManager.getConnection("jdbc:hsqldb:mem:test-db","sa","");  
		} catch (ClassNotFoundException e1) {
			log.error(e1.getMessage()+" "+e1,e1);
		}
		
		createFacultyTable();
		createStudentTable();
		createClassTable();
		createEnrollmentTable();
		
		addRowsInTOStudentTable();
		addRowsToFaculty();
		addRowsInTOClassTable();
		addRowsInTOEnrollmentTable();
		
		/*selectDatasFromFacultyTable();
		selectDatasFromStudentTable();
		selectDatasFromClassTable();
		selectDatasFromEnrollmentTable();*/
		
		zapytanie1();
		zapytanie2();
		zapytanie3();
		zapytanie4();
		zapytanie5();
		zapytanie6();
		zapytanie7();
		
		dbConn.close();
	}
	public static void zapytanie1() throws SQLException {
		PreparedStatement ps=null;
		ResultSet rs = null;
		try{
			String st = "SELECT idStudent,name FROM Student";
			ps = dbConn.prepareStatement(st);
			rs = ps.executeQuery();
			ps.close();
			log.info("\nWynik zapytania 1:");
			while(rs.next()){
				int id = rs.getInt("idStudent");
				String name = rs.getString("name");
				
				log.info("pkey="+id+",name="+name);
			}
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}
	
	public static void zapytanie2() throws SQLException{
		PreparedStatement ps=null;
		ResultSet rs = null;
		try{
			String st= "SELECT idStudent, name FROM STUDENT S "+
							 "WHERE idStudent NOT IN (SELECT fkey_student FROM ENROLLMENT AS E)";
			
			ps = dbConn.prepareStatement(st);
			rs = ps.executeQuery();
			ps.close();
			log.info("\nWynik zapytania 2:");
			while(rs.next()){
				int id = rs.getInt("idStudent");
				String name = rs.getString("name");
				
				log.info("pkey="+id+",name="+name);
			}
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(),e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}		
	}
	public static void zapytanie3() throws SQLException {
		PreparedStatement ps=null;
		ResultSet rs = null;
		try{
			String st = "SELECT s.idStudent,s.name "+ 
						"FROM Student s, Enrollment e "+
						"where s.idStudent = e.fkey_student "+
						"and s.sex='female' and e.fkey_class=1002";
			
			ps = dbConn.prepareStatement(st);
			rs = ps.executeQuery();
			ps.close();
			log.info("\nWynik zapytania 3:");
			while(rs.next()){
				int id = rs.getInt("idStudent");
				String name = rs.getString("name");
				
				log.info("pkey=" + id+",name="+name);
			}
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}
	
	public static void zapytanie4() throws SQLException {
		PreparedStatement ps=null;
		ResultSet rs = null;
		try{	
			String st = "SELECT f.name FROM FACULTY F, CLASS C "+
						"WHERE F.IDFACULTY=C.FKEY_FACULTY "+
						"AND C.IDCLASS = (SELECT idClass FROM Class "+
						"WHERE idClass NOT IN (SELECT fkey_class FROM Enrollment AS E))";
			
			ps = dbConn.prepareStatement(st);
			rs = ps.executeQuery();
			ps.close();
			log.info("\nWynik zapytania 4:");
			while(rs.next()){
				String name = rs.getString("name");
				log.info("name="+name);
			}
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}
	
	public static void zapytanie5() throws SQLException {
		PreparedStatement ps=null;
		ResultSet rs = null;
		try{
			String st = 
					"SELECT MAX(s.age) as age FROM STUDENT S, ENROLLMENT E "+
					"WHERE S.idStudent = E.fkey_student "+
					"AND E.fkey_class= "+
					"(SELECT idClass FROM CLASS "+
					"WHERE name= ?)";
			
			ps = dbConn.prepareStatement(st);
			ps.setString(1, "Introduction to labour law");
			rs = ps.executeQuery();
			ps.close();
			log.info("\nWynik zapytania 5:");
			while(rs.next()){
				int age = rs.getInt("age");
				log.info("age="+age);
			}
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}
	
	public static void zapytanie6() throws SQLException {
		PreparedStatement ps=null;
		ResultSet rs = null;
		try{	
			String st = 
					"SELECT C.NAME FROM CLASS C, ENROLLMENT E "+
					"WHERE C.IDCLASS = E.FKEY_CLASS "+
					"GROUP BY C.NAME "+
					"HAVING COUNT(E.FKEY_CLASS)>=2";
			
			ps = dbConn.prepareStatement(st);
			rs = ps.executeQuery();
			ps.close();
			log.info("\nWynik zapytania 6:");
			while(rs.next()){
				String name = rs.getString("name");
				log.info("name="+name);
			}
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}
	
	
	public static void zapytanie7() throws SQLException {
		PreparedStatement ps=null;
		ResultSet rs = null;
		try{
			String st = 
					"SELECT level,AVG(age) AS SR FROM Student "+
					"GROUP BY level "+
					"ORDER BY level asc";
			
			ps = dbConn.prepareStatement(st);
			rs = ps.executeQuery();
			ps.close();
			log.info("\nWynik zapytania 7:");
			while(rs.next()){
				int level = rs.getInt("level");
				int avg = rs.getInt("SR");
				log.info("level="+level+",avg="+avg);
			}
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}
	
	
	/*public static void selectDatasFromFacultyTable() throws SQLException {
		PreparedStatement ps=null;
		ResultSet rs = null;
		
		try {
			String st = "SELECT * FROM Faculty";
			ps = dbConn.prepareStatement(st);
			rs = ps.executeQuery();
			ps.close();
			log.info("Faculty Table:");
			while(rs.next()){
				int id = rs.getInt("idFaculty");
				String name = rs.getString("name");
				
				log.info(id+" "+name);
			}
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}*/

	public static void addRowsToFaculty() {

		try{
			dbConn.setAutoCommit(false);
			PreparedStatement ps = null;
			String statment="INSERT INTO Faculty(idFaculty,name)VALUES(?,?)";
			ps = dbConn.prepareStatement(statment);
			
			for(int i=0; i<faculties.length; i++){
				ps.setInt(1, 100+i);
				ps.setString(2,faculties[i]);
				ps.addBatch();
			}
			ps.executeBatch();
			dbConn.commit();
			ps.close();
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}
	/*public static void selectDatasFromStudentTable() throws SQLException {
		PreparedStatement ps=null;
		ResultSet rs = null;
		
		String st = "SELECT * FROM Student";
		ps = dbConn.prepareStatement(st);
		rs = ps.executeQuery();
		ps.close();
		log.info("\nStudents Table:");
		while(rs.next()){
			int id = rs.getInt("idStudent");
			String name = rs.getString("name");
			String sex = rs.getString("sex");
			int age = rs.getInt("age");
			int level = rs.getInt("level");
			
			log.info(id+" "+name+" "+sex+" "+age+" "+level);
		}
	}*/
	public static void addRowsInTOStudentTable() {
		try{
			dbConn.setAutoCommit(false);
			PreparedStatement ps = null;
			String statment="INSERT INTO Student(idStudent,name,sex,age,level)"+
							"VALUES(?,?,?,?,?)";
			ps = dbConn.prepareStatement(statment);
			
			for(int i=0; i<studentsNameArray.length; i++){
				ps.setInt(1,i+1);
				ps.setString(2,studentsNameArray[i]);
				ps.setString(3,studentsSex[i]);
				ps.setInt(4,studentsAge[i]);
				ps.setInt(5,studentsLvl[i]);
				ps.addBatch();
			}
			ps.executeBatch();
			dbConn.commit();
			ps.close();
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}
	
	/*public static void selectDatasFromClassTable() throws SQLException {
		PreparedStatement ps=null;
		ResultSet rs = null;
		
		String st = "SELECT * FROM Class";
		ps = dbConn.prepareStatement(st);
		rs = ps.executeQuery();
		ps.close();
		log.info("\nClass Table SELECT ALL:");
		while(rs.next()){
			int id = rs.getInt("idClass");
			int fkey = rs.getInt("fkey_faculty");
			String name = rs.getString("name");
			
			log.info(id+" "+name+" "+fkey);
		}
	}*/
	
	public static void addRowsInTOClassTable() {
		try{
			dbConn.setAutoCommit(false);
			PreparedStatement ps = null;
			String statment="INSERT INTO Class(idClass,fkey_faculty,name)"+
							"VALUES(?,?,?)";
			ps = dbConn.prepareStatement(statment);
			
			for(int i=0; i<classesName.length; i++){
				ps.setInt(1,1000+i);
				ps.setInt(2,classesFKeys[i]);
				ps.setString(3,classesName[i]);
				ps.addBatch();
			}
			ps.executeBatch();
			dbConn.commit();
			ps.close();
			
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}
	
	public static void addRowsInTOEnrollmentTable() {
		try{
			dbConn.setAutoCommit(false);
			PreparedStatement ps = null;
			String statment="INSERT INTO Enrollment(fkey_student,fkey_class)"+
							"VALUES(?,?)";
			ps = dbConn.prepareStatement(statment);
			
			for(int i=0; i<enrollmentStudentsFk.length; i++){
				ps.setInt(1,enrollmentStudentsFk[i]);
				ps.setInt(2,enrollmentClassesFk[i]);
				ps.addBatch();
			}
			ps.executeBatch();
			dbConn.commit();
			ps.close();
			
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}
	
	/*public static void selectDatasFromEnrollmentTable() throws SQLException {
		PreparedStatement ps=null;
		ResultSet rs = null;
		
		String st = "SELECT * FROM Enrollment";
		ps = dbConn.prepareStatement(st);
		rs = ps.executeQuery();
		ps.close();
		log.info("\nStudents Table:");
		while(rs.next()){
			int fkey_student = rs.getInt("fkey_student");
			int fkey_class = rs.getInt("fkey_class");
			log.info(fkey_student+" "+fkey_class);
		}
	}*/
	
	public static void createStudentTable() {
		try {
			PreparedStatement ps = null;
			String statment = "CREATE TABLE IF NOT EXISTS Student("+
								"idStudent INT NOT NULL,"+
								"name VARCHAR(200) NOT NULL,"+
								"sex VARCHAR(7) NOT NULL,"+
								"age INT NOT NULL,"+
								"level INT NOT NULL,"+
								"PRIMARY KEY(idStudent))";
			
			ps = dbConn.prepareStatement(statment);
			boolean rs = ps.execute();
			if(rs)log.info("Utworzono tabele Student");
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}

	public static void createFacultyTable() {
		try {
			PreparedStatement ps = null;
			String statment = "CREATE TABLE IF NOT EXISTS Faculty("+
			"idFaculty INT NOT NULL,"+
			"name VARCHAR(200) NOT NULL,"+
			"PRIMARY KEY(idFaculty))";
			
			ps = dbConn.prepareStatement(statment);
			boolean rs = ps.execute();
			if(rs)log.info("Utworzono tabele Faculty");
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}
	
	public static void createClassTable() {
		try {
			PreparedStatement ps = null;
			String statment = "CREATE TABLE IF NOT EXISTS Class("+
			"idClass INT NOT NULL,"+
			"fkey_faculty INT NOT NULL,"+
			"name VARCHAR(200) NOT NULL,"+
			"PRIMARY KEY(idClass),"+
			"FOREIGN KEY (fkey_faculty) "+
			"REFERENCES Faculty(idFaculty))";
			
			ps = dbConn.prepareStatement(statment);
			boolean rs = ps.execute();
			if(rs)log.info("Utworzono tabele Class");
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}
	
	public static void createEnrollmentTable() {
		try {
			PreparedStatement ps = null;
			String statment = "CREATE TABLE IF NOT EXISTS Enrollment("+
			"fkey_student INT NOT NULL,"+
			"fkey_class INT NOT NULL,"+
			"FOREIGN KEY (fkey_student) "+
			"REFERENCES Student(idStudent),"+
			"FOREIGN KEY (fkey_class) "+
			"REFERENCES Class(idClass))";
			
			ps = dbConn.prepareStatement(statment);
			boolean rs = ps.execute();
			if(rs)log.info("Utworzono tabele Enrollment");
		} catch (SQLException e) {
			log.error(e.getSQLState()+" "+e.getMessage(), e);
		}
		 catch (Exception e) {
				log.error(e.getMessage(),e);
		}
	}
}


import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import oracle.spatial.geometry.JGeometry;
import oracle.sql.STRUCT;


public class hw2 {
    public static void main(String[] args) {
        JDBCSpatial jdbc = new JDBCSpatial();
		//jdbc.initializeDB();
        Connection conn;
        conn = jdbc.createConection();
        //execute the statement pass args and conn
        jdbc.executeQuery(conn, args);

   }
}    

 class JDBCSpatial{
     
     public enum Query_Type {
         window , within , nn , demo ;
     }

     public void executeQuery(Connection conn , String[] args){
         
         if(args.length == 0){
            System.out.println("\nPlease enter correct parameters : window , within or nn!!!");
	        closeDB(conn);
	        System.exit(0);
	}
	 
         try {
             Statement stmt;
             String sql = null ;
             int count = 0 ;
             boolean id_set = false , name_set= false;
             Query_Type query_type = null;
             
             stmt = conn.createStatement();
             
             try{
                query_type = Query_Type.valueOf(args[0]);
             }catch(IllegalArgumentException e){
                 System.out.println("\nIllegel Query type: "+args[0]);
                 closeDB(conn);
                 System.exit(0);
             }
             
             switch(query_type){
                 case window:
            if(args.length != 6){
                System.out.println("\nMissing parameteres dude!!!");
                closeDB(conn);
                System.exit(0);
       	}
	             {
                     id_set = true;
                     if( args[1].equals("firebuilding") ){
             sql =
                     "SELECT b.id from building b , firebuilding f"
                     +" where b.name = f.name" 
                     +" INTERSECT" 
                     +" SELECT b.id from building b"
                     +" WHERE sdo_relate(b.geometry,SDO_geometry"
                     + "(2003,NULL,NULL,SDO_elem_info_array(1,1003,3),SDO_ordinate_array("+args[2]+","+args[3]+","+args[4]+","+args[5]+")),"
                     + "'mask=inside')='TRUE'" ;
                     
                 }
                     else{
             sql =
                     "SELECT "+args[1]+".id from "+args[1]
                     +" WHERE sdo_relate("+args[1]+".geometry,SDO_geometry"
                     + "(2003,NULL,NULL,SDO_elem_info_array(1,1003,3),SDO_ordinate_array("+args[2]+","+args[3]+","+args[4]+","+args[5]+")),"
                     + "'mask=inside')='TRUE'" ;
                         
                     }
                 }
                     break;

                 
                 case within:
              if(args.length != 4){
                System.out.println("\nMissing parameteres dude!!!");
                closeDB(conn);
                System.exit(0);
       	}
                     id_set = true;
                     if( args[1].equals("firebuilding") ){
             sql =
                     "SELECT b.id from building b , firebuilding f"
                     +" where b.name = f.name" 
                     +" INTERSECT" 
                     +" SELECT  b1.id"
                    +" FROM building b1 , building b2"
                    +" WHERE b2.Name = '"+args[2]+"'"
                    +" AND SDO_WITHIN_DISTANCE( b1.geometry, b2.geometry , 'distance ="+ args[3]+"') = 'TRUE'"
                     +"AND b1.Name <> '"+args[2]+"'";
                     
                 }
                     else if (args[1].equals("building")){
             sql =
                    "SELECT  b1.id"
                    +" FROM "+args[1]+" b1 , building b2"
                    +" WHERE b2.Name = '"+args[2]+"'"
                    +" AND SDO_WITHIN_DISTANCE( b1.geometry, b2.geometry , 'distance ="+args[3]+"') = 'TRUE'"
                      +"AND b1.Name <> '"+args[2]+"'";;
                         
                     }
                     else if (args[1].equals("firehydrant")){
             sql =
                    "SELECT  b1.id"
                    +" FROM "+args[1]+" b1 , building b2"
                    +" WHERE b2.Name = '"+args[2]+"'"
                    +" AND SDO_WITHIN_DISTANCE( b1.geometry, b2.geometry , 'distance ="+ args[3]+"') = 'TRUE'";
                   }
                     
                     break;
                 
                 case nn:
		if(args.length != 4){
                       System.out.println("\nMissing parameteres dude!!");
                       closeDB(conn);
                       System.exit(0);
           	}

                     id_set = true;
                     int o_extraF = Integer.parseInt(args[3])+1;
                     
               if( args[1].equals("firebuilding") ){
                    sql = "Select ID from (SELECT b2.id, SDO_GEOM.SDO_DISTANCE(b2.geometry, b1.geometry, 0.005)"
                        +" FROM building b1, building b2"
                        +" WHERE b1.id = '"+args[2]+"' AND"
                        +" b2.name" 
                        +" IN(select b3.name from building b3 , firebuilding fb where fb.name=b3.name)"
                        + "AND b2.id <> '"+args[2]+"'" 
                        +" order by 2 asc)"
                        +" where rownum <="+args[3];
               }
                     else if(args[1].equals("building")){
                     int o_extraB = Integer.parseInt(args[3])+1;
                     sql = "SELECT b2.id"
                            +" FROM building b1 ,"+args[1]+" b2"
                            +" WHERE b1.id = '"+args[2]+"'"
                            +" AND SDO_NN(b2.geometry,b1.geometry , 'sdo_num_res="+o_extraB+"') = 'TRUE'"
                             + "AND b2.id <> '"+args[2]+"'";
                     }
                     
                     else if(args[1].equals("firehydrant")){
                    sql = "SELECT f.id"
                            +" FROM building b1 ,"+args[1]+" f"
                            +" WHERE b1.id = '"+args[2]+"'"
                            +" AND SDO_NN(f.geometry,b1.geometry , 'sdo_num_res="+args[3]+"') = 'TRUE'";

                     }
                     
                     break;

                 case demo:  
              if(args.length != 2){
                System.out.println("\nMissing parameteres dude!!");
                closeDB(conn);
                System.exit(0);
       	}
             switch (args[1]) {
                 case "1":
                    name_set = true;
                     sql = "select b.name"
                             +" from building b"
                             +" where b.name like 'S%' AND"
                             +" b.name NOT IN (select name from firebuilding)" ;
                     break;
                 case "2":
                     sql =  "select /*+ LEADING(b) INDEX(f fire2_spatial_idx) */ b.name ,f.id "
                             +" from building b,firehydrant f"
                             +" where SDO_NN(f.geometry, b.geometry ,'sdo_num_res=5')= 'TRUE'"
                             +" AND b.id IN ("
                             +" select i.id"
                             +" from firebuilding fb,building i "
                             +" where i.name = fb.name )";
                     break;
                 case "3":
                     id_set = true;
                     sql = "select f1.id"
                             +" from firehydrant f1, building b1"
                             +" where SDO_WITHIN_DISTANCE(b1.geometry,f1.geometry,'distance =  120 ')='TRUE'"
                             +" group by(f1.id) having count(b1.name) ="
                             +" ("
                             +" select max(count(b.name))"
                             +" from firehydrant f, building b"
                             +" where SDO_WITHIN_DISTANCE(b.geometry,f.geometry,'distance =  120 ')='TRUE'"
                             +" group by (f.id) )";
                     break;
                 case "4":
                     sql = "select * from("
                             +" select f.id AS FireHydrant_ID, count(f.id) AS RNN"
                             +" from building b , firehydrant f"
                             +" where SDO_NN(f.geometry, b.geometry ,'sdo_num_res=1')= 'TRUE'"
                             +" group by (f.id) order by count(f.id) desc"
                             +") where rownum <= 5";
                     break;
                 case "5":
                     sql
                              = "SELECT SDO_AGGR_MBR(geometry) FROM building "
                             +" where name LIKE '%HE'";
                     break;
                 default:
                     System.out.println("There is no query number "+args[1]+" exist!!!");
					 closeDB(conn);
					 System.exit(0);
             }
 			 
         }
    
             
             ResultSet rs = stmt.executeQuery( sql ) ;
             
             
             while(rs.next()){
                 if( id_set ){
                    String b_id = rs.getString("id");
                    System.out.println("\n"+query_type+":"+args[1]+ " Id is: "+ b_id);
                 }
                 if(name_set){
                    String b_name = rs.getString("name");
                    System.out.println("Name is : "+ b_name);
                 }

                 if(args[0].equals("demo") && args[1].equals("2")){
                    String d2_id = rs.getString("id");
                    String d2_name = rs.getString("name");
                    if(count == 0) System.out.println("  F_ID   B_Name\n");
                    System.out.format("%5s%7s \n" , d2_id, d2_name);
                 
                 }
                 
                 if(args[0].equals("demo") && args[1].equals("4")){
                    String d4_id = rs.getString(1);
                    String d4_RNN = rs.getString(2);
                    if(count == 0) System.out.println("  F_ID     RNN\n");
                    System.out.format("%5s%7s \n",d4_id,d4_RNN );
                     
                 }

                 if(args[0].equals("demo") && args[1].equals("5")){
                     double[] o_array = null;
                     STRUCT st = (oracle.sql.STRUCT) rs.getObject(1);
                     JGeometry j_geom = JGeometry.load(st);
                     o_array = j_geom.getOrdinatesArray();
                     System.out.println("Lower Left Coardinates:( "+o_array[0]+" , "+o_array[1]+") \nUpper Right Cooardinates: ( "+o_array[2]+" , "+o_array[3]+")");
                 }
                 
 
  		 count++;
             }
             if(count == 0) {
                 System.out.println(" no rows selected");
             }
             else{
                 System.out.println("\n"+count+" rows selected");
             }
             id_set = false;
             name_set  = false;

         } catch (SQLException ex) {
             System.out.println(" Error in your SQL !!: "+ ex);
         }
            closeDB(conn);
     }
     
     
     public Connection createConection(){
         Connection conn = null;
        final String DB_URL = "jdbc:oracle:thin:@localhost:1521:orcl";
        final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
        final String USER = "system";
        final String PASS = "oracle";
        // Register the JDBC driver
        try {
            
        Class.forName(JDBC_DRIVER);
        System.out.println("Oracle JDBC Driver Registered!");
            
            //open connection
        System.out.println("Connecting to database....");
        conn = DriverManager.getConnection(DB_URL,USER,PASS);
		System.out.println("WOW!! Database Connected!!!");
    }   catch (SQLException | ClassNotFoundException ex) {
	        System.out.println("Something fishy!!!"+ex);
        }
         return conn;
     }   
     
    public void  initializeDB(){
        Connection conn;
        Statement stmt = null;
        final String DB_URL = "jdbc:oracle:thin:@localhost:1521:orcl";
        final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
        final String USER = "system";
        final String PASS = "oracle";
         BufferedReader building = null , firehydrant = null , firebuilding = null;
                    // Register the JDBC driver
        try {
            
        Class.forName(JDBC_DRIVER);
        System.out.println("Oracle JDBC Driver Registered!");
            
            //open connection
        System.out.println("Connecting to database");
        conn = DriverManager.getConnection(DB_URL,USER,PASS);
        stmt = conn.createStatement();
                //Connection conn;
        System.out.println("Creating statement ...");
            String sql = null;
        
            sql= "CREATE TABLE building"+
                 " (ID VARCHAR2(32) ,"+ 
                 "NAME VARCHAR2(32),"+ 
                 "VERTEX integer,"+ 
                 "GEOMETRY SDO_GEOMETRY,"
                 + "PRIMARY KEY(ID))";
            stmt.executeUpdate(sql);
            sql=
             "CREATE TABLE firehydrant"+
             "(ID VARCHAR2(32),"+ 
             "GEOMETRY SDO_GEOMETRY,"
              + "PRIMARY KEY(ID))";
           stmt.executeUpdate(sql);
           sql = "CREATE TABLE firebuilding"+
                   "(NAME varchar2(32))";
           stmt.executeUpdate(sql);
           
        firehydrant = new BufferedReader(new FileReader("C:\\Users\\Lavish\\Documents\\hw2\\firehydrant.xy"));
        building = new BufferedReader(new FileReader("C:\\Users\\Lavish\\Documents\\hw2\\building.xy"));
        firebuilding = new BufferedReader(new FileReader("C:\\Users\\Lavish\\Documents\\hw2\\firebuilding.txt"));
 
        String read = null;
	    int count = 0;
    
        
        while((read = firebuilding.readLine()) != null ){
           int updated = 0; 
           sql = "INSERT INTO firebuilding VALUES('"
                   +read+"')" ;
           updated = stmt.executeUpdate(sql);
		   count++;
		   System.out.println("Number of Rows Updated/Inserted - firehbuilding "+count);
           
        }
		count = 0;
             // for fetching data from firehyrant.xy
            while ((read = firehydrant.readLine()) != null) {
               String ordinates = ""; 
               String fireHydrantID = "" ;
               int updated = 0;
	           
               String[] splited = read.split(",");
               ordinates = splited[1]+ "," + splited[2] ;
               fireHydrantID = splited[0].trim();
                sql = "INSERT INTO firehydrant VALUES('"
                        +fireHydrantID+"',"+
                        "SDO_GEOMETRY( "+
                        "2001,"+
                        "NULL,"+
                        "SDO_POINT_TYPE("+ordinates+", NULL),"+
                        "NULL,"+
                        "NULL))";

                updated = stmt.executeUpdate(sql);
				count++;
                System.out.println("Number of Rows Updated/Inserted - firehydrant "+count);
            }       
			count = 0;			
        // for fetching data from building.xy
            while ((read = building.readLine()) != null) {
                String ordinates = "";
                String buildingID = "";
                String buildingName = "";
                int updated =0;
                int XYPoints = 0; 
				
                String[] splited = read.split(",");
                XYPoints = (Integer.parseInt(splited[2].trim()));
                int totalPoints = 2* XYPoints;
                for(int i=0 ; i< totalPoints; i++){
                    ordinates +=  splited[i+3] + ", " ;
                }
                ordinates += splited[3]+ "," + splited[4];
                   
                buildingID = splited[0].trim();
                buildingName = splited[1].trim();
                // Execute the Query
        
                //String sql;
                sql = "INSERT INTO building VALUES(" +
                        "'"+buildingID+"'" + ","  +
                        "'"+buildingName+"'," +
                        XYPoints+"," +
                        "SDO_GEOMETRY(" +
                        "2003," +
                        "NULL," +
                        "NULL," +
                        "SDO_ELEM_INFO_ARRAY(1,1003,1)," +
                        "SDO_ORDINATE_ARRAY("+ordinates+")" +
                        ")" +
                        ")";
    
                    updated = stmt.executeUpdate(sql);
					count++;
                    System.out.println("Number of Rows Updated/Inserted - buildings"+count);
            }
            
                conn.close();
                 //   stmt = conn.createStatement();
                } catch (Exception ex) {
                    //Logger.getLogger(USCGUIBuiding.class.getName()).log(Level.SEVERE, null, ex);
                    System.out.println(ex);
        }
                
      
    } 
    public boolean closeDB(Connection conn){
        try {
            conn.close();
        } catch (SQLException ex) {
        System.out.println("\nOOPs something went wrong while closing DB "+ex);
        }
        
      return true;  
    }
}


    
    


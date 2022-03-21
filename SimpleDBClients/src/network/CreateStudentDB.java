package network;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

import simpledb.jdbc.network.NetworkDriver;

public class CreateStudentDB {
    public static void main(String[] args) {
        Driver d = new NetworkDriver();
        String url = "jdbc:simpledb://localhost";

        try (Connection conn = d.connect(url, null);
             Statement stmt = conn.createStatement()) {
            String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
            stmt.executeUpdate(s);
            System.out.println("Table STUDENT created.");

            s = "create index sid on student(sid) using hash";
            stmt.executeUpdate(s);
            System.out.println("hash index on sid created.");

            s = "create index maj on student(majorid) using btree";
            stmt.executeUpdate(s);
            System.out.println("hash index on sid created.");
            s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
            String[] studvals = {"(1, 'Marion', 4, 2024)",
                "(2, 'Selia', 3, 2025)",
                "(3, 'Veriee', 8, 2024)",
                "(4, 'Kennan', 4, 2023)",
                "(5, 'Bronnie', 6, 2022)",
                "(6, 'Mitchael', 4, 2024)",
                "(7, 'Sheilah', 9, 2022)",
                "(8, 'Vale', 2, 2024)",
                "(9, 'Mariya', 7, 2022)",
                "(10, 'Bruis', 9, 2024)",
                "(11, 'Stephine', 2, 2024)",
                "(12, 'Belinda', 3, 2024)",
                "(13, 'Max', 2, 2024)",
                "(14, 'Yorker', 5, 2023)",
                "(15, 'Nerty', 7, 2023)",
                "(16, 'Wylie', 10, 2024)",
                "(17, 'Chrisy', 5, 2025)",
                "(18, 'Elbertine', 6, 2024)",
                "(19, 'Beatrisa', 9, 2024)",
                "(20, 'Lew', 2, 2023)",
                "(21, 'Estella', 8, 2022)",
                "(22, 'Adena', 1, 2023)",
                "(23, 'Abramo', 2, 2023)",
                "(24, 'Bree', 10, 2022)",
                "(25, 'Ninnette', 3, 2023)",
                "(26, 'Lianne', 4, 2022)",
                "(27, 'Valdemar', 2, 2022)",
                "(28, 'Ezmeralda', 9, 2024)",
                "(29, 'Valene', 4, 2024)",
                "(30, 'Gavra', 5, 2023)",
                "(31, 'Charity', 3, 2023)",
                "(32, 'Darsie', 1, 2023)",
                "(33, 'Celestyn', 3, 2025)",
                "(34, 'Damien', 4, 2024)",
                "(35, 'Fifi', 10, 2023)",
                "(36, 'Reed', 5, 2024)",
                "(37, 'Annmarie', 10, 2025)",
                "(38, 'Willie', 4, 2023)",
                "(39, 'Analiese', 8, 2024)",
                "(40, 'Carmelle', 6, 2025)",
                "(41, 'Paolo', 3, 2022)",
                "(42, 'Cammy', 10, 2022)",
                "(43, 'Hi', 9, 2025)",
                "(44, 'Roch', 8, 2025)",
                "(45, 'Maurene', 8, 2025)",
                "(46, 'Wildon', 7, 2022)",
                "(47, 'Cash', 1, 2024)",
                "(48, 'Cristal', 7, 2023)",
                "(49, 'Rozelle', 10, 2024)",
                "(50, 'Godfree', 6, 2023)",
                "(51, 'Tanya', 4, 2023)",
                "(52, 'Druci', 10, 2024)",
                "(53, 'Hastie', 9, 2025)",
                "(54, 'Warren', 8, 2024)",
                "(55, 'Leanna', 7, 2022)",
                "(56, 'Berk', 3, 2025)",
                "(57, 'Nico', 3, 2024)",
                "(58, 'Kary', 3, 2022)",
                "(59, 'Engracia', 9, 2023)",
                "(60, 'Shea', 7, 2022)",
                "(61, 'Lockwood', 5, 2022)",
                "(62, 'Maure', 5, 2023)",
                "(63, 'Seka', 5, 2024)",
                "(64, 'Wayne', 3, 2025)",
                "(65, 'Birgit', 2, 2022)",
                "(66, 'Debby', 7, 2022)",
                "(67, 'Cyrillus', 6, 2023)",
                "(68, 'Audra', 8, 2025)",
                "(69, 'Mariette', 1, 2022)",
                "(70, 'Cooper', 10, 2025)",
                "(71, 'Ossie', 7, 2023)",
                "(72, 'Ivory', 6, 2025)",
                "(73, 'Correna', 6, 2022)",
                "(74, 'Johnette', 3, 2022)",
                "(75, 'Thaddus', 5, 2024)",
                "(76, 'Diego', 5, 2025)",
                "(77, 'Evin', 6, 2025)",
                "(78, 'Alfonso', 5, 2022)",
                "(79, 'Richard', 5, 2022)",
                "(80, 'Jaymie', 5, 2025)",
                "(81, 'Dina', 6, 2025)",
                "(82, 'Clare', 5, 2023)",
                "(83, 'Rip', 9, 2022)",
                "(84, 'Vassily', 4, 2025)",
                "(85, 'Aeriell', 9, 2022)",
                "(86, 'Chandra', 2, 2022)",
                "(87, 'Giavani', 10, 2022)",
                "(88, 'Chad', 5, 2025)",
                "(89, 'Cybill', 1, 2024)",
                "(90, 'Elli', 6, 2023)",
                "(91, 'Shea', 6, 2022)",
                "(92, 'Aurelea', 1, 2025)",
                "(93, 'Jared', 8, 2023)",
                "(94, 'Tiertza', 1, 2022)",
                "(95, 'Kalindi', 6, 2022)",
                "(96, 'Burnard', 3, 2023)",
                "(97, 'Scotty', 3, 2022)",
                "(98, 'Garek', 3, 2022)",
                "(99, 'Kip', 10, 2025)",
                "(100, 'Harcourt', 2, 2025)"
            };

            for (int i = 0; i < studvals.length; i++)
                stmt.executeUpdate(s + studvals[i]);
            System.out.println("STUDENT records inserted.");

            s = "create table DEPT(DId int, DName varchar(8))";
            stmt.executeUpdate(s);
            System.out.println("Table DEPT created.");

            s = "insert into DEPT(DId, DName) values ";
            String[] deptvals = {"(1, 'Skiba')",
                "(2, 'Tavu')",
                "(3, 'Roombo')",
                "(4, 'Muxo')",
                "(5, 'Voonyx')",
                "(6, 'Jabbee')",
                "(7, 'Edge')",
                "(8, 'Jatri')",
                "(9, 'Mita')",
                "(10, 'Tag')",
                "(11, 'Shuffle')",
                "(12, 'Babble')",
                "(13, 'Skiba')",
                "(14, 'Oodoo')",
                "(15, 'Abata')",
                "(16, 'Word')",
                "(17, 'Jazzy')",
                "(18, 'Wikiz')",
                "(19, 'Blue')",
                "(20, 'Roomm')",
                "(21, 'Twitter')",
                "(22, 'Skin')",
                "(23, 'Einti')",
                "(24, 'Gabtype')",
                "(25, 'Brain')",
                "(26, 'Kimia')",
                "(27, 'Oyondu')",
                "(28, 'Feedfi')",
                "(29, 'Ozu')",
                "(30, 'Skidoo')",
                "(31, 'Twitt')",
                "(32, 'Skiba')",
                "(33, 'Flash')",
                "(34, 'Pixo')",
                "(35, 'Ailane')",
                "(36, 'Rhyzio')",
                "(37, 'Skyble')",
                "(38, 'Yakido')",
                "(39, 'Thoug')",
                "(40, 'witwire')",
                "(41, 'Rcube')",
                "(42, 'Leen')",
                "(43, 'Oba')",
                "(44, 'Quaxo')",
                "(45, 'Pixobo')",
                "(46, 'Mita')",
                "(47, 'Voonte')",
                "(48, 'Topic')",
                "(49, 'Dynaz')",
                "(50, 'zoom')"
            };

            for (int i = 0; i < deptvals.length; i++)
                stmt.executeUpdate(s + deptvals[i]);
            System.out.println("DEPT records inserted.");

            s = "create table COURSE(CId int, Title varchar(20), DeptId int)";
            stmt.executeUpdate(s);
            System.out.println("Table COURSE created.");

            s = "insert into COURSE(CId, Title, DeptId) values ";
            String[] coursevals = {
                "(1, 'Economics', 5)",
                "(2, 'Dramatics', 37)",
                "(3, 'Basic Math', 33)",
                "(4, 'History', 7)",
                "(5, 'Resource Program', 23)",
                "(6, 'Grammar', 48)",
                "(7, 'Science', 33)",
                "(8, 'Basic Math', 28)",
                "(9, 'Ancient Civilizations', 31)",
                "(10, 'Design and technology', 40)",
                "(11, 'Earth Science', 25)",
                "(12, 'Handwriting', 47)",
                "(13, 'Handwriting', 7)",
                "(14, 'Speech', 13)",
                "(15, 'Sociology', 10)",
                "(16, 'Ecology', 19)",
                "(17, 'Design and technology', 20)",
                "(18, 'American Literature', 4)",
                "(19, 'Physical Education', 8)",
                "(20, 'Science', 27)",
                "(21, 'Handwriting', 45)",
                "(22, 'Latin', 46)",
                "(23, 'Health', 16)",
                "(24, 'Dramatics', 46)",
                "(25, 'German', 49)",
                "(26, 'Geography', 20)",
                "(27, 'Physical Education', 12)",
                "(28, 'French', 48)",
                "(29, 'Geography', 32)",
                "(30, 'Accounting', 11)",
                "(31, 'Handwriting', 49)",
                "(32, 'Design and technology', 33)",
                "(33, 'Art', 20)",
                "(34, 'Modern Literature', 41)",
                "(35, 'Ecology', 12)",
                "(36, 'American Literature', 13)",
                "(37, 'Mathematics', 19)",
                "(38, 'Instrumental Music', 33)",
                "(39, 'Earth Science', 6)",
                "(40, 'Music', 36)",
                "(41, 'French', 35)",
                "(42, 'Leadership', 11)",
                "(43, 'Leadership', 5)",
                "(44, 'Latin', 33)",
                "(45, 'Spanish', 9)",
                "(46, 'Handwriting', 39)",
                "(47, 'Physical Education', 2)",
                "(48, 'Ecology', 2)",
                "(49, 'Art', 14)",
                "(50, 'Design and technology', 14)",
                "(51, 'History', 38)",
                "(52, 'Ancient Civilizations', 46)",
                "(53, 'LOGIC', 40)",
                "(54, 'Handwriting', 4)",
                "(55, 'Physical Education', 43)",
                "(56, 'Ancient Civilizations', 48)",
                "(57, 'Mathematics', 1)",
                "(58, 'Health', 16)",
                "(59, 'Spanish', 36)",
                "(60, 'French', 45)",
                "(61, 'Physical Education', 19)",
                "(62, 'Design and technology', 17)",
                "(63, 'Science', 17)",
                "(64, 'Resource Program', 19)",
                "(65, 'LOGIC', 38)",
                "(66, 'Health', 14)",
                "(67, 'German', 49)",
                "(68, 'Economics', 38)",
                "(69, 'Music', 9)",
                "(70, 'English IV', 16)",
                "(71, 'Geography', 44)",
                "(72, 'Science', 2)",
                "(73, 'Grammar', 3)",
                "(74, 'Ancient Civilizations', 21)",
                "(75, 'English IV', 10)",
                "(76, 'Leadership', 13)",
                "(77, 'Language Arts', 2)",
                "(78, 'Resource Program', 43)",
                "(79, 'Resource Program', 5)",
                "(80, 'Accounting', 26)",
                "(81, 'Latin', 32)",
                "(82, 'Sociology', 34)",
                "(83, 'Sociology', 6)",
                "(84, 'Instrumental Music', 19)",
                "(85, 'French', 46)",
                "(86, 'Modern Literature', 37)",
                "(87, 'Mathematics', 11)",
                "(88, 'Resource Program', 28)",
                "(89, 'German', 45)",
                "(90, 'Earth Science', 9)",
                "(91, 'Design and technology', 20)",
                "(92, 'Handwriting', 33)",
                "(93, 'Speech', 21)",
                "(94, 'French', 7)",
                "(95, 'German', 30)",
                "(96, 'Design and technology', 10)",
                "(97, 'Economics', 41)",
                "(98, 'English IV', 22)",
                "(99, 'Health', 27)",
                "(100, 'Resource Program', 33)"
            };
            for (int i = 0; i < coursevals.length; i++)
                stmt.executeUpdate(s + coursevals[i]);
            System.out.println("COURSE records inserted.");


/*
            s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
            stmt.executeUpdate(s);
            System.out.println("Table SECTION created.");

            s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
            String[] sectvals = {"(13, 12, 'turing', 2018)",
                    "(23, 12, 'turing', 2019)",
                    "(33, 32, 'newton', 2019)",
                    "(43, 32, 'einstein', 2017)",
                    "(53, 62, 'brando', 2018)"};
            for (int i = 0; i < sectvals.length; i++)
                stmt.executeUpdate(s + sectvals[i]);
            System.out.println("SECTION records inserted.");

*/


            s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
            stmt.executeUpdate(s);
            System.out.println("Table ENROLL created.");

            s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
            String[] enrollvals = {
                "(1, 1, 1, 'R+')",
                "(2, 1, 2, 'Z+')",
                "(3, 1, 3, 'N+')",
                "(4, 1, 4, 'W+')",
                "(5, 2, 5, 'W+')",
                "(6, 2, 6, 'I+')",
                "(7, 2, 7, 'V+')",
                "(8, 3, 8, 'V+')",
                "(9, 3, 9, 'H+')",
                "(10, 4, 10, 'Q+')",
                "(11, 4, 11, 'O+')",
                "(12, 4, 12, 'S+')",
                "(13, 5, 13, 'U+')",
                "(14, 14, 14, 'K+')",
                "(15, 15, 15, 'L+')",
                "(16, 16, 16, 'E+')",
                "(17, 17, 17, 'F+')",
                "(18, 18, 18, 'E+')",
                "(19, 19, 19, 'B+')",
                "(20, 20, 20, 'C+')",
                "(21, 21, 21, 'C+')",
                "(22, 22, 22, 'M+')",
                "(23, 23, 23, 'N+')",
                "(24, 24, 24, 'C+')",
                "(25, 25, 25, 'K+')",
                "(26, 26, 26, 'I+')",
                "(27, 27, 27, 'W+')",
                "(28, 28, 28, 'P+')",
                "(29, 29, 29, 'Q+')",
                "(30, 30, 30, 'N+')",
                "(31, 31, 31, 'E+')",
                "(32, 32, 32, 'G+')",
                "(33, 33, 33, 'L+')",
                "(34, 34, 34, 'B+')",
                "(35, 35, 35, 'H+')",
                "(36, 36, 36, 'U+')",
                "(37, 37, 37, 'Z+')",
                "(38, 38, 38, 'W+')",
                "(39, 39, 39, 'N+')",
                "(40, 40, 40, 'I+')",
                "(41, 41, 41, 'E+')",
                "(42, 42, 42, 'Z+')",
                "(43, 43, 43, 'N+')",
                "(44, 44, 44, 'O+')",
                "(45, 45, 45, 'K+')",
                "(46, 46, 46, 'W+')",
                "(47, 47, 47, 'Y+')",
                "(48, 48, 48, 'V+')",
                "(49, 49, 49, 'B+')",
                "(50, 50, 50, 'T+')",
                "(51, 51, 51, 'D+')",
                "(52, 52, 52, 'N+')",
                "(53, 53, 53, 'T+')",
                "(54, 54, 54, 'A+')",
                "(55, 55, 55, 'G+')",
                "(56, 56, 56, 'G+')",
                "(57, 57, 57, 'J+')",
                "(58, 58, 58, 'R+')",
                "(59, 59, 59, 'I+')",
                "(60, 60, 60, 'Z+')",
                "(61, 61, 61, 'N+')",
                "(62, 62, 62, 'D+')",
                "(63, 63, 63, 'P+')",
                "(64, 64, 64, 'D+')",
                "(65, 65, 65, 'B+')",
                "(66, 66, 66, 'J+')",
                "(67, 67, 67, 'E+')",
                "(68, 68, 68, 'K+')",
                "(69, 69, 69, 'A+')",
                "(70, 70, 70, 'U+')",
                "(71, 71, 71, 'A+')",
                "(72, 72, 72, 'B+')",
                "(73, 73, 73, 'E+')",
                "(74, 74, 74, 'Q+')",
                "(75, 75, 75, 'X+')",
                "(76, 76, 76, 'L+')",
                "(77, 77, 77, 'Z+')",
                "(78, 78, 78, 'H+')",
                "(79, 79, 79, 'W+')",
                "(80, 80, 80, 'A+')",
                "(81, 81, 81, 'B+')",
                "(82, 82, 82, 'E+')",
                "(83, 83, 83, 'F+')",
                "(84, 84, 84, 'D+')",
                "(85, 85, 85, 'J+')",
                "(86, 86, 86, 'L+')",
                "(87, 87, 87, 'T+')",
                "(88, 88, 88, 'V+')",
                "(89, 89, 89, 'Y+')",
                "(90, 90, 90, 'F+')",
                "(91, 91, 91, 'S+')",
                "(92, 92, 92, 'W+')",
                "(93, 93, 93, 'W+')",
                "(94, 94, 94, 'O+')",
                "(95, 95, 95, 'N+')",
                "(96, 96, 96, 'D+')",
                "(97, 97, 97, 'D+')",
                "(98, 98, 98, 'L+')",
                "(99, 99, 99, 'E+')",
                "(100, 100, 100, 'A+')"
            };
            for (int i = 0; i < enrollvals.length; i++)
                stmt.executeUpdate(s + enrollvals[i]);
            System.out.println("ENROLL records inserted.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;

import java.sql.*;

public class XMLParser {
    Document movieXML;
    Document starXML;
    Document starMovieXML;

    private void parseXmlFile() {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

        try {
            DocumentBuilder db = dbf.newDocumentBuilder();
            movieXML = db.parse("mains243.xml");
            starXML = db.parse("actors63.xml");
            starMovieXML = db.parse("casts124.xml");

        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
        } catch (SAXException se) {
            se.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private String parseStars(Connection connection) throws SQLException {
        String starErrors = "Inconsistencies @ actors63.xml\n";
        connection.setAutoCommit(false);

        Element starDoc = starXML.getDocumentElement();
        NodeList starList = starDoc.getElementsByTagName("actor");

        String getMaxId = "SELECT max(id) as 'id' FROM stars;";
        PreparedStatement maxIdStatement = connection.prepareStatement(getMaxId);
        ResultSet maxIdSet = maxIdStatement.executeQuery();
        String strMaxId = "";

        String insertVal = "INSERT INTO stars(id, name, birthYear) VALUES (?, ?, ?)";
        PreparedStatement insert = connection.prepareStatement(insertVal);

        String getStar = "SELECT * FROM stars WHERE name = ? AND birthYear = ?;";
        PreparedStatement starCheck = connection.prepareStatement(getStar);

        if(maxIdSet.next())
            strMaxId = maxIdSet.getString("id");

        int maxIdNum = Integer.parseInt(strMaxId.substring(2));
        maxIdNum++;

        int rowCount = 1;

        if(starList != null && starList.getLength() > 0)
        {
            for(int i = 0; i < starList.getLength(); i++)
            {
                String starName = getTextValue((Element) starList.item(i), "stagename");
                int starDob = getIntValue((Element) starList.item(i), "dob");
                String maxId = "nm" + maxIdNum;

                starCheck.setString(1, starName);
                starCheck.setInt(2, starDob);
                ResultSet starSet = starCheck.executeQuery();

                if(starSet.next())
                    starErrors += "Duplicate in actors63.xml @ <stagename>" + starName  + "</stagename>, <dob>" + starDob + "</dob>\n";
                else {
                    insert.setString(1, maxId);
                    insert.setString(2, starName);

                    if (starDob == -1) {
                        System.out.println(rowCount + ". Inserting into stars: (" + maxId + ", " + starName + ", " + "N/A)");
                        insert.setString(3, null);
                    } else {
                        System.out.println(rowCount + ". Inserting into stars: (" + maxId + ", " + starName + ", " + starDob + ")");
                        insert.setInt(3, starDob);
                    }

                    insert.execute();
                    rowCount++;
                    maxIdNum++;
                }
            }
            connection.commit();

            maxIdStatement.close();
            maxIdSet.close();
            insert.close();
            starCheck.close();
        }
        return starErrors;
    }

    private String parseMovie(Connection connection) throws SQLException {
        connection.setAutoCommit(false);

        String getMovie = "SELECT * FROM movies WHERE title = ? AND director = ? AND year = ?;";
        PreparedStatement movieCheck = connection.prepareStatement(getMovie);

        String insertVal = "INSERT INTO movies(id, title, director, year) VALUES(?, ?, ?, ?);";
        PreparedStatement insert = connection.prepareStatement(insertVal);

        String insertGiMQuery = "INSERT INTO genres_in_movies(genreId, movieId) VALUES(?, ?);";
        PreparedStatement insertGiM = connection.prepareStatement(insertGiMQuery);

        String getMaxId = "SELECT max(id) as 'id' FROM movies;";
        PreparedStatement maxIdStatement = connection.prepareStatement(getMaxId);
        ResultSet maxIdSet = maxIdStatement.executeQuery();

        String getId = "SELECT * FROM genres WHERE name = ?;";
        PreparedStatement getIdStatement = connection.prepareStatement(getId);

        String strMaxId = "";
        if(maxIdSet.next()) strMaxId = maxIdSet.getString("id");
        int maxIdNum = Integer.parseInt(strMaxId.substring(2));
        maxIdNum++;

        Element docEle = movieXML.getDocumentElement();

        NodeList directorList = docEle.getElementsByTagName("directorfilms");

        int rowCount = 1;
        String errors = "Inconsistencies @ mains243.xml\n";
        if(directorList != null && directorList.getLength() > 0) {
            for(int i = 0; i < directorList.getLength(); i++) {
                String dirName = "";

                if(getTextValue((Element) directorList.item(i),"dirname") == null) {
                    errors += "Error at: " + getTextValue((Element) directorList.item(i), "dirid") + " (Invalid director)\n";
                    continue;
                }
                else
                    dirName = getTextValue((Element) directorList.item(i),"dirname");

                Node item = directorList.item(i);
                Element e = (Element) item;

                NodeList filmList = e.getElementsByTagName("film");

                for(int j = 0; j < filmList.getLength(); j++)
                {

                    try{
                        String maxId = "tt0" + maxIdNum;
                        String movieTitle = getTextValue((Element) filmList.item(j), "t");

                        int movieYear = getIntValue((Element) filmList.item(j), "year");

                        if(movieTitle == null) {
                            errors += "Invalid tag <t></t> @: <fid>" + getTextValue((Element) filmList.item(j), "fid") + "</fid>\n";
                            continue;
                        }

                        if(movieYear == -1)
                            errors += "Invalid tag <year></year> @ <fid>: " + getTextValue((Element) filmList.item(j), "fid") + "</fid>\n";
                        else{
                            insert.setString(1, maxId);
                            insert.setString(2, movieTitle);
                            insert.setString(3, dirName);
                            insert.setInt(4, movieYear);

                            movieCheck.setString(1, movieTitle);
                            movieCheck.setString(2, dirName);
                            movieCheck.setInt(3, movieYear);

                            Node genreItem = filmList.item(j);
                            Element genreEle = (Element) genreItem;
                            NodeList genreList = genreEle.getElementsByTagName("cats");

                            String parseGenre = "";
                            for(int k = 0; k < genreList.getLength(); k++) {
                                if (!getTextValue((Element) genreList.item(k), "cat").equals("null")) {
                                    parseGenre += processGenres(getTextValue((Element) genreList.item(k), "cat"));
                                }
                            }

                            ResultSet getDupes = movieCheck.executeQuery();
                            if(getDupes.next())
                                errors += "Duplicate entry from mains243.xml: " + getTextValue((Element) filmList.item(j), "fid") + "\n";
                            else {
                                rowCount++;
                                System.out.println(rowCount + ". Inserting into movies: (" + maxId + ", " + movieTitle + ", " + dirName + ", " + parseGenre + ", " + movieYear + ")");
                                insert.execute();

                                getIdStatement.setString(1, parseGenre);

                                ResultSet getIdSet = getIdStatement.executeQuery();
                                if(getIdSet.next())
                                {
                                    String genreId = getIdSet.getString("id");
                                    try{
                                        insertGiM.setString(1, genreId);
                                        insertGiM.setString(2, maxId);

                                        insertGiM.execute();
                                    } catch(Exception gExcept){}
                                }

                                maxIdNum++;
                                getIdSet.close();

                            }
                        }
                    }
                    catch(Exception movieParseE){ }
                }
            }
            connection.commit();

            movieCheck.close();
            insert.close();
            insertGiM.close();
            maxIdStatement.close();
            maxIdSet.close();
            getIdStatement.close();
        }
        return errors;
    }

    private String parseStarMovie(Connection connection) throws SQLException, IOException {
        connection.setAutoCommit(false);

        String smErrors = "Inconsistencies @ casts124.xml: \n";

        String getMaxId = "SELECT max(id) as 'id' FROM movies;";
        PreparedStatement maxIdStatement = connection.prepareStatement(getMaxId);
        ResultSet maxIdSet = maxIdStatement.executeQuery();

        String insertVal = "INSERT INTO stars_in_movies VALUES(?, ?);";
        PreparedStatement insert = connection.prepareStatement(insertVal);

        String strMaxId = "";
        if(maxIdSet.next()) strMaxId = maxIdSet.getString("id");
        int maxIdNum = Integer.parseInt(strMaxId.substring(2));
        maxIdNum++;

        Element starMovieDoc = starMovieXML.getDocumentElement();
        int getCount = 1;

        String starQuery = "SELECT * FROM stars WHERE name = ?;";
        PreparedStatement checkStar = connection.prepareStatement(starQuery);

        String movieQuery = "SELECT * FROM movies WHERE title = ? AND director = ?;";
        PreparedStatement checkMovie = connection.prepareStatement(movieQuery);

        NodeList directorList = starMovieDoc.getElementsByTagName("dirfilms");
        if(directorList != null && directorList.getLength() > 0) {
            for(int i = 0; i < directorList.getLength(); i++) {

                if(getTextValue((Element) directorList.item(i),"is").equals(null)) {
                    continue;
                }
                String dirName = getTextValue((Element) directorList.item(i),"is");
                Node n = directorList.item(i);
                Element e = (Element) n;

                NodeList filmList = e.getElementsByTagName("m");

                if(filmList != null && filmList.getLength() > 0 && n.getNodeType() == Node.ELEMENT_NODE) {
                    for (int j = 0; j < filmList.getLength(); j++) {
                        try {
                            String movieTitle = getTextValue((Element) filmList.item(j), "t");
                            String starName = getTextValue((Element) filmList.item(j), "a");

                            if (getTextValue((Element) filmList.item(j), "t").equals(null) || getTextValue((Element) filmList.item(j), "a").equals(null)) {
                                continue;
                            }

                            String movieId = null;
                            String starId = null;

                            checkMovie.setString(1, movieTitle);
                            checkMovie.setString(2, dirName);
                            ResultSet getMovie = checkMovie.executeQuery();

                            checkStar.setString(1, starName);
                            ResultSet getStar = checkStar.executeQuery();

                            if (getMovie.next() && getStar.next()) {
                                movieId = getMovie.getString("id");
                                starId = getStar.getString("id");
                            }
                            else {
                                getStar.close();
                                getMovie.close();
                                continue;
                            }


                            if (!starId.equals(null) && !movieId.equals(null)) {
                                System.out.println(getCount + ". Inserting into stars_in_movies: (" + starId + ", " + movieId + ")");
                                insert.setString(1, starId);
                                insert.setString(2, movieId);
                                insert.addBatch();
                                getCount++;
                            }

                            getStar.close();
                            getMovie.close();
                        } catch (Exception starMovieE) {
                            smErrors += "Could not parse title or star name @ <f>" + getTextValue((Element) e.getElementsByTagName("m").item(j),"f") +
                                    "</f> in casts124.xml\n";

                        }
                    }
                }
            }
            insert.executeBatch();
            connection.commit();

            maxIdSet.close();
            maxIdStatement.close();
            checkMovie.close();
            checkStar.close();
            insert.close();
        }
        return smErrors;
    }

    private String parseGenres(Connection connection) throws SQLException, IOException {
        connection.setAutoCommit(true);

        Element movieDoc = movieXML.getDocumentElement();

        NodeList genreList = movieDoc.getElementsByTagName("cats");
        String searchGenre = "SELECT * FROM genres WHERE name = ?;";
        PreparedStatement statement = connection.prepareStatement(searchGenre);

        String insertGenre = "INSERT INTO genres(name) VALUES(?);";
        PreparedStatement insertStatement = connection.prepareStatement(insertGenre);

        int rowCount = 1;

        for(int i = 0; i < genreList.getLength(); i++)
        {
            try {
                String genre = processGenres(getTextValue((Element) genreList.item(i), "cat"));

                statement.setString(1, genre);
                ResultSet genreSet = statement.executeQuery();

                if(genreSet.next())
                    continue;
                else {
                    System.out.println(rowCount + ". Inserting into genres: " + genre);

                    insertStatement.setString(1, genre);
                    insertStatement.execute();
                    rowCount++;
                }
                genreSet.close();
            }catch(Exception genreE){
            }
        }
        insertStatement.close();
        statement.close();
        return "";
    }

    private String processGenres(String genre) throws SQLException
    {
        String tempGenre = "None";

        if(genre.toLowerCase().equals("docu"))
            tempGenre = "Documentary";
        else if(genre.toLowerCase().equals("comd"))
            tempGenre = "Comedy";
        else if(genre.toLowerCase().equals("dram"))
            tempGenre = "Drama";
        else if(genre.toLowerCase().equals("horr"))
            tempGenre = "Horror";
        else if(genre.toLowerCase().equals("susp"))
            tempGenre = "Thriller";
        else if(genre.toLowerCase().equals("west"))
            tempGenre = "Western";
        else if(genre.toLowerCase().equals("s.f.") || genre.toLowerCase().equals("scifi"))
            tempGenre = "Sci-Fi";
        else if(genre.toLowerCase().equals("advt"))
            tempGenre = "Adventure";
        else if(genre.toLowerCase().equals("myst"))
            tempGenre = "Mystery";
        else if(genre.toLowerCase().equals("tv"))
            tempGenre = "TV-show";
        else if(genre.toLowerCase().equals("tvs"))
            tempGenre = "TV-series";
        else if(genre.toLowerCase().equals("biop"))
            tempGenre = "Bio-Pic";
        else if(genre.toLowerCase().equals("noir"))
            tempGenre = "Black";
        else if(genre.toLowerCase().equals("porn"))
            tempGenre = "Pornography";
        else if(genre.toLowerCase().equals("cnr") || genre.toLowerCase().equals("cnrb"))
            tempGenre = "Cops/Robbers";
        else if(genre.toLowerCase().equals("romt"))
            tempGenre = "Romantic";
        else if(genre.toLowerCase().equals("tvm"))
            tempGenre = "TV-miniseries";
        else if(genre.toLowerCase().equals("musc") || genre.toLowerCase().equals("muscl"))
            tempGenre = "Musical";
        else if(genre.toLowerCase().equals("actn"))
            tempGenre = "Action";
        else if(genre.toLowerCase().equals("hist"))
            tempGenre = "History";
        else if(genre.toLowerCase().equals("fant"))
            tempGenre = "Fantasy";
        else if(genre.toLowerCase().equals("cart"))
            tempGenre = "Cartoon";
        else if(genre.toLowerCase().equals("epic"))
            tempGenre = "Epic";
        else
            tempGenre = "None";

        return tempGenre;
    }

    private String deleteDupes(Connection connection) throws SQLException
    {
        connection.setAutoCommit(false);

        String allDupes = "";
        String getDupes = "SELECT *\n" +
                "FROM duplicateStarMovie\n" +
                "WHERE count > 1;";
        PreparedStatement dupes = connection.prepareStatement(getDupes);
        ResultSet dupeSet = dupes.executeQuery();

        String getDelete = "DELETE FROM stars_in_movies WHERE movieId = ? AND starId = ?;";
        PreparedStatement deleteStatement = connection.prepareStatement(getDelete);

        while(dupeSet.next())
        {
            int count = dupeSet.getInt("count");
            String movieId = dupeSet.getString("movieId");
            String starId = dupeSet.getString("starId");

            for(int i = 0; i < count - 1; i++)
            {
                deleteStatement.setString(1, movieId);
                deleteStatement.setString(2, starId);

                deleteStatement.execute();
            }

            allDupes += "Duplicate entry parsed from casts124.xml: (" + movieId + ", " + starId + ")\n";
        }

        connection.commit();

        deleteStatement.close();
        dupes.close();
        dupeSet.close();

        return allDupes;
    }

    private String getTextValue(Element ele, String tagName) {
        String textVal = null;
        NodeList nl = ele.getElementsByTagName(tagName);
        if (nl != null && nl.getLength() > 0) {
            Element el = (Element) nl.item(0);
            textVal = el.getFirstChild().getNodeValue();
        }

        return textVal;
    }

    private int getIntValue(Element ele, String tagName) {
        int num = -1;
        try{
            num = Integer.parseInt(getTextValue(ele, tagName));
        }
        catch(Exception e){
            return num;
        }
        return num;
    }

    public static void main(String[] args) throws Exception{
        //Class.forName("com.mysql.jdbc.Driver");

        //Connection connection = DriverManager.getConnection("jdbc:" + Parameters.dbtype + ":///" + Parameters.dbname + "?autoReconnect=true&useSSL=false",
               // Parameters.username, Parameters.password);

        String loginUser = "mytestuser";
        String loginPasswd = "My6$Password";
        //String loginUrl = "jdbc:mysql://localhost:3306/moviedb_test4";

        //String loginUrl = "jdbc:mysql://localhost:3306/moviedb_test4?allowLoadLocalInfile=true";

        String loginUrl = "jdbc:mysql://localhost:3306/moviedb?allowLoadLocalInfile=true";


        Class.forName("com.mysql.jdbc.Driver").newInstance();
        Connection connection = DriverManager.getConnection(loginUrl, loginUser, loginPasswd);
        BufferedWriter writer = new BufferedWriter(new FileWriter("errors.txt"));

        String outputErrors = "";

        XMLParser dpe = new XMLParser();

        dpe.parseXmlFile();
        dpe.parseGenres(connection);
        outputErrors += dpe.parseMovie(connection);
        outputErrors += dpe.parseStars(connection);
        outputErrors += dpe.parseStarMovie(connection);
        outputErrors += dpe.deleteDupes(connection);

        writer.write(outputErrors);

        writer.close();
        connection.close();
    }

}
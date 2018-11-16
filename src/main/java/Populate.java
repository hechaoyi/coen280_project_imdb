import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class Populate {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java Populate /path/to/data/dir/");
            Runtime.getRuntime().exit(1);
        }
        String dir = args[0], jdbc = "jdbc:oracle:thin:che/1425329@127.0.0.1:1521:db11g";
        Class.forName("oracle.jdbc.OracleDriver");
        try (Connection conn = DriverManager.getConnection(jdbc)) {
            truncateAllData(conn);
            populateMovies(Path.of(dir, "movies.dat"), conn);
            populateMovieGenres(Path.of(dir, "movie_genres.dat"), conn);
            populateMovieCountries(Path.of(dir, "movie_countries.dat"), conn);
            populateMovieLocations(Path.of(dir, "movie_locations.dat"), conn);
            populateTags(Path.of(dir, "tags.dat"), conn);
            populateMovieTags(Path.of(dir, "movie_tags.dat"), conn);
        }
    }

    private static final Map<String, Integer> genres = new HashMap<>();
    private static final Map<String, Integer> countries = new HashMap<>();

    private static void truncateAllData(Connection conn) throws SQLException {
        conn.createStatement().execute("TRUNCATE TABLE movie_genre");
        conn.createStatement().execute("DELETE FROM genre WHERE 1=1");
        conn.createStatement().execute("TRUNCATE TABLE movie_location");
        conn.createStatement().execute("TRUNCATE TABLE movie_country");
        conn.createStatement().execute("DELETE FROM country WHERE 1=1");
        conn.createStatement().execute("TRUNCATE TABLE movie_tag");
        conn.createStatement().execute("DELETE FROM tag WHERE 1=1");
        conn.createStatement().execute("DELETE FROM movie WHERE 1=1");
        System.out.println("all data truncated.");
    }

    private static void batch(Path file, Consumer<String[]> perLine, Runnable perBatch) throws Exception {
        var counter = new AtomicInteger();
        Files.lines(file, Charset.forName("ISO-8859-1")).map(line -> line.split("\t", -1)).forEach(segments -> {
            try {
                Integer.parseInt(segments[0]);
            } catch (NumberFormatException e) {
                return; // ignored
            }
            try {
                if (perLine.accept(segments) && counter.incrementAndGet() % 1000 == 0)
                    perBatch.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        if (counter.get() % 1000 != 0)
            perBatch.run();
        System.out.println(counter.get() + " entities populated in " + file);
    }

    private static void populateMovies(Path file, Connection conn) throws Exception {
        var stmt = conn.prepareStatement("INSERT INTO movie(id,title,year," +
                "all_critics_rating,top_critics_rating,audience_rating," +
                "all_critics_num,top_critics_num,audience_num," +
                "pic_url) VALUES(?,?,?,?,?,?,?,?,?,?)");
        batch(file, segments -> {
            stmt.setInt(1, Integer.parseInt(segments[0]));
            stmt.setString(2, segments[1]);
            stmt.setInt(3, Integer.parseInt(segments[5]));
            if (segments[7].equals("\\N")) {
                for (int i = 4; i <= 10; i++)
                    stmt.setString(i, "");
            } else {
                stmt.setString(4, segments[7]);
                stmt.setString(5, segments[12]);
                stmt.setString(6, segments[17]);
                stmt.setInt(7, Integer.parseInt(segments[8]));
                stmt.setInt(8, Integer.parseInt(segments[13]));
                stmt.setInt(9, Integer.parseInt(segments[18]));
                stmt.setString(10, segments[20]);
            }
            stmt.addBatch();
            return true;
        }, () -> stmt.executeBatch());
    }

    private static int withDictionary(Map<String, Integer> dict, String key, PreparedStatement stmt) {
        return dict.computeIfAbsent(key, __ -> {
            try {
                stmt.setString(1, key);
                stmt.execute();
                try (var rs = stmt.getGeneratedKeys()) {
                    rs.next();
                    return rs.getInt(1);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void populateMovieGenres(Path file, Connection conn) throws Exception {
        var genreStmt = conn.prepareStatement("INSERT INTO genre(id,name) VALUES(genre_seq.nextval,?)",
                new String[]{"id"});
        var stmt = conn.prepareStatement("INSERT INTO movie_genre(movie_id,genre_id) VALUES(?,?)");
        batch(file, segments -> {
            stmt.setInt(1, Integer.parseInt(segments[0]));
            stmt.setInt(2, withDictionary(genres, segments[1], genreStmt));
            stmt.addBatch();
            return true;
        }, () -> stmt.executeBatch());
    }

    private static void populateMovieCountries(Path file, Connection conn) throws Exception {
        var countryStmt = conn.prepareStatement("INSERT INTO country(id,name) VALUES(country_seq.nextval,?)",
                new String[]{"id"});
        var stmt = conn.prepareStatement("INSERT INTO movie_country(movie_id,country_id) VALUES(?,?)");
        batch(file, segments -> {
            if (segments[1].isBlank())
                return false;
            stmt.setInt(1, Integer.parseInt(segments[0]));
            stmt.setInt(2, withDictionary(countries, segments[1], countryStmt));
            stmt.addBatch();
            return true;
        }, () -> stmt.executeBatch());
    }

    private static void populateMovieLocations(Path file, Connection conn) throws Exception {
        var countryStmt = conn.prepareStatement("INSERT INTO country(id,name) VALUES(country_seq.nextval,?)",
                new String[]{"id"});
        var stmt = conn.prepareStatement("INSERT INTO movie_location(movie_id,country_id,loc2,loc3,loc4) VALUES(?,?,?,?,?)");
        batch(file, segments -> {
            if (segments[1].isBlank())
                return false;
            stmt.setInt(1, Integer.parseInt(segments[0]));
            stmt.setInt(2, withDictionary(countries, segments[1], countryStmt));
            stmt.setString(3, segments[2]);
            stmt.setString(4, segments[3]);
            stmt.setString(5, segments[4]);
            stmt.addBatch();
            return true;
        }, () -> stmt.executeBatch());
    }

    private static void populateTags(Path file, Connection conn) throws Exception {
        var stmt = conn.prepareStatement("INSERT INTO tag(id,name) VALUES(?,?)");
        batch(file, segments -> {
            stmt.setInt(1, Integer.parseInt(segments[0]));
            stmt.setString(2, segments[1]);
            stmt.addBatch();
            return true;
        }, () -> stmt.executeBatch());
    }

    private static void populateMovieTags(Path file, Connection conn) throws Exception {
        var stmt = conn.prepareStatement("INSERT INTO movie_tag(movie_id,tag_id,weight) VALUES(?,?,?)");
        batch(file, segments -> {
            stmt.setInt(1, Integer.parseInt(segments[0]));
            stmt.setString(2, segments[1]);
            stmt.setInt(3, Integer.parseInt(segments[2]));
            stmt.addBatch();
            return true;
        }, () -> stmt.executeBatch());
    }
}

@FunctionalInterface
interface Consumer<T> {
    boolean accept(T t) throws Exception;
}

@FunctionalInterface
interface Runnable {
    void run() throws Exception;
}
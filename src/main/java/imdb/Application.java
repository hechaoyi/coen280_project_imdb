package imdb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootApplication
@RestController
@RequiredArgsConstructor
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final BeanPropertyRowMapper<Entity> entityMapper = BeanPropertyRowMapper.newInstance(Entity.class);
    private final BeanPropertyRowMapper<Movie> movieMapper = BeanPropertyRowMapper.newInstance(Movie.class);
    private final BeanPropertyRowMapper<Tag> tagMapper = BeanPropertyRowMapper.newInstance(Tag.class);

    @RequestMapping("/initialize")
    public Map<String, List<Entity>> initialize() {
        return Map.of(
                "genres", jdbcTemplate.query("SELECT id,name FROM genre", entityMapper),
                "countries", jdbcTemplate.query("SELECT id,name FROM country", entityMapper));
    }

    @RequestMapping("/prepare")
    public Map<String, Object> prepare(String genres, String countries, String locations,
                                       String startYear, String endYear,
                                       String ratingOp, String ratingVal, String reviewsOp, String reviewsVal,
                                       @RequestParam(defaultValue = "true") boolean relationshipBetweenAttributes) {
        return Map.of(
                "sql", query(genres, countries, locations, startYear, endYear,
                        ratingOp, ratingVal, reviewsOp, reviewsVal, relationshipBetweenAttributes,
                        new ArrayList<>(), null),
                "availableGenres", availableGenres(countries, locations, startYear, endYear,
                        ratingOp, ratingVal, reviewsOp, reviewsVal, relationshipBetweenAttributes),
                "availableCountries", availableCountries(genres, locations, startYear, endYear,
                        ratingOp, ratingVal, reviewsOp, reviewsVal, relationshipBetweenAttributes),
                "availableLocations", availableLocations(genres, countries, startYear, endYear,
                        ratingOp, ratingVal, reviewsOp, reviewsVal, relationshipBetweenAttributes));
    }

    private List<Integer> availableGenres(String countries, String locations,
                                          String startYear, String endYear,
                                          String ratingOp, String ratingVal,
                                          String reviewsOp, String reviewsVal,
                                          boolean relationshipBetweenAttributes) {
        String sql = "SELECT DISTINCT genre_id FROM movie_genre g WHERE 1=1";
        if (!relationshipBetweenAttributes)
            return jdbcTemplate.getJdbcTemplate().queryForList(sql, Integer.class);
        MapSqlParameterSource params = new MapSqlParameterSource();
        if (countries != null && !countries.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie_country WHERE movie_id=g.movie_id AND country_id IN (:countries))";
            params.addValue("countries", Arrays.asList(countries.split(",")));
        }
        if (locations != null && !locations.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie_location WHERE movie_id=g.movie_id AND country_id IN (:locations))";
            params.addValue("locations", Arrays.asList(locations.split(",")));
        }
        if (startYear != null && !startYear.isBlank() || endYear != null && !endYear.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie WHERE id=g.movie_id AND year BETWEEN :startYear AND :endYear)";
            if (startYear == null || startYear.isBlank())
                startYear = "1900";
            if (endYear == null || endYear.isBlank())
                endYear = "2100";
            params.addValue("startYear", startYear).addValue("endYear", endYear);
        }
        if (ratingOp != null && !ratingOp.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie WHERE id=g.movie_id AND FLOOR(all_critics_rating)" + op(ratingOp) + ":ratingVal)";
            params.addValue("ratingVal", ratingVal);
        }
        if (reviewsOp != null && !reviewsOp.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie WHERE id=g.movie_id AND all_critics_num" + op(reviewsOp) + ":reviewsVal)";
            params.addValue("reviewsVal", reviewsVal);
        }
        return jdbcTemplate.queryForList(sql, params, Integer.class);
    }

    private List<Integer> availableCountries(String genres, String locations,
                                             String startYear, String endYear,
                                             String ratingOp, String ratingVal,
                                             String reviewsOp, String reviewsVal,
                                             boolean relationshipBetweenAttributes) {
        String sql = "SELECT DISTINCT country_id FROM movie_country c WHERE 1=1";
        if (!relationshipBetweenAttributes)
            return jdbcTemplate.getJdbcTemplate().queryForList(sql, Integer.class);
        MapSqlParameterSource params = new MapSqlParameterSource();
        if (genres != null && !genres.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie_genre WHERE movie_id=c.movie_id AND genre_id IN (:genres))";
            params.addValue("genres", Arrays.asList(genres.split(",")));
        }
        if (locations != null && !locations.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie_location WHERE movie_id=c.movie_id AND country_id IN (:locations))";
            params.addValue("locations", Arrays.asList(locations.split(",")));
        }
        if (startYear != null && !startYear.isBlank() || endYear != null && !endYear.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie WHERE id=c.movie_id AND year BETWEEN :startYear AND :endYear)";
            if (startYear == null || startYear.isBlank())
                startYear = "1900";
            if (endYear == null || endYear.isBlank())
                endYear = "2100";
            params.addValue("startYear", startYear).addValue("endYear", endYear);
        }
        if (ratingOp != null && !ratingOp.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie WHERE id=c.movie_id AND FLOOR(all_critics_rating)" + op(ratingOp) + ":ratingVal)";
            params.addValue("ratingVal", ratingVal);
        }
        if (reviewsOp != null && !reviewsOp.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie WHERE id=c.movie_id AND all_critics_num" + op(reviewsOp) + ":reviewsVal)";
            params.addValue("reviewsVal", reviewsVal);
        }
        return jdbcTemplate.queryForList(sql, params, Integer.class);
    }

    private List<Integer> availableLocations(String genres, String countries,
                                             String startYear, String endYear,
                                             String ratingOp, String ratingVal,
                                             String reviewsOp, String reviewsVal,
                                             boolean relationshipBetweenAttributes) {
        String sql = "SELECT DISTINCT country_id FROM movie_location l WHERE 1=1";
        if (!relationshipBetweenAttributes)
            return jdbcTemplate.getJdbcTemplate().queryForList(sql, Integer.class);
        MapSqlParameterSource params = new MapSqlParameterSource();
        if (genres != null && !genres.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie_genre WHERE movie_id=l.movie_id AND genre_id IN (:genres))";
            params.addValue("genres", Arrays.asList(genres.split(",")));
        }
        if (countries != null && !countries.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie_country WHERE movie_id=l.movie_id AND country_id IN (:countries))";
            params.addValue("countries", Arrays.asList(countries.split(",")));
        }
        if (startYear != null && !startYear.isBlank() || endYear != null && !endYear.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie WHERE id=l.movie_id AND year BETWEEN :startYear AND :endYear)";
            if (startYear == null || startYear.isBlank())
                startYear = "1900";
            if (endYear == null || endYear.isBlank())
                endYear = "2100";
            params.addValue("startYear", startYear).addValue("endYear", endYear);
        }
        if (ratingOp != null && !ratingOp.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie WHERE id=l.movie_id AND FLOOR(all_critics_rating)" + op(ratingOp) + ":ratingVal)";
            params.addValue("ratingVal", ratingVal);
        }
        if (reviewsOp != null && !reviewsOp.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie WHERE id=l.movie_id AND all_critics_num" + op(reviewsOp) + ":reviewsVal)";
            params.addValue("reviewsVal", reviewsVal);
        }
        return jdbcTemplate.queryForList(sql, params, Integer.class);
    }

    private String query(String genres, String countries, String locations,
                         String startYear, String endYear,
                         String ratingOp, String ratingVal, String reviewsOp, String reviewsVal,
                         boolean relationshipBetweenAttributes,
                         List<String> criteria, MapSqlParameterSource params) {
        if (genres != null && !genres.isBlank()) {
            criteria.add("EXISTS(SELECT 1 FROM movie_genre WHERE movie_id=m.id AND genre_id IN (:genres))");
            if (params != null)
                params.addValue("genres", Arrays.asList(genres.split(",")));
        }
        if (countries != null && !countries.isBlank()) {
            criteria.add("EXISTS(SELECT 1 FROM movie_country WHERE movie_id=m.id AND country_id IN (:countries))");
            if (params != null)
                params.addValue("countries", Arrays.asList(countries.split(",")));
        }
        if (locations != null && !locations.isBlank()) {
            criteria.add("EXISTS(SELECT 1 FROM movie_location WHERE movie_id=m.id AND country_id IN (:locations))");
            if (params != null)
                params.addValue("locations", Arrays.asList(locations.split(",")));
        }
        if (startYear != null && !startYear.isBlank() || endYear != null && !endYear.isBlank()) {
            criteria.add("m.year BETWEEN :startYear AND :endYear");
            if (params != null) {
                if (startYear == null || startYear.isBlank())
                    startYear = "1900";
                if (endYear == null || endYear.isBlank())
                    endYear = "2100";
                params.addValue("startYear", startYear).addValue("endYear", endYear);
            }
        }
        if (ratingOp != null && !ratingOp.isBlank()) {
            criteria.add("FLOOR(m.all_critics_rating)" + op(ratingOp) + ":ratingVal");
            if (params != null)
                params.addValue("ratingVal", ratingVal);
        }
        if (reviewsOp != null && !reviewsOp.isBlank()) {
            criteria.add("m.all_critics_num" + op(reviewsOp) + ":reviewsVal");
            if (params != null)
                params.addValue("reviewsVal", reviewsVal);
        }
        String select = "SELECT m.id,title,year,pic_url," +
                "all_critics_rating,top_critics_rating,audience_rating," +
                "all_critics_num,top_critics_num,audience_num" +
                " FROM movie m";
        if (criteria.isEmpty())
            return select + " ORDER BY id";
        if (criteria.size() == 1 || relationshipBetweenAttributes)
            return String.format("%s\n  WHERE %s\nORDER BY id",
                    select, String.join("\n  AND ", criteria));
        return String.format("%s JOIN (\n  SELECT id FROM movie m\n    WHERE %s\n) m2 ON m.id=m2.id ORDER BY m2.id",
                select, String.join("\n  UNION\n  SELECT id FROM movie m\n    WHERE ", criteria));
    }

    private String op(String op) {
        return op.equals("≤") ? "<=" : op.equals("≥") ? ">=" : op;
    }

    @RequestMapping("/movies")
    public Page movies(String genres, String countries, String locations,
                       String startYear, String endYear,
                       String ratingOp, String ratingVal, String reviewsOp, String reviewsVal,
                       @RequestParam(defaultValue = "true") boolean relationshipBetweenAttributes,
                       @RequestParam(defaultValue = "1") int page) {
        List<String> criteria = new ArrayList<>();
        MapSqlParameterSource params = new MapSqlParameterSource();
        String sql = query(genres, countries, locations, startYear, endYear,
                ratingOp, ratingVal, reviewsOp, reviewsVal, relationshipBetweenAttributes, criteria, params);
        int count = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM (" + sql + ")", params, Integer.class);
        sql = "SELECT * FROM (SELECT ROWNUM rn, o.* FROM (" + sql + ") o WHERE ROWNUM<=:upper) WHERE rn>:lower";
        params.addValue("upper", page * 10).addValue("lower", (page - 1) * 10);
        List<Movie> movies = jdbcTemplate.query(sql, params, movieMapper);
        fillInSubData(movies);
        List<Tag> tags = null;
        if (criteria.size() > 1 && relationshipBetweenAttributes)
            tags = fillInTagData(criteria, params);
        return new Page(movies, count, tags);
    }

    private void fillInSubData(List<Movie> movies) {
        if (movies.isEmpty())
            return;
        var moviesMap = movies.stream().collect(Collectors.toMap(m -> String.valueOf(m.getId()), m -> m));
        jdbcTemplate.queryForList(
                "SELECT movie_id,name FROM movie_country JOIN country ON country_id=id WHERE movie_id IN (:movies)",
                Map.of("movies", moviesMap.keySet()))
                .forEach(e -> {
                    var movie = moviesMap.get(e.get("MOVIE_ID").toString());
                    if (movie != null)
                        movie.setCountry((String) e.get("NAME"));
                });
        jdbcTemplate.queryForList(
                "SELECT movie_id,name FROM movie_genre JOIN genre ON genre_id=id WHERE movie_id IN (:movies)",
                Map.of("movies", moviesMap.keySet()))
                .forEach(e -> {
                    var movie = moviesMap.get(e.get("MOVIE_ID").toString());
                    if (movie != null)
                        movie.getGenres().add((String) e.get("NAME"));
                });
        jdbcTemplate.queryForList(
                "SELECT movie_id,name,loc2,loc3,loc4 FROM movie_location JOIN country ON country_id=id WHERE movie_id IN (:movies)",
                Map.of("movies", moviesMap.keySet()))
                .forEach(e -> {
                    var movie = moviesMap.get(e.get("MOVIE_ID").toString());
                    if (movie != null) {
                        String location = (String) e.get("NAME"), loc2 = (String) e.get("LOC2"), loc3 = (String) e.get("LOC3"), loc4 = (String) e.get("LOC4");
                        if (loc2 != null) {
                            location += " " + loc2;
                            if (loc3 != null) {
                                location += " " + loc3;
                                if (loc4 != null)
                                    location += " " + loc4;
                            }
                        }
                        movie.getLocations().add(location);
                    }
                });
    }

    private List<Tag> fillInTagData(List<String> criteria, MapSqlParameterSource params) {
        String sql = String.format("FROM movie m JOIN movie_tag ON m.id=movie_id WHERE %s",
                String.join(" AND ", criteria));
        int count = jdbcTemplate.queryForObject("SELECT COUNT(DISTINCT tag_id) " + sql, params, Integer.class);
        if (count > 200)
            return null;
        sql = "SELECT name,weight FROM tag JOIN (" +
                "SELECT tag_id,SUM(weight) weight " + sql + " GROUP BY tag_id" +
                ") ON id=tag_id ORDER BY weight DESC";
        return jdbcTemplate.query(sql, params, tagMapper);
    }
}

@Data
class Movie {
    private int id;
    private String title;
    private int year;
    private String picUrl;
    private Float allCriticsRating;
    private Float topCriticsRating;
    private Float audienceRating;
    private Integer allCriticsNum;
    private Integer topCriticsNum;
    private Integer audienceNum;
    private String country;
    private List<String> genres = new ArrayList<>();
    private List<String> locations = new ArrayList<>();
}

@Data
class Entity {
    private int id;
    private String name;
}

@Data
class Tag {
    private String name;
    private int weight;
}

@Data
@AllArgsConstructor
class Page {
    private List<Movie> movies;
    private int count;
    private List<Tag> tags;
}
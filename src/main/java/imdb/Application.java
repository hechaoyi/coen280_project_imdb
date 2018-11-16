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

    @RequestMapping("/initialize")
    public Map<String, List<Entity>> initialize() {
        return Map.of(
                "genres", jdbcTemplate.query("SELECT id,name FROM genre", entityMapper),
                "countries", jdbcTemplate.query("SELECT id,name FROM country", entityMapper));
    }

    @RequestMapping("/prepare")
    public Map<String, Object> prepare(String genres, String countries, String locations,
                                       String startYear, String endYear,
                                       @RequestParam(defaultValue = "true") boolean relationshipBetweenAttributes) {
        return Map.of(
                "sql", query(genres, countries, locations, startYear, endYear, relationshipBetweenAttributes, null),
                "availableGenres", availableGenres(countries, locations, startYear, endYear, relationshipBetweenAttributes),
                "availableCountries", availableCountries(genres, locations, startYear, endYear, relationshipBetweenAttributes),
                "availableLocations", availableLocations(genres, countries, startYear, endYear, relationshipBetweenAttributes));
    }

    private List<Integer> availableGenres(String countries, String locations,
                                          String startYear, String endYear, boolean relationshipBetweenAttributes) {
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
        return jdbcTemplate.queryForList(sql, params, Integer.class);
    }

    private List<Integer> availableCountries(String genres, String locations,
                                             String startYear, String endYear, boolean relationshipBetweenAttributes) {
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
        return jdbcTemplate.queryForList(sql, params, Integer.class);
    }

    private List<Integer> availableLocations(String genres, String countries,
                                             String startYear, String endYear, boolean relationshipBetweenAttributes) {
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
        return jdbcTemplate.queryForList(sql, params, Integer.class);
    }

    private String query(String genres, String countries, String locations,
                         String startYear, String endYear,
                         boolean relationshipBetweenAttributes, MapSqlParameterSource params) {
        List<String> criteria = new ArrayList<>();
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
        if (criteria.isEmpty())
            return "SELECT id,title,year FROM movie ORDER BY id";
        if (criteria.size() == 1 || relationshipBetweenAttributes)
            return String.format("SELECT id,title,year FROM movie m\n  WHERE %s\nORDER BY id",
                    String.join("\n  AND ", criteria));
        return String.format("SELECT * FROM (\n  SELECT id,title,year FROM movie m\n    WHERE %s\n) ORDER BY id",
                String.join("\n  UNION\n  SELECT id,title,year FROM movie m\n    WHERE ", criteria));
    }

    @RequestMapping("/movies")
    public Page movies(String genres, String countries, String locations,
                       String startYear, String endYear,
                       @RequestParam(defaultValue = "true") boolean relationshipBetweenAttributes,
                       @RequestParam(defaultValue = "1") int page) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String sql = query(genres, countries, locations, startYear, endYear, relationshipBetweenAttributes, params);
        int count = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM (" + sql + ")", params, Integer.class);
        sql = "SELECT * FROM (SELECT ROWNUM rn, o.* FROM (" + sql + ") o WHERE ROWNUM<=:upper) WHERE rn>:lower";
        params.addValue("upper", page * 10).addValue("lower", (page - 1) * 10);
        return new Page(jdbcTemplate.query(sql, params, movieMapper), count);
    }
}

@Data
class Movie {
    private int id;
    private String title;
    private int year;
}

@Data
class Entity {
    private int id;
    private String name;
}

@Data
@AllArgsConstructor
class Page {
    private List<Movie> movies;
    private int count;
}
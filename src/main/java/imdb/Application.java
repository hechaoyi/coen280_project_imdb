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
    public Map<String, Object> prepare(String genres, String countries, String locations) {
        return Map.of(
                "sql", query(genres, countries, locations, null),
                "availableGenres", availableGenres(countries, locations),
                "availableCountries", availableCountries(genres, locations),
                "availableLocations", availableLocations(genres, countries));
    }

    private List<Integer> availableGenres(String countries, String locations) {
        String sql = "SELECT DISTINCT genre_id FROM movie_genre g WHERE 1=1";
        MapSqlParameterSource params = new MapSqlParameterSource();
        if (countries != null && !countries.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie_country WHERE movie_id=g.movie_id AND country_id IN (:countries))";
            params.addValue("countries", Arrays.asList(countries.split(",")));
        }
        if (locations != null && !locations.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie_location WHERE movie_id=g.movie_id AND country_id IN (:locations))";
            params.addValue("locations", Arrays.asList(locations.split(",")));
        }
        return jdbcTemplate.queryForList(sql, params, Integer.class);
    }

    private List<Integer> availableCountries(String genres, String locations) {
        String sql = "SELECT DISTINCT country_id FROM movie_country c WHERE 1=1";
        MapSqlParameterSource params = new MapSqlParameterSource();
        if (genres != null && !genres.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie_genre WHERE movie_id=c.movie_id AND genre_id IN (:genres))";
            params.addValue("genres", Arrays.asList(genres.split(",")));
        }
        if (locations != null && !locations.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie_location WHERE movie_id=c.movie_id AND country_id IN (:locations))";
            params.addValue("locations", Arrays.asList(locations.split(",")));
        }
        return jdbcTemplate.queryForList(sql, params, Integer.class);
    }

    private List<Integer> availableLocations(String genres, String countries) {
        String sql = "SELECT DISTINCT country_id FROM movie_location l WHERE 1=1";
        MapSqlParameterSource params = new MapSqlParameterSource();
        if (genres != null && !genres.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie_genre WHERE movie_id=l.movie_id AND genre_id IN (:genres))";
            params.addValue("genres", Arrays.asList(genres.split(",")));
        }
        if (countries != null && !countries.isBlank()) {
            sql += " AND EXISTS(SELECT 1 FROM movie_country WHERE movie_id=l.movie_id AND country_id IN (:countries))";
            params.addValue("countries", Arrays.asList(countries.split(",")));
        }
        return jdbcTemplate.queryForList(sql, params, Integer.class);
    }

    private String query(String genres, String countries, String locations, MapSqlParameterSource params) {
        var sql = new StringBuilder("SELECT id,title,year FROM movie m WHERE 1=1");
        if (genres != null && !genres.isBlank()) {
            sql.append("\n  AND EXISTS(SELECT 1 FROM movie_genre WHERE movie_id=m.id AND genre_id IN (:genres))");
            if (params != null)
                params.addValue("genres", Arrays.asList(genres.split(",")));
        }
        if (countries != null && !countries.isBlank()) {
            sql.append("\n  AND EXISTS(SELECT 1 FROM movie_country WHERE movie_id=m.id AND country_id IN (:countries))");
            if (params != null)
                params.addValue("countries", Arrays.asList(countries.split(",")));
        }
        if (locations != null && !locations.isBlank()) {
            sql.append("\n  AND EXISTS(SELECT 1 FROM movie_location WHERE movie_id=m.id AND country_id IN (:locations))");
            if (params != null)
                params.addValue("locations", Arrays.asList(locations.split(",")));
        }
        return sql.append("\nORDER BY id").toString();
    }

    @RequestMapping("/movies")
    public Page movies(String genres, String countries, String locations,
                       @RequestParam(defaultValue = "1") int page) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String sql = query(genres, countries, locations, params);
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
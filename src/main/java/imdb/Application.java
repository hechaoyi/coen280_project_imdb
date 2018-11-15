package imdb;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;

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

    @RequestMapping("/genres")
    public List<Entity> genres() {
        String sql = "SELECT id,name FROM genre";
        return jdbcTemplate.query(sql, entityMapper);
    }

    @RequestMapping("/countries")
    public List<Entity> countries() {
        String sql = "SELECT id,name FROM country";
        return jdbcTemplate.query(sql, entityMapper);
    }

    @RequestMapping("/movies")
    public List<Movie> movies(String genres, String countries, String locations) {
        String sql = "  SELECT id,title,year FROM movie m WHERE 1=1";
        MapSqlParameterSource params = new MapSqlParameterSource();
        if (genres != null && !genres.isBlank()) {
            sql += "\n    AND EXISTS(SELECT 1 FROM movie_genre WHERE movie_id=m.id AND genre_id IN (:genres))";
            params.addValue("genres", Arrays.asList(genres.split(",")));
        }
        if (countries != null && !countries.isBlank()) {
            sql += "\n    AND EXISTS(SELECT 1 FROM movie_country WHERE movie_id=m.id AND country_id IN (:countries))";
            params.addValue("countries", Arrays.asList(countries.split(",")));
        }
        if (locations != null && !locations.isBlank()) {
            sql += "\n    AND EXISTS(SELECT 1 FROM movie_location WHERE movie_id=m.id AND country_id IN (:locations))";
            params.addValue("locations", Arrays.asList(locations.split(",")));
        }
        sql = "SELECT * FROM (\n" + sql + "\n) WHERE ROWNUM<=20";
        return jdbcTemplate.query(sql, params, movieMapper);
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
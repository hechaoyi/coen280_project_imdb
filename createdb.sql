CREATE TABLE movie
(
  id                 INTEGER       PRIMARY KEY,
  title              VARCHAR2(255) NOT NULL,
  year               INTEGER       NOT NULL,
  all_critics_rating NUMBER(3,1),
  top_critics_rating NUMBER(3,1),
  audience_rating    NUMBER(3,1),
  all_critics_num    INTEGER,
  top_critics_num    INTEGER,
  audience_num       INTEGER,
  pic_url            VARCHAR2(255)
);
CREATE INDEX idx_movie__year               ON movie (year);
CREATE INDEX idx_movie__all_critics_rating ON movie (all_critics_rating);
CREATE INDEX idx_movie__all_critics_num    ON movie (all_critics_num);



CREATE TABLE genre
(
  id   INTEGER       PRIMARY KEY,
  name VARCHAR2(255) NOT NULL UNIQUE
);

CREATE TABLE movie_genre
(
  movie_id INTEGER NOT NULL,
  genre_id INTEGER NOT NULL,
  CONSTRAINT movie_genre__movie__fk FOREIGN KEY (movie_id) REFERENCES movie(id) ON DELETE CASCADE,
  CONSTRAINT movie_genre__genre__fk FOREIGN KEY (genre_id) REFERENCES genre(id) ON DELETE CASCADE
);
CREATE INDEX idx_movie_genre__movie_id ON movie_genre (movie_id);
CREATE INDEX idx_movie_genre__genre_id ON movie_genre (genre_id);



CREATE TABLE country
(
  id   INTEGER       PRIMARY KEY,
  name VARCHAR2(255) NOT NULL UNIQUE
);

CREATE TABLE movie_country
(
  movie_id   INTEGER NOT NULL UNIQUE,
  country_id INTEGER NOT NULL,
  CONSTRAINT movie_country__movie__fk   FOREIGN KEY (movie_id)   REFERENCES movie(id)   ON DELETE CASCADE,
  CONSTRAINT movie_country__country__fk FOREIGN KEY (country_id) REFERENCES country(id) ON DELETE CASCADE
);
-- CREATE INDEX idx_movie_country__movie_id   ON movie_country (movie_id);
CREATE INDEX idx_movie_country__country_id ON movie_country (country_id);

CREATE TABLE movie_location
(
  movie_id   INTEGER NOT NULL,
  country_id INTEGER NOT NULL,
  loc2 VARCHAR2(255),
  loc3 VARCHAR2(255),
  loc4 VARCHAR2(255),
  CONSTRAINT movie_location__movie__fk   FOREIGN KEY (movie_id)   REFERENCES movie(id)   ON DELETE CASCADE,
  CONSTRAINT movie_location__country__fk FOREIGN KEY (country_id) REFERENCES country(id) ON DELETE CASCADE
);
CREATE INDEX idx_movie_location__movie_id   ON movie_location (movie_id);
CREATE INDEX idx_movie_location__country_id ON movie_location (country_id);



CREATE TABLE tag
(
  id   INTEGER       PRIMARY KEY,
  name VARCHAR2(255) NOT NULL UNIQUE
);

CREATE TABLE movie_tag
(
  movie_id INTEGER NOT NULL,
  tag_id   INTEGER NOT NULL,
  weight   INTEGER NOT NULL,
  CONSTRAINT movie_tag__movie__fk FOREIGN KEY (movie_id) REFERENCES movie(id) ON DELETE CASCADE,
  CONSTRAINT movie_tag__tag__fk   FOREIGN KEY (tag_id)   REFERENCES tag(id)   ON DELETE CASCADE
);
CREATE INDEX idx_movie_tag__movie__tag ON movie_tag (movie_id, tag_id);



CREATE SEQUENCE genre_seq START WITH 1;
CREATE SEQUENCE country_seq START WITH 1;

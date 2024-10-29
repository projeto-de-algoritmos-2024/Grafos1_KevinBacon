CREATE TABLE title_basics (
    tconst TEXT NOT NULL,
    titleType TEXT NOT NULL,
    primaryTitle TEXT NOT NULL,
    originalTitle TEXT NOT NULL,
    isAdult BOOLEAN NOT NULL,
    startYear INTEGER NOT NULL,
    endYear INTEGER,
    runtimeMinutes INTEGER NOT NULL,
    CONSTRAINT title_basics_PK PRIMARY KEY (tconst)
);

CREATE TABLE title_genres (
    tconst TEXT NOT NULL,
    genre TEXT NOT NULL,
    CONSTRAINT title_genres_UK UNIQUE(tconst, genre),
    CONSTRAINT title_genres_title_basics_FK FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
);

CREATE TABLE title_akas (
    titleId TEXT NOT NULL,
    ordering INTEGER NOT NULL,
    title TEXT NOT NULL,
    region TEXT NOT NULL,
    language TEXT NOT NULL,
    isOriginalTitle BOOLEAN,
    CONSTRAINT title_akas_PK PRIMARY KEY (titleId),
    CONSTRAINT title_akas_title_basics_FK FOREIGN KEY (titleId) REFERENCES title_basics(tconst)
);

CREATE TABLE akas_types (
    titleAkasId INTEGER NOT NULL,
    type TEXT NOT NULL,
    CONSTRAINT akas_types_UK UNIQUE (titleAkasId, type),
    CONSTRAINT akas_types_title_akas_FK FOREIGN KEY (titleAkasId) REFERENCES title_akas(titleId)
);

CREATE TABLE akas_attributes (
    titleAkasId INTEGER NOT NULL,
    attribute TEXT NOT NULL,
    CONSTRAINT akas_attributes_UK UNIQUE(titleAkasId, attribute),
    CONSTRAINT akas_attributes_title_akas_FK FOREIGN KEY (titleAkasId) REFERENCES title_akas(titleId)
);

CREATE TABLE crew_directors (
    tconst TEXT NOT NULL,
    nconst TEXT NOT NULL,
    CONSTRAINT crew_directors_UK UNIQUE(tconst, nconst),
    CONSTRAINT crew_directors_title_basics_FK FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    CONSTRAINT crew_directors_name_basics_FK FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

CREATE TABLE crew_writers (
    tconst TEXT NOT NULL,
    nconst TEXT NOT NULL,
    CONSTRAINT crew_writers_UK UNIQUE(tconst, nconst),
    CONSTRAINT crew_writers_title_basics_FK FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    CONSTRAINT crew_writers_name_basics_FK FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

CREATE TABLE title_episode (
    tconst TEXT NOT NULL,
    parentTconst TEXT NOT NULL,
    seasonNumber INTEGER NOT NULL,
    episodeNumber INTEGER NOT NULL,
    CONSTRAINT title_episode_PK PRIMARY KEY (tconst),
    CONSTRAINT title_episode_tconst_FK FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    CONSTRAINT title_episode_parent_FK FOREIGN KEY (parentTconst) REFERENCES title_basics(tconst)
);

CREATE TABLE title_principals (
    tconst TEXT NOT NULL,
    nconst TEXT NOT NULL,
    ordering INTEGER NOT NULL,
    category TEXT NOT NULL,
    job TEXT,
    characters TEXT,
    CONSTRAINT title_principals_UK UNIQUE (tconst, nconst),
    CONSTRAINT title_principals_tconst_FK FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    CONSTRAINT title_principals_nconst_FK FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

CREATE TABLE title_ratings (
    tconst TEXT PRIMARY KEY,
    averageRating REAL,
    numVotes INTEGER,
    CONSTRAINT title_ratings_tconst_FK FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
);

CREATE TABLE name_basics (
    nconst TEXT PRIMARY KEY,
    primaryName TEXT NOT NULL,
    birthYear INTEGER NOT NULL,
    deathYear INTEGER
);

CREATE TABLE name_professions (
    nconst TEXT NOT NULL,
    profession TEXT NOT NULL,
    CONSTRAINT name_professions_UK UNIQUE(nconst, profession),
    CONSTRAINT name_professions_nconst_FK FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

CREATE TABLE known_for_titles (
    nconst TEXT NOT NULL,
    tconst TEXT NOT NULL,
    CONSTRAINT known_for_titles_UK UNIQUE(nconst, tconst),
    CONSTRAINT known_for_titles_nconst_FK FOREIGN KEY (nconst) REFERENCES name_basics(nconst),
    CONSTRAINT known_for_titles_tconst_FK FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
);

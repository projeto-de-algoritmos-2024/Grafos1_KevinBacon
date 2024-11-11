CREATE TABLE title_basics (
    tconst TEXT NOT NULL,
    titleType TEXT NOT NULL,
    primaryTitle TEXT NOT NULL,
    originalTitle TEXT NOT NULL,
    isAdult INTEGER NOT NULL,
    startYear INTEGER,
    endYear INTEGER,
    runtimeMinutes INTEGER,
    CONSTRAINT title_basics_PK PRIMARY KEY (tconst)
);

CREATE TABLE genres (
    tconst TEXT NOT NULL,
    genre TEXT NOT NULL,
    CONSTRAINT title_genres_UQ UNIQUE(tconst, genre),
    CONSTRAINT title_genres_title_basics_FK FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
);

CREATE TABLE name_basics (
    nconst TEXT NOT NULL,
    primaryName TEXT,
    birthYear INTEGER,
    deathYear INTEGER,
    CONSTRAINT name_basics_PK PRIMARY KEY (nconst)
);

CREATE TABLE primaryProfession (
    nconst TEXT NOT NULL,
    profession TEXT NOT NULL,
    CONSTRAINT name_professions_UK UNIQUE(nconst, profession),
    CONSTRAINT name_professions_nconst_FK FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

CREATE TABLE knownForTitles (
    nconst TEXT NOT NULL,
    title TEXT NOT NULL,
    CONSTRAINT known_for_titles_UK UNIQUE(nconst, title),
    CONSTRAINT known_for_titles_nconst_FK FOREIGN KEY (nconst) REFERENCES name_basics(nconst),
    CONSTRAINT known_for_titles_tconst_FK FOREIGN KEY (title) REFERENCES title_basics(tconst)
);

CREATE TABLE crew_directors (
    tconst TEXT NOT NULL,
    director TEXT NOT NULL,
    CONSTRAINT crew_directors_UK UNIQUE(tconst, director),
    CONSTRAINT crew_directors_title_basics_FK FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    CONSTRAINT crew_directors_name_basics_FK FOREIGN KEY (director) REFERENCES name_basics(nconst)
);

CREATE TABLE crew_writers (
    tconst TEXT NOT NULL,
    writer TEXT NOT NULL,
    CONSTRAINT crew_writers_UK UNIQUE(tconst, writer),
    CONSTRAINT crew_writers_title_basics_FK FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    CONSTRAINT crew_writers_name_basics_FK FOREIGN KEY (writer) REFERENCES name_basics(nconst)
);

CREATE TABLE title_akas (
    titleId TEXT NOT NULL,
    ordering INTEGER NOT NULL,
    title TEXT NOT NULL,
    region TEXT,
    language TEXT,
    isOriginalTitle BOOLEAN,
    CONSTRAINT title_akas_PK PRIMARY KEY (titleId, ordering),
    CONSTRAINT title_akas_title_basics_FK FOREIGN KEY (titleId) REFERENCES title_basics(tconst)
);

CREATE TABLE types (
    titleId INTEGER NOT NULL,
    ordering INTEGER NOT NULL,
    type TEXT NOT NULL,
    CONSTRAINT akas_types_UK UNIQUE (titleId, ordering, type),
    CONSTRAINT akas_types_title_akas_FK FOREIGN KEY (titleId, ordering) REFERENCES title_akas(titleId, ordering)
);

CREATE TABLE attributes (
    titleId INTEGER NOT NULL,
    ordering INTEGER NOT NULL,
    attribute TEXT NOT NULL,
    CONSTRAINT akas_attributes_UK UNIQUE(titleId, ordering, attribute),
    CONSTRAINT akas_attributes_title_akas_FK FOREIGN KEY (titleId, ordering) REFERENCES title_akas(titleId, ordering)
);

CREATE TABLE title_episode (
    tconst TEXT NOT NULL,
    parentTconst TEXT NOT NULL,
    seasonNumber INTEGER,
    episodeNumber INTEGER,
    CONSTRAINT title_episode_PK PRIMARY KEY (tconst),
    CONSTRAINT title_episode_tconst_FK FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    CONSTRAINT title_episode_parent_FK FOREIGN KEY (parentTconst) REFERENCES title_basics(tconst)
);

CREATE TABLE title_principals (
    tconst TEXT NOT NULL,
    ordering INTEGER NOT NULL,
    nconst TEXT NOT NULL,
    category TEXT NOT NULL,
    job TEXT,
    characters TEXT,
    CONSTRAINT title_principals_UK UNIQUE (tconst, ordering, nconst),
    CONSTRAINT title_principals_tconst_FK FOREIGN KEY (tconst) REFERENCES title_basics(tconst),
    CONSTRAINT title_principals_nconst_FK FOREIGN KEY (nconst) REFERENCES name_basics(nconst)
);

CREATE TABLE title_ratings (
    tconst TEXT NOT NULL,
    averageRating REAL,
    numVotes INTEGER,
    CONSTRAINT title_ratings_PK PRIMARY KEY (tconst),
    CONSTRAINT title_ratings_tconst_FK FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
);
#----------------------------------------CREACIÓN DE LA BASE DE DATOS--------------------------------------------------
CREATE DATABASE proyectobdd01 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;;
USE proyectobdd01;

DROP PROCEDURE IF EXISTS creacion_tablas ;
DELIMITER $$
CREATE PROCEDURE creacion_tablas ()
BEGIN
    DROP TABLE IF EXISTS Production_Countries_temp;
    SET @sql_text = 'CREATE TABLE Production_Countries_temp (
        iso_3166_1 varchar(10) NOT NULL,
        Name varchar(155) NOT NULL
    );';
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    DROP TABLE IF EXISTS production_Companies_temp;
    SET @sql_text = 'CREATE TABLE Production_Companies_temp (
        id_pc int NOT NULL,
        Name varchar(155) NOT NULL
    );';
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    DROP TABLE IF EXISTS spoken_languages_temp;
    SET @sql_text = 'CREATE TABLE spoken_languages_temp (
        iso_639_1 varchar(10) NOT NULL,
        Name varchar(155) DEFAULT NULL
    );';
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    DROP TABLE IF EXISTS People_temp;
    SET @sql_text = 'CREATE TABLE `People_temp` (
        `id` int NOT NULL,
        `name_` varchar(155) DEFAULT NULL,
        `gender` int NOT NULL
    );';
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END$$
DELIMITER ;
CALL creacion_tablas();
#----------------------------------------------PROCEDURES PEOPLE-----------------------------------------------
DROP PROCEDURE IF EXISTS cursor_People ;
DELIMITER $$
CREATE PROCEDURE cursor_People ()
BEGIN
     DECLARE done INT DEFAULT FALSE ;
     DECLARE jsonData json ;
     DECLARE jsonIdPeople int;
     DECLARE jsonGender int;
     DECLARE jsonName_ varchar(155);
     DECLARE i INT;
 -- Declarar el cursor
 DECLARE myCursor
  CURSOR FOR
   SELECT JSON_EXTRACT(CONVERT(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(crew, '"', '\''), '{\'', '{"'),
    '\': \'', '": "'),'\', \'', '", "'),'\': ', '": '),', \'', ', "')
    USING UTF8mb4 ), '$[*]') FROM movie_dataset;
 -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
 DECLARE CONTINUE HANDLER
  FOR NOT FOUND SET done = TRUE ;
 -- Abrir el cursor
 OPEN myCursor  ;
 cursorLoop: LOOP
  FETCH myCursor INTO jsonData;
  -- Controlador para buscar cada uno de lso arrays
    SET i = 0;
  -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
  IF done THEN
   LEAVE  cursorLoop ;
  END IF ;
  WHILE(JSON_EXTRACT(jsonData, CONCAT('$[', i, ']')) IS NOT NULL) DO
  SET jsonName_ = IFNULL(JSON_EXTRACT(jsonData,  CONCAT('$[', i, '].name')), '') ;
  SET jsonGender = IFNULL(JSON_EXTRACT(jsonData, CONCAT('$[', i,'].gender')), '') ;
  SET jsonIdPeople = IFNULL(JSON_EXTRACT(jsonData, CONCAT('$[', i,'].id')), '') ;
  SET i = i + 1;
  SET @sql_text = CONCAT(' INSERT People_temp VALUES (', jsonIdPeople, ', ', jsonName_, ',',jsonGender,'); ');
PREPARE stmt FROM @sql_text;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
  END WHILE;
 END LOOP ;
    DROP TABLE IF EXISTS People;
    SET @sql_text = 'CREATE TABLE `People` (
        `id` int NOT NULL,
        `name_` varchar(155) DEFAULT NULL,
        `gender` int NOT NULL,
        PRIMARY KEY (id,gender)
    );';
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

     INSERT INTO People (id,gender,name_)
    SELECT DISTINCT id,gender,name_
    FROM People_temp;
    DROP TABLE People_temp;
 CLOSE myCursor ;
END$$
DELIMITER ;
CALL cursor_People();
#----------------------------------------------PROCEDURES Movies PRIMER PASO-----------------------------------------------
DROP PROCEDURE IF EXISTS Procedure_Movie ;
DELIMITER $$
CREATE PROCEDURE Procedure_Movie()
BEGIN
      DROP TABLE IF EXISTS Movies;
        CREATE TABLE `Movies` (
      `budget` bigint DEFAULT NULL,
      `homepage` varchar(255) DEFAULT NULL,
      `id` int NOT NULL,
      `keywords_` text DEFAULT NULL,
      `original_language` varchar(5) NOT NULL,
      `original_title` varchar(255) DEFAULT NULL,
      `overview` text,
      `popularity` double DEFAULT NULL,
      `release_date` varchar(25) DEFAULT NULL,
      `revenue` bigint DEFAULT NULL,
      `runtime` varchar(255) DEFAULT NULL,
      `tagline` varchar(255) DEFAULT NULL,
      `title` varchar(255) DEFAULT NULL,
      `vote_average` float DEFAULT NULL,
      `vote_count` int DEFAULT NULL,
      PRIMARY KEY (`id`)
    );
      INSERT INTO Movies (budget, homepage,id,keywords_,original_language,original_title,overview,popularity,release_date,
                          revenue,runtime,tagline,title,vote_average,vote_count)
        SELECT budget, homepage,id,keywords,original_language,original_title,overview,popularity,release_date,revenue,runtime,tagline,title,vote_average,vote_count
        FROM movie_dataset;
END $$
DELIMITER ;
CALL Procedure_Movie();
#----------------------------------------------PROCEDURES SpokenLanguages Primera Parte ------------------------------------
DROP PROCEDURE IF EXISTS Procedurejson_spokenLenguages ;
DELIMITER $$
CREATE PROCEDURE Procedurejson_spokenLenguages ()
BEGIN
 DECLARE done INT DEFAULT FALSE ;
 DECLARE jsonData json ;
 DECLARE jsonId varchar(250) ;
 DECLARE jsonLabel varchar(250) ;
 DECLARE i INT;

 -- Declarar el cursor
 DECLARE myCursor
  CURSOR FOR
   SELECT JSON_EXTRACT(CONVERT(spoken_languages USING UTF8MB4), '$[*]') FROM movie_dataset;

 -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
 DECLARE CONTINUE HANDLER
  FOR NOT FOUND SET done = TRUE ;
 -- Abrir el cursor
 OPEN myCursor  ;
 cursorLoop: LOOP
  FETCH myCursor INTO jsonData;
  -- Controlador para buscar cada uno de lso arrays
    SET i = 0;
  -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
  IF done THEN
   LEAVE  cursorLoop ;
  END IF ;

  WHILE(JSON_EXTRACT(jsonData, CONCAT('$[', i, ']')) IS NOT NULL) DO

  SET jsonId = IFNULL(JSON_EXTRACT(jsonData,  CONCAT('$[', i, '].iso_639_1')), '') ;
  SET jsonLabel = IFNULL(JSON_EXTRACT(jsonData, CONCAT('$[', i,'].name')), '') ;
  SET i = i + 1;
  SET @sql_text = CONCAT('INSERT INTO spoken_languages_temp VALUES (', REPLACE(jsonId,'\'',''), ', ', jsonLabel, '); ');
PREPARE stmt FROM @sql_text;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
  END WHILE;

 END LOOP ;
    DROP TABLE IF EXISTS spoken_languages;
    CREATE TABLE spoken_languages AS
    SELECT Distinct iso_639_1,name
    FROM spoken_languages_temp ;

    ALTER TABLE spoken_languages
    ADD PRIMARY KEY (iso_639_1);
    DROP TABLE spoken_languages_temp;
 CLOSE myCursor ;
END$$
DELIMITER ;

CALL Procedurejson_spokenLenguages ();
#----------------------------------------------PROCEDURES SpokenLanguages Segunda Parte ------------------------------------
DROP TABLE IF EXISTS Movies_spokenLanguages;
CREATE TABLE Movies_spokenLanguages (
      `ISO_639_1` varchar(255) NOT NULL,
      `Id_mv` int NOT NULL,
      PRIMARY KEY (`ISO_639_1`,`Id_mv`),
      FOREIGN KEY (`Id_mv`) REFERENCES `Movies` (`id`),
      FOREIGN KEY (`ISO_639_1`) REFERENCES `spoken_Languages` (`ISO_639_1`)
    );
DROP PROCEDURE IF EXISTS ProcedureRelacion_spokenLenguages ;
DELIMITER $$
CREATE PROCEDURE ProcedureRelacion_spokenLenguages ()
BEGIN
 DECLARE done INT DEFAULT FALSE ;
 DECLARE jsonData json ;
 DECLARE jsonId varchar(250) ;
 DECLARE jsonLabel varchar(250) ;
 DECLARE resultSTR LONGTEXT DEFAULT '';
 DECLARE i INT;
 DECLARE idmv INT;

 -- Declarar el cursor
 DECLARE myCursor
  CURSOR FOR
   SELECT id,JSON_EXTRACT(CONVERT(spoken_languages USING UTF8MB4), '$[*]') FROM movie_dataset;

 -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
 DECLARE CONTINUE HANDLER
  FOR NOT FOUND SET done = TRUE ;
 -- Abrir el cursor
 OPEN myCursor  ;
 cursorLoop: LOOP
  FETCH myCursor INTO idmv ,jsonData;
  -- Controlador para buscar cada uno de lso arrays
    SET i = 0;
  -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
  IF done THEN
   LEAVE  cursorLoop ;
  END IF ;
  WHILE(JSON_EXTRACT(jsonData, CONCAT('$[', i, ']')) IS NOT NULL) DO

  SET jsonId = IFNULL(JSON_EXTRACT(jsonData,  CONCAT('$[', i, '].iso_639_1')), '') ;
  SET i = i + 1;
  SET @sql_text = CONCAT('INSERT INTO Movies_spokenLanguages VALUES (', REPLACE(jsonId,'\'',''), ', ', idmv, '); ');
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END WHILE;
 END LOOP ;
 CLOSE myCursor ;
END$$
DELIMITER ;
CALL ProcedureRelacion_spokenLenguages ();
#----------------------------------------------PROCEDURES Production Companies Primera Parte ------------------------------------
DROP PROCEDURE IF EXISTS Procedure_ProductionCompanies;
DELIMITER $$
CREATE PROCEDURE Procedure_ProductionCompanies ()
BEGIN
    DECLARE done INT DEFAULT FALSE ;
    DECLARE jsonData json ;
    DECLARE jsonId varchar(250) ;
    DECLARE jsonLabel varchar(250) ;
    DECLARE i INT;

    -- Declarar el cursor
    DECLARE myCursor
        CURSOR FOR
        SELECT JSON_EXTRACT(CONVERT(production_companies USING UTF8MB4), '$[*]') FROM movie_dataset ;

    -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
    DECLARE CONTINUE HANDLER
        FOR NOT FOUND SET done = TRUE ;
    -- Abrir el cursor
    OPEN myCursor  ;
    cursorLoop: LOOP
        FETCH myCursor INTO jsonData;
        -- Controlador para buscar cada uno de lso arrays
        SET i = 0;
        -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
        IF done THEN
            LEAVE  cursorLoop ;
        END IF ;

        WHILE(JSON_EXTRACT(jsonData, CONCAT('$[', i, ']')) IS NOT NULL) DO

                SET jsonId = IFNULL(JSON_EXTRACT(jsonData,  CONCAT('$[', i, '].id')), '') ;
                SET jsonLabel = IFNULL(JSON_EXTRACT(jsonData, CONCAT('$[', i,'].name')), '') ;
                SET i = i + 1;
                SET @sql_text = CONCAT('INSERT INTO Production_Companies_temp VALUES (', REPLACE(jsonId,'\'',''), ', ', jsonLabel, '); ');
                PREPARE stmt FROM @sql_text;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
            END WHILE;

    END LOOP ;
    CREATE TABLE Production_Companies AS
    SELECT Distinct id_pc,name
    FROM Production_Companies_temp ;

    ALTER TABLE Production_Companies
        ADD PRIMARY KEY (id_pc);

    DROP TABLE production_Companies_temp;
    CLOSE myCursor ;
END$$
DELIMITER ;

CALL Procedure_ProductionCompanies ();
#----------------------------------------------PROCEDURES Production Companies Segunda Parte ------------------------------------
DROP TABLE IF EXISTS Movies_ProductionCompanies;
CREATE TABLE `Movies_ProductionCompanies` (
  `id_pc` int NOT NULL,
  `id_mv` int NOT NULL,
  PRIMARY KEY (`id_mv`,`id_pc`),
  FOREIGN KEY (`id_mv`) REFERENCES `Movies` (`id`),
  FOREIGN KEY (`id_pc`) REFERENCES `Production_Companies` (`id_pc`)
);

DROP PROCEDURE IF EXISTS ProcedureRelacion_ProductionCompanies ;
DELIMITER $$
CREATE PROCEDURE ProcedureRelacion_ProductionCompanies ()
BEGIN
    DECLARE done INT DEFAULT FALSE ;
    DECLARE jsonData json ;
    DECLARE jsonId varchar(250) ;
    DECLARE i INT;
    DECLARE idmv INT;

    -- Declarar el cursor
    DECLARE myCursor
        CURSOR FOR
        SELECT id,JSON_EXTRACT(CONVERT(production_companies USING UTF8MB4), '$[*]') FROM movie_dataset;

    -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
    DECLARE CONTINUE HANDLER
        FOR NOT FOUND SET done = TRUE ;
    -- Abrir el cursor
    OPEN myCursor  ;
    cursorLoop: LOOP
        FETCH myCursor INTO idmv ,jsonData;
        -- Controlador para buscar cada uno de lso arrays
        SET i = 0;
        -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
        IF done THEN
            LEAVE  cursorLoop ;
        END IF ;

        WHILE(JSON_EXTRACT(jsonData, CONCAT('$[', i, ']')) IS NOT NULL) DO

                SET jsonId = IFNULL(JSON_EXTRACT(jsonData,  CONCAT('$[', i, '].id')), '') ;
                SET i = i + 1;
                SET @sql_text = CONCAT('INSERT INTO Movies_ProductionCompanies VALUES (', REPLACE(jsonId,'\'',''), ', ', idmv, '); ');
                PREPARE stmt FROM @sql_text;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
            END WHILE;

    END LOOP ;

    CLOSE myCursor ;
END$$
DELIMITER ;

CALL ProcedureRelacion_ProductionCompanies ();
#----------------------------------------------PROCEDURES Production Countries Primera Parte ------------------------------------
DROP PROCEDURE IF EXISTS Procedure_ProductionCountries;
DELIMITER $$
CREATE PROCEDURE Procedure_ProductionCountries ()
BEGIN
    DECLARE done INT DEFAULT FALSE ;
    DECLARE jsonData json ;
    DECLARE jsonId varchar(250) ;
    DECLARE jsonLabel varchar(250) ;
    DECLARE i INT;

    -- Declarar el cursor
    DECLARE myCursor
        CURSOR FOR
        SELECT JSON_EXTRACT(CONVERT(production_Countries USING UTF8MB4), '$[*]') FROM movie_dataset;

    -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
    DECLARE CONTINUE HANDLER
        FOR NOT FOUND SET done = TRUE ;
    -- Abrir el cursor
    OPEN myCursor  ;
    cursorLoop: LOOP
        FETCH myCursor INTO jsonData;
        -- Controlador para buscar cada uno de lso arrays
        SET i = 0;
        -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
        IF done THEN
            LEAVE  cursorLoop ;
        END IF ;

        WHILE(JSON_EXTRACT(jsonData, CONCAT('$[', i, ']')) IS NOT NULL) DO

                SET jsonId = IFNULL(JSON_EXTRACT(jsonData,  CONCAT('$[', i, '].iso_3166_1')), '') ;
                SET jsonLabel = IFNULL(JSON_EXTRACT(jsonData, CONCAT('$[', i,'].name')), '') ;
                SET i = i + 1;
                SET @sql_text = CONCAT('INSERT INTO Production_Countries_temp VALUES (', REPLACE(jsonId,'\'',''), ', ', jsonLabel, '); ');
                PREPARE stmt FROM @sql_text;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
            END WHILE;

    END LOOP ;

    CREATE TABLE Production_Countries AS
    SELECT Distinct iso_3166_1,name
    FROM Production_Countries_temp ;

    ALTER TABLE Production_Countries
        ADD PRIMARY KEY (iso_3166_1);

    DROP TABLE Production_Countries_temp;
    CLOSE myCursor ;
END$$
DELIMITER ;

CALL Procedure_ProductionCountries ();
#----------------------------------------------PROCEDURES Production Countries Segunda Parte ------------------------------------
CREATE TABLE `Movies_Countries` (
  `Id_mv` int NOT NULL,
  `ISO_3166_1` varchar(255) NOT NULL,
  PRIMARY KEY (`Id_mv`,`ISO_3166_1`),
  FOREIGN KEY (`Id_mv`) REFERENCES `Movies` (`id`),
  FOREIGN KEY (`ISO_3166_1`) REFERENCES `Production_Countries` (`ISO_3166_1`)
);

DROP PROCEDURE IF EXISTS ProcedureRelacion_ProductionCountries ;
DELIMITER $$
CREATE PROCEDURE ProcedureRelacion_ProductionCountries ()
BEGIN
    DECLARE done INT DEFAULT FALSE ;
    DECLARE jsonData json ;
    DECLARE jsonId varchar(250) ;
    DECLARE i INT;
    DECLARE idmv INT;
    -- Declarar el cursor
    DECLARE myCursor
        CURSOR FOR
        SELECT id,JSON_EXTRACT(CONVERT(production_countries USING UTF8MB4), '$[*]') FROM movie_dataset;
    -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
    DECLARE CONTINUE HANDLER
        FOR NOT FOUND SET done = TRUE ;
    -- Abrir el cursor
    OPEN myCursor  ;
    cursorLoop: LOOP
        FETCH myCursor INTO idmv ,jsonData;
        -- Controlador para buscar cada uno de los arrays
        SET i = 0;
        -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
        IF done THEN
            LEAVE  cursorLoop ;
        END IF ;
        WHILE(JSON_EXTRACT(jsonData, CONCAT('$[', i, ']')) IS NOT NULL) DO

                SET jsonId = IFNULL(JSON_EXTRACT(jsonData,  CONCAT('$[', i, '].iso_3166_1')), '') ;
                SET i = i + 1;
                SET @sql_text = CONCAT('INSERT INTO Movies_Countries (ISO_3166_1,Id_mv) VALUES (', REPLACE(jsonId,'\'',''), ', ', idmv, '); ');
                PREPARE stmt FROM @sql_text;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
            END WHILE;
    END LOOP ;
    CLOSE myCursor ;
END$$
DELIMITER ;

CALL ProcedureRelacion_ProductionCountries ();
#----------------------------------------------PROCEDURES crew ------------------------------------
CREATE TABLE `Crew` (
  `id_People` int NOT NULL,
  `id_mv` int NOT NULL,
  `name_Job` varchar(255) NOT NULL,
  `credit_id` varchar(105) NOT NULL,
  `department` varchar(25) NOT NULL,
  PRIMARY KEY (`id_People`,`name_Job`,`id_mv`),
  FOREIGN KEY (`id_People`) REFERENCES `People` (`id`),
  FOREIGN KEY (`id_mv`) REFERENCES `Movies` (`id`)
);

DROP PROCEDURE IF EXISTS Procedure_Crew ;
DELIMITER $$
CREATE PROCEDURE Procedure_Crew ()
BEGIN
     DECLARE done INT DEFAULT FALSE ;
     DECLARE jsonData json ;
     DECLARE jsonIdPeople int;
     DECLARE jsonNameJob VARCHAR(255);
     DECLARE jsonCreditId VARCHAR(105);
     DECLARE jsonDepartment VARCHAR(25);
     DECLARE idmv INT;
     DECLARE i INT;
 -- Declarar el cursor
 DECLARE myCursor
  CURSOR FOR
   SELECT id,JSON_EXTRACT(CONVERT(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(crew, '"', '\''), '{\'', '{"'),
    '\': \'', '": "'),'\', \'', '", "'),'\': ', '": '),', \'', ', "')
    USING UTF8mb4 ), '$[*]') FROM movie_dataset;
 -- Declarar el handler para NOT FOUND (esto es marcar cuando el cursor ha llegado a su fin)
 DECLARE CONTINUE HANDLER
  FOR NOT FOUND SET done = TRUE ;
 -- Abrir el cursor
 OPEN myCursor  ;
 cursorLoop: LOOP
  FETCH myCursor INTO idmv,jsonData;
  -- Controlador para buscar cada uno de los arrays
    SET i = 0;
  -- Si alcanzo el final del cursor entonces salir del ciclo repetitivo
  IF done THEN
   LEAVE  cursorLoop ;
  END IF ;
  WHILE(JSON_EXTRACT(jsonData, CONCAT('$[', i, ']')) IS NOT NULL) DO
  SET jsonIdPeople = IFNULL(JSON_EXTRACT(jsonData,  CONCAT('$[', i, '].id')), '') ;
  SET jsonNameJob = IFNULL(JSON_EXTRACT(jsonData, CONCAT('$[', i,'].job')), '') ;
  SET jsonCreditId = IFNULL(JSON_EXTRACT(jsonData, CONCAT('$[', i,'].credit_id')), '') ;
  SET jsonDepartment = IFNULL(JSON_EXTRACT(jsonData, CONCAT('$[', i,'].department')), '') ;
  SET i = i + 1;
  SET @sql_text = CONCAT('INSERT INTO Crew VALUES (',jsonIdPeople,',',idmv,',',jsonNameJob,',',jsonCreditId,',',jsonDepartment,');');
PREPARE stmt FROM @sql_text;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
  END WHILE;
 END LOOP ;
 CLOSE myCursor ;
END$$
DELIMITER ;
CALL Procedure_Crew();
#----------------------------------------------PROCEDURES Status ------------------------------------
DROP PROCEDURE IF EXISTS Procedure_Status ;
DELIMITER $$
    CREATE PROCEDURE Procedure_Status()
    BEGIN
        DROP TABLE IF EXISTS Status;
    CREATE TABLE Status (status VARCHAR(25)) AS SELECT DISTINCT status
    FROM movie_dataset;

    ALTER TABLE Status
    ADD PRIMARY KEY (status);
        DROP TABLE IF EXISTS Status_movie;
    #Creación e INSERTS movie-status
    CREATE TABLE Status_movie (id INT NOT NULL,
                                status VARCHAR(25))AS SELECT id, st.status AS status
    FROM movie_dataset movie_status, Status st
    WHERE movie_status.status = st.status;

    ALTER TABLE status_movie
    ADD FOREIGN KEY (id) REFERENCES Movies(id),
        ADD FOREIGN KEY (status) REFERENCES Status(status);

END $$
DELIMITER ;
CALL Procedure_Status();
#----------------------------------------------PROCEDURES Directors ------------------------------------
DROP PROCEDURE IF EXISTS Procedure_Directors ;
DELIMITER $$
    CREATE PROCEDURE Procedure_Directors()
    BEGIN
        DROP TABLE IF EXISTS Directors;
    CREATE TABLE `Directors` (
      `id_People` int NOT NULL,
      `id_mv` int NOT NULL,
      PRIMARY KEY (`id_People`,`id_mv`),
      FOREIGN KEY (`id_People`) REFERENCES `Crew` (`id_People`),
      FOREIGN KEY (`id_mv`) REFERENCES `Movies` (`id`)
    );
        INSERT IGNORE INTO Directors(id_People, id_mv)
            SELECT  c.id_People, c.id_mv FROM Crew c,People p,movie_dataset m
            WHERE  c.name_Job = 'Director' AND p.id = c.id_People AND m.id = c.id_mv;

END $$
DELIMITER ;
CALL Procedure_Directors();

#----------------------------------------------PROCEDURES Cast ------------------------------------
DROP TABLE IF EXISTS cast_;
CREATE TABLE `cast_` (
  `id_mv` int NOT NULL,
  `id_People` int NOT NULL,
  PRIMARY KEY (`id_mv`,`id_People`),
  FOREIGN KEY (`id_mv`) REFERENCES `Movies` (`id`),
  FOREIGN KEY (`id_People`) REFERENCES `People` (`id`)
);

DROP PROCEDURE IF EXISTS Procedure_Cast_ ; -- n
DELIMITER $$
    CREATE PROCEDURE Procedure_Cast_()
    BEGIN
    DROP TABLE IF EXISTS cast_;
    CREATE TABLE `cast_` (
        `id_mv` int NOT NULL,
        `id_People` int NOT NULL,
        PRIMARY KEY (`id_mv`,`id_People`),
        FOREIGN KEY (`id_mv`) REFERENCES `Movies` (`id`),
        FOREIGN KEY (`id_People`) REFERENCES `People` (`id`)
    );
        INSERT IGNORE INTO cast_(id_People, id_mv)
            SELECT c.id_People, c.id_mv FROM Crew c,People p,movie_dataset m
            WHERE p.id = c.id_People AND m.id = c.id_mv;

END $$
DELIMITER ;
CALL Procedure_Cast_();

DROP PROCEDURE IF EXISTS Procedure_Cast_2 ; -- y?
DELIMITER $$
    CREATE PROCEDURE Procedure_Cast_2()
    BEGIN
    DROP TABLE IF EXISTS cast_;
    CREATE TABLE `cast_` (
        `id_mv` int NOT NULL,
        `cast` text NOT NULL,
        PRIMARY KEY (`id_mv`),
        FOREIGN KEY (`id_mv`) REFERENCES `Movies` (`id`)
    );
        INSERT IGNORE INTO cast_(id_mv, cast)
            SELECT m.cast, n.id FROM Movies n,movie_dataset m
            WHERE  m.id = n.id;

END $$
DELIMITER ;
CALL Procedure_Cast_2();

#----------------------------------------------PROCEDURES Genres ------------------------------------
DROP PROCEDURE IF EXISTS Procedure_Genres ;
DELIMITER $$
    CREATE PROCEDURE Procedure_Genres()
    BEGIN
        DROP TABLE IF EXISTS genres;
    CREATE TABLE genres(genres VARCHAR(100)) AS
    SELECT DISTINCT (
    SUBSTRING_INDEX(SUBSTRING_INDEX(genres,' ', 5), ' ', -1)) AS genres
    FROM movie_dataset;
    DELETE
    FROM genres
    WHERE genres IS NULL;

    ALTER TABLE genres
        ADD PRIMARY KEY (genres);
    DROP TABLE IF EXISTS Movies_genres;
    CREATE TABLE Movies_genres AS
        SELECT tg.genres, id
        FROM genres tg, movie_dataset mv
        WHERE INSTR(mv.genres, tg.genres )>0;

    ALTER TABLE Movies_genres
    ADD FOREIGN KEY (id)
        REFERENCES Movies(id),
        ADD FOREIGN KEY (genres)
            REFERENCES genres(genres);
END $$
DELIMITER ;
CALL Procedure_Genres();

-- -----------------------------------------------------Crew Replace---------------------------------------------
UPDATE movie_dataset
SET crew = JSON_EXTRACT(CONVERT(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(crew, '"', '\''), '{\'', '{"'),
    '\': \'', '": "'),'\', \'', '", "'),'\': ', '": '),', \'', ', "')
    USING UTF8mb4 ), '$[*]')
WHERE NOT JSON_VALID(crew);

-- ---------------------------------------------Consultas----------------------------------------------
-- Compañias productoras y número de producciones
SELECT production_companies.name, COUNT(movies_productioncompanies.id_mv) as total_produc
FROM production_companies
JOIN movies_productioncompanies ON production_companies.id_pc = movies_productioncompanies.id_pc
GROUP BY production_companies.name
ORDER BY total_produc DESC;

-- número lenguas distintas (original_language)
SELECT original_language, COUNT(original_language) AS OgLang_Num
FROM movies
GROUP BY original_language
ORDER BY OgLang_Num DESC;

-- spoken language --
SELECT ISO_639_1, COUNT(ISO_639_1) AS Num_spoken
FROM movies_spokenlanguages
GROUP BY ISO_639_1
ORDER BY Num_spoken DESC;

-- cantidad jobs --
SELECT people.name_, COUNT(*) AS Num_jobs
FROM crew
JOIN people ON crew.id_People = people.id
GROUP BY people.name_
ORDER BY Num_jobs DESC;

SELECT DISTINCT people.name_, COUNT(*) AS Num_Jobs
FROM crew, people
WHERE id_People = id
GROUP BY name_
ORDER BY Num_Jobs DESC;

-- Jobs con mas cantidad de personas
SELECT name_Job, COUNT(name_Job) as Num_people
FROM crew
GROUP BY name_Job
ORDER BY COUNT(name_Job) DESC;

-- Péliculas con/sin director --
SELECT (SELECT COUNT(*) FROM movies m
            WHERE EXISTS (SELECT 1 FROM directors d
                                WHERE m.id = d.id_mv)) AS Movies_con_director,
        (SELECT COUNT(*) FROM movies m
            WHERE NOT EXISTS (SELECT 1 FROM directors d
                                WHERE m.id = d.id_mv)) AS Movies_sin_director
;

-- directores que no constan en crew--...
SELECT people.name_
FROM directors
JOIN people ON directors.id_People = people.id
WHERE NOT EXISTS (
    SELECT 1
    FROM movie_dataset
    WHERE JSON_SEARCH(JSON_EXTRACT(crew, '$[*].job'), 'one', 'director') IS NOT NULL
    AND directors.id_mv = movie_dataset.id
)
GROUP BY people.name_;

SELECT people.name_
FROM directors
JOIN people ON directors.id_People = people.id
WHERE NOT EXISTS (
    SELECT 1
    FROM movie_dataset
    WHERE JSON_SEARCH(crew, 'one', directors.id_People) IS NOT NULL
    AND directors.id_mv = movie_dataset.id
)
GROUP BY people.name_;

SELECT JSON_EXTRACT(crew, '$[*].id') FROM movie_dataset;

SELECT id_People
FROM directors
WHERE id_People NOT IN (SELECT id_People FROM crew WHERE name_Job = 'Director') ; -- este

-- consultas
SELECT COUNT(*) as has_homepage
FROM movies
WHERE homepage IS NOT NULL;

SELECT (SELECT COUNT(*) FROM movies WHERE homepage IS NULL) AS Movies_sin_homepage,
        (SELECT COUNT(*) FROM movies WHERE homepage IS NOT NULL) AS Movies_con_homepage
FROM DUAL;

SELECT DISTINCT homepage, COUNT(*) as Instancias_homepage
FROM movies
GROUP BY homepage
ORDER BY Instancias_homepage DESC;

-- Cuantos personajes distintos hay en cast (mal)
SELECT COUNT(id_People) FROM cast_; -- Asumiendo que un actor tiene personaje distinto en cada película

SELECT SUM(personajes) FROM (SELECT COUNT(id_People) AS personajes FROM cast_) AS Cantidad_personajes; -- mal
-- ------------------------------------------------Limpieza cast------------------------------------------------
-- Crear la tabla temporal
DROP TABLE IF EXISTS movies_temp;
DROP TABLE IF EXISTS Cast_temp;
CREATE TABLE Cast_temp AS
SELECT id, cast as cast_original,
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE
    (REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cast), ' Jr.', '_Jr#'),
'. ', '#-' ),
 'Helena Bonham Carter',
 'Helena Bonham_Carter'),
 'See Tao', 'See_Tao'),
 ' T. ', ' T._'),
 ' J. ', ' J._'),
 'R. ', 'R._'),
 ' C. ', '_C. '),
 ' D. ', ' D._'),
 ' E. ', '_E. '),
 ' B. ', '_B. '),
 'S. ', 'S._'),
 ' L. ', ' L._'),
 'St. ', 'St._'),
 'A. J. ', 'A._J. '),
 'F. ', 'F._'),
 'A. ', 'A._'),
 'M.C#-Gainey', 'M.C. Gainey'),
 'Church', 'Church t'),
 'James Badge Dale', 'James Badge_Dale'),
 'Tommy Lee Jones', 'Tommy Lee_Jones'),
 'Max von Sydow', 'Max von_Sydow' ),
 'Dakota Blue Richards', 'Dakota Blue_Richards' ),
 'Rihanna', 'Rihanna *' ),
 'Bryce Dallas Howard', 'Bryce Dallas_Howard' ),
 'Larry the Cable Guy', 'Larry the_Cable_Guy' ),
 'John Michael Higgins', 'John Michael_Higgins' ),
 'R#-D#-Call', 'R.D. Call' ),
 'T.J#-Miller', 'T.J. Miller' ),
 'J.K#-Simmons', 'J.K. Simmons' ),
 'Jonathan Rhys Meyers', 'Jonathan Rhys_Meyers' ),
 'Jada Pinkett Smith', 'Jada Pinkett_Smith' ),
 'Jonny Lee Miller', 'Jonny Lee_Miller' ),
 'Benicio del Toro', 'Benicio del_Toro' ),
 'E.G#-Daily', 'E.G. Daily' ),
 'Johnny A#-Sanchez', 'Johnny A._Sanchez' ),
 'Cedric the Entertainer', 'Cedric the_Entertainer' ),
 'Jackie Earle Haley', 'Jackie Earle_Haley' ),
 'Philip Seymour Hoffman', 'Philip Seymour_Hoffman' ),
 'Sacha Baron Cohen', 'Sacha Baron_Cohen' ),
 'Billy Bob Thornton', 'Billy Bob_Thornton' ),
 'Lara Flynn Boyle', 'Lara Flynn_Boyle' ),
 'Jeffrey Dean Morgan', 'Jeffrey Dean_Morgan' ),
 'D.J#-Cotrona', 'D.J. Cotrona' ),
 'Pink', 'Pink *' ),
 'Tim Blake Nelson', 'Tim Blake_Nelson' ),
 'Will Yun Lee', 'Will Yun_Lee' ),
 'Jamie Lee Curtis', 'Jamie Lee_Curtis' ),
 'Dick Van Dyke', 'Dick Van_Dyke' ),
 'Neil Patrick Harris', 'Neil Patrick_Harris' ),
 'G#-W#-Bailey', 'G.W. Bailey' ),
 'de France', 'de_France' ),
 'Mario Van Peebles', 'Mario Van_Peebles' ),
 'Anika Noni Rose', 'Anika Noni_Rose' ),
 'Michael Clarke Duncan', 'Michael Clarke_Duncan' ),
 'Robert De Niro', 'Robert De_Niro' ),
 'Casper Van Dien', 'Casper Van_Dien' ),
 'David Hyde Pierce', 'David Hyde_Pierce' ),
 'Hadley Belle Miller', 'Hadley Belle_Miller' ),
 'Sarah Jessica Parker', 'Sarah Jessica_Parker' ),
 'Edward James Olmos', 'Edward James_Olmos' ),
 'Seann William Scott', 'Seann William_Scott' ),
 'Mary Elizabeth Winstead', 'Mary Elizabeth_Winstead' ),
 'Mr#-T', 'Mr. T' ),
 'Matthew Gray Gubler', 'Matthew Gray_Gubler' ),
 'Carice van Houten', 'Carice van_Houten' ),
 'Haley Joel Osment', 'Haley Joel_Osment' ),
 'James Earl Jones', 'James Earl_Jones' ),
 'Frances de la Tour', 'Frances de_la_Tour' ),
 'Marcia Gay Harden', 'Marcia Gay_Harden' ),
 'Sarah Michelle Gellar', 'Sarah Michelle_Gellar' ),
 'Philip Baker Hall', 'Philip Baker_Hall' ),
 'Lisa Ann Walter', 'Lisa Ann_Walter' ),
 'David Ogden Stiers', 'David Ogden_Stiers' ),
 'Nelly', 'Nelly *' ),
 'Jennifer Jason Leigh', 'Jennifer Jason_Leigh' ),
 'Marcia Gay Harden', 'Marcia Gay_Harden' ),
 'Dee Bradley Baker', 'Dee Bradley_Baker' ),
 'Julio Oscar Mechoso', 'Julio Oscar_Mechoso' ),
 'Jessica Brooks Grant', 'Jessica Brooks_Grant' ),
 'L.Q#-Jones', 'L.Q. Jones' ),
 'lafur Darri', 'lafur_Darri' ),
 'Darin De Paul', 'Darin De_Paul' ),
 'n Alonso Barona', 'n Alonso_Barona' ),
 'Jonathan Taylor Thomas', 'Jonathan Taylor_Thomas' ),
 'Mo''Nique', 'Mo''Nique *' ),
 'Wanda De Jesus', 'Wanda De_Jesus' ),
 'Cyndi Mayo Davis', 'Cyndi Mayo_Davis' ),
 'Sarah Wayne Callies', 'Sarah Wayne_Callies' ),
 'Thomas Ian Nicholas', 'Thomas Ian_Nicholas' ) as cast
FROM movie_dataset;

-- Consultar
SELECT id, cast,
LENGTH(cast) - LENGTH(replace (cast, ' ', '')) AS CantidadEspacios,
LOCATE('.', cast) AS ExistePuntos
FROM cast_temp ;

UPDATE cast_temp
SET cast = REPLACE(cast, ' ', ', ')
WHERE cast LIKE '% %';

-- ---------------------------homepage (no necesario)-------------------------------------


DROP PROCEDURE IF EXISTS Procedure_Homepage ;
DELIMITER $$
    CREATE PROCEDURE Procedure_Homepage()
    BEGIN

    DROP TABLE IF EXISTS Homepage;
    CREATE TABLE `Homepage` (
      `id_mv` int NOT NULL,
      `homepage` varchar(250),
      PRIMARY KEY (`id_mv`),
      FOREIGN KEY (`id_mv`) REFERENCES `Movies` (`id`)
    );

    INSERT INTO Homepage(id_mv, homepage)
            SELECT m.id, m.homepage FROM movies m
            WHERE  m.homepage != '' OR m.homepage IS NOT NULL;

END $$
DELIMITER ;
CALL Procedure_Homepage();

-- --------------EXCEL -----------------

-- Presupuesto science
SELECT SUM(budget) AS Presupuesto_total_science
FROM movie_dataset
WHERE genres
          LIKE '%Science Fiction%';

-- Released y rumored
SELECT *
FROM movie_dataset
WHERE status = 'Rumored' OR status = 'Released' ;

SELECT * FROM movies WHERE id IN (SELECT id FROM movie_dataset WHERE status =  'Released' OR status = 'Rumored')
-- Películas por status
SELECT (SELECT COUNT(*) FROM movie_dataset m
            WHERE status = 'Released') AS Status_released,
            (SELECT COUNT(*) FROM movie_dataset m
                WHERE status = 'Post Production') AS Status_unreleased,
                (SELECT COUNT(*) FROM movie_dataset m
                    WHERE status = 'Rumored') AS Status_rumored
;

-- personas en crew trabajo
SELECT id_People, name_Job
FROM crew LIMIT 20;

-- Jobs y departamentos
SELECT DISTINCT name_Job, department
FROM crew;

-- titulo != og_titulo
SELECT title, original_title
FROM movies
WHERE title != original_title;
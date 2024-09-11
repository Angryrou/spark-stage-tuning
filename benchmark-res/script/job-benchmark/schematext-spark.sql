CREATE TABLE aka_name (
                          id INT  ,
                          person_id INT ,
                          name STRING,
                          imdb_index STRING,
                          name_pcode_cf STRING,
                          name_pcode_nf STRING,
                          surname_pcode STRING,
                          md5sum STRING
);

CREATE TABLE aka_title (
                           id INT  ,
                           movie_id INT ,
                           title STRING,
                           imdb_index STRING,
                           kind_id INT ,
                           production_year INT,
                           phonetic_code STRING,
                           episode_of_id INT,
                           season_nr INT,
                           episode_nr INT,
                           note STRING,
                           md5sum STRING
);

CREATE TABLE cast_info (
                           id INT  ,
                           person_id INT ,
                           movie_id INT ,
                           person_role_id INT,
                           note STRING,
                           nr_order INT,
                           role_id INT
);

CREATE TABLE char_name (
                           id INT  ,
                           name STRING ,
                           imdb_index STRING,
                           imdb_id INT,
                           name_pcode_nf STRING,
                           surname_pcode STRING,
                           md5sum STRING
);

CREATE TABLE comp_cast_type (
                                id INT  ,
                                kind STRING
);

CREATE TABLE company_name (
                              id INT  ,
                              name STRING ,
                              country_code STRING,
                              imdb_id INT,
                              name_pcode_nf STRING,
                              name_pcode_sf STRING,
                              md5sum STRING
);

CREATE TABLE company_type (
                              id INT  ,
                              kind STRING
);

CREATE TABLE complete_cast (
                               id INT  ,
                               movie_id INT,
                               subject_id INT ,
                               status_id INT
);

CREATE TABLE info_type (
                           id INT  ,
                           info STRING
);

CREATE TABLE keyword (
                         id INT  ,
                         keyword STRING ,
                         phonetic_code STRING
);

CREATE TABLE kind_type (
                           id INT  ,
                           kind STRING
);

CREATE TABLE link_type (
                           id INT  ,
                           link STRING
);

CREATE TABLE movie_companies (
                                 id INT  ,
                                 movie_id INT ,
                                 company_id INT ,
                                 company_type_id INT ,
                                 note STRING
);

CREATE TABLE movie_info_idx (
                                id INT  ,
                                movie_id INT ,
                                info_type_id INT ,
                                info STRING ,
                                note STRING
);

CREATE TABLE movie_keyword (
                               id INT  ,
                               movie_id INT ,
                               keyword_id INT
);

CREATE TABLE movie_link (
                            id INT  ,
                            movie_id INT ,
                            linked_movie_id INT ,
                            link_type_id INT
);

CREATE TABLE name (
                      id INT  ,
                      name STRING ,
                      imdb_index STRING,
                      imdb_id INT,
                      gender STRING,
                      name_pcode_cf STRING,
                      name_pcode_nf STRING,
                      surname_pcode STRING,
                      md5sum STRING
);

CREATE TABLE role_type (
                           id INT  ,
                           role STRING
);

CREATE TABLE title (
                       id INT  ,
                       title STRING ,
                       imdb_index STRING,
                       kind_id INT ,
                       production_year INT,
                       imdb_id INT,
                       phonetic_code STRING,
                       episode_of_id INT,
                       season_nr INT,
                       episode_nr INT,
                       series_years STRING,
                       md5sum STRING
);

CREATE TABLE movie_info (
                            id INT  ,
                            movie_id INT ,
                            info_type_id INT ,
                            info STRING ,
                            note STRING
);

CREATE TABLE person_info (
                             id INT  ,
                             person_id INT ,
                             info_type_id INT ,
                             info STRING ,
                             note STRING
);

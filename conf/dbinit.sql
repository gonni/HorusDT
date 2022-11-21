create table TERM_DIST (
                           TERM_NO INT not null AUTO_INCREMENT PRIMARY KEY,
                           BASE_TERM varchar(64),
                           COMP_TERM varchar(64),
                           DIST_VAL double,
                           T_RANGE_MIN_AGO INT,
                           SEED_NO INT,
                           GRP_TS long
);

create table DT_LDA_TOPICS (
                               LDA_NO INT not null AUTO_INCREMENT PRIMARY KEY,
                               TOPIC_NO int,
                               TERM varchar(64),
                               SCORE double,
                               START_MIN_AGO int,
                               SEED_NO int,
                               GRP_TS long
);


create table DT_TFIDF (
                          TFIDF_NO INT not null AUTO_INCREMENT PRIMARY KEY,
                          TOKEN varchar(64),
                          DOC_ID long,
                          TF int,
                          DF int,
                          IDF double,
                          TFIDF double,
                          START_MIN_AGO int,
                          SEED_NO int,
                          GRP_TS long
) ENGINE=InnoDB AUTO_INCREMENT=7957 DEFAULT CHARSET=utf8mb4 ;

CREATE TABLE `dt_term_score` (
                                 `TS_NO` int(11) NOT NULL AUTO_INCREMENT,
                                 `TOKEN` varchar(64) DEFAULT NULL,
                                 `AVG_TFIDF` double DEFAULT NULL,
                                 `AVG_DF` int(11) DEFAULT NULL,
                                 `DATA_RANGE_MIN` int(11) DEFAULT NULL,
                                 `SEED_NO` int(11) DEFAULT NULL,
                                 `GRP_TS` long,
                                 PRIMARY KEY (`TS_NO`)
) ENGINE=InnoDB AUTO_INCREMENT=7957 DEFAULT CHARSET=utf8mb4 ;


create table DT_JOB_LOG (
                            LOG_NO INT not null AUTO_INCREMENT PRIMARY KEY,
                            JOB_NAME varchar(64),
                            STATUS varchar(4),
                            FINISHED_AT datetime default current_timestamp
) ;

CREATE TABLE DT_TOPIC_TDM (
                              `TT_NO` int(11) NOT NULL AUTO_INCREMENT,
                              `BASE_TERM` varchar(164) DEFAULT NULL,
                              `NEAR_TERM` varchar(164) DEFAULT NULL,
                              `TOPIC_SCORE` double DEFAULT NULL,
                              `SEED_NO` int(11) DEFAULT NULL,
                              `GRP_TS` long,
                              PRIMARY KEY (`TT_NO`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;

create table DT_TEST_TEMP (
                              TLOG_NO INT not null AUTO_INCREMENT PRIMARY KEY,
                              RES mediumtext
) ;
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
);
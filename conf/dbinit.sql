create table DT_LDA_TOPICS (
                               LDA_NO INT not null AUTO_INCREMENT PRIMARY KEY,
                               TOPIC_NO int,
                               TERM varchar(64),
                               SCORE double,
                               START_MIN_AGO int,
                               SEED_NO int,
                               GRP_TS long
);
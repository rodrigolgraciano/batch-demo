DROP TABLE championship IF EXISTS;

create table championship(id BIGINT  IDENTITY NOT NULL PRIMARY KEY,
position INT not null,
pilot VARCHAR(30));
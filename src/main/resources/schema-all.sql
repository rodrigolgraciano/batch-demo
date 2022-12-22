DROP TABLE championship IF EXISTS;

create table rider
(
    id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    position INT NOT NULL,
    name     VARCHAR(30) NOT NULL,
    team     VARCHAR(50) NOT NULL,
    times    VARCHAR(30) NOT NULL
);

create table championship
(
    id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    position INT not null,
    pilot    VARCHAR(30)
);

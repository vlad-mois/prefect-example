CREATE TABLE IF NOT EXISTS images (
    url TEXT PRIMARY KEY,
    ts DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS processing (
    flow CHARACTER(36) NOT NULL,
    url TEXT NOT NULL
);
CREATE INDEX idx_processing_flow ON processing (flow);

CREATE TABLE IF NOT EXISTS results (
    image TEXT NOT NULL,
    link TEXT NOT NULL,
    ts DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
);

INSERT INTO images (url) VALUES
    ("https://tlk.s3.yandex.net/wsdm2020/photos/8ca087fe33065d75327cafdb8720204b.jpg"),
    ("https://tlk.s3.yandex.net/wsdm2020/photos/d0c9eb8737f48df5964d93b08ec0d758.jpg"),
    ("https://tlk.s3.yandex.net/wsdm2020/photos/9245eed8aa1d1e6f5d5d39d00ab044c6.jpg"),
    ("https://tlk.s3.yandex.net/wsdm2020/photos/0aff4fc1edbe6096a9a517092902627f.jpg"),
    ("http://tolokaadmin.s3.yandex.net/demo/abb61898-c886-4e20-b7cd-c0d359ddbb9a")
;

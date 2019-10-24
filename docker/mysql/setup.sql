
CREATE DATABASE IF NOT EXISTS gokurou_dev CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS gokurou_dev.urls (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    gwn INTEGER NOT NULL,
    tab_joined_url MEDIUMTEXT CHARACTER SET ascii NOT NULL,
    randomized_order BIGINT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX gwn_randomized_order_index(gwn, randomized_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE DATABASE IF NOT EXISTS gokurou_test CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS gokurou_test.urls (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    gwn INTEGER NOT NULL,
    tab_joined_url MEDIUMTEXT CHARACTER SET ascii NOT NULL,
    randomized_order BIGINT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX gwn_randomized_order_index(gwn, randomized_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

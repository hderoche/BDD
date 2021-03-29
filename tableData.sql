CREATE TABLE `data` (
  `id` varchar(45) NOT NULL,
  `event-type` varchar(20) NOT NULL,
  `occuredOn` datetime NOT NULL,
  `version` int NOT NULL,
  `graph-id` varchar(20) NOT NULL,
  `nature` varchar(15) NOT NULL,
  `object-name` varchar(15) NOT NULL,
  `path` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
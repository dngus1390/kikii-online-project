CREATE TABLE `ridership` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '승,하차 테이블 id',
  `hour` TINYINT NOT NULL COMMENT '0~23시',
  `ride_type` ENUM('승차','하차') NOT NULL COMMENT '승하차구분',
  `passenger_cnt` INT NOT NULL COMMENT '승,하차 승객수',
  `route_id` INT NOT NULL COMMENT '노선 아이디',
  `stop_id` INT NOT NULL COMMENT '정류장 id',
  `date_id` INT NOT NULL COMMENT '날짜 id',
  PRIMARY KEY (`id`),
  UNIQUE (`date_id`, `route_id`, `stop_id`, `hour`, `ride_type`),
  CONSTRAINT `FK_RIDERSHIP_ROUTE` FOREIGN KEY (`route_id`) REFERENCES `route`(`route_id`),
  CONSTRAINT `FK_RIDERSHIP_STOP`  FOREIGN KEY (`stop_id`)  REFERENCES `bus_stop`(`stop_id`),
  CONSTRAINT `FK_RIDERSHIP_DATE`  FOREIGN KEY (`date_id`)  REFERENCES `dim_date`(`date_id`)
);

COMMIT;
CREATE TABLE `bus_stop` (
  `stop_id` INT NOT NULL AUTO_INCREMENT COMMENT '정류장 id',
  `standard_stop_id` BIGINT NOT NULL COMMENT '표준버스정류장ID(100000001 등)',
  `ars_id` VARCHAR(20) NOT NULL COMMENT '버스정류장ARS번호(1001 등)',
  `stop_name` VARCHAR(200) NOT NULL COMMENT '역명',
  PRIMARY KEY (`stop_id`)
);
-- UNIQUE (`standard_stop_id`)

COMMIT;
CREATE TABLE `route` (
  `route_id` INT NOT NULL AUTO_INCREMENT COMMENT '노선 아이디',
  `route_no` VARCHAR(20) NOT NULL COMMENT '노선번호(470, N37 등)',
  `route_name` VARCHAR(255) NOT NULL COMMENT '노선명',
  `vehicle_type_cd` VARCHAR(10) NULL,
  `vehicle_type_nm` VARCHAR(50) NULL,
  PRIMARY KEY (`route_id`)
);

--   UNIQUE (`route_no`, `route_name`)

COMMIT;
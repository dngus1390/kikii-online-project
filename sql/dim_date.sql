CREATE TABLE `dim_date` (
  `date_id` INT NOT NULL AUTO_INCREMENT COMMENT '날짜 id',
  `ymd` DATE NOT NULL COMMENT '월의 1일로 저장',
  `year` INT NOT NULL,
  `month` TINYINT NOT NULL,
  PRIMARY KEY (`date_id`)
);

COMMIT;
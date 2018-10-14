# file_metadata schema

# --- !Ups

CREATE TABLE IF NOT EXISTS `file_metadata` (
  `id` varchar(250) NOT NULL,
  `file_path` varchar(1000) NOT NULL,
  `dataset_name` varchar(250) NOT NULL,
  `created_date` varchar(250) NOT NULL,
  `updated_date` varchar(250) NOT NULL,
  `user_id` int(11) NOT NULL,
  `number_of_rows` int(11) DEFAULT NULL,
  `number_of_columns` int(11) DEFAULT NULL,
  `file_size` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

# --- !Downs

DROP TABLE file_metadata;
# column_metadata schema

# --- !Ups

CREATE TABLE IF NOT EXISTS `column_metadata` (
  `id` varchar(250) NOT NULL,
  `name` varchar(250) NOT NULL,
  `position` int(11) NOT NULL,
  `datatype` varchar(250) NOT NULL,
  `format` varchar(1000) NOT NULL,
  `separator` tinyint(1) NOT NULL,
  `decimal` int(11) NOT NULL,
  `dataset_id` varchar(250) NOT NULL,
  `visibility` tinyint(1) NOT NULL,
  `calculated` tinyint(1) NOT NULL,
  `formula` longtext NOT NULL,
  `metrics` longtext NOT NULL,
  `created_date` varchar(250) NOT NULL,
  `modified_date` varchar(250) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

# --- !Downs

DROP TABLE column_metadata;
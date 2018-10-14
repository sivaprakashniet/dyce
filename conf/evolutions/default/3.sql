# auth_user_object_permissions schema

# --- !Ups

CREATE TABLE IF NOT EXISTS `acl` (
  `id` varchar(250) NOT NULL,
  `user_id` varchar(250) NOT NULL,
  `object_id` varchar(250) NOT NULL,
  `resource_type` varchar(250) DEFAULT NULL,
  `privilege` enum('*','read','delete','update') NOT NULL,
  `permission` enum('allow','deny') NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

# --- !Downs

DROP TABLE auth_user_object_permissions;
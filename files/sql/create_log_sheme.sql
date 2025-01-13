create schema if not exists "log";

create table if not exists
	log.logt (
		execution_datetime TIMESTAMP not null,
		event_datetime TIMESTAMP not null,
		event_name VARCHAR(100) not null
);
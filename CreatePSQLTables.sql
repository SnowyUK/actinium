/*
 Creates the profiles and events table for a postgresql database
 */

drop table if exists profiler.events;
drop table if exists profiler.profiles;

create table profiler.profiles
(
	id serial not null
		constraint profiles_pk
			primary key,
	name text,
	comment text,
	started timestamp default CURRENT_TIMESTAMP,
	ended timestamp
);

create unique index profiles_id_uindex
	on profiler.profiles (id);

create table profiler.events
(
	id serial not null
		constraint events_pk
			primary key,
	profile_id integer
		constraint events_profiles_id_fk
			references profiler.profiles
				on update cascade on delete cascade,
	state text,
	started timestamp,
	ended timestamp,
	records integer,
	comment text
);

create unique index events_id_uindex
	on profiler.events (id);


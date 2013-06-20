create table fds_jobs (
          run_time timestamp,
          stage   varchar2(128),  
          profile varchar2(128),  
          cmt     varchar2(128),  
          input_path varchar2(128),   
          output_path varchar2(128),   
          file_count number,
          processing_seconds number         
);

create table fds_kti (
             profile       varchar2(128),
             source_file   varchar2(128),
             name          varchar2(128),
             time_index    number,
             latitude      number,
             longitude     number,            
			 base_file_path varchar2(96)
         );
         
create table fds_phase (
             profile       varchar2(128),
             source_file   varchar2(128),
             name          varchar2(128),
             time_index    number,
             duration      number,
			 stop_edge	   number,
			 base_file_path varchar2(96)
         );
alter table fds_phase add base_file_path varchar2(96);

create table fds_kpv (
             profile       varchar2(128),
             source_file   varchar2(128),
             name          varchar2(128),
             time_index    number,
             value         number,             
			 base_file_path varchar2(96)
         );
alter table fds_kpv add units varchar2(48);

create table fds_convert (
  run_time              timestamp,
  source_file           varchar2(128),  
  fleet_family          varchar2(32),     
  fleet_series          varchar2(32), 
  tail_number           varchar2(32), 
  lfl                   varchar2(128), 
  output_file           varchar2(128), 
  status                varchar2(32), 
  file_size_meg         number, 
  flight_hours          number, 
  conversion_seconds    number
);


create table fds_processing_time (
	run_time		timestamp,
	source_file		varchar2(128),  --an hdf5 file
	stage			varchar2(64),   --split, cleanse, analyze, profile
	profile		varchar2(128),
	file_size_meg    number, 
	processing_seconds number,
     status            varchar2(32),
	 epoch			number,
	 cmt			varchar2(96)
);

create table fds_flight_record (	
	source_file	    varchar2(128) primary key,
	tail_number	    varchar2(32),
	fleet_series	varchar2(32),
	operator	    varchar2(128),
	analyzer_version	varchar2(16),
	flight_type	    varchar2(128),
	analysis_time	timestamp,
   LIFTOFF_MIN NUMBER,
   TOP_OF_CLIMB_MIN number,
   TOP_OF_DESCENT_MIN number,
   TOUCHDOWN_MIN number,

	off_blocks_time	timestamp,
	takeoff_time	timestamp,
	landing_time	timestamp,
	on_blocks_time	timestamp,
	duration	    number,
	orig_icao		varchar2(5),
	orig_iata		varchar2(5),
	orig_elevation	number,
	orig_rwy		varchar2(5),
	orig_rwy_length	number,
	dest_icao		varchar2(5),
	dest_iata		varchar2(5),
	dest_elevation	number,
	dest_rwy		varchar2(5),
	dest_rwy_length	number,
	glideslope_angle	number,
      landing_count    number,
      go_around_count  number,
      touch_and_go_count number,
	other_json		varchar2(4000),
      recorded_parameters clob,
	  file_path varchar2(96),
	  base_file_path varchar2(96)
);
alter table fds_flight_record add file_path varchar2(96);
alter table fds_flight_record add base_file_path varchar2(96);

alter table fds_flight_record add (
   LIFTOFF_MIN NUMBER,
   TOP_OF_CLIMB_MIN number,
   TOP_OF_DESCENT_MIN number,
   TOUCHDOWN_MIN number
 );


alter table fds_kpv add (file_repository varchar2(128));
alter table fds_kpv add (profile_set varchar2(96));

update fds_phase set profile_set='base' where profile='base'

DROP INDEX fire1_spatial_idx FORCE;
DROP INDEX fire2_spatial_idx FORCE;
delete from user_sdo_geom_metadata where table_name = 'BUILDING';
delete from user_sdo_geom_metadata where table_name = 'FIREHYDRANT';
drop table firebuilding;
drop table building;
drop table firehydrant;


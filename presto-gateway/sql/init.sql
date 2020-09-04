CREATE SCHEMA IF NOT EXISTS prestogateway;

CREATE TABLE IF NOT EXISTS prestogateway.clusters ( name VARCHAR(256) PRIMARY KEY,
   location VARCHAR (256),
   coordinator_url VARCHAR (256),
   admin_name VARCHAR(256),
   admin_password VARCHAR(256),
   active BOOLEAN );

CREATE TABLE IF NOT EXISTS prestogateway.routingpolicy ( name VARCHAR(256) PRIMARY KEY,
   ruleids jsonb );

CREATE TABLE IF NOT EXISTS prestogateway.routingrules ( name VARCHAR(256) PRIMARY KEY,
     type VARCHAR(256), properties VARCHAR(256) );

CREATE TABLE IF NOT EXISTS prestogateway.executing_queries (query_id VARCHAR(256) PRIMARY KEY,
 query_text text, cluster_url VARCHAR(256),
  user_name VARCHAR (256), source VARCHAR(256),
   created VARCHAR(256), query_info jsonb);


CREATE TABLE IF NOT EXISTS prestogateway.catalogs (
    name VARCHAR(256), location VARCHAR (256));

CREATE TABLE IF NOT EXISTS prestogateway.clusters_catalogs(id SERIAL PRIMARY KEY, cluster_name VARCHAR(256),
 catalog_name VARCHAR(256));

CREATE TABLE IF NOT EXISTS prestogateway.query_stats(query_id VARCHAR(256) PRIMARY KEY,
 cluster_url VARCHAR(256), query_info text);

CREATE TABLE IF NOT EXISTS prestogateway.query_stats_json(query_id varchar(256) primary key,
 cluster_url varchar(256), query_info jsonb);

CREATE TABLE IF NOT EXISTS prestogateway.users(name varchar(256) primary key,
 password varchar(1024));

CREATE TABLE IF NOT EXISTS prestogateway.groups(name varchar(256) primary key);

CREATE USER hive WITH PASSWORD 'root123';

CREATE DATABASE metastore;

GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;

-- Copyright (c) 2013 Snowplow Analytics Ltd. All rights reserved.
--
-- This program is licensed to you under the Apache License Version 2.0,
-- and you may not use this file except in compliance with the Apache License Version 2.0.
-- You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the Apache License Version 2.0 is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
--
-- Version:     0.4.0
-- URL:         -
--
-- Authors:     Yali Sassoon, Alex Dean, Peter van Wesep
-- Copyright:   Copyright (c) 2013 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

-- Create the schema
CREATE SCHEMA atomic;

-- Create events table
CREATE TABLE atomic.events (
	-- App
	app_id varchar(255) encode lzo,
	platform varchar(255) encode lzo,
	-- Date/time
	etl_tstamp timestamp,                              -- Added in 0.4.0
	collector_tstamp timestamp not null,
	dvce_tstamp timestamp,
	-- Event
	event varchar(128) encode lzo,
	                                                   -- Removed event_vendor in 0.4.0
	event_id char(36) not null unique,
	txn_id int,
	-- Namespacing and versioning
	name_tracker varchar(128) encode lzo,
	v_tracker varchar(100) encode lzo,
	v_collector varchar(100) encode lzo not null,
	v_etl varchar(100) encode lzo not null,
	-- User and visit
	user_id varchar(255) encode lzo,
	user_ipaddress varchar(19) encode lzo,
	user_fingerprint varchar(50) encode lzo,
	domain_userid varchar(16) encode lzo,
	domain_sessionidx smallint,
	network_userid varchar(38) encode lzo,
	-- Location
	geo_country char(2) encode lzo,
	geo_region char(2) encode lzo,
	geo_city varchar(75) encode lzo,
	geo_zipcode varchar(15) encode lzo,
	geo_latitude double precision encode bytedict,
	geo_longitude double precision encode bytedict,
	geo_region_name varchar(100) encode lzo,     -- Added in 0.4.0
	-- IP lookups
	ip_isp varchar(100) encode lzo,              -- Added in 0.4.0
	ip_organization varchar(100) encode lzo,     -- Added in 0.4.0
	ip_domain varchar(100) encode lzo,           -- Added in 0.4.0
	ip_netspeed varchar(100) encode lzo,         -- Added in 0.4.0
	-- Page
	page_url varchar(4096) encode lzo,
	page_title varchar(2000) encode lzo,
	page_referrer varchar(4096) encode lzo,
	-- Page URL components
	page_urlscheme varchar(16) encode lzo,
	page_urlhost varchar(255) encode lzo,
	page_urlport int encode lzo,
	page_urlpath varchar(1000) encode lzo,
	page_urlquery varchar(3000) encode lzo,
	page_urlfragment varchar(255) encode lzo,
	-- Referrer URL components
	refr_urlscheme varchar(16) encode lzo,
	refr_urlhost varchar(255) encode lzo,
	refr_urlport int,
	refr_urlpath varchar(1000) encode lzo,
	refr_urlquery varchar(3000) encode lzo,
	refr_urlfragment varchar(255) encode lzo,
	-- Referrer details
	refr_medium varchar(25) encode lzo,
	refr_source varchar(50) encode lzo,
	refr_term varchar(255) encode lzo,
	-- Marketing
	mkt_medium varchar(255) encode lzo,
	mkt_source varchar(255) encode lzo,
	mkt_term varchar(255) encode lzo,
	mkt_content varchar(500) encode lzo,
	mkt_campaign varchar(255) encode lzo,
	-- Custom contexts
	contexts varchar(10000) encode lzo,
	-- Custom structured event
	se_category varchar(255) encode lzo,
	se_action varchar(255) encode lzo,
	se_label varchar(255) encode lzo,
	se_property varchar(255) encode lzo,
	se_value double precision,
	-- Custom unstructured event
	                                                   -- Removed ue_name in 0.4.0
	unstruct_event varchar(10000) encode lzo,          -- Renamed ue_properties to unstruct_event in 0.4.0
	-- Ecommerce
	tr_orderid varchar(255) encode raw,
	tr_affiliation varchar(255) encode text255,
	tr_total dec(18,2),
	tr_tax dec(18,2),
	tr_shipping dec(18,2),
	tr_city varchar(255) encode text32k,
	tr_state varchar(255) encode text32k,
	tr_country varchar(255) encode text32k,
	ti_orderid varchar(255) encode raw,
	ti_sku varchar(255) encode text32k,
	ti_name varchar(255) encode text32k,
	ti_category varchar(255) encode text255,
	ti_price dec(18,2),
	ti_quantity int,
	-- Page ping
	pp_xoffset_min integer,
	pp_xoffset_max integer,
	pp_yoffset_min integer,
	pp_yoffset_max integer,
	-- User Agent
	useragent varchar(1000) encode lzo,
	-- Browser
	br_name varchar(50) encode lzo,
	br_family varchar(50) encode lzo,
	br_version varchar(50) encode lzo,
	br_type varchar(50) encode lzo,
	br_renderengine varchar(50) encode lzo,
	br_lang varchar(255) encode lzo,
	br_features_pdf boolean,
	br_features_flash boolean,
	br_features_java boolean,
	br_features_director boolean,
	br_features_quicktime boolean,
	br_features_realplayer boolean,
	br_features_windowsmedia boolean,
	br_features_gears boolean ,
	br_features_silverlight boolean,
	br_cookies boolean,
	br_colordepth varchar(12) encode lzo,
	br_viewwidth integer,
	br_viewheight integer,
	-- Operating System
	os_name varchar(50) encode lzo,
	os_family varchar(50)  encode lzo,
	os_manufacturer varchar(50)  encode lzo,
	os_timezone varchar(255)  encode lzo,
	-- Device/Hardware
	dvce_type varchar(50)  encode lzo,
	dvce_ismobile boolean,
	dvce_screenwidth integer,
	dvce_screenheight integer,
	-- Document
	doc_charset varchar(128) encode lzo,
	doc_width integer,
	doc_height integer,
	CONSTRAINT event_id_040_pk PRIMARY KEY(event_id)
)
DISTSTYLE KEY
DISTKEY (event_id)
INTERLEAVED SORTKEY (collector_tstamp, app_id, event);

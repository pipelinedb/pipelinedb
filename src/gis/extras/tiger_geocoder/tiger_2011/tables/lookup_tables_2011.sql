--SET search_path TO tiger, public;
SELECT tiger.SetSearchPathForInstall('tiger');
-- Create direction lookup table
DROP TABLE IF EXISTS tiger.direction_lookup;
CREATE TABLE direction_lookup (name VARCHAR(20) PRIMARY KEY, abbrev VARCHAR(3));
INSERT INTO direction_lookup (name, abbrev) VALUES('WEST', 'W');
INSERT INTO direction_lookup (name, abbrev) VALUES('W', 'W');
INSERT INTO direction_lookup (name, abbrev) VALUES('SW', 'SW');
INSERT INTO direction_lookup (name, abbrev) VALUES('SOUTH-WEST', 'SW');
INSERT INTO direction_lookup (name, abbrev) VALUES('SOUTHWEST', 'SW');
INSERT INTO direction_lookup (name, abbrev) VALUES('SOUTH-EAST', 'SE');
INSERT INTO direction_lookup (name, abbrev) VALUES('SOUTHEAST', 'SE');
INSERT INTO direction_lookup (name, abbrev) VALUES('SOUTH_WEST', 'SW');
INSERT INTO direction_lookup (name, abbrev) VALUES('SOUTH_EAST', 'SE');
INSERT INTO direction_lookup (name, abbrev) VALUES('SOUTH', 'S');
INSERT INTO direction_lookup (name, abbrev) VALUES('SOUTH WEST', 'SW');
INSERT INTO direction_lookup (name, abbrev) VALUES('SOUTH EAST', 'SE');
INSERT INTO direction_lookup (name, abbrev) VALUES('SE', 'SE');
INSERT INTO direction_lookup (name, abbrev) VALUES('S', 'S');
INSERT INTO direction_lookup (name, abbrev) VALUES('NW', 'NW');
INSERT INTO direction_lookup (name, abbrev) VALUES('NORTH-WEST', 'NW');
INSERT INTO direction_lookup (name, abbrev) VALUES('NORTHWEST', 'NW');
INSERT INTO direction_lookup (name, abbrev) VALUES('NORTH-EAST', 'NE');
INSERT INTO direction_lookup (name, abbrev) VALUES('NORTHEAST', 'NE');
INSERT INTO direction_lookup (name, abbrev) VALUES('NORTH_WEST', 'NW');
INSERT INTO direction_lookup (name, abbrev) VALUES('NORTH_EAST', 'NE');
INSERT INTO direction_lookup (name, abbrev) VALUES('NORTH', 'N');
INSERT INTO direction_lookup (name, abbrev) VALUES('NORTH WEST', 'NW');
INSERT INTO direction_lookup (name, abbrev) VALUES('NORTH EAST', 'NE');
INSERT INTO direction_lookup (name, abbrev) VALUES('NE', 'NE');
INSERT INTO direction_lookup (name, abbrev) VALUES('N', 'N');
INSERT INTO direction_lookup (name, abbrev) VALUES('EAST', 'E');
INSERT INTO direction_lookup (name, abbrev) VALUES('E', 'E');
CREATE INDEX direction_lookup_abbrev_idx ON direction_lookup (abbrev);



-- Create secondary unit lookup table
DROP TABLE IF EXISTS tiger.secondary_unit_lookup;
CREATE TABLE secondary_unit_lookup (name VARCHAR(20) PRIMARY KEY, abbrev VARCHAR(5));
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('APARTMENT', 'APT');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('APT', 'APT');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('BASEMENT', 'BSMT');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('BSMT', 'BSMT');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('BUILDING', 'BLDG');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('BLDG', 'BLDG');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('DEPARTMENT', 'DEPT');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('DEPT', 'DEPT');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('FLOOR', 'FL');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('FL', 'FL');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('FRONT', 'FRNT');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('FRNT', 'FRNT');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('HANGAR', 'HNGR');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('HNGR', 'HNGR');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('LOBBY', 'LBBY');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('LBBY', 'LBBY');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('LOT', 'LOT');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('LOWER', 'LOWR');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('LOWR', 'LOWR');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('OFFICE', 'OFC');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('OFC', 'OFC');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('PENTHOUSE', 'PH');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('PH', 'PH');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('PIER', 'PIER');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('REAR', 'REAR');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('ROOM', 'RM');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('RM', 'RM');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('SIDE', 'SIDE');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('SLIP', 'SLIP');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('SPACE', 'SPC');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('SPC', 'SPC');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('STOP', 'STOP');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('SUITE', 'STE');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('STE', 'STE');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('TRAILER', 'TRLR');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('TRLR', 'TRLR');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('UNIT', 'UNIT');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('UPPER', 'UPPR');
INSERT INTO secondary_unit_lookup (name, abbrev) VALUES ('UPPR', 'UPPR');
CREATE INDEX secondary_unit_lookup_abbrev_idx ON secondary_unit_lookup (abbrev);



-- Create state lookup table
DROP TABLE IF EXISTS tiger.state_lookup;
CREATE TABLE state_lookup (st_code INTEGER PRIMARY KEY, name VARCHAR(40) UNIQUE, abbrev VARCHAR(3) UNIQUE, statefp char(2) UNIQUE);
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Alabama', 'AL', '01');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Alaska', 'AK', '02');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('American Samoa', 'AS', '60');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Arizona', 'AZ', '04');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Arkansas', 'AR', '05');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('California', 'CA', '06');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Colorado', 'CO', '08');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Connecticut', 'CT', '09');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Delaware', 'DE', '10');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('District of Columbia', 'DC', '11');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Federated States of Micronesia', 'FM', '64');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Florida', 'FL', '12');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Georgia', 'GA', '13');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Guam', 'GU', '66');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Hawaii', 'HI', '15');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Idaho', 'ID', '16');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Illinois', 'IL', '17');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Indiana', 'IN', '18');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Iowa', 'IA', '19');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Kansas', 'KS', '20');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Kentucky', 'KY', '21');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Louisiana', 'LA', '22');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Maine', 'ME', '23');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Marshall Islands', 'MH', '68');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Maryland', 'MD', '24');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Massachusetts', 'MA', '25');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Michigan', 'MI', '26');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Minnesota', 'MN', '27');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Mississippi', 'MS', '28');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Missouri', 'MO', '29');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Montana', 'MT', '30');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Nebraska', 'NE', '31');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Nevada', 'NV', '32');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('New Hampshire', 'NH', '33');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('New Jersey', 'NJ', '34');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('New Mexico', 'NM', '35');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('New York', 'NY', '36');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('North Carolina', 'NC', '37');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('North Dakota', 'ND', '38');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Northern Mariana Islands', 'MP', '69');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Ohio', 'OH', '39');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Oklahoma', 'OK', '40');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Oregon', 'OR', '41');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Palau', 'PW', '70');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Pennsylvania', 'PA', '42');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Puerto Rico', 'PR', '72');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Rhode Island', 'RI', '44');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('South Carolina', 'SC', '45');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('South Dakota', 'SD', '46');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Tennessee', 'TN', '47');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Texas', 'TX', '48');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Utah', 'UT', '49');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Vermont', 'VT', '50');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Virgin Islands', 'VI', '78');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Virginia', 'VA', '51');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Washington', 'WA', '53');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('West Virginia', 'WV', '54');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Wisconsin', 'WI', '55');
INSERT INTO state_lookup (name, abbrev, st_code) VALUES ('Wyoming', 'WY', '56');
-- NOTE: fix later -- this is wrong for those - state code ones
UPDATE state_lookup SET statefp = lpad(st_code::text,2,'0');


-- Create street type lookup table
DROP TABLE IF EXISTS tiger.street_type_lookup;
CREATE TABLE street_type_lookup (name VARCHAR(50) PRIMARY KEY, abbrev VARCHAR(50), is_hw boolean NOT NULL DEFAULT false);
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ALLEE', 'Aly');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ALLEY', 'Aly');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ALLY', 'Aly');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ALY', 'Aly');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ANEX', 'Anx');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ANNEX', 'Anx');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ANNX', 'Anx');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ANX', 'Anx');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ARC', 'Arc');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ARCADE', 'Arc');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('AV', 'Ave');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('AVE', 'Ave');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('AVEN', 'Ave');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('AVENU', 'Ave');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('AVENUE', 'Ave');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('AVN', 'Ave');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('AVNUE', 'Ave');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BAYOO', 'Byu');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BAYOU', 'Byu');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BCH', 'Bch');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BEACH', 'Bch');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BEND', 'Bnd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BND', 'Bnd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BLF', 'Blf');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BLUF', 'Blf');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BLUFF', 'Blf');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BLUFFS', 'Blfs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BOT', 'Btm');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BOTTM', 'Btm');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BOTTOM', 'Btm');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BTM', 'Btm');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BLVD', 'Blvd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BOUL', 'Blvd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BOULEVARD', 'Blvd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BOULV', 'Blvd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BR', 'Br');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BRANCH', 'Br');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BRNCH', 'Br');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BRDGE', 'Brg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BRG', 'Brg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BRIDGE', 'Brg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BRK', 'Brk');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BROOK', 'Brk');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BROOKS', 'Brks');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BURG', 'Bg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BURGS', 'Bgs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BYP', 'Byp');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BYPA', 'Byp');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BYPAS', 'Byp');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BYPASS', 'ByP');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BYPS', 'Byp');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CAMP', 'Cp');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CMP', 'Cp');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CP', 'Cp');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CANYN', 'Cyn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CANYON', 'Cyn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CNYN', 'Cyn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CYN', 'Cyn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CAPE', 'Cpe');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CPE', 'Cpe');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CAUSEWAY', 'Cswy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CAUSWAY', 'Cswy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CSWY', 'Cswy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CEN', 'Ctr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CENT', 'Ctr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CENTER', 'Ctr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CENTR', 'Ctr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CENTRE', 'Ctr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CNTER', 'Ctr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CNTR', 'Ctr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CTR', 'Ctr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CENTERS', 'Ctrs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CIR', 'Cir');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CIRC', 'Cir');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CIRCL', 'Cir');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CIRCLE', 'Cir');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRCL', 'Cir');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRCLE', 'Cir');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CIRCLES', 'Cirs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CLF', 'Clf');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CLIFF', 'Clf');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CLFS', 'Clfs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CLIFFS', 'Clfs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CLB', 'Clb');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CLUB', 'Clb');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('COMMON', 'Cmn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('COR', 'Cor');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CORNER', 'Cor');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CORNERS', 'Cors');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CORS', 'Cors');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('COURSE', 'Crse');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRSE', 'Crse');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('COURT', 'Ct');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRT', 'Ct');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CT', 'Ct');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('COURTS', 'Cts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('COVE', 'Cv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CV', 'Cv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('COVES', 'Cvs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CK', 'Crk');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CR', 'Crk');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CREEK', 'Crk');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRK', 'Crk');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRECENT', 'Cres');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRES', 'Cres');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRESCENT', 'Cres');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRESENT', 'Cres');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRSCNT', 'Cres');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRSENT', 'Cres');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRSNT', 'Cres');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CREST', 'Crst');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CROSSING', 'Xing');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRSSING', 'Xing');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRSSNG', 'Xing');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('XING', 'Xing');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CROSSROAD', 'Xrd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CURVE', 'Curv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DALE', 'Dl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DL', 'Dl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DAM', 'Dm');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DM', 'Dm');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DIV', 'Dv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DIVIDE', 'Dv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DV', 'Dv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DVD', 'Dv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DR', 'Dr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DRIV', 'Dr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DRIVE', 'Dr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DRV', 'Dr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DRIVES', 'Drs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('EST', 'Est');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ESTATE', 'Est');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ESTATES', 'Ests');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ESTS', 'Ests');
--INSERT INTO street_type_lookup (name, abbrev) VALUES ('EXP', 'Expy');
--INSERT INTO street_type_lookup (name, abbrev) VALUES ('EXPR', 'Expy');
--INSERT INTO street_type_lookup (name, abbrev) VALUES ('EXPRESS', 'Expy');
--INSERT INTO street_type_lookup (name, abbrev) VALUES ('EXPRESSWAY', 'Expy');
--INSERT INTO street_type_lookup (name, abbrev) VALUES ('EXPW', 'Expy');
--INSERT INTO street_type_lookup (name, abbrev) VALUES ('EXPY', 'Expy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('EXT', 'Ext');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('EXTENSION', 'Ext');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('EXTN', 'Ext');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('EXTNSN', 'Ext');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('EXTENSIONS', 'Exts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('EXTS', 'Exts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FALL', 'Fall');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FALLS', 'Fls');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FLS', 'Fls');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FERRY', 'Fry');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FRRY', 'Fry');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FRY', 'Fry');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FIELD', 'Fld');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FLD', 'Fld');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FIELDS', 'Flds');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FLDS', 'Flds');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FLAT', 'Flt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FLT', 'Flt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FLATS', 'Flts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FLTS', 'Flts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FORD', 'Frd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FRD', 'Frd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FORDS', 'Frds');
--INSERT INTO street_type_lookup (name, abbrev) VALUES ('FOREST', 'Frst');
--INSERT INTO street_type_lookup (name, abbrev) VALUES ('FORESTS', 'Frst');
--INSERT INTO street_type_lookup (name, abbrev) VALUES ('FRST', 'Frst');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FORG', 'Frg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FORGE', 'Frg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FRG', 'Frg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FORGES', 'Frgs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FORK', 'Frk');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FRK', 'Frk');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FORKS', 'Frks');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FRKS', 'Frks');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FORT', 'Ft');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FRT', 'Ft');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FT', 'Ft');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GARDEN', 'Gdn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GARDN', 'Gdn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GDN', 'Gdn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GRDEN', 'Gdn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GRDN', 'Gdn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GARDENS', 'Gdns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GDNS', 'Gdns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GRDNS', 'Gdns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GATEWAY', 'Gtwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GATEWY', 'Gtwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GATWAY', 'Gtwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GTWAY', 'Gtwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GTWY', 'Gtwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GLEN', 'Gln');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GLN', 'Gln');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GLENS', 'Glns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GREEN', 'Grn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GRN', 'Grn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GREENS', 'Grns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GROV', 'Grv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GROVE', 'Grv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GRV', 'Grv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GROVES', 'Grvs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HARB', 'Hbr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HARBOR', 'Hbr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HARBR', 'Hbr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HBR', 'Hbr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HRBOR', 'Hbr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HARBORS', 'Hbrs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HAVEN', 'Hvn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HAVN', 'Hvn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HVN', 'Hvn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HEIGHT', 'Hts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HEIGHTS', 'Hts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HGTS', 'Hts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HT', 'Hts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HTS', 'Hts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HILL', 'Hl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HL', 'Hl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HILLS', 'Hls');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HLS', 'Hls');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HLLW', 'Holw');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HOLLOW', 'Holw');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HOLLOWS', 'Holw');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HOLW', 'Holw');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HOLWS', 'Holw');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('INLET', 'Inlt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('INLT', 'Inlt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('IS', 'Is');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ISLAND', 'Is');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ISLND', 'Is');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ISLANDS', 'Iss');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ISLNDS', 'Iss');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ISS', 'Iss');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ISLE', 'Isle');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ISLES', 'Isle');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('JCT', 'Jct');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('JCTION', 'Jct');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('JCTN', 'Jct');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('JUNCTION', 'Jct');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('JUNCTN', 'Jct');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('JUNCTON', 'Jct');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('JCTNS', 'Jcts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('JCTS', 'Jcts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('JUNCTIONS', 'Jcts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('KEY', 'Ky');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('KY', 'Ky');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('KEYS', 'Kys');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('KYS', 'Kys');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('KNL', 'Knl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('KNOL', 'Knl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('KNOLL', 'Knl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('KNLS', 'Knls');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('KNOLLS', 'Knls');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LAKE', 'Lk');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LK', 'Lk');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LAKES', 'Lks');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LKS', 'Lks');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LAND', 'Land');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LANDING', 'Lndg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LNDG', 'Lndg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LNDNG', 'Lndg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LA', 'Ln');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LANE', 'Ln');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LANES', 'Ln');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LN', 'Ln');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LGT', 'Lgt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LIGHT', 'Lgt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LIGHTS', 'Lgts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LF', 'Lf');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LOAF', 'Lf');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LCK', 'Lck');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LOCK', 'Lck');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LCKS', 'Lcks');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LOCKS', 'Lcks');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LDG', 'Ldg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LDGE', 'Ldg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LODG', 'Ldg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LODGE', 'Ldg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LOOP', 'Loop');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LOOPS', 'Loop');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MALL', 'Mall');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MANOR', 'Mnr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MNR', 'Mnr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MANORS', 'Mnrs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MNRS', 'Mnrs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MDW', 'Mdw');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MEADOW', 'Mdw');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MDWS', 'Mdws');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MEADOWS', 'Mdws');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MEDOWS', 'Mdws');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MEWS', 'Mews');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MILL', 'Ml');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ML', 'Ml');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MILLS', 'Mls');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MLS', 'Mls');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MISSION', 'Msn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MISSN', 'Msn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MSN', 'Msn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MSSN', 'Msn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MOTORWAY', 'Mtwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MNT', 'Mt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MOUNT', 'Mt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MT', 'Mt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MNTAIN', 'Mtn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MNTN', 'Mtn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MOUNTAIN', 'Mtn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MOUNTIN', 'Mtn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MTIN', 'Mtn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MTN', 'Mtn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MNTNS', 'Mtns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MOUNTAINS', 'Mtns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('NCK', 'Nck');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('NECK', 'Nck');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ORCH', 'Orch');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ORCHARD', 'Orch');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ORCHRD', 'Orch');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('OVAL', 'Oval');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('OVL', 'Oval');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('OVERPASS', 'Opas');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PARK', 'Park');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PK', 'Park');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PRK', 'Park');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PARKS', 'Park');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PARKWAY', 'Pkwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PARKWY', 'Pkwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PKWAY', 'Pkwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PKWY', 'Pkwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PKY', 'Pkwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PARKWAYS', 'Pkwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PKWYS', 'Pkwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PASS', 'Pass');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PASSAGE', 'Psge');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PATH', 'Path');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PATHS', 'Path');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PIKE', 'Pike');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PIKES', 'Pike');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PINE', 'Pne');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PINES', 'Pnes');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PNES', 'Pnes');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PL', 'Pl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PLACE', 'Pl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PLAIN', 'Pln');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PLN', 'Pln');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PLAINES', 'Plns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PLAINS', 'Plns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PLNS', 'Plns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PLAZA', 'Plz');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PLZ', 'Plz');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PLZA', 'Plz');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('POINT', 'Pt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PT', 'Pt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('POINTS', 'Pts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PTS', 'Pts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PORT', 'Prt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PRT', 'Prt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PORTS', 'Prts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PRTS', 'Prts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PR', 'Pr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PRAIRIE', 'Pr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PRARIE', 'Pr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PRR', 'Pr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RAD', 'Radl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RADIAL', 'Radl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RADIEL', 'Radl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RADL', 'Radl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RAMP', 'Ramp');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RANCH', 'Rnch');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RANCHES', 'Rnch');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RNCH', 'Rnch');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RNCHS', 'Rnch');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RAPID', 'Rpd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RPD', 'Rpd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RAPIDS', 'Rpds');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RPDS', 'Rpds');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('REST', 'Rst');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RST', 'Rst');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RDG', 'Rdg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RDGE', 'Rdg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RIDGE', 'Rdg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RDGS', 'Rdgs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RIDGES', 'Rdgs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RIV', 'Riv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RIVER', 'Riv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RIVR', 'Riv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RVR', 'Riv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RD', 'Rd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ROAD', 'Rd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RDS', 'Rds');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ROADS', 'Rds');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ROW', 'Row');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RUE', 'Rue');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RUN', 'Run');
-- Start newly added 2011-7-12 --
INSERT INTO street_type_lookup (name, abbrev)
 VALUES 
 ('SERVICE DRIVE', 'Svc Dr'),
 ('SERVICE DR', 'Svc Dr'),
 ('SERVICE ROAD', 'Svc Rd'),
 ('SERVICE RD', 'Svc Rd') ;
-- end newly added 2011-07-12 --
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SHL', 'Shl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SHOAL', 'Shl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SHLS', 'Shls');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SHOALS', 'Shls');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SHOAR', 'Shr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SHORE', 'Shr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SHR', 'Shr');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SHOARS', 'Shrs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SHORES', 'Shrs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SHRS', 'Shrs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SKYWAY', 'Skwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SPG', 'Spg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SPNG', 'Spg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SPRING', 'Spg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SPRNG', 'Spg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SPGS', 'Spgs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SPNGS', 'Spgs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SPRINGS', 'Spgs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SPRNGS', 'Spgs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SPUR', 'Spur');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SPURS', 'Spur');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SQ', 'Sq');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SQR', 'Sq');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SQRE', 'Sq');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SQU', 'Sq');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SQUARE', 'Sq');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SQRS', 'Sqs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SQUARES', 'Sqs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STA', 'Sta');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STATION', 'Sta');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STATN', 'Sta');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STN', 'Sta');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STRA', 'Stra');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STRAV', 'Stra');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STRAVE', 'Stra');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STRAVEN', 'Stra');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STRAVENUE', 'Stra');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STRAVN', 'Stra');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STRVN', 'Stra');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STRVNUE', 'Stra');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STREAM', 'Strm');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STREME', 'Strm');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STRM', 'Strm');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('ST', 'St');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STR', 'St');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STREET', 'St');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STRT', 'St');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STREETS', 'Sts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SMT', 'Smt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SUMIT', 'Smt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SUMITT', 'Smt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SUMMIT', 'Smt');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TER', 'Ter');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TERR', 'Ter');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TERRACE', 'Ter');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('THROUGHWAY', 'Trwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRACE', 'Trce');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRACES', 'Trce');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRCE', 'Trce');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRACK', 'Trak');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRACKS', 'Trak');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRAK', 'Trak');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRK', 'Trak');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRKS', 'Trak');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRAFFICWAY', 'Trfy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRFY', 'Trfy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TR', 'Trl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRAIL', 'Trl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRAILS', 'Trl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRL', 'Trl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRLS', 'Trl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TUNEL', 'Tunl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TUNL', 'Tunl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TUNLS', 'Tunl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TUNNEL', 'Tunl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TUNNELS', 'Tunl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TUNNL', 'Tunl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('UNDERPASS', 'Upas');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('UN', 'Un');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('UNION', 'Un');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('UNIONS', 'Uns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VALLEY', 'Vly');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VALLY', 'Vly');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VLLY', 'Vly');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VLY', 'Vly');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VALLEYS', 'Vlys');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VLYS', 'Vlys');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VDCT', 'Via');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VIA', 'Via');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VIADCT', 'Via');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VIADUCT', 'Via');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VIEW', 'Vw');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VW', 'Vw');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VIEWS', 'Vws');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VWS', 'Vws');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VILL', 'Vlg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VILLAG', 'Vlg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VILLAGE', 'Vlg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VILLG', 'Vlg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VILLIAGE', 'Vlg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VLG', 'Vlg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VILLAGES', 'Vlgs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VLGS', 'Vlgs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VILLE', 'Vl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VL', 'Vl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VIS', 'Vis');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VIST', 'Vis');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VISTA', 'Vis');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VST', 'Vis');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('VSTA', 'Vis');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('WALK', 'Walk');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('WALKS', 'Walk');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('WALL', 'Wall');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('WAY', 'Way');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('WY', 'Way');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('WAYS', 'Ways');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('WELL', 'Wl');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('WELLS', 'Wls');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('WLS', 'Wls');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BYU', 'Byu');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BLFS', 'Blfs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BRKS', 'Brks');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BG', 'Bg');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('BGS', 'Bgs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CTRS', 'Ctrs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CIRS', 'Cirs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CMN', 'Cmn');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CTS', 'Cts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CVS', 'Cvs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CRST', 'Crst');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('XRD', 'Xrd');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('CURV', 'Curv');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('DRS', 'Drs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FRDS', 'Frds');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('FRGS', 'Frgs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GLNS', 'Glns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GRNS', 'Grns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('GRVS', 'Grvs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('HBRS', 'Hbrs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('LGTS', 'Lgts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MTWY', 'Mtwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('MTNS', 'Mtns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('OPAS', 'Opas');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PSGE', 'Psge');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('PNE', 'Pne');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('RTE', 'Rte');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SKWY', 'Skwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('SQS', 'Sqs');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('STS', 'Sts');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('TRWY', 'Trwy');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('UPAS', 'Upas');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('UNS', 'Uns');
INSERT INTO street_type_lookup (name, abbrev) VALUES ('WL', 'Wl');

-- prefix and suffix street names for highways and foreign named roads 
-- where street type is at front of streetname instead of after
-- these usually have numbers for street names and often have spaces in type
INSERT INTO street_type_lookup (name, abbrev, is_hw) 
SELECT name, abbrev, true
    FROM (VALUES
        ('CAM', 'Cam'),
        ('CAM.', 'Cam'),
        ('CAMINO', 'Cam'),
        ('CO HWY', 'Co Hwy'),
        ('COUNTY HWY', 'Co Hwy'),
        ('COUNTY HIGHWAY', 'Co Hwy'),
        ('COUNTY HIGH WAY', 'Co Hwy'),
        ('COUNTY ROAD', 'Co Rd'),
        ('COUNTY RD', 'Co Rd'),
        ('CO RD', 'Co Rd'),
        ('CORD', 'Co Rd'),
        ('CO RTE', 'Co Rte'),
        ('COUNTY ROUTE', 'Co Rte'),
        ('CO ST AID HWY', 'Co St Aid Hwy'),
        ('EXP', 'Expy'),
        ('EXPR', 'Expy'),
        ('EXPRESS', 'Expy'),
        ('EXPRESSWAY', 'Expy'),
        ('EXPW', 'Expy'),
        ('EXPY', 'Expy'),
        ('FARM RD', 'Farm Rd'),
        ('FIRE RD', 'Fire Rd'),
        ('FOREST RD', 'Forest Rd'),
        ('FOREST ROAD', 'Forest Rd'),
        ('FOREST RTE', 'Forest Rte'),
        ('FOREST ROUTE', 'Forest Rte'),
        ('FREEWAY', 'Fwy'),
        ('FREEWY', 'Fwy'),
        ('FRWAY', 'Fwy'),
        ('FRWY', 'Fwy'),
        ('FWY', 'Fwy'),
        ('HIGHWAY', 'Hwy'),
        ('HIGHWY', 'Hwy'),
        ('HIWAY', 'Hwy'),
        ('HIWY', 'Hwy'),
        ('HWAY', 'Hwy'),
        ('HWY', 'Hwy'),
        ('I', 'I-'),
        ('I-', 'I-'),
        ('INTERSTATE', 'I-'),
        ('INTERSTATE ROUTE', 'I-'),
        ('INTERSTATE RTE', 'I-'),
        ('INTERSTATE RTE.', 'I-'),
        ('INTERSTATE RT', 'I-'),
        ('LOOP', 'Loop'),
        ('ROUTE', 'Rte'),
        ('RTE', 'Rte'),
        ('RT', 'Rte'),
        ('STATE HWY', 'State Hwy'),
        ('STATE HIGHWAY', 'State Hwy'),
        ('STATE HIGH WAY', 'State Hwy'),
        ('STATE RD', 'State Rd'),
        ('STATE ROAD', 'State Rd'),
        ('STATE ROUTE', 'State Rte'),
        ('STATE RTE', 'State Rte'),
        ('TPK', 'Tpke'),
        ('TPKE', 'Tpke'),
        ('TRNPK', 'Tpke'),
        ('TRPK', 'Tpke'),
        ('TURNPIKE', 'Tpke'),
        ('TURNPK', 'Tpke'),
        ('US HWY', 'US Hwy'),
        ('US HIGHWAY', 'US Hwy'),
        ('US HIGH WAY', 'US Hwy'),
        ('U.S.', 'US Hwy'),
        ('US RTE', 'US Rte'),
        ('US ROUTE', 'US Rte'),
        ('US RT', 'US Rte'),
        ('USFS HWY', 'USFS Hwy'),
        ('USFS HIGHWAY', 'USFS Hwy'),
        ('USFS HIGH WAY', 'USFS Hwy'),
        ('USFS RD', 'USFS Rd'),
        ('USFS ROAD', 'USFS Rd')
           ) t(name, abbrev)
           WHERE t.name NOT IN(SELECT name FROM street_type_lookup);
CREATE INDEX street_type_lookup_abbrev_idx ON street_type_lookup (abbrev);

-- Create place and countysub lookup tables
DROP TABLE IF EXISTS tiger.place_lookup;
CREATE TABLE place_lookup (
    st_code INTEGER,
    state   VARCHAR(2),
    pl_code INTEGER,
    name    VARCHAR(90),
    PRIMARY KEY (st_code,pl_code)
);

/**
INSERT INTO place_lookup
  SELECT
    pl.state::integer   as st_code,
    sl.abbrev           as state,
    pl.placefp::integer as pl_code,
    pl.name             as name
  FROM
    pl99_d00 pl
    JOIN state_lookup sl ON (pl.state = lpad(sl.st_code,2,'0'))
  GROUP BY pl.state, sl.abbrev, pl.placefp, pl.name;
**/
CREATE INDEX place_lookup_name_idx ON place_lookup (soundex(name));
CREATE INDEX place_lookup_state_idx ON place_lookup (state);

DROP TABLE IF EXISTS tiger.county_lookup;
CREATE TABLE county_lookup (
    st_code INTEGER,
    state   VARCHAR(2),
    co_code INTEGER,
    name    VARCHAR(90),
    PRIMARY KEY (st_code, co_code)
);

/**
INSERT INTO county_lookup
  SELECT
    co.state::integer    as st_code,
    sl.abbrev            as state,
    co.county::integer   as co_code,
    co.name              as name
  FROM
    co99_d00 co
    JOIN state_lookup sl ON (co.state = lpad(sl.st_code,2,'0'))
  GROUP BY co.state, sl.abbrev, co.county, co.name;
**/
CREATE INDEX county_lookup_name_idx ON county_lookup (soundex(name));
CREATE INDEX county_lookup_state_idx ON county_lookup (state);

DROP TABLE IF EXISTS tiger.countysub_lookup;
CREATE TABLE countysub_lookup (
    st_code INTEGER,
    state   VARCHAR(2),
    co_code INTEGER,
    county  VARCHAR(90),
    cs_code INTEGER,
    name    VARCHAR(90),
    PRIMARY KEY (st_code, co_code, cs_code)
);

/**
INSERT INTO countysub_lookup
  SELECT
    cs.state::integer    as st_code,
    sl.abbrev            as state,
    cs.county::integer   as co_code,
    cl.name              as county,
    cs.cousubfp::integer as cs_code,
    cs.name              as name
  FROM
    cs99_d00 cs
    JOIN state_lookup sl ON (cs.state = lpad(sl.st_code,2,'0'))
    JOIN county_lookup cl ON (cs.state = lpad(cl.st_code,2,'0') AND cs.county = cl.co_code)
  GROUP BY cs.state, sl.abbrev, cs.county, cl.name, cs.cousubfp, cs.name;
**/
CREATE INDEX countysub_lookup_name_idx ON countysub_lookup (soundex(name));
CREATE INDEX countysub_lookup_state_idx ON countysub_lookup (state);

DROP TABLE IF EXISTS tiger.zip_lookup_all;
CREATE TABLE zip_lookup_all (
    zip     INTEGER,
    st_code INTEGER,
    state   VARCHAR(2),
    co_code INTEGER,
    county  VARCHAR(90),
    cs_code INTEGER,
    cousub  VARCHAR(90),
    pl_code INTEGER,
    place   VARCHAR(90),
    cnt     INTEGER
);

/** SET work_mem = '2GB';

INSERT INTO zip_lookup_all
  SELECT *,count(*) as cnt FROM
  (SELECT
    zipl                 as zip,
    rl.statel            as st_code,
    sl.abbrev            as state,
    rl.countyl           as co_code,
    cl.name              as county,
    rl.cousubl           as cs_code,
    cs.name              as countysub,
    rl.placel            as pl_code,
    pl.name              as place
  FROM
    roads_local rl
    JOIN state_lookup sl ON (rl.statel = lpad(sl.st_code,2,'0'))
    LEFT JOIN county_lookup cl ON (rl.statel = lpad(cl.st_code,2,'0') AND rl.countyl = cl.co_code)
    LEFT JOIN countysub_lookup cs ON (rl.statel = lpad(cs.st_code,2,'0') AND rl.countyl = cs.co_code AND rl.cousubl = cs.cs_code)
    LEFT JOIN place_lookup pl ON (rl.statel = lpad(pl.st_code,2,'0') AND rl.placel = pl.pl_code)
  WHERE zipl IS NOT NULL
  UNION ALL
  SELECT
    zipr                 as zip,
    rl.stater            as st_code,
    sl.abbrev            as state,
    rl.countyr           as co_code,
    cl.name              as county,
    rl.cousubr           as cs_code,
    cs.name              as countysub,
    rl.placer            as pl_code,
    pl.name              as place
  FROM
    roads_local rl
    JOIN state_lookup sl ON (rl.stater = lpad(sl.st_code,2,'0'))
    LEFT JOIN county_lookup cl ON (rl.stater = lpad(cl.st_code,2,'0') AND rl.countyr = cl.co_code)
    LEFT JOIN countysub_lookup cs ON (rl.stater = lpad(cs.st_code,2,'0') AND rl.countyr = cs.co_code AND rl.cousubr = cs.cs_code)
    LEFT JOIN place_lookup pl ON (rl.stater = lpad(pl.st_code,2,'0') AND rl.placer = pl.pl_code)
  WHERE zipr IS NOT NULL
  ) as subquery
  GROUP BY zip, st_code, state, co_code, county, cs_code, countysub, pl_code, place;
**/
DROP TABLE IF EXISTS tiger.zip_lookup_base;
CREATE TABLE zip_lookup_base (
    zip     varchar(5),
    state   VARCHAR(40),
    county  VARCHAR(90),
    city    VARCHAR(90),
    statefp varchar(2),
    PRIMARY KEY (zip)
);

-- INSERT INTO zip_lookup_base
-- Populate through magic
-- If anyone knows of a good, public, free, place to pull this information from, that'd be awesome to have...

DROP TABLE IF EXISTS tiger.zip_lookup;
CREATE TABLE zip_lookup (
    zip     INTEGER,
    st_code INTEGER,
    state   VARCHAR(2),
    co_code INTEGER,
    county  VARCHAR(90),
    cs_code INTEGER,
    cousub  VARCHAR(90),
    pl_code INTEGER,
    place   VARCHAR(90),
    cnt     INTEGER,
    PRIMARY KEY (zip)
);

DROP TABLE IF EXISTS tiger.zcta500;
/**
INSERT INTO zip_lookup
  SELECT
    DISTINCT ON (zip)
    zip,
    st_code,
    state,
    co_code,
    county,
    cs_code,
    cousub,
    pl_code,
    place,
    cnt
  FROM zip_lookup_all
  ORDER BY zip,cnt desc;
  **/
DROP TABLE IF EXISTS tiger.county;
CREATE TABLE county
(
  gid SERIAL NOT NULL,
  statefp character varying(2),
  countyfp character varying(3),
  countyns character varying(8),
  cntyidfp character varying(5) NOT NULL,
  "name" character varying(100),
  namelsad character varying(100),
  lsad character varying(2),
  classfp character varying(2),
  mtfcc character varying(5),
  csafp character varying(3),
  cbsafp character varying(5),
  metdivfp character varying(5),
  funcstat character varying(1),
  aland bigint,
  awater  double precision,
  intptlat character varying(11),
  intptlon character varying(12),
  the_geom geometry,
  CONSTRAINT uidx_county_gid UNIQUE (gid),
  CONSTRAINT pk_tiger_county PRIMARY KEY (cntyidfp),
  CONSTRAINT enforce_dims_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_geom CHECK (geometrytype(the_geom) = 'MULTIPOLYGON'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_geom CHECK (st_srid(the_geom) = 4269)
);
CREATE INDEX idx_tiger_county ON county USING btree (countyfp);

DROP TABLE IF EXISTS tiger.state;
CREATE TABLE state
(
  gid serial NOT NULL,
  region character varying(2),
  division character varying(2),
  statefp character varying(2),
  statens character varying(8),
  stusps character varying(2) NOT NULL,
  "name" character varying(100),
  lsad character varying(2),
  mtfcc character varying(5),
  funcstat character varying(1),
  aland bigint,
  awater bigint,
  intptlat character varying(11),
  intptlon character varying(12),
  the_geom geometry,
  CONSTRAINT uidx_tiger_state_stusps UNIQUE (stusps),
  CONSTRAINT uidx_tiger_state_gid UNIQUE (gid),
  CONSTRAINT pk_tiger_state PRIMARY KEY (statefp),
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_the_geom CHECK (geometrytype(the_geom) = 'MULTIPOLYGON'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 4269)
);
CREATE INDEX idx_tiger_state_the_geom_gist ON state USING gist(the_geom);

DROP TABLE IF EXISTS tiger.place;
CREATE TABLE place
(
  gid serial NOT NULL,
  statefp character varying(2),
  placefp character varying(5),
  placens character varying(8),
  plcidfp character varying(7) PRIMARY KEY,
  "name" character varying(100),
  namelsad character varying(100),
  lsad character varying(2),
  classfp character varying(2),
  cpi character varying(1),
  pcicbsa character varying(1),
  pcinecta character varying(1),
  mtfcc character varying(5),
  funcstat character varying(1),
  aland bigint,
  awater bigint,
  intptlat character varying(11),
  intptlon character varying(12),
  the_geom geometry,
  CONSTRAINT uidx_tiger_place_gid UNIQUE (gid),
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_the_geom CHECK (geometrytype(the_geom) = 'MULTIPOLYGON'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 4269)
);
CREATE INDEX tiger_place_the_geom_gist ON place USING gist(the_geom);

DROP TABLE IF EXISTS tiger.zip_state;
CREATE TABLE zip_state
(
  zip character varying(5) NOT NULL,
  stusps character varying(2) NOT NULL,
  statefp character varying(2),
  CONSTRAINT zip_state_pkey PRIMARY KEY (zip, stusps)
);

DROP TABLE IF EXISTS tiger.zip_state_loc;
CREATE TABLE zip_state_loc
(
  zip character varying(5) NOT NULL,
  stusps character varying(2) NOT NULL,
  statefp character varying(2),
  place varchar(100),
  CONSTRAINT zip_state_loc_pkey PRIMARY KEY (zip, stusps, place)
);

DROP TABLE IF EXISTS tiger.cousub;
CREATE TABLE cousub
(
  gid serial NOT NULL,
  statefp character varying(2),
  countyfp character varying(3),
  cousubfp character varying(5),
  cousubns character varying(8),
  cosbidfp character varying(10) NOT NULL PRIMARY KEY,
  "name" character varying(100),
  namelsad character varying(100),
  lsad character varying(2),
  classfp character varying(2),
  mtfcc character varying(5),
  cnectafp character varying(3),
  nectafp character varying(5),
  nctadvfp character varying(5),
  funcstat character varying(1),
  aland numeric(14),
  awater numeric(14),
  intptlat character varying(11),
  intptlon character varying(12),
  the_geom geometry,
  CONSTRAINT uidx_cousub_gid UNIQUE (gid),
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_the_geom CHECK (geometrytype(the_geom) = 'MULTIPOLYGON'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 4269)
);

CREATE INDEX tige_cousub_the_geom_gist ON cousub USING gist(the_geom);

DROP TABLE IF EXISTS tiger.edges;
CREATE TABLE edges
(
  gid SERIAL NOT NULL  PRIMARY KEY,
  statefp character varying(2),
  countyfp character varying(3),
  tlid bigint,
  tfidl numeric(10),
  tfidr numeric(10),
  mtfcc character varying(5),
  fullname character varying(100),
  smid character varying(22),
  lfromadd character varying(12),
  ltoadd character varying(12),
  rfromadd character varying(12),
  rtoadd character varying(12),
  zipl character varying(5),
  zipr character varying(5),
  featcat character varying(1),
  hydroflg character varying(1),
  railflg character varying(1),
  roadflg character varying(1),
  olfflg character varying(1),
  passflg character varying(1),
  divroad character varying(1),
  exttyp character varying(1),
  ttyp character varying(1),
  deckedroad character varying(1),
  artpath character varying(1),
  persist character varying(1),
  gcseflg character varying(1),
  offsetl character varying(1),
  offsetr character varying(1),
  tnidf numeric(10),
  tnidt numeric(10),
  the_geom geometry,
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_the_geom CHECK (geometrytype(the_geom) = 'MULTILINESTRING'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 4269)
);
CREATE INDEX idx_edges_tlid ON edges USING btree(tlid);
CREATE INDEX idx_tiger_edges_countyfp ON edges USING btree(countyfp);
CREATE INDEX idx_tiger_edges_the_geom_gist ON edges USING gist(the_geom);

DROP TABLE IF EXISTS tiger.addrfeat;
CREATE TABLE addrfeat
(
  gid serial not null primary key,
  tlid bigint,
  statefp character varying(2) NOT NULL,
  aridl character varying(22),
  aridr character varying(22),
  linearid character varying(22),
  fullname character varying(100),
  lfromhn character varying(12),
  ltohn character varying(12),
  rfromhn character varying(12),
  rtohn character varying(12),
  zipl character varying(5),
  zipr character varying(5),
  edge_mtfcc character varying(5),
  parityl character varying(1),
  parityr character varying(1),
  plus4l character varying(4),
  plus4r character varying(4),
  lfromtyp character varying(1),
  ltotyp character varying(1),
  rfromtyp character varying(1),
  rtotyp character varying(1),
  offsetl character varying(1),
  offsetr character varying(1),
  the_geom geometry,
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_the_geom CHECK (geometrytype(the_geom) = 'LINESTRING'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 4269)
);
CREATE INDEX idx_addrfeat_geom_gist ON addrfeat USING gist(the_geom );
CREATE INDEX idx_addrfeat_tlid ON addrfeat USING btree(tlid);
CREATE INDEX idx_addrfeat_zipl ON addrfeat USING btree(zipl);
CREATE INDEX idx_addrfeat_zipr ON addrfeat USING btree(zipr);


DROP TABLE IF EXISTS tiger.faces;
CREATE TABLE faces
(
gid serial NOT NULL PRIMARY KEY,
  tfid numeric(10,0),
  statefp00 varchar(2),
  countyfp00 varchar(3),
  tractce00 varchar(6),
  blkgrpce00 varchar(1),
  blockce00 varchar(4),
  cousubfp00 varchar(5),
  submcdfp00 varchar(5),
  conctyfp00 varchar(5),
  placefp00 varchar(5),
  aiannhfp00 varchar(5),
  aiannhce00 varchar(4),
  comptyp00 varchar(1),
  trsubfp00 varchar(5),
  trsubce00 varchar(3),
  anrcfp00 varchar(5),
  elsdlea00 varchar(5),
  scsdlea00 varchar(5),
  unsdlea00 varchar(5),
  uace00 varchar(5),
  cd108fp varchar(2),
  sldust00 varchar(3),
  sldlst00 varchar(3),
  vtdst00 varchar(6),
  zcta5ce00 varchar(5),
  tazce00 varchar(6),
  ugace00 varchar(5),
  puma5ce00 varchar(5),
  statefp varchar(2),
  countyfp varchar(3),
  tractce varchar(6),
  blkgrpce varchar(1),
  blockce varchar(4),
  cousubfp varchar(5),
  submcdfp varchar(5),
  conctyfp varchar(5),
  placefp varchar(5),
  aiannhfp varchar(5),
  aiannhce varchar(4),
  comptyp varchar(1),
  trsubfp varchar(5),
  trsubce varchar(3),
  anrcfp varchar(5),
  ttractce varchar(6),
  tblkgpce varchar(1),
  elsdlea varchar(5),
  scsdlea varchar(5),
  unsdlea varchar(5),
  uace varchar(5),
  cd111fp varchar(2),
  sldust varchar(3),
  sldlst varchar(3),
  vtdst varchar(6),
  zcta5ce varchar(5),
  tazce varchar(6),
  ugace varchar(5),
  puma5ce varchar(5),
  csafp varchar(3),
  cbsafp varchar(5),
  metdivfp varchar(5),
  cnectafp varchar(3),
  nectafp varchar(5),
  nctadvfp varchar(5),
  lwflag varchar(1),
  "offset" varchar(1),
  atotal double precision,
  intptlat varchar(11),
  intptlon varchar(12),
  the_geom geometry,
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_the_geom CHECK (geometrytype(the_geom) = 'MULTIPOLYGON'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 4269)
);
CREATE INDEX idx_tiger_faces_tfid ON faces USING btree (tfid);
CREATE INDEX idx_tiger_faces_countyfp ON faces USING btree(countyfp);
CREATE INDEX tiger_faces_the_geom_gist ON faces USING gist(the_geom);

DROP TABLE IF EXISTS tiger.featnames;
CREATE TABLE featnames
(
  gid SERIAL NOT NULL,
  tlid bigint,
  fullname character varying(100),
  "name" character varying(100),
  predirabrv character varying(15),
  pretypabrv character varying(50),
  prequalabr character varying(15),
  sufdirabrv character varying(15),
  suftypabrv character varying(50),
  sufqualabr character varying(15),
  predir character varying(2),
  pretyp character varying(3),
  prequal character varying(2),
  sufdir character varying(2),
  suftyp character varying(3),
  sufqual character varying(2),
  linearid character varying(22),
  mtfcc character varying(5),
  paflag character varying(1),
  CONSTRAINT featnames_pkey PRIMARY KEY (gid)
);
ALTER TABLE featnames ADD COLUMN statefp character varying(2);
CREATE INDEX idx_tiger_featnames_snd_name ON featnames USING btree (soundex(name));
CREATE INDEX idx_tiger_featnames_lname ON featnames USING btree (lower(name));
CREATE INDEX idx_tiger_featnames_tlid_statefp ON featnames USING btree (tlid,statefp);

CREATE TABLE addr
(
  gid SERIAL NOT NULL,
  tlid bigint,
  fromhn character varying(12),
  tohn character varying(12),
  side character varying(1),
  zip character varying(5),
  plus4 character varying(4),
  fromtyp character varying(1),
  totyp character varying(1),
  fromarmid integer,
  toarmid integer,
  arid character varying(22),
  mtfcc character varying(5),
  CONSTRAINT addr_pkey PRIMARY KEY (gid)
);
ALTER TABLE addr ADD COLUMN statefp character varying(2);

CREATE INDEX idx_tiger_addr_tlid_statefp ON addr USING btree(tlid,statefp);
CREATE INDEX idx_tiger_addr_zip ON addr USING btree (zip);

--DROP TABLE IF EXISTS tiger.zcta5;
CREATE TABLE zcta5
(
  gid serial NOT NULL,
  statefp character varying(2),
  zcta5ce character varying(5),
  classfp character varying(2),
  mtfcc character varying(5),
  funcstat character varying(1),
  aland double precision,
  awater double precision,
  intptlat character varying(11),
  intptlon character varying(12),
  partflg character varying(1),
  the_geom geometry,
  CONSTRAINT uidx_tiger_zcta5_gid UNIQUE (gid),
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_the_geom CHECK (geometrytype(the_geom) = 'MULTIPOLYGON'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 4269),
  CONSTRAINT pk_tiger_zcta5_zcta5ce PRIMARY KEY (zcta5ce,statefp)
 );

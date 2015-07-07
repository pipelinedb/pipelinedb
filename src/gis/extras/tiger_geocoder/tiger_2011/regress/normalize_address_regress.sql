\timing
SELECT '#887' As ticket, * FROM normalize_address('2450 N COLORADO ST, PHILADELPHIA, PA, 19132');
SELECT '#1051a' As ticket, * FROM normalize_address('212 3rd Ave N Suite 560, Minneapolis, MN 55401');
SELECT '#1051b' As ticket, * FROM normalize_address('3937 43RD AVE S, MINNEAPOLIS, MN 55406');
SELECT '#1051c' As ticket, * FROM normalize_address('212 N 3rd Ave, Minneapolis, MN 55401');
-- City missing ,  -- NOTE this one won't normalize right if you don't have MN data loaded
SELECT '#1051d' As ticket, * FROM normalize_address('212 3rd Ave N Minneapolis, MN 55401'); 
-- comma in wrong spot
SELECT * FROM normalize_address('529 Main Street, Boston MA, 02129');
-- comma in right spot
SELECT * FROM normalize_address('529 Main Street, Boston,MA 02129');
-- partial address
SELECT * FROM normalize_address('529 Main Street, Boston, MA');
-- Full address with suite using ,
SELECT * FROM normalize_address('529 Main Street, Apt 201, Boston, MA 02129');
-- Full address with apart using space
SELECT * FROM normalize_address('529 Main Street Apt 201, Boston, MA 02129');
-- Partial address with apartment
SELECT * FROM normalize_address('529 Main Street, Apt 201, Boston, MA');

--This one fails so lead out for now
SELECT '#1108a' As ticket, * FROM normalize_address('529 Main Street, Suite 201, Boston, MA 02129');

-- Partial and Mangled zipcodes
SELECT '#1073a' As ticket, * FROM normalize_address('212 3rd Ave N, MINNEAPOLIS, MN 553404');
SELECT '#1073b' As ticket, * FROM normalize_address('212 3rd Ave N, MINNEAPOLIS, MN 55401-');
SELECT '#1073c' As ticket, * FROM normalize_address('529 Main Street, Boston, MA 021');

-- comma in wrong position
SELECT '#1086a' As ticket, * FROM normalize_address('949 N 3rd St, New Hyde Park, NY, 11040');

-- comma in right position --
SELECT '#1086b' As ticket, * FROM normalize_address('949 N 3rd St, New Hyde Park, NY 11040');

-- country roads and highways with spaces in street type
SELECT '#1076a' As ticket, * FROM normalize_address('16725 Co Rd 24, Plymouth, MN 55447'); 
SELECT '#1076b' As ticket, * FROM normalize_address('16725 County Road 24, Plymouth, MN 55447'); 
SELECT '#1076c' As ticket, * FROM normalize_address('13800 County Hwy 9, Andover, MN 55304');
SELECT '#1076d' As ticket, * FROM normalize_address('13800 9, Andover, MN 55304');
-- this one is a regular street that happens to have a street type as the name
SELECT '#1076e' As ticket, * FROM normalize_address('14 Forest Road, Acton, MA');

-- A country road with a letter name and direction 
-- NOTE this doesn't completely normalize right since the direction W is being cut off --
SELECT '#1076f' As ticket, * FROM normalize_address('1940 County Road C W, Roseville, MN 55113'); 

-- Route with a name that sounds like a direction --
SELECT '#1076g' As ticket, * FROM normalize_address('3900 Route 6, Eastham, Massachusetts 02642');

-- Street that has same name as type  --
SELECT '#1076h' As ticket, * FROM normalize_address('4533 PARK AVE S, MINNEAPOLIS, MN 55407');
-- same street with alternate county name
SELECT '#1076i' As ticket, * FROM normalize_address('4533 County Road 33, MINNEAPOLIS, MN 55407'); 

-- Same case of street type that has name as a type --
-- this matches -
SELECT '#1109a' As ticket, * from normalize_address('4373 LAKE DRIVE, ROBBINSDALE, MN 55422');

-- this failed --
SELECT '#1109b' As ticket, * from normalize_address('4373 LAKE DR, ROBBINSDALE, MN 55422');

-- another type (Is) that is part of street name but a compound street name
SELECT '#1074a' As ticket, * FROM normalize_address('3420 RHODE ISLAND AVE S, ST. LOUIS PARK, MN 55426');

-- another type that is part of street name --
SELECT '#1074b' As ticket, * FROM normalize_address('26 Court Street, Boston,MA 02109');

-- service roads and interstates
SELECT '#1112a' As ticket, * FROM normalize_address('8401 W 35W Service Dr NE, Blaine, MN 55449');
SELECT '#1112b' As ticket, * FROM normalize_address('8401 35W, Blaine, MN 55449');
SELECT '#1112c' As ticket, * FROM normalize_address('8401 35W West, Blaine, MN 55449');
SELECT '#1112d' As ticket, * FROM normalize_address('8401 West 35W, Blaine, MN 55449');
SELECT '#1112e' As ticket, * FROM normalize_address('8401 W 35W, Blaine, MN 55449');

-- Testing pretty print of highway addresses
-- These tests excerpted from Brian Hamlin's CASS failures
-- in #1077
SELECT '#1125a' As ticket, pprint_addy(normalize_address('19596 COUNTY ROAD 480, COLCORD, OK 74338'));
SELECT '#1125b' As ticket, pprint_addy(addy), addy.* FROM normalize_address('4345 353 Rte, SALAMANCA, NY 14779') AS addy;
SELECT '#1125c' As ticket, pprint_addy(addy), addy.* FROM normalize_address('19799 STATE ROUTE O, COSBY, MO 64436') AS addy;

-- some more to test interstate permutations
SELECT '#1125d' As ticket, pprint_addy(addy), addy.* FROM normalize_address('Interstate 90,Boston, MA') As addy;
-- this one is wrong (because the lack of space trips it up) but will fix later
SELECT '#1125e' As ticket, pprint_addy(addy), addy.* FROM normalize_address('I-90,Boston, MA') As addy;
SELECT '#1125f' As ticket, pprint_addy(addy), addy.* FROM normalize_address('I 90,Boston, MA') As addy;

-- location with prefixes getting caught in post prefix
SELECT '#1310a' As ticket, pprint_addy(addy), addy.* FROM normalize_address('1110 W CAPITOL AVE, WEST SACRAMENTO, CA') As addy;

-- #1614 County Rd
SELECT '#1614a' As ticket, pprint_addy(addy), addy.* FROM normalize_address('8435 COUNTY RD 20 SE, ROCHESTER, MN 55904') As addy;
SELECT '#1614b' As ticket, pprint_addy(addy), addy.* FROM normalize_address('3208 U.S. 52, Rochester, MN 55901') As addy;
\timing

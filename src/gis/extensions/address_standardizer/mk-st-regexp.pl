#!/usr/bin/perl -w
use strict;
use Regexp::Assemble;

# TODO
#   Add prefix types like:
#       Ave(nue)? of( the)? names
#       Ave(nue)? (d'|du|de)(la)?\s?names
#       Ave(nue|nida)? \w{1,2}
#       calle names
#       suffix of( the)? names
#       route (\d+([a-z]|bus(iness)?)(by(pass))?
#       business (\d+([a-z]|bus(iness)?)(by(pass))?
#       (interstate|I-) \d+\s*[nsew]?
#
#   Add better number recognizer
#       dir num dir num dir
#       dir num letter
#       num? fraction   123 1/2
#
#   Add patterns to recognize intersections
#       street & street, city, state
#       
#
#  Probably the following were removed as they are PREFIX TYPES
#  RTE, ROUTE
#  CALLE
#
#  and maybe RUE
#  RUE can be either: RUE d'la whatever; Charles Rue
#
#  Many of the SUFFIX TYPES can be used in a prefix contexted like:
#  AVENUE of the Americas
#


#my @cities = split(/[\r\n]+/, qx(cat usps-city-names.txt));

# ==============================


my @stwords = qw(
ALLEE
ALLEY
ALLY
ALY
ANEX
ANNEX
ANNX
ANX
ARC
ARCADE
AV
AVE
AVEN
AVENU
AVENUE
AVN
AVNUE
BAYOO
BAYOU
BCH
BEACH
BEND
BG
BGS
BLF
BLFS
BLUF
BLUFF
BLUFFS
BLVD
BND
BOT
BOTTM
BOTTOM
BOUL
BOULEVARD
BOULV
BR
BRANCH
BRDGE
BRG
BRIDGE
BRK
BRKS
BRNCH
BROOK
BROOKS
BTM
BURG
BURGS
BYP
BYPA
BYPAS
BYPASS
BYPS
BYU
CAMP
CANYN
CANYON
CAPE
CAUSEWAY
CAUSWAY
CEN
CENT
CENTER
CENTERS
CENTR
CENTRE
CIR
CIRC
CIRCL
CIRCLE
CIRCLES
CIRS
CK
CLB
CLF
CLFS
CLIFF
CLIFFS
CLUB
CMN
CMP
CNTER
CNTR
CNYN
COMMON
COR
CORNER
CORNERS
CORS
COURSE
COURT
COURTS
COVE
COVES
CP
CPE
CR
CRCL
CRCLE
CRECENT
CREEK
CRES
CRESCENT
CRESENT
CREST
CRK
CROSSING
CROSSROAD
CRSCNT
CRSE
CRSENT
CRSNT
CRSSING
CRSSNG
CRST
CRT
CSWY
CT
CTR
CTRS
CTS
CURV
CURVE
CV
CVS
CYN
DALE
DAM
DIV
DIVIDE
DL
DM
DR
DRIV
DRIVE
DRIVES
DRS
DRV
DV
DVD
EST
ESTATE
ESTATES
ESTS
EXP
EXPR
EXPRESS
EXPRESSWAY
EXPW
EXPY
EXT
EXTENSION
EXTENSIONS
EXTN
EXTNSN
EXTS
FALL
FALLS
FERRY
FIELD
FIELDS
FLAT
FLATS
FLD
FLDS
FLS
FLT
FLTS
FORD
FORDS
FOREST
FORESTS
FORG
FORGE
FORGES
FORK
FORKS
FORT
FRD
FRDS
FREEWAY
FREEWY
FRG
FRGS
FRK
FRKS
FRRY
FRST
FRT
FRWAY
FRWY
FRY
FT
FWY
GARDEN
GARDENS
GARDN
GATEWAY
GATEWY
GATWAY
GDN
GDNS
GLEN
GLENS
GLN
GLNS
GRDEN
GRDN
GRDNS
GREEN
GREENS
GRN
GRNS
GROV
GROVE
GROVES
GRV
GRVS
GTWAY
GTWY
HARB
HARBOR
HARBORS
HARBR
HAVEN
HAVN
HBR
HBRS
HEIGHT
HEIGHTS
HGTS
HIGHWAY
HIGHWY
HILL
HILLS
HIWAY
HIWY
HL
HLLW
HLS
HOLLOW
HOLLOWS
HOLW
HOLWS
HRBOR
HT
HTS
HVN
HWAY
HWY
INLET
INLT
IS
ISLAND
ISLANDS
ISLE
ISLES
ISLND
ISLNDS
ISS
JCT
JCTION
JCTN
JCTNS
JCTS
JUNCTION
JUNCTIONS
JUNCTN
JUNCTON
KEY
KEYS
KNL
KNLS
KNOL
KNOLL
KNOLLS
KY
KYS
LA
LAKE
LAKES
LAND
LANDING
LANE
LANES
LCK
LCKS
LDG
LDGE
LF
LGT
LGTS
LIGHT
LIGHTS
LINE
LK
LKS
LN
LNDG
LNDNG
LOAF
LOCK
LOCKS
LODG
LODGE
LOOP
LOOPS
MALL
MANOR
MANORS
MDW
MDWS
MEADOW
MEADOWS
MEDOWS
MEWS
MILL
MILLS
MISSION
MISSN
ML
MLS
MNR
MNRS
MNT
MNTAIN
MNTN
MNTNS
MOTORWAY
MOUNT
MOUNTAIN
MOUNTAINS
MOUNTIN
MSN
MSSN
MT
MTIN
MTN
MTNS
MTWY
NCK
NECK
OPAS
ORCH
ORCHARD
ORCHRD
OVAL
OVERPASS
OVL
PARK
PARKS
PARKWAY
PARKWAYS
PARKWY
PASS
PASSAGE
PATH
PATHS
PIKE
PIKES
PINE
PINES
PK
PKWAY
PKWY
PKWYS
PKY
PL
PLACE
PLAIN
PLAINES
PLAINS
PLAZA
PLN
PLNS
PLZ
PLZA
PNE
PNES
POINT
POINTS
PORT
PORTS
PR
PRAIRIE
PRARIE
PRK
PRR
PRT
PRTS
PSGE
PT
PTS
RAD
RADIAL
RADIEL
RADL
RAMP
RANCH
RANCHES
RAPID
RAPIDS
RD
RDG
RDGE
RDGS
RDS
REST
RIDGE
RIDGES
RIV
RIVER
RIVR
RNCH
RNCHS
ROAD
ROADS
ROW
RPD
RPDS
RST
RUE
RUN
RVR
SHL
SHLS
SHOAL
SHOALS
SHOAR
SHOARS
SHORE
SHORES
SHR
SHRS
SKWY
SKYWAY
SMT
SPG
SPGS
SPNG
SPNGS
SPRING
SPRINGS
SPRNG
SPRNGS
SPUR
SPURS
SQ
SQR
SQRE
SQRS
SQS
SQU
SQUARE
SQUARES
ST
STA
STATION
STATN
STN
STR
STRA
STRAV
STRAVE
STRAVEN
STRAVENUE
STRAVN
STREAM
STREET
STREETS
STREME
STRM
STRT
STRVN
STRVNUE
STS
SUMIT
SUMITT
SUMMIT
TER
TERR
TERRACE
THROUGHWAY
TPK
TPKE
TR
TRACE
TRACES
TRACK
TRACKS
TRAFFICWAY
TRAIL
TRAILS
TRAK
TRCE
TRFY
TRK
TRKS
TRL
TRLS
TRNPK
TRPK
TRWY
TUNEL
TUNL
TUNLS
TUNNEL
TUNNELS
TUNNL
TURNPIKE
TURNPK
UN
UNDERPASS
UNION
UNIONS
UNS
UPAS
VALLEY
VALLEYS
VALLY
VDCT
VIA
VIADCT
VIADUCT
VIEW
VIEWS
VILL
VILLAG
VILLAGE
VILLAGES
VILLE
VILLG
VILLIAGE
VIS
VIST
VISTA
VL
VLG
VLGS
VLLY
VLY
VLYS
VST
VSTA
VW
VWS
WALK
WALKS
WALL
WAY
WAYS
WELL
WELLS
WL
WLS
WY
XING
XRD
);
# ==============================

my @secwords = qw(
APARTMENT
APT
BASEMENT
BLDG
BSMT
BUILDING
DEPARTMENT
DEPT
FL
FLOOR
FRNT
FRONT
HANGAR
HNGR
LBBY
LOBBY
LOT
LOWER
LOWR
OFC
OFFICE
PENTHOUSE
PH
PIER
REAR
RM
ROOM
SIDE
SLIP
SPACE
SPC
STE
STOP
SUITE
TRAILER
TRLR
UNIT
UPPER
UPPR
);

my @dirs = qw(
NORTH N NORD
SOUTH S SUD
EAST  E EST
WEST  W OEST O
NORTHEAST NE
NORTHWEST NW
SOUTHEAST SE
SOUTHWEST SW
NORTH-EAST N-E
NORTH-WEST N-W
SOUTH-EAST S-E
SOUTH-WEST S-W
);

my @saints = (
"st",
"st.",
"ste",
"ste.",
"saint",
);

my $re;
my $l = Regexp::Assemble->new(flags => "i");
#$re = $l->set(modifiers=>'i')->list2re(@cities);
#$re =~ s/\\/\\\\/g;
#my $cities = $re;

#print "    static const char *cities = \n";
#while ($re =~ s/^(.{1,75})//) {
#    print "  \"$1\"\n";
#}
#print "  ;\n";


$l->add(@stwords);
$re = $l->re;
$re =~ s/\\/\\\\/g;
$re =~ s/\?\^/?-xism/g;
my $sttype = $re;
#print "    static const char *sttype = \"$re\";\n\n";

$l->add(@secwords);
$re = $l->re;
$re =~ s/\\/\\\\/g;
$re =~ s/\?\^/?-xism/g;
my $unittype = $re;
#print "    static const char *unittype = \"$re\";\n\n";

$l->add(@dirs);
$re = $l->re;
$re =~ s/\\/\\\\/g;
$re =~ s/\?\^/?-xism/g;
my $dirs = $re;
#print "    static const char *dirtype = \"$re\";\n\n";

$l->add(@saints);
$re = $l->re;
$re =~ s/\\/\\\\/g;
$re =~ s/\?\^/?-xism/g;
my $saint = $re;
#print "    static const char *saints = \"$re\";\n\n";

my $word  = "\\\\w+";
my $words = "($word(\\\\s$word)*)";

my @reg = ();
#push @reg, "(?:,\\\\s*)([^,]+)\$";
#push @reg, "\\\\b($cities)\$";
push @reg, "(?:\\\\b$sttype\\\\s(?:$dirs\\\\s))($dirs\\\\s$words)\$";
push @reg, "(?:\\\\b$sttype\\\\s(?:$dirs\\\\s))($dirs\\\\s$saint\\\\s$words)\$";
push @reg, "(?:\\\\b$sttype\\\\s)($dirs\\\\s$saint\\\\s$words)\$";
push @reg, "(?:\\\\b$sttype\\\\s)($saint\\\\s$words)\$";
push @reg, "(?:\\\\b$sttype\\\\s)($dirs\\\\s$words)\$";
push @reg, "(?:\\\\b$sttype\\\\s)($words)\$";
push @reg, "(?:\\\\s)($dirs\\\\s$words)\$";
push @reg, "^(?:\\\\d+\\\\s(?:(?:\\\\w+\\\\s)$sttype))()\$";
push @reg, "^(?:\\\\d+\\\\s(?:(?:\\\\w+\\\\s)*\\\\w+\\\\s))($word)\$";

my $nn = scalar @reg;
print "    const int nreg = $nn;\n";
print "    static const char *t_regx[$nn] = {\n        \"";
print join("\",\n        \"", @reg);
print "\"\n    };\n";



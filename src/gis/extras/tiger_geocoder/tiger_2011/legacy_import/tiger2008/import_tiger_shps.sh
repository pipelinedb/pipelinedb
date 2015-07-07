#!/bin/bash

shopt -s nullglob

# This is the root directory of the TIGER data.
BASE="TIGER2008"

# This is the set base specified by Census
SETBASE="tl_2008"

# This is the schema prefix, all schemas will be created using this prefix.
SCHEMA_PREFIX="tiger"

# Skip Census 2000 data if there is current data?
SKIP00="false"

# First, handle the national data
TMPDIR=`mktemp -d tiger_tmpXXXX`

# SRID to load the data with
SRID=4269
DEFAULT_SRID=4269
# Host to connect to

if [ -z "${PGHOST}" ]; then
  HOST="localhost"
else
  HOST=${PGHOST}
fi
# Database to use
if [ -z "${PGDATABASE}" ];then
  DB="tiger"
else
  DB=${PGDATABASE}
fi

# postgres username
if [ -z ${PGUSER} ]; then
  DBUSER=`whoami`
else
  DBUSER=${PGUSER}
fi

# postgres port
if [ -z ${PGPORT} ]; then
  DBPORT=5432
else
  DBPORT=${PGPORT}
fi

# rm command
RM="/bin/rm"

# PSQL location
PSQL="psql"

# Encoding to use
ENCODING="LATIN1"

# If we are processing national-level data
NATIONAL="false"

# If we are processing state-level data
STATELVL="true"

# If we are processing a specific state
STATES=''

# If we are processing county-level data
COUNTYLVL="true"

# If we are processing a specific county
COUNTIES='*'

# If we are dropping tables before loading them
DROP="false"

# If we are dropping the schema before loading
DROP_SCHEMA="false"

# how verbose
DEBUG='false'
QUIET='false'

function table_from_filename () {
  local FILE="$1"
  TBL=`basename $FILE .shp`
  TBL=`basename ${TBL} .dbf`
  TBL=`echo ${TBL} | cut -d_ -f4`
  
}

function error () {
  echo '' >&2
  echo "$1" >&2
  echo '' >&2
}

function debug () {
  if [ ${DEBUG} = "true" ]; then
    echo "\* $@" >&2
  fi
}
function note () {
  if [ ! ${QUIET} = 'true' ]; then
    echo "$@"
  fi
}
function unzip_files_matching () {
  local PAT=$1
  local ZIPFILES="${PAT}*.zip"
  if [ -z "${ZIPFILES}" ]; then
    error "$BASE/${FILEBASE}_${NATLAYERS}.zip did not match anything!"
  else
    for zipfile in ${ZIPFILES}; do
      local BASENAME=`basename $zipfile .zip`
      if [ ${SKIP00} = 'true' ]; then
        echo ${BASENAME}| egrep -q '00$'
        if [ $? -eq 0 ]; then
          continue
        fi
      fi
      note "Unzipping $BASENAME..."
      unzip -q -d $TMPDIR $zipfile
    done
  fi
}
            
function reproject () {
  FILE="$1"
  local DIRNAME=`dirname ${FILE}`
  local BASE=`basename ${FILE}`
  SRID="$2"
  which ogr2ogr > /dev/null 2>&1
  if [ $? -ne 0 ]; then
    error "ogr2ogr not found. You may not specify -r"
    exit 1
  fi
  NEWFILE="${DIRNAME}/${SRID}_${BASE}"
  ogr2ogr \
    -overwrite -t_srs "EPSG:${SRID}" \
    -f 'ESRI Shapefile' \
    "${NEWFILE}" "${FILE}"
  if [ $? -ne 0 ]; then
    error "error reprojecting file ${FILE} into ${NEWFILE}"
    exit 1;
  fi
}

function addcols () {
  local SCHEMA=$1
  local TABLE=$2
  local FIPS=`echo ${SCHEMA} | awk -F_ '/_[0-9][0-9]$/ {print $NF}'`

  if [ -z "${FIPS}" ]; then
    error "cannot find fips code for ${SCHEMA} - that is probably not good"
    return 1
  fi
  echo ${TABLE}| egrep -q '00$'
  if [ $? -eq 0 ]; then
    STATEFP='statefp00'
  else
    STATEFP='statefp'
  fi
  # add statefp where needed
  ${PSQL_CMD} -t -c "\d ${SCHEMA}.${TABLE}"| egrep -q "^ +${STATEFP} "
  if [ $? -eq 1 ]; then
    ${PSQL_CMD_NULL} -c "ALTER TABLE ${SCHEMA}.${TABLE} ADD COLUMN ${STATEFP} varchar(2) not null DEFAULT ('${FIPS}');"
  fi
  # add statefp check everywhere
  ${PSQL_CMD_NULL} -c "ALTER TABLE ${SCHEMA}.${TABLE} ADD  CHECK (${STATEFP} = '${FIPS}');"
  #${PSQL_CMD_NULL} -c "CREATE INDEX ${TABLE}_${STATEFP}_idx ON ${SCHEMA}.${TABLE} USING btree($STATEFP);"
  
}

function loadshp () {
  local FILE="$1"
  local TABLE="$2"
  local BASESHP=`basename $FILE .shp`
  local DROPTBL=""
  local CMD_EXTRAS=''
  local NEWFILE=''
  
  if [ "$DROP" = "true" ]; then
    DROPTBL="-d"
  fi
  note Loading ${FILE} into ${SCHEMA}.${TABLE}
  if [ "${DEBUG}" = 'true' ]; then
    :
  else
    CMD_EXTRAS=''
  fi

  if [ ${DEFAULT_SRID} = ${SRID} ]; then
    NEWFILE=${FILE}
  else
    reproject "${FILE}" ${SRID}
    note "using reprojected file: ${NEWFILE}"
  fi
  shp2pgsql \
    $DROPTBL \
    -I  \
    -s $SRID \
    -W ${ENCODING} \
    "${NEWFILE}" \
    "${SCHEMA}.${TABLE}"\
    ${CMD_EXTRAS} \
    | (echo set client_min_messages=fatal\; ;cat -) \
    | ${PSQL_CMD_NULL} \
    | egrep -v '^(INSERT INTO|BEGIN;|END;)' # you really don't want to see a zillion insert statements
    addcols "$SCHEMA" "$TABLE"
}

function loaddbf () {
  local FILE="$1"
  local TABLE="$2"
  local BASESHP=`basename $FILE .dbf`
  local DROPTBL=""
  if [ "$DROP" = "true" ]; then
    DROPTBL="-d"
  fi
  note Loading ${FILE} into ${SCHEMA}.${TABLE}
  shp2pgsql \
    $DROPTBL \
    -W ${ENCODING} \
    -n \
    "${FILE}" \
    "${SCHEMA}.${TABLE}" \
    | (echo set client_min_messages=fatal\; ;cat -) \
    | ${PSQL_CMD_NULL} \
    | egrep -v '^(INSERT INTO|BEGIN;|END;)' # you really don't want to see a zillion insert statements
  addcols "$SCHEMA" "$TABLE"
}

function create_schema () {
 local SCHEMA="$1"
 local EXT=''
  if [ "${DROP_SCHEMA}" = "true" ]; then
    EXT="drop schema if exists $SCHEMA cascade;"
  fi
  cat<<EOT  | (echo 'set client_min_messages=fatal;';cat -) | ${PSQL_CMD_NULL}
  $EXT
  create schema $SCHEMA;
EOT
}

function usage () {
  cat >&2 <<EOT  
  -q               Be quieter (shp2pgsql does not always cooperate here)
  -n  glob         Load national-level data matching pattern 
                   (use 'all' to match, well all)
                   (default: skip national data)

  -s  statecode    Load data for specific state code 
                   (default: all state level files)
                   setting this to 'none' skips loading of state files.

  -c  countycode   Load data for specific county code within state given)
                   (default: all state and county-level files)
                   setting this to 'none' skips loading of county level files.

  -X                Drop schemas before loading the data.
                    Tables will not be dropped individually, since they will 
                    be dropped with the schema.

  -S  prefix       Created schemas will be prefixed with this (default: tiger)

  -D               Drop tables before creating/loading the data (default is to not drop)
 
  -h  hostname     Database host (default: \$PGHOST if defined, else 'localhost')
  -u  username     Database username (default: \$PGUSER if defined else your username)
  -d  dbname       Database name (default: \$PGDATABASE if defined else 'tiger')
  -p  dbport       Database port (default: \$PGPORT if defined else 5432)
 
  -B  directory    Specify base directory of Tiger Files (default: ./TIGER2008)

  -r  SRID         If given, and is different from the default SRID (see: -R), 
                   then reproject to this SRID before import. (requires ogr2ogr be installed)

  -i               Ignore files matching *00.shp,i.e. from 2000. (default false)
  Uncommon options:

  -b  file_prefix  String that matches the beginning of individual tiger files (default: tl_2008)
  -R  SRID         SRID of datafiles. There is probably no reason to change this 
                   (default: 4269 aka NAD83)
  -E  encoding     Character encoding of files (default: LATIN1)
  -v               Enable some extra verbosity. (implies no -q)
EOT
  exit 1;
}

while getopts  "n:s:c:H:u:d:DB:b:E:S:hvXr:R:qp:iM" flag; do
  case "$flag" in
    n)  NATIONAL="true"; NATLAYERS="$OPTARG";;
    s)  STATELVL="true"; STATES="$OPTARG";;
    c)  COUNTYLVL="true"; COUNTIES="$OPTARG";;
    H)  HOST="$OPTARG";;
    u)  DBUSER="$OPTARG";;
    d)  DB="$OPTARG";;
    D)  DROP="true";;
    p)  DBPORT="$OPTARGS";;
    X)  DROP_SCHEMA="true";;
    B)  BASE="$OPTARG";;
    b)  SETBASE="$OPTARG";;
    E)  ENCODING="$OPTARG";;
    S)  SCHEMA_PREFIX="$OPTARG";;
    h)  usage ;;
    r)  SRID="$OPTARG";;
    R)  DEFAULT_SRID="$OPTARG";;
    v)  DEBUG="true";;
    q)  QUIET="true";;
    i)  SKIP00="true";;
    M)  DO_MERGE="true";;
    [?]) usage ;;
  esac
done
if [ "${DO_MERGE}" = 'true' ]; then
  NATIONAL='false'
  STATELVL='false'
  COUNTYLVL='false'
fi
#
#
# do some initial setup
#
#
if [ "${NATLAYERS}" = "all" ]; then
  NATLAYERS='*'
fi

if [ "${DROP_SCHEMA}" = 'true' ]; then
  # handled by cascading schema drop
  DROP='false'
fi

if [ -z "${STATES}" -o "${STATES}" = 'all' ]; then
  STATES="[0-9][0-9]"
fi

if [ "${STATES}" = 'none' ]; then
  STATELVL='false'
fi

if [ "${COUNTIES}" = 'none' ]; then
  COUNTYLVL='false'
fi

# how do we call psql
PSQL_CMD_ARGS="-U ${DBUSER} -d ${DB} -h ${HOST} -p ${DBPORT} -q -1 --set VERBOSITY=terse"
if [ "${DEBUG}" = 'true' ]; then
  PSQL_CMD_EXTRAS='-e'
else
  PSQL_CMD_EXTRAS=''
fi
PSQL_CMD="${PSQL} ${PSQL_CMD_EXTRAS} ${PSQL_CMD_ARGS}"
PSQL_CMD_NULL="${PSQL} -o /dev/null ${PSQL_CMD_EXTRAS} ${PSQL_CMD_ARGS}|| (error 'psql failed';exit 1)"
# Handle case where we were given a 5-digit county
echo $COUNTIES | grep -qE '[0-9]{5}'
if [ $? -eq 0 ]; then
  STATES=`echo $COUNTIES | cut -c1,2`
  COUNTIES=`echo $COUNTIES | cut -c3,4,5`
fi

if [ ! "${COUNTIES}" = '*' -a \( "${STATES}" = '[0-9][0-9]' -o \! "${STATELVL}" = 'true' \) ]; then
  error "You must specify a state if you want to specify a county"
  usage
fi

#
#
# Now start your imports
#
#
if [ "$NATIONAL" = "true" ]; then
  note "National level..."
  # Create the national schema
  SCHEMA="${SCHEMA_PREFIX}_us"
  create_schema ${SCHEMA}
  # Loop through the national files, they're in the base directory with a
  # file glob of $SETBASE_us_*.zip
  FILEBASE="${SETBASE}_us"
  unzip_files_matching "${BASE}/${FILEBASE}_${NATLAYERS}"
  for file in ${TMPDIR}/*.shp; do
    table_from_filename $file
    loadshp "$file" "$TBL"
  done
fi

# Loop through the state directories
if [ "$STATELVL" = "true"  -o "${COUNTLVL}" = 'true' ]; then
  note ""
  note "State level..."
  DIRS="${BASE}/${STATES}_*"
  if [ -z "${DIRS}" ]; then
    error "$BASE/${STATES}_\* did not match anything!"
  fi
  for statedir in ${DIRS}; do
    STATE=`basename $statedir | cut -f1 -d_`
    SCHEMA="${SCHEMA_PREFIX}_${STATE}"
    FILEBASE="${SETBASE}_${STATE}"

    note "Processing state-level for $STATE..."

    create_schema $SCHEMA
    if [ "$STATELVL" = "true" ]; then
      unzip_files_matching "$statedir/${FILEBASE}" 
      for file in $TMPDIR/${FILEBASE}_*.shp; do
        table_from_filename "$file"
        loadshp "$file" "$TBL"
      done
    fi
    if [ "$COUNTYLVL" = "true" ]; then
      note ""
      note "Processing county-level for $STATE..."
      CODIRS="$statedir/${STATE}${COUNTIES}_*"
      if [ -z ${CODIRS} ]; then
        error "$statedir/${STATE}${COUNTIES}_\* did not match anything!"
      fi
      for codir in ${CODIRS}; do
        COUNTY=`basename $codir | cut -c3- | cut -f1 -d_`
        FILEBASE="${SETBASE}_${STATE}${COUNTY}"
        unzip_files_matching "$codir/${FILEBASE}"
        for shpfile in $TMPDIR/${FILEBASE}_*.shp; do
          table_from_filename "$shpfile"
          loadshp "$shpfile" "$TBL"
        done
        # If there is no .shp file, then look for just a .dbf file to load.
        for dbffile in ${TMPDIR}/${FILEBASE}_*.dbf; do
          DIR=`dirname $dbffile`
          SHP=`basename $dbffile .dbf`.shp
          if [ ! -e "$DIR/$SHP" ]; then
            table_from_filename "$dbffile"
            loaddbf "$dbffile" "$TBL"
          else
            note "Skipping dbffile ${dbffile} because it is part of a shapefile"
          fi
        done
      done
    fi
  done
fi

# Remove temp dir
rm -rf ${TMPDIR}

if [ "${DO_MERGE}" = 'true' ]; then
  MYSCHEMAS=`${PSQL_CMD} -t -c '\\dn' | egrep "^ +${SCHEMA_PREFIX}" | sed -e 's/|.*//'`
  TABLES=`(for schema in $MYSCHEMAS; do
    ${PSQL_CMD} -t -c "\\dt ${schema}."
  done) | cut -d\| -f 2 | sort -u`
  for table in $TABLES; do
    VIEW=''
    for schema in ${MYSCHEMAS}; do
      ${PSQL_CMD} -t -c "\dt ${schema}.${table}" | egrep -q "${schema}.*${table}"
      if [ $? -eq 0 ]; then
        # it's OK if we hit this a bunch, right?
        COLS=`${PSQL_CMD} -c "\\copy (select * from ${table} limit 1) TO STDOUT CSV HEADER" | head -1 | sed -e 's/^gid,//' -e 's/,/","/g'`
        COLS="\"$COLS\""
        VIEW="${VIEW} SELECT ${COLS} from $schema.$table UNION ALL "
        cat<<EOT | ${PSQL_CMD_NULL}
          DROP TABLE IF EXISTS ${table} cascade;
          CREATE TABLE ${table} (like $schema.$table including indexes including constraints);
EOT
      fi
    done
    VIEW=`echo $VIEW| sed -e 's/UNION ALL *$/;/'`
    cat<<EOT | ${PSQL_CMD_NULL}
    drop sequence if exists ${table}_gid_seq; create sequence ${table}_gid_seq;
    alter table ${table} drop constraint ${table}_statefp_check;
    alter table ${table} alter column gid set default nextval('${table}_gid_seq'::regclass);
    insert into ${table} (${COLS}) ${VIEW};
EOT
      TYPE=`${PSQL_CMD} -t -c "select type from geometry_columns where f_table_name='${table}' limit 1" | egrep '(POLY|LINE)'| sed 's/ //g'`

      if [ -z "${TYPE}" ]; then
        continue
      else
        echo ${TYPE} > /dev/null
        cat<<EOT | ${PSQL_CMD_NULL}
        --create index ${table}_the_geom_idx on public.${table} using gist(the_geom gist_geometry_ops);
        delete from geometry_columns where f_table_name='${table}' and f_table_schema='public';
        insert into geometry_columns values ('','public','${table}','the_geom',2,${SRID},'${TYPE}');
EOT
      fi  
  done
fi

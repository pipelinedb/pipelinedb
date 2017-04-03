#!/bin/bash

if [ "$1" = "pipelinedb" ]; then
    mkdir -p "${PIPELINEDB_DATA}"
    chmod 0700 "${PIPELINEDB_DATA}"
    chown -R pipeline "${PIPELINEDB_DATA}"

    if [ -z "$(ls -A ${PIPELINEDB_DATA})" ]; then
        gosu pipeline pipeline-init --encoding=UTF8 -D ${PIPELINEDB_DATA}

        if [ "$PIPELINEDB_PASSWORD" ]; then
            PASS="PASSWORD '${PIPELINEDB_PASSWORD}'"
            AUTH_METHOD=md5
        else
            cat >&2 <<-EOWARN
                ****************************************************
                WARNING: No password has been set for the database.
                         This will allow anyone with access to the
                         PipelineDB port to access your database.
                         In Docker's default configuration, this is
                         effectively any other container on the same
                         system.
                         Use "-e PIPELINEDB_PASSWORD=password" to set
                         it in "docker run".
                ****************************************************
EOWARN
            PASS=
            AUTH_METHOD=trust
        fi

        { echo; echo "host all all 0.0.0.0/0 ${AUTH_METHOD}"; } \
            | gosu pipeline tee -a "${PIPELINEDB_DATA}/pg_hba.conf" \
            > /dev/null

        gosu pipeline pipeline-ctl \
            -D "${PIPELINEDB_DATA}" \
            -o "-c listen_addresses='localhost'" \
            -w start

        : ${PIPELINEDB_USER:="pipeline"}
        : ${PIPELINEDB_DB:=$PIPELINEDB_USER}

        pipeline=( pipeline -v ON_ERROR_STOP=1 )

        if [ "${PIPELINEDB_DB}" != "pipeline" ]; then
            "${pipeline[@]}" --username pipeline <<-EOSQL
                CREATE DATABASE "${PIPELINEDB_DB}" ;
EOSQL
            echo
        fi

        if [ "${PIPELINEDB_USER}" == "pipeline" ]; then
            OPERATION="ALTER"
        else
            OPERATION="CREATE"
        fi

        pipeline --username pipeline <<-EOSQL
            ${OPERATION} USER "${PIPELINEDB_USER}" WITH SUPERUSER ${PASS};
EOSQL
        echo

        pipeline+=( --username "${PIPELINEDB_USER}" --dbname "${PIPELINEDB_DB}" )

        if [ -f /docker-entrypoint-init.d/pipelinedb.conf ]; then
            echo "$0: installing pipelinedb.conf"
            cp /docker-entrypoint-init.d/pipelinedb.conf "${PIPELINEDB_DATA}"
        fi

        for f in /docker-entrypoint-init.d/*; do
            case "${f}" in
                pipelinedb.conf)    ;;
                *.sh)               echo "$0: running ${f}"; . "${f}" ;;
                *.sql)              echo "$0: running ${f}"; "${pipeline[@]}" < "${f}"; echo ;;
                *.sql.gz)           echo "$0: running ${f}"; gunzip -c "${f}" | "${pipeline[@]}"; echo ;;
                *)                  echo "$0: ignoring ${f}" ;;
            esac
        done

        gosu pipeline pipeline-ctl -D ${PIPELINEDB_DATA} -m fast -w stop

        sed -i -e "s/^\(#listen_addresses.*\)$/listen_addresses = '*'\n\1/g" "${PIPELINEDB_DATA}/pipelinedb.conf"

        chmod 640       "${PIPELINEDB_DATA}/pipelinedb.conf"
        chown pipeline  "${PIPELINEDB_DATA}/pipelinedb.conf"

        echo
        echo 'PipelineDB init process complete; ready for start up.'
        echo
    fi
    exec gosu pipeline pipelinedb -D ${PIPELINEDB_DATA}
fi
exec "$@"

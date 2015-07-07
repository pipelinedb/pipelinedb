#!/bin/sh
#
#
# PostGIS Bootstrapping Script
#
giveup()
{
        echo
        echo "  Something went wrong, giving up!"
        echo
        exit 1
}

OSTYPE=`uname -s`

#
# Solaris has GNU versions of utils with a g prefix
#
for grep in ggrep grep; do
    GREP=`which $grep 2>/dev/null`
    if test -x "${GREP}"; then
        break;
    fi
done

for sed in gsed sed; do
    SED=`which $sed 2>/dev/null`
    if test -x "${SED}"; then
        break;
    fi
done

#
# Find and run Autoconf
#
AUTOCONF=`which autoconf 2>/dev/null`
if [ ! ${AUTOCONF} ]; then
    echo "Missing autoconf!"
    exit
fi
AUTOCONF_VER=`${AUTOCONF} --version | ${GREP} -E "^.*[0-9]$" | ${SED} 's/^.* //'`

for aclocal in aclocal aclocal-1.10 aclocal-1.9; do
    ACLOCAL=`which $aclocal 2>/dev/null`
    if test -x "${ACLOCAL}"; then
        break;
    fi
done
if [ ! ${ACLOCAL} ]; then
    echo "Missing aclocal!"
    exit
fi
ACLOCAL_VER=`${ACLOCAL} --version | ${GREP} -E "^.*[0-9]$" | ${SED} 's/^.* //'`

for libtoolize in glibtoolize libtoolize; do
    LIBTOOLIZE=`which $libtoolize 2>/dev/null`
    if test -x "${LIBTOOLIZE}"; then
        break;
    fi
done
if [ ! ${LIBTOOLIZE} ]; then
    echo "Missing libtoolize!"
    exit
fi
LIBTOOLIZE_VER=`${LIBTOOLIZE} --version | ${GREP} -E "^.*[0-9]\.[0-9]" | ${SED} 's/^.* //'`
LIBTOOLIZE_MAJOR_VER=`echo ${LIBTOOLIZE_VER} | cut -f1 -d'.'`

# TODO: Check libtool version and add --install option only for 1.9b+
LTOPTS="--force --copy"
if test ${LIBTOOLIZE_MAJOR_VER} -ge 2; then
    LTOPTS="${LTOPTS} --install"
fi

echo "* Running ${LIBTOOLIZE} (${LIBTOOLIZE_VER})"
echo "   OPTIONS = ${LTOPTS}"
${LIBTOOLIZE} ${LTOPTS} || giveup

echo "* Running $ACLOCAL (${ACLOCAL_VER})"
${ACLOCAL} -I macros || giveup

echo "* Running ${AUTOCONF} (${AUTOCONF_VER})"
${AUTOCONF} || giveup

if test -f "${PWD}/configure"; then
    echo "======================================"
    echo "Now you are ready to run './configure'"
    echo "======================================"
else
    echo "  Failed to generate ./configure script!"
    giveup
fi

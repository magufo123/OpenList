#!/bin/sh

umask ${UMASK}

if [ "$1" = "version" ]; then
  ./openlist version
else

  chown -R ${PUID}:${PGID} /opt/openlist/
  exec su-exec ${PUID}:${PGID} runsvdir /opt/service
fi
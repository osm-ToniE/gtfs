#!bin/bash

# prepare environment for being called by 'cron': if not called in tty read ~/.ptna-config

tty -s

tty_ret=$?

if [ $tty_ret -ne 0 ]
then
    echo $(date "+%Y-%m-%d %H:%M:%S %Z") "gtfs-cron-update.sh: start"

    if [ -f ~/.ptna-config ]
    then
        source ~/.ptna-config

        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "$(top -bn1 | grep -i '^.CPU')"
        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "$(df | grep 'osm')"
        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "gtfs-all-feeds.sh -cDT"

        gtfs-all-feeds.sh -cDT | tee "$PTNA_WORK_LOC/gtfs-all-feeds-cDT.log"

        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "gtfs-update-from-file.sh"

        gtfs-update-from-file.sh -l -f "$PTNA_WORK_LOC/gtfs-all-feeds-cDT.log"

        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "$(top -bn1 | grep -i '^.CPU')"
        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "$(df | grep 'osm')"
        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "gtfs-cron-update.sh: done"
    else
        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "~/.ptna-config not found, terminating"
        exit 1
    fi
else
    echo $(date "+%Y-%m-%d %H:%M:%S %Z") "Called from a 'tty', terminating"
    exit 2
fi

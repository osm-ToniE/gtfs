#!bin/bash

# prepare environment for being called by 'cron': if not called in tty read ~/.ptna-config

tty -s

tty_ret=$?

if [ $tty_ret -ne 0 ]
then
    if [ -f ~/.ptna-config ]
    then
        source ~/.ptna-config
    else
        echo $(date "+%Y-%m-%d %H:%M:%S %Z") "~/.ptna-config not found, terminating"
        exit 1
    fi
fi

echo "#####################################"
echo ""
echo $(date "+%Y-%m-%d %H:%M:%S %Z") "gtfs-all-feeds.sh -cDT"
echo ""
echo "#####################################"

gtfs-all-feeds.sh -cDT | tee ~/tmp/gtfs-all-feeds.log

echo "#####################################"
echo ""
echo $(date "+%Y-%m-%d %H:%M:%S %Z") "gtfs-update-from-file.sh"
echo ""
echo "#####################################"

gtfs-update-from-file.sh

echo "#####################################"
echo ""
echo $(date "+%Y-%m-%d %H:%M:%S %Z") "gtfs-cron-update.sh: done"
echo ""
echo "#####################################"

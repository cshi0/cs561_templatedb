#!sh
hexdump -n8 -e '2/4 "%d " "\n"' $1
hexdump -s 8 -e "1/1 \"%d \" $2/4 \"%d \" \"\n\"" $1
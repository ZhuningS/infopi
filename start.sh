sleep 2
rm -rf /tmp/info_pi/*.*
/usr/bin/python3 /home/pi/infopi/src/starter.py --tmpfs /tmp/info_pi --port 5000

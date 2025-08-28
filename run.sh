docker run -it --rm -v /root/script:/script \
-v /etc/ceph:/etc/ceph \
-v /container/omapdata:/script/omapdata \
--network host \
mypython python3 /script/$1 "${@:2}"
#!/usr/bin/perl -w

#use strict;
use Socket;
#
sub wake {
   my ($mac, $ip, $port) = @_;

        my ($raddr, $them, $proto, $pkt);

        foreach (split /:/, $mac) {
           $pkt .= chr(hex($_));
        }

        $pkt = chr(0xFF) x 6 . $pkt x 16;
        # Allocate socket and send packet
        $raddr = gethostbyname($ip);
        $them = pack_sockaddr_in($port, $raddr);
        $proto = getprotobyname('udp');

        socket(S, AF_INET, SOCK_DGRAM, $proto) or die "socket : $!";
        setsockopt(S, SOL_SOCKET, SO_BROADCAST, 1) or die "setsockopt : $!";

        print "Sending magic packet to $ip:$port with $mac\n";

        send(S, $pkt, 0, $them) or die "send : $!";
        close S;
}

# 00:0c:6e:fa:72:d2
#wake('00:0C:6E:FA:72:D2', '192.168.1.255', 109);
#wake('00:0C:6E:FA:72:D2', 'nbs.zapto.org', 109);
wake('00:0C:6E:FA:72:D2', '192.168.0.156', 109);
#wake('c8:bc:c8:e0:f7:79', 'nbs.zapto.org', 109);
#wake('c8:bc:c8:a3:43:37', 'nbs.zapto.org', 109);
#wake('00:0C:6E:FA:72:D2', '192.168.1.109', 12345);


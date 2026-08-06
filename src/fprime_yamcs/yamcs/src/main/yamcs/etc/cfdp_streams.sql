create stream cfdp_in (gentime TIMESTAMP, seqNum INT, rectime TIMESTAMP, pdu BINARY)
create stream cfdp_out (gentime TIMESTAMP, seqNum INT, pdu BINARY)

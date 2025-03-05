scp /opt/script/*.csv 10.150.4.48:/opt/neo4j/import/
scp datalake/neo4j-cmd.sh 10.150.4.48:/tmp
ssh 10.150.4.48 "chmod 777 /tmp/neo4j-cmd.sh && sh /tmp/neo4j-cmd.sh"
#!/usr/bin/env python3
"""
Sets up Hive and Hadoop configurations inside WSL environment.
"""
import os
import shutil

HIVE_CONF = "/opt/hive/conf/hive-site.xml"
HIVE_SITE_CONTENT = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:derby:;databaseName=/var/crisisguard/hive/metastore_db;create=true</value>
        <description>JDBC connect string for a JDBC metastore</description>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>org.apache.derby.jdbc.EmbeddedDriver</value>
        <description>Driver class name for a JDBC metastore</description>
    </property>
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>/crisisguard/warehouse</value>
        <description>location of default database for the warehouse</description>
    </property>
    <property>
        <name>hive.metastore.schema.verification</name>
        <value>false</value>
    </property>
    <property>
        <name>datanucleus.schema.autoCreateAll</name>
        <value>true</value>
    </property>
</configuration>
"""

if __name__ == "__main__":
    if os.path.exists("/opt/hive/conf"):
        with open(HIVE_CONF, "w") as f:
            f.write(HIVE_SITE_CONTENT)
        print(f"Successfully configured {HIVE_CONF}")
    else:
        print("Hive conf directory not found.")

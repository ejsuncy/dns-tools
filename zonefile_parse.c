#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <my_global.h>
#include <mysql.h>
#include <sys/stat.h>
#include "zonefile_parse.h"


#define MAX_TLD_LENGTH	12
#define MAX_DOMAIN_LENGTH 64
#define MAX_NAMESERVER_LENGTH 128
#define BUFFER_LENGTH	512
#define QUERY_LENGTH	12800
int parseZoneFileContents(char* filename, char* date, MYSQL* dbcon);

int main(int argc, char* argv[]) {
	if (argc < 3) {
		printf("Usage: zonefile_parse filename date\n");
		return 1;
	}
	//parseZoneFile("/data/untarplane/2013-11-13/com.zone", "2013-11-13");
	parseZoneFile(argv[1], argv[2]);
        return 0;
}

int parseZoneFile(char* filename, char* date) {
    MYSQL* dbcon;
    dbcon = mysql_init(NULL);

    if (dbcon == NULL) {
            fprintf(stderr, "Error %u: %s\n", mysql_errno(dbcon), mysql_error(dbcon));
            exit(1);
    }
    if (mysql_real_connect(dbcon, DBHOST, DBUSER, DBPASS, DBSCHEMA, 0, NULL, 0) == NULL) {
            fprintf(stderr, "Error %u: %s\n", mysql_errno(dbcon), mysql_error(dbcon));
            exit(1);
    }

    char create_query[QUERY_LENGTH];

    // sprintf(create_query, "CREATE TABLE IF NOT EXISTS `%s` ( \
    //                 `Domain` varchar(63) NOT NULL, \
    //                 `TLD` varchar(4) NOT NULL, \
    //                 `Nameserver` varchar(63), \
    //                 `Date_Retrieved` datetime, \
    //                 PRIMARY KEY (`Domain`, `TLD`, `Nameserver`, `Date_Retrieved`) \
    // ) ENGINE=MyISAM DEFAULT CHARSET=utf8;", DB_ZONES_TABLE);

    mysql_query(dbcon, create_query);

    sprintf(create_query, "CREATE TABLE IF NOT EXISTS `%s` ( \
                    `Domain` varchar(63) NOT NULL, \
                    `TLD` varchar(4) NOT NULL, \
					`Date_Last_Seen` datetime,\
                    `Registrar` varchar(32), \
                    `DNSSEC_Enabled` int(1), \
                    `Date_Retrieved` datetime, \
                    PRIMARY KEY (`Domain`, `TLD`) \
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;", DB_DOMAINS_TABLE);

    mysql_query(dbcon, create_query);
	
    parseZoneFileContents(filename, date, dbcon);
    mysql_close(dbcon);

    return 0;
}

int parseZoneFileContents(char* filename, char* date, MYSQL* dbcon) {
        FILE* curFile;
	struct stat s;
	char* ret;
	char buffer[BUFFER_LENGTH];
	char tld[MAX_TLD_LENGTH];
	char displayTLD[MAX_TLD_LENGTH];
	char domain[MAX_DOMAIN_LENGTH];
	char previousDomain[MAX_DOMAIN_LENGTH];
	char nameserver[MAX_NAMESERVER_LENGTH];
	char domainQuery[QUERY_LENGTH];
	char nsQuery[QUERY_LENGTH];
	int tldFound = 0;
	int tldFinished = 0;
	int queryCounter = 0;
	int domainQueryLength = 0;
	int nsQueryLength = 0;

	if (strstr(filename, "com.") != NULL) {
		sprintf(tld, "COM.");
		sprintf(displayTLD, "COM");
	}
	else if (strstr(filename, "net.") != NULL) {
		sprintf(tld, "NET.");
		sprintf(displayTLD, "NET");
	}
	else {
		printf("Skipping unsupported file: %s\n", filename);
		return 0;
	}

	sprintf(previousDomain, "="); // Prime previous domain with invalid char
    
    curFile = fopen(filename, "r");
    if (curFile == NULL) {
            return 0;
    }

	if (stat(filename, &s) != 0) {
		printf("error\n");
	}

    while (!feof(curFile)) {
        ret = fgets(buffer, sizeof(buffer), curFile);
		if (!ret) {
			break;
		}
		if (!tldFound) {
			if (strncmp(buffer, tld, strlen(tld)) == 0) {
				//printf("TLD found\n");
				tldFound = 1;
			}
			continue;
		}
		if (tldFound && !tldFinished) {
			if (strncmp(buffer, tld, strlen(tld)) != 0) {
				//printf("TLD finished: %s\n", buffer);
				tldFinished = 1;
			}
			else {
				continue;
			}
		}
		if (sscanf(buffer, "%s NS %s\n", domain, nameserver) == 2) {
			
			if (queryCounter == 0) {	//setup the initial query strings
				sprintf(domainQuery, "INSERT INTO %s (Domain, TLD, Date_Last_Seen, Date_Retrieved) VALUES ('%s','%s','%s','%s')", DB_DOMAINS_TABLE, domain, displayTLD, date, date);
				// sprintf(nsQuery, "INSERT IGNORE INTO %s (Domain, TLD, Nameserver, Date_Retrieved) VALUES ('%s', '%s', '%s', '%s')", DB_ZONES_TABLE, domain, displayTLD, nameserver, date);

				domainQueryLength = strlen(domainQuery);
				// nsQueryLength = strlen(nsQuery);
			}
			else if (queryCounter > 0 && queryCounter % 100 == 0) {	//every multiple of 100, send off the query
				//save domain name info
				
				sprintf(domainQuery+domainQueryLength, " ON DUPLICATE KEY UPDATE `Date_Last_Seen` = '%s'", date);

				if (mysql_query(dbcon, domainQuery)){
					printf("Error with query: %s\n", domainQuery);
					printf("MySQL Error: %s\n", mysql_error(dbcon));
				}

				// //save nameserver info
				// if (mysql_query(dbcon, nsQuery)){
				// 	printf("Error with query: %s\n", nsQuery);
				// 	printf("MySQL Error: %s\n", mysql_error(dbcon));
				// }

				//reset the domain and nameserver query strings
				sprintf(domainQuery, "INSERT INTO %s (Domain, TLD, Date_Last_Seen, Date_Retrieved) VALUES ('%s', '%s', '%s', '%s')", DB_DOMAINS_TABLE, domain, displayTLD, date, date);
				domainQueryLength = strlen(domainQuery);

				// sprintf(nsQuery, "INSERT IGNORE INTO %s (Domain, TLD, Nameserver, Date_Retrieved) VALUES ('%s', '%s', '%s', '%s')", DB_ZONES_TABLE, domain, displayTLD, nameserver, date);
				// nsQueryLength = strlen(nsQuery);
			}
			else {	//append to existing query string
				sprintf(domainQuery+domainQueryLength, ",('%s', '%s', '%s', '%s')",domain, displayTLD, date, date);
				domainQueryLength += strlen(domainQuery+domainQueryLength);

				// sprintf(nsQuery+nsQueryLength, ",('%s', '%s', '%s', '%s')", domain, displayTLD, nameserver, date);
				// nsQueryLength += strlen(nsQuery+nsQueryLength);
			}
			queryCounter++;
		}
		else {
			//send off remaining queries (not multiple of 100)
			sprintf(domainQuery+domainQueryLength, " ON DUPLICATE KEY UPDATE Date_Last_Seen = '%s'", date);

			if (mysql_query(dbcon, domainQuery)){
				printf("Error with query: %s", domainQuery);
				printf("MySQL Error: %s\n", mysql_error(dbcon));
			}

			// if (mysql_query(dbcon, nsQuery)){
			// 	printf("Error with query: %s", nsQuery);
			// 	printf("MySQL Error: %s\n", mysql_error(dbcon));
			// }

			printf("%s Zone File Parsed\n", date);
			break;
		}
	}
        
	sprintf(domainQuery, "REPLACE INTO `%s` values ('%s','%s',1) ", DB_DATES_TABLE, date, displayTLD);
	
	if(mysql_query(dbcon, domainQuery)){
		printf("Error with query: %s", domainQuery);
		printf("MySQL Error: %s\n", mysql_error(dbcon));
	}

    fclose(curFile);
	return 0;
}


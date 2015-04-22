#ifndef _ZONEFILE_PARSE_H
#define _ZONEFILE_PARSE_H
#include <my_global.h>

#define DBHOST          "localhost"
#define DBUSER          "dbuser"
#define DBPASS          "dbpassword"
#define DBSCHEMA        "dbname"
#define DB_DATES_TABLE	"Dates"
#define DB_ZONES_TABLE	"Zones"
#define DB_DOMAINS_TABLE "Domains"

int parseZoneFile(char* filename, char* date);

#endif

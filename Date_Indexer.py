import argparse
import traceback
import sys
from sqlalchemy import *
from sqlalchemy.ext.declarative import *
from sqlalchemy.orm import *
from dbConfig import *
import os
import datetime
Base = declarative_base()

from dbConfig import *

class Date_Indexer:
    def __init__(self):
        self.options = self.parse_arguments()
        self.setup_db()
        self.index_dates()
        self.write_index_file()

    def parse_arguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-d", "--directory", type=str, action="store", help="directory for storing registration data", default=".")
        parser.add_argument("-v", "--verbose", action="store_true", help="verbose output")
        return parser.parse_args()

    def index_dates(self):
        
        dates = sorted(next(os.walk(self.options.directory))[1])
        
        for d in dates:
            d = datetime.datetime.strptime(d, "%Y-%m-%d")
            Date.insert(self.session, d, self.options)
        
        self.session.commit()

        

    def write_index_file(self):
        
        date_results = Date.get_unparsed_dates(self.session)

        with open("index_file.txt", "w") as index_file: 
            for d in date_results:
                index_file.write(d.Date.date().strftime("%Y-%-m-%-d") + '\n')
            
       
    def setup_db(self):
        if self.options.verbose:
            sys.stdout.write("-Setting up Database...")
        try:
            self.db = create_engine('mysql://%s:%s@%s' % (DB_USER,DB_PASSWORD,DB_SERVER))
            self.db.execute('USE %s' % (DB_NAME))
            Base.metadata.create_all(self.db)
            self.session = sessionmaker(bind=self.db)()

            if self.options.verbose:
                print "Success!"
        except:
            if self.options.verbose:
                print "FAILED!"
            print traceback.format_exc()
            sys.exit()

class Date(Base):
    __tablename__ = DB_DATES_TABLE
    Date = Column("Date", DateTime, index=True, nullable=False, primary_key=True)
    TLD = Column("TLD", String(4), nullable=False, primary_key=True, default="COM")
    Parsed = Column("Parsed", Boolean, nullable=False, default=False)

    @staticmethod
    def insert(session, date, options):
        if not session.query(Date).filter_by(Date=date).count(): #date is not in db
            d = Date(Date=date, Parsed=False)
            
            if options.verbose:
                print "--adding date to db: %s" % (date)

            session.add(d)

        else:
            if options.verbose:
                print "%s is already in the database" % (date)

    @staticmethod
    def get_unparsed_dates(session):
        return session.query(Date).filter_by(Parsed=False)

if __name__ == '__main__':
    indexer = Date_Indexer()

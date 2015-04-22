# script
import argparse
import traceback
import sys
from dateutil import rrule

# sql queries
from sqlalchemy import *
from sqlalchemy.types import DateTime
from sqlalchemy.ext.declarative import *
from sqlalchemy.orm import *
Base = declarative_base()

# db config
from dbConfig import *

#plotting
import numpy
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

'''Database Schema'''
class Date(Base):
    __tablename__ = DB_DATES_TABLE
    Date = Column("Date", String(10), index=True, nullable=False, primary_key=True)
    TLD = Column("TLD", String(4), nullable=False, primary_key=True, default="COM")
    Parsed = Column("Parsed", Boolean, nullable=False, default=False)

    @staticmethod
    def get_unparsed_dates(session):
        return session.query(Date).filter_by(Parsed=False)

    @staticmethod
    def get_parsed_dates(session):
        return [d.Date for d in session.query(Date).filter_by(Parsed=True, TLD="COM").all()]

class Zone(Base):
    __tablename__ = DB_ZONES_TABLE
    Domain = Column("Domain", String(63), nullable=False, primary_key = True)
    TLD = Column("TLD", String(4), nullable=False, primary_key=True)
    Nameserver = Column("Nameserver", String(63), primary_key=True)
    Date_Retrieved = Column("Date_Retrieved", DateTime, primary_key=True)

class Domain(Base):
    __tablename__ = DB_DOMAINS_TABLE
    Domain = Column("Domain", String(63), nullable=False, primary_key=True)
    TLD = Column("TLD", String(4), nullable=False, primary_key=True)
    Date_Last_Seen = Column("Date_Last_Seen", DateTime)
    Registrar = Column("Registrar", String(32))
    DNSSEC_Enabled = Column("DNSSEC_Enabled", Boolean)
    Date_Retrieved = Column("Date_Retrieved", DateTime)

    @staticmethod
    def get_added_counts(session, tld=None):
        if tld:
            return session.query(Domain.Date_Retrieved, func.count(Domain.Domain)).filter(Domain.TLD == tld).group_by(Domain.Date_Retrieved).all()
        else:
            return session.query(Domain.Date_Retrieved, func.count(Domain.Domain)).group_by(Domain.Date_Retrieved).all()

    @staticmethod
    def get_removed_counts(session, tld=None):
        if tld:
            return session.query(Domain.Date_Last_Seen, func.count(Domain.Domain)).filter(Domain.TLD == tld).group_by(Domain.Date_Last_Seen).all()
        else:
            return session.query(Domain.Date_Last_Seen, func.count(Domain.Domain)).group_by(Domain.Date_Last_Seen).all()

    @staticmethod
    def get_count_for_date(session, d, tld=None):
        if tld:
            return session.query(func.count(Domain.Domain)).filter(Domain.TLD == tld, Domain.Date_Retrieved < d, Domain.Date_Last_Seen > d).all()
        else:
            return session.query(func.count(Domain.Domain)).filter(Domain.Date_Retrieved < d, Domain.Date_Last_Seen > d).all()


    @staticmethod
    def get_retrieval_range(session):
        return session.query(func.min(Domain.Date_Retrieved), func.max(Domain.Date_Retrieved)).all()[0]

'''Study'''
class Study:
    def __init__(self):
        #parse arguments
        self.args = self.parse_arguments()
        self.setup_db()
        self.run_queries('COM')
        self.run_queries('NET')

    def parse_arguments(self):
        parser = argparse.ArgumentParser(prog="dns_study", description="A script to query the DNS Registration database", 
            add_help=True)
        return parser.parse_args()


    def setup_db(self):
        try:
            self.db = create_engine('mysql://%s:%s@%s' % (DB_USER,DB_PASSWORD,DB_SERVER))
            self.db.execute('USE %s' % (DB_NAME))
            Base.metadata.create_all(self.db)
            self.session = sessionmaker(bind=self.db)()
        except:
            print traceback.format_exc()
            sys.exit()

    
    def run_queries(self, tld=None):
        '''Run the following queries on the DNS registration database.'''
        

        '''Plot a bar graph showing number of domains added and removed each day'''
        added = {date:count for (date,count) in Domain.get_added_counts(self.session, tld)}
        removed = {date:-count for (date,count) in Domain.get_removed_counts(self.session, tld)}

        self.plot_added_removed(added, removed, tld=tld)
        

        '''Plot a bar graph showing the net gain of domains each day'''
        net = {}

        for date in added:
            net[date] = added[date]

        for date in removed:
            if date in net:
                net[date] = net[date] + removed[date]
            else:
                net[date] = removed[date]
                
        self.plot_net_gain(sorted(net), [net[x] for x in sorted(net)], title="Net Gain of %s Domains Over Time" % (tld), 
                            xlabel="Date", ylabel="Number of Domains (Net Gain)", file="%s_net_gain.png"%(tld), log=True, tld=tld)

        
        '''Plot the quantity of domains each day'''
        (minimum, maximum) = Domain.get_retrieval_range(self.session)

        data = {}

        for date in list(rrule.rrule(rrule.DAILY,dtstart=minimum, until=maximum)):
            count = Domain.get_count_for_date(self.session, date, tld)[0][0]
            
            if date in net:
                data[date] = count + net[date]
            else:
                data[date] = count

        
        self.plot_line(sorted(data), [data[x] for x in sorted(data)], title="Quantity of %s \nDomain Registration Names"%(tld),
                         xlabel="Time", ylabel="Number of Domain Names", file='%s_quantity.png'%(tld))


    def plot_line(self, x_vals, y_vals, title, xlabel, ylabel, file, log=False, scatter=False):
        '''A general plotting function to create graphic plots.'''
        plt.clf()
        
        fig, ax = plt.subplots()
        
        if scatter:
            ax.scatter(x_vals, y_vals, s=1)
        else:
            ax.plot(x_vals, y_vals)
        
        if log:
            plt.yscale('symlog')
        
       
        fig.autofmt_xdate()

    	plt.title(title)
    	plt.xlabel(xlabel)
    	plt.ylabel(ylabel)
        plt.tight_layout()
        plt.savefig(file)

    def plot_added_removed(self, domains_added, domains_removed, tld):
        '''A plotting function to create bar graphs.'''
        plt.clf()
        
        fig, ax = plt.subplots()

        added = ax.bar(sorted(domains_added), [domains_added[x] for x in sorted(domains_added)], color='green', linewidth=0)
        removed = ax.bar(sorted(domains_removed), [domains_removed[x] for x in sorted(domains_removed)],color='red', linewidth=0)
        plt.yscale('symlog')

        ax.legend((added[0], removed[0]), ('Domains Added','Domains Removed'))

        fig.autofmt_xdate()
        plt.title('%s Domains Added and Removed Over Time' % (tld))
        plt.xlabel('Date')
        plt.ylabel('Number of Domains Added or Removed')
        plt.tight_layout()
        plt.savefig('%s_bar_graph.png' % (tld))
        
    def plot_net_gain(self, x_vals, y_vals, title, xlabel, ylabel, file, log, tld):
        '''A plotting function to create bar graphs.'''
        plt.clf()
        
        fig, ax = plt.subplots()

        colors = []

        for count in y_vals:
            if count >= 0:
                colors.append('green')
            else:
                colors.append('red')

        ax.bar(x_vals, y_vals, color=colors, linewidth=0)
        plt.yscale('symlog')

        fig.autofmt_xdate()
        plt.title('Net Gain of %s Domains Over Time' % (tld))
        plt.xlabel('Date')
        plt.ylabel('Net Gain')
        plt.tight_layout()
        plt.savefig('%s_net_gain.png' % (tld))

if __name__ == '__main__':
    s = Study()

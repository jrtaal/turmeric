#!/usr/bin/python

import os
import sys
import getopt
import subprocess
import datetime

import logging
logger = logging.getLogger("scripts.createdb")
logging.basicConfig(stream=sys.stdout, level=10, format = "%(message)s")
root = "./"
import sqlalchemy
from sqlalchemy.engine.url import make_url, URL

import getpass

DEFAULTADMINUSER = "postgres"
import logging
logger = logging.getLogger("create_db")

import hashlib
import tempfile

__help = """
%(cmd)s <config> <command> [-W] [--adminuser=<adminusername>] [--hostname=<hostname] [--port=<port>] [--url=<url>]

config should be a paste config file. Can be suffixed with #<name> to find the right section.

Command can be:
    drop                    Drop a database
    setup                   Create a new database, with the right credentials

    backup|dump             Make a backup
    restore <signature>     Restore a backup described by signature

    info                    Show information about the current state
    show                    Show a list of available backups

    populate  [test|large]
                            Populate a database with initial information. If test or large is provided fake data is ingested, large gives a larger set
    clean                   Cleans a database.

The clean, drop, restore, populate commands automatically make a backup prior to carrying out the task to prevent errors.

    --adminuser=<adminusername>  the postgres adminuser with sufficient rights to create and drop databases
    --hostname=<hostname>        the hostname of the postgres server, if empty localhost is used
    --port=<port>                the portnumber of the postgres server
    --url=<url>                  the target database-uri (overrides the one found in the configfile) 
    -W                           asks for a password for the admin user

"""

class DBManager(object):
    def __init__(self, url, settings = dict(), adminuser = DEFAULTADMINUSER, hostname = None, port = None, password = None):
        self.settings = settings
        self.adminuser = adminuser
        dburl = url
        url = make_url(dburl)
        self.target_url = url
        if not hostname:
            hostname = url.host if url.host != "localhost" else None
        if hostname and not port:
            port = url.port

        self.admin_url = URL( drivername = "postgres",  username = self.adminuser, database="postgres",
                         password = password, host = hostname, port = port)
        self.admin_conn = None
        self.target_conn = None

        self.backup_path = os.path.join(root, "var/backup")
        logger.warn("Using destination URI %s", url)
        logger.warn("Using admin URI %s", self.admin_url)
        
    def connect_dbadmin(self):
        if self.admin_conn and not self.admin_conn.closed:
            return self.admin_conn
        logger.info("Connecting as admin user %s to %s on %s", self.admin_url.username, "postgres", self.admin_url.host)
        engine = sqlalchemy.create_engine(self.admin_url) #"postgres://%s@/postgres" % self.adminuser ) 
        conn = engine.connect()
        conn.execute("COMMIT")
        self.admin_conn = conn
        return conn

    def connect_target(self):
        if self.target_conn and not self.target_conn.closed:
            return self.target_conn
        logger.info("Connecting as user %s to %s on %s", self.target_url.username, self.target_url.database, self.target_url.host)
        engine = sqlalchemy.create_engine(self.target_url)
        conn = engine.connect()
        conn.execute("COMMIT")
        self.target_conn = conn
        return conn

    def connect(self, database = None, username = None, password = None):
        engine = sqlalchemy.create_engine(URL( drivername = "postgres",
                                               username = username or self.target_url.username,
                                               database = database or  self.target_url.database,
                                               password = password or self.target_url.password,
                                               host = self.target_url.host, port = self.target_url.port)
        )
        conn = engine.connect()
        conn.execute("COMMIT")
        return conn
        
    def create_user(self):
        conn = self.connect_dbadmin()
        logger.info("Creating database user %s with password %s", self.target_url.username, self.target_url.password)
        result = conn.execute("CREATE USER %s WITH PASSWORD '%s'" % (self.target_url.username, self.target_url.password) )
        if not result.rowcount :
            raise "Could not create user %s" % self.target_url.username
        conn.close()

    def create_db(self):
        conn = self.connect_dbadmin()
        logger.info("Creating database %s with owner %s", self.target_url.database, self.target_url.username)
        result = conn.execute("CREATE DATABASE %s WITH OWNER %s" % (self.target_url.database, self.target_url.username) )
        if not result.rowcount :
            raise "Could not create database %s" % self.target_url.database
        conn.close()

    def grant_user(self):
        conn = self.connect_dbadmin()
        logger.info("Granting privileges to user %s", self.target_url.username)
        result = conn.execute("GRANT ALL PRIVILEGES ON DATABASE %s TO %s" % (self.target_url.database, self.target_url.username))
        if not result.rowcount :
            raise "Could not grant privileges to user %s" % self.target_url.username


    def make_gis(self):
        url = URL( drivername = "postgres",  username = self.adminuser, database=self.target_url.database,
                         password = self.admin_url.password, host = self.admin_url.host, port = self.admin_url.port)
        engine = sqlalchemy.create_engine(url)
        conn = engine.connect()
        #conn = self.connect_target()
        logger.info("Creating extension postgis")
        result = conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        if not result.rowcount :
            raise "Could not create postgis extension"

        logger.info("Creating extension postgis_topology")
        result = conn.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology")
        if not result.rowcount :
            raise "Could not create postgis_topology extension"

        conn.execute("GRANT ALL ON SCHEMA topology to %s" % self.target_url.username)
        for tab in ("spatial_ref_sys", "topology","layer"):
            result = conn.execute("ALTER TABLE %s OWNER TO %s " % (tab, self.target_url.username))

    def setup_db(self):
        self.safe_create_user()
        
        self.safe_create_db()

        self.make_gis()

        self.grant_user()

    def safe_create_user(self):
        
        conn = self.connect_dbadmin()
        result = conn.execute("SELECT usename FROM pg_user WHERE usename = '%s' " % self.target_url.username)
        if not result.rowcount :
            self.create_user()

    def safe_create_db(self):
        conn = self.connect_dbadmin()
        result = conn.execute("SELECT datname FROM pg_database WHERE datname = '%s' " %  self.target_url.database)
        if not result.rowcount:
            self.create_db()


    def backup(self, message = "organic backup"):
        gitversion = subprocess.check_output(["git","describe","--long"]).split("\n")[0].strip()
        datestr = datetime.datetime.utcnow().isoformat()
        if not os.path.exists(self.backup_path):
            os.mkdir(self.backup_path)
        hostargs = []

        url = self.target_url
        if url.host:
            hostargs += ["--host=%s" % url.host]
        if url.username:
            hostargs += ["--username=%s" % url.username]
        if url.port:
            hostargs += ["--port=%d" % url.port]

        env = dict(os.environ)
        env["PGPASSWORD"]  = self.target_url.password  ## SECURITY RISK?
        backupfn = os.path.join(self.backup_path,"db__%s__%s__%s__%s.sql" % (self.target_url.database, datestr, gitversion, message ) )
        args = ["pg_dump"] + hostargs + ["--format=c",  self.target_url.database]
        logger.info("Backing up Database %s (%s)", self.target_url.database, hostargs)
        logger.info("Calling: " + " ".join(args))
        err = tempfile.NamedTemporaryFile()
        subprocess.call(args, env=env, stdout = open(backupfn,"w"), stderr = err)

    def restore(self, backup):
        backup_f = None
        options = self.find_backups(True)
        for option in options:
            if backup == option[4]:
                backup_f = backup
                break
            if backup == option[0]:
                backup_f = option[4]
                break
            if backup == self._hash_option(option) or (
                    self._hash_option(option).startswith(backup) and not all( self._hash_option(opt).startswith(backup) for opt in option if option != opt)):
                backup_f = option[4]
                break

        if not backup_f:
            logger.info("Could not find the specified backup file using %s", backup)
            return
        logger.info("Restoring %s to %s", backup_f, self.target_url.database)
        hostargs = []
        if self.target_url.host:
            hostargs += ["--host=%s" % self.target_url.host]
        if self.target_url.username:
            hostargs += ["--username=%s" % self.target_url.username]
        if self.target_url.port:
            hostargs += ["--port=%d" % self.target_url.port]

        try:
            conn = self.connect_target()
            self.backup("before restore")
        except:
            self.safe_create_user()
        
            self.safe_create_db()
            


        env = dict(os.environ)
        env["PGPASSWORD"]  = self.target_url.password  ## SECURITY RISK?

        args = ["pg_restore", "-d", self.target_url.database] + hostargs 
        logger.info("Restoring database %s (%s)", self.target_url.database, hostargs)
        logger.info("Calling: " + " ".join(args))

        datafp = open(os.path.join(self.backup_path, backup_f),"r")
        log = tempfile.NamedTemporaryFile()
        err = tempfile.NamedTemporaryFile()
        p = subprocess.call(args, env=env, stdin = datafp, stdout = log, stderr = err)

        
    def dropdb(self, backup = True):
        if backup:
            self.backup("before drop")
        conn = self.connect(database = "postgres")
        logger.info("Dropping database %s", self.target_url.database)
        result = conn.execute("DROP DATABASE %s" % self.target_url.database)
        if not result.rowcount :
            raise "Could not create database %s" % self.target_url.database
        conn.close()

    def info(self):
        conn = self.connect_target()
        result1 = conn.execute("SELECT usename FROM pg_user WHERE usename = '%s' " % self.target_url.username)
        if result1.rowcount :
            logger.info("Database user %s exists", self.target_url.username)
        
        result2 = conn.execute("SELECT datname FROM pg_database WHERE datname = '%s' " %  self.target_url.database)
        if result2.rowcount:
            logger.info("Database %s exists", self.target_url.database)

        if result1.rowcount and result2.rowcount:
            try:
                self.connect_target()
            except:
                logger.warn("Could not connect to database %s", self.target_url.database)
            else:
                logger.info("Connection to database %s ok", self.target_url.database)

        options = self.find_backups()
        self._show_options(options)
        

    def show(self):
        options = self.find_backups(True)
        self._show_options(options)

    def populate(self):
        raise NotImplementedError
        
    @staticmethod
    def _hash_option(option):
        return hashlib.md5(str(option)).digest().encode("base64")[0:6]
        
    def _show_options(self, options):
        if options:
            from babel.dates import format_datetime
            logger.info("The following backups were found:" )
            logger.info("Hash".ljust(7) + "Database".ljust(21) + "Date".ljust(31) + "Size".rjust(9) + " GIT version".ljust(22) + "Message")
            for opt in options:
                dt = datetime.datetime.strptime(opt[1], "%Y-%m-%dT%H:%M:%S.%f")
                sz = os.path.getsize(os.path.join(self.backup_path, opt[4]))
                logger.info( "%-6s %-20s %-30s %9d %-20s %s" % (self._hash_option(opt), opt[0], format_datetime(dt),
                                                                sz, opt[2], opt[3]) )
        else:
            logger.info("No backups were found")
        
            
    def find_backups(self, all = False):
        try:
            dir = os.listdir(self.backup_path)
        except OSError:
            return []
        files = []
        for f in dir:
            if f.startswith("db__"):
                name, ext = os.path.splitext(f)
                try:
                    _,db,date,version,message = name.split("__")
                    files.append((db,date,version,message,f))
                except:
                    logger.warn("This filename does not have the correct format: %s", name)
        if not all:
            options = [f for f in files if f[0] == self.target_url.database]
        else:
            options = files
        options.sort(key = lambda x:(x[0],x[1]), reverse=True) # order by date
        return options

    def clean(self):
        self.backup("before clean")
        conn = self.connect_target()

        engine = conn.engine
        from lifeshare.model import TraversalBase
        metadata = TraversalBase.metadata
        metadata.bind = engine
        metadata.drop_all()
    


def main(argv=sys.argv, quiet = False):
    options, args = getopt.gnu_getopt( argv[1:], "W", [ "adminuser=", "hostname=", "port=", "url="])
                                      
    opts = dict( [ (k[2:],v) for k,v in options if k.startswith("--")] )

    if ("-W" in [k for k,v in options]):
        opts['password'] = getpass.getpass("Password for database user %s" % (opts.get('adminuser') or DEFAULTADMINUSER) )

    url = opts.pop('url', None)

    global root
    if args:
        from ConfigParser import SafeConfigParser
        cfg = args[0]
        parser = SafeConfigParser(dict(here=os.path.dirname(os.path.abspath(cfg)) ))
        parser.read(cfg)
        settings = dict(parser.items("dbmanage"))
        #settings = get_appsettings(cfg)
        root = settings.get("root", os.path.dirname(os.path.abspath(cfg)))
        if not url:
            url = settings.get('sqlalchemy.url')
    else:
        root = "./"
        settings = {}
    cmd = args[1] if len(args)>1 else "info"

    if not url:
        print "Please specify a configuration file or a sqlalchemy db-uri"
        return 0

    manager = DBManager(url, settings = settings, **opts)
    if cmd == "setup":
        manager.setup_db()
    elif cmd in ("dump", "backup"):
        manager.backup(message = args[2] if len(args)>2 else "organic backup" )
    elif cmd == "restore":
        manager.restore(args[2])
    elif cmd == "drop":
        manager.dropdb()
    elif cmd == "info":
        manager.info()
    elif cmd == "show":
        manager.show()
    elif cmd == "populate":
        manager.populate(*args[2:])
    elif cmd == "clean":
        manager.clean()
    elif cmd == "help":
        print __help % dict(cmd = argv[0])
    else:
        logger.warn("Command %s is not recognized", cmd)
    

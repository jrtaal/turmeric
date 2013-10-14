#!/usr/bin/python

import os
import sys
import getopt
import subprocess
import datetime

import logging
logger = logging.getLogger("scripts.createdb")
logging.basicConfig(stream=sys.stdout, level=10, format = "%(message)s")

import sqlalchemy
from sqlalchemy.engine.url import make_url, URL

import getpass

DEFAULTADMINUSER = "postgres"
import logging
logger = logging.getLogger("create_db")

import hashlib
import tempfile

class DBManager(object):
    valid_commands = ( "info", "backup","restore", "init", "populate", "drop","show", "clean", "dump", "setup")
    
    def __init__(self, url, root ="", adminuser = DEFAULTADMINUSER, hostname = None, port = None, password = None, message = ""):
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
        self.root = root
        self.message = message
        self.backup_path = os.path.join(root, "var/backup")
        self.logger = logger
        
        self.progress("Using destination URI %s", url)
        self.progress("Using admin URI %s", self.admin_url)
        
    def progress(self, message, *args, **kwargs):
        self.logger.info(" * " + message +"\n",  *args, **kwargs)
        
    def perform_command(self, cmd, *args, **kwargs):
        if cmd in self.valid_commands:
            getattr(self, "turmeric_" + cmd)(*args, **kwargs)
        else:
            raise NotImplementedError("Command %s is not implemented" % cmd)
            
    def connect_dbadmin(self):
        if self.admin_conn and not self.admin_conn.closed:
            return self.admin_conn
        self.progress("Connecting as admin user %s to %s on %s", self.admin_url.username, "postgres", self.admin_url.host)
        engine = sqlalchemy.create_engine(self.admin_url) #"postgres://%s@/postgres" % self.adminuser ) 
        conn = engine.connect()
        conn.execute("COMMIT")
        self.admin_conn = conn
        return conn

    def connect_target(self):
        if self.target_conn and not self.target_conn.closed:
            return self.target_conn
        self.progress("Connecting as user %s to %s on %s", self.target_url.username, self.target_url.database, self.target_url.host)
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
        self.progress("Creating database user %s with password %s", self.target_url.username, self.target_url.password)
        result = conn.execute("CREATE USER %s WITH PASSWORD '%s'" % (self.target_url.username, self.target_url.password) )
        if not result.rowcount :
            raise "Could not create user %s" % self.target_url.username
        conn.close()

    def create_db(self):
        conn = self.connect_dbadmin()
        self.progress("Creating database %s with owner %s", self.target_url.database, self.target_url.username)
        result = conn.execute("CREATE DATABASE %s WITH OWNER %s" % (self.target_url.database, self.target_url.username) )
        if not result.rowcount :
            raise "Could not create database %s" % self.target_url.database
        conn.close()

    def grant_user(self):
        conn = self.connect_dbadmin()
        self.progress("Granting privileges to user %s", self.target_url.username)
        result = conn.execute("GRANT ALL PRIVILEGES ON DATABASE %s TO %s" % (self.target_url.database, self.target_url.username))
        if not result.rowcount :
            raise "Could not grant privileges to user %s" % self.target_url.username

        
            
    def make_gis(self):
        url = URL( drivername = "postgres",  username = self.adminuser, database=self.target_url.database,
                         password = self.admin_url.password, host = self.admin_url.host, port = self.admin_url.port)
        engine = sqlalchemy.create_engine(url)
        conn = engine.connect()
        #conn = self.connect_target()
        self.progress("Creating extension postgis")
        result = conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        if not result.rowcount :
            raise "Could not create postgis extension"

        self.progress("Creating extension postgis_topology")
        result = conn.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology")
        if not result.rowcount :
            raise "Could not create postgis_topology extension"

        conn.execute("GRANT ALL ON SCHEMA topology to %s" % self.target_url.username)
        for tab in ("spatial_ref_sys", "topology","layer"):
            result = conn.execute("ALTER TABLE %s OWNER TO %s " % (tab, self.target_url.username))

    def turmetic_setup(self):
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


    def turmeric_backup(self):
        return self.backup(message = self.message or "organic backup")

    def get_version(self):
        try:
            installed_version = subprocess.check_output(["git","describe","--long"]).split("\n")[0].strip()
        except:
            installed_version = open(os.path.join(self.root,"VERSION")).read()
        return installed_version
        
    def backup(self, message):
        installed_version = self.get_version()
        
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
        backupfn = os.path.join(self.backup_path,"db__%s__%s__%s__%s.sql" % (self.target_url.database, datestr, installed_version, message ) )
        args = ["pg_dump"] + hostargs + ["--format=c",  self.target_url.database]
        self.progress("Backing up Database %s (%s)", self.target_url.database, hostargs)
        self.progress("Calling: " + " ".join(args))
        err = tempfile.NamedTemporaryFile()
        subprocess.call(args, env=env, stdout = open(backupfn,"w"), stderr = err)
        self.progress("Backup writen to %s", backupfn)

        data = [ backupfn.split("__") ]
        self._show_options(data)
        
        
    def turmeric_restore(self, backup):
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
            self.progress("Could not find the specified backup file using %s", backup)
            return
        self.progress("Restoring %s to %s", backup_f, self.target_url.database)
        hostargs = []
        if self.target_url.host:
            hostargs += ["--host=%s" % self.target_url.host]
        if self.target_url.username:
            hostargs += ["--username=%s" % self.target_url.username]
        if self.target_url.port:
            hostargs += ["--port=%d" % self.target_url.port]

        try:
            conn = self.connect_target()
            self.backup("before restore: %s" % self.message)
        except:
            self.safe_create_user()
        
            self.safe_create_db()
            


        env = dict(os.environ)
        env["PGPASSWORD"]  = self.target_url.password  ## SECURITY RISK?

        args = ["pg_restore", "-d", self.target_url.database] + hostargs 
        self.progress("Restoring database %s (%s)", self.target_url.database, hostargs)
        self.progress("Calling: " + " ".join(args))

        datafp = open(os.path.join(self.backup_path, backup_f),"r")
        log = tempfile.NamedTemporaryFile()
        err = tempfile.NamedTemporaryFile()
        p = subprocess.call(args, env=env, stdin = datafp, stdout = log, stderr = err)

        
    def turmeric_dropdb(self, backup = True):
        if backup:
            self.backup("before drop: %s" % self.message)
        conn = self.connect(database = "postgres")
        self.progress("Dropping database %s", self.target_url.database)
        result = conn.execute("DROP DATABASE %s" % self.target_url.database)
        if not result.rowcount :
            raise "Could not create database %s" % self.target_url.database
        conn.close()

    def turmeric_info(self):
        conn = self.connect_target()
        result1 = conn.execute("SELECT usename FROM pg_user WHERE usename = '%s' " % self.target_url.username)
        if result1.rowcount :
            self.progress("Database user %s exists", self.target_url.username)
        
        result2 = conn.execute("SELECT datname FROM pg_database WHERE datname = '%s' " %  self.target_url.database)
        if result2.rowcount:
            self.progress("Database %s exists", self.target_url.database)

        if result1.rowcount and result2.rowcount:
            try:
                self.connect_target()
            except:
                logger.warn("Could not connect to database %s", self.target_url.database)
            else:
                self.progress("Connection to database %s ok", self.target_url.database)

        options = self.find_backups()
        print("The following backups were found:" )
        self._show_options(options)
        

    def turmeric_show(self, hash=None):
        options = self.find_backups(True)
        if hash:
            options = [o for o in options if self._hash_option(o).startswith(hash)]
            self._show_options(options)
        else:
            print("The following backups were found:" )
            self._show_options(options)

    def turmeric_populate(self):
        raise NotImplementedError
        
    @staticmethod
    def _hash_option(option):
        return hashlib.md5(str(option)).digest().encode("base64")[0:6]
        
    def _show_options(self, options):
        if options:
            from babel.dates import format_datetime
            print("Hash".ljust(7) + "Database".ljust(21) + "Date".ljust(31) + "Size".rjust(9) + " Version".ljust(22) + "Message")
            for opt in options:
                dt = datetime.datetime.strptime(opt[1], "%Y-%m-%dT%H:%M:%S.%f")
                sz = os.path.getsize(os.path.join(self.backup_path, opt[4]))
                print( "%-6s %-20s %-30s %9d %-20s %s" % (self._hash_option(opt), opt[0], format_datetime(dt),
                                                                sz, opt[2], opt[3]) )
        else:
            print("No backups were found")
        
            
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

    def turmeric_clean(self):
        self.backup("before clean: %s" % self.message)
        conn = self.connect_target()

        engine = conn.engine
        from lifeshare.model import TraversalBase
        metadata = TraversalBase.metadata
        metadata.bind = engine
        metadata.drop_all()
    
def main(argv=sys.argv, quiet = False):
    from argparse import ArgumentParser

    parser = ArgumentParser(description= """A database management tool. Turmeric supports creation, initialisation, populating and backup up databases.
                            """)
    parser.add_argument("command", help = "Command to perform.",
                        choices = DBManager.valid_commands,
                        default = "info")
    parser.add_argument("arguments", nargs ="*", help = "arguments to pass on to the command", default = [] )
    parser.add_argument("--config", help = "INI-file with a [turmeric] section", default = "app.ini")
    parser.add_argument("--adminuser", help = "name of user with administrator rights", default = "postgres")
    parser.add_argument("--hostname", help = "host to connect to", default = "localhost")
    parser.add_argument("--port", help = "port to connect to ")
    parser.add_argument("--url", help = "Target database uri (overrides the one in the config.ini file)", )
    parser.add_argument("--root", help = "Target directory. Backups are stored in <root>/var/backup/", )
    parser.add_argument("--message", "-m", help = "Commit message")
    parser.add_argument("-W", dest = "askpw", help = "Ask for password", type = bool, default = False)
    
    opts = parser.parse_args()

    if opts.askpw:
        opts.password = getpass.getpass("Password for database user %s" % (opts.adminuser or DEFAULTADMINUSER) )
    else:
        opts.password = ""
        
    from ConfigParser import SafeConfigParser, NoSectionError
    parser = SafeConfigParser(dict(here=os.path.dirname(os.path.abspath(opts.config)) ))
    parser.read(opts.config)
        
    try:
        settings = dict(parser.items("turmeric"))
    except NoSectionError:
        try:
            settings = dict(parser.items("dbmanage"))
        except:
            settings = {}
            
    if not opts.root:
        opts.root = settings.get("root", os.path.dirname(os.path.abspath(opts.config)))
    if not opts.url:
        opts.url = settings.get('sqlalchemy.url')

    if not opts.url:
        print "Please specify a configuration file or a sqlalchemy db-uri"
        return 0

    manager = DBManager(opts.url, root = opts.root, message = opts.message, adminuser = opts.adminuser, port = opts.port, hostname = opts.hostname, password = opts.password)

    return manager.perform_command( opts.command, *opts.arguments)

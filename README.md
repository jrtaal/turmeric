TURMERIC
=======

A SQLAlchemy based database management tool. Turmeric supports creation, initialisation, populating and backup up databases.



```
# turmeric --help

usage: turmeric [-h] [--config CONFIG] [--adminuser ADMINUSER]
                [--hostname HOSTNAME] [--port PORT] [--url URL] [--root ROOT]
                [--message MESSAGE] [-W ASKPW]
                command [arguments [arguments ...]]

A SQLAlchemy based database management tool. Turmeric supports creation,
initialisation, populating and backup up databases.

positional arguments:
  command               Command to perform
  arguments             arguments to pass on to the command

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG       INI-file with a [turmeric] section
  --adminuser ADMINUSER
                        name of user with administrator rights
  --hostname HOSTNAME   host to connect to
  --port PORT           port to connect to
  --url URL             Target database uri (overrides the one in the
                        config.ini file)
  --root ROOT           Target directory. Backups are stored in
                        <root>/var/backup/
  --message MESSAGE, -m MESSAGE
                        Commit message
  -W ASKPW              Ask for password
```

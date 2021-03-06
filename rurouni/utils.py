# coding: utf-8
import os
from os.path import dirname, basename, abspath, splitext


def run_twistd_plugin(filename):
    from twisted.scripts.twistd import runApp
    from twisted.scripts.twistd import ServerOptions
    from rurouni.conf import get_parser

    bin_dir = dirname(abspath(filename))
    root_dir = dirname(bin_dir)
    os.environ.setdefault('GRAPHITE_ROOT', root_dir)

    program = splitext(basename(filename))[0]
    parser = get_parser()
    (options, args) = parser.parse_args()

    if not args:
        parser.print_usage()
        return

    twistd_options = []
    try:
        from twisted.internet import epollreactor
        twistd_options.append('--reactor=epoll')
    except:
        pass

    if options.debug or options.nodaemon:
        twistd_options.append('--nodaemon')
    if options.pidfile:
        twistd_options.extend(['--pidfile', options.pidfile])
    if options.umask:
        twistd_options.extend(['--umask', options.umask])

    twistd_options.append(program)

    if options.debug:
        twistd_options.append('--debug')
    for name, value in vars(options).items():
        if (value is not None and
                name not in ('debug', 'nodaemon', 'pidfile', 'umask')):
            twistd_options.extend(["--%s" % name.replace("_", '-'),
                                  value])

    twistd_options.extend(args)
    config = ServerOptions()
    config.parseOptions(twistd_options)
    runApp(config)

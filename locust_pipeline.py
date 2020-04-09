import logging
import time
import sys
import signal
import gevent
import locust
import socket
from locust.main import (parse_options, find_locustfile, load_locustfile,
    parse_timespan, print_stats, print_percentile_stats, write_stat_csvs,
    print_error_report, setup_logging, console_logger, print_task_ratio,
    get_task_ratio_dict, stats_printer, stats_writer)
from locust.main import events, runners, web
from locust.runners import LocalLocustRunner, MasterLocustRunner, SlaveLocustRunner

version = locust.__version__

class LocustPipeline(object):
    options = None
    logger = None
    locustfile = None
    docstring = None
    locusts = None
    locust_classes = None

    def __init__(self, options={}):
        self.set_options(options)
        self.set_logger()
        self.set_locust_file(self.options.locustfile)
        self.get_locusts_from_file()
        self.get_locust_classes_from_locusts()

    def set_options(self, options):
        self.options = options

    def set_logger(self):
        # setup logging
        if not self.options.skip_log_setup:
            setup_logging(self.options.loglevel, self.options.logfile)
        self.logger = logging.getLogger(__name__)

    def set_locust_file(self, locustfile):
        self.locustfile = find_locustfile(locustfile)

        if not locustfile:
            self.logger.error("Could not find any locustfile! Ensure file ends in '.py' and see --help for available options.")
            sys.exit(1)

        if locustfile == "locust.py":
            self.logger.error("The locustfile must not be named `locust.py`. Please rename the file and try again.")
            sys.exit(1)

    def get_locusts_from_file(self):
        self.docstring, self.locusts = load_locustfile(self.locustfile)
        if self.options.list_commands:
            console_logger.info("Available Locusts:")
            for name in self.locusts:
                console_logger.info("    " + name)
            sys.exit(0)

        if not self.locusts:
            self.logger.error("No Locust class found!")
            sys.exit(1)

    def get_locust_classes_from_locusts(self):
        # make sure specified Locust exists
        if self.options.locust_classes:
            missing = set(self.options.locust_classes) - set(self.locusts.keys())
            if missing:
                self.logger.error("Unknown Locust(s): %s\n" % (", ".join(missing)))
                sys.exit(1)
            else:
                names = set(self.options.locust_classes) & set(self.locusts.keys())
                self.locust_classes = [self.locusts[n] for n in names]
        else:
            # list() call is needed to consume the dict_view object in Python 3
            self.locust_classes = list(self.locusts.values())


    #########################  RUN  #########################################
    def run(self):
        if self.options.show_task_ratio:
            console_logger.info("\n Task ratio per locust class")
            console_logger.info( "-" * 80)
            print_task_ratio(self.locust_classes)
            console_logger.info("\n Total task ratio")
            console_logger.info("-" * 80)
            print_task_ratio(self.locust_classes, total=True)
            sys.exit(0)
        if self.options.show_task_ratio_json:
            from json import dumps
            task_data = {
                "per_class": get_task_ratio_dict(self.locust_classes),
                "total": get_task_ratio_dict(self.locust_classes, total=True)
            }
            console_logger.info(dumps(task_data))
            sys.exit(0)

        if self.options.run_time:
            if not self.options.no_web:
                self.logger.error("The --run-time argument can only be used together with --no-web")
                sys.exit(1)
            if self.options.slave:
                self.logger.error("--run-time should be specified on the master node, and not on slave nodes")
                sys.exit(1)
            try:
                self.options.run_time = parse_timespan(self.options.run_time)
            except ValueError:
                self.logger.error("Valid --run-time formats are: 20, 20s, 3m, 2h, 1h20m, 3h30m10s, etc.")
                sys.exit(1)
            def spawn_run_time_limit_greenlet():
                self.logger.info("Run time limit set to %s seconds" % self.options.run_time)
                def timelimit_stop():
                    self.logger.info("Time limit reached. Stopping Locust.")
                    runners.locust_runner.quit()
                gevent.spawn_later(self.options.run_time, timelimit_stop)

        if self.options.step_time:
            if not self.options.step_load:
                self.logger.error("The --step-time argument can only be used together with --step-load")
                sys.exit(1)
            if self.options.slave:
                self.logger.error("--step-time should be specified on the master node, and not on slave nodes")
                sys.exit(1)
            try:
                self.options.step_time = parse_timespan(self.options.step_time)
            except ValueError:
                self.logger.error("Valid --step-time formats are: 20, 20s, 3m, 2h, 1h20m, 3h30m10s, etc.")
                sys.exit(1)

        if self.options.master:
            runners.locust_runner = MasterLocustRunner(self.locust_classes, self.options)
        elif self.options.slave:
            try:
                runners.locust_runner = SlaveLocustRunner(self.locust_classes, self.options)
            except socket.error as e:
                self.logger.error("Failed to connect to the Locust master: %s", e)
                sys.exit(-1)
        else:
            runners.locust_runner = LocalLocustRunner(self.locust_classes, self.options)
        # main_greenlet is pointing to runners.locust_runner.greenlet by default, it will point the web greenlet later if in web mode
        main_greenlet = runners.locust_runner.greenlet

        if self.options.no_web:
            if self.options.master:
                while len(runners.locust_runner.clients.ready) < self.options.expect_slaves:
                    logging.info("Waiting for slaves to be ready, %s of %s connected",
                                 len(runners.locust_runner.clients.ready), self.options.expect_slaves)
                    time.sleep(1)
            if self.options.step_time:
                runners.locust_runner.start_stepload(self.options.num_clients, self.options.hatch_rate, self.options.step_clients, self.options.step_time)
            elif not self.options.slave:
                runners.locust_runner.start_hatching(self.options.num_clients, self.options.hatch_rate)
                # make locusts are spawned
                time.sleep(1)
        elif not self.options.slave:
            # spawn web greenlet
            self.logger.info("Starting web monitor at http://%s:%s" % (self.options.web_host or "*", self.options.port))
            main_greenlet = gevent.spawn(web.start, self.locust_classes, self.options)

        if self.options.run_time:
            spawn_run_time_limit_greenlet()

        stats_printer_greenlet = None
        if not self.options.only_summary and (self.options.print_stats or (self.options.no_web and not self.options.slave)):
            # spawn stats printing greenlet
            stats_printer_greenlet = gevent.spawn(stats_printer)

        if self.options.csvfilebase:
            gevent.spawn(stats_writer, self.options.csvfilebase, self.options.stats_history_enabled)


        ########################  SHUTDOWN  ####################################
        def shutdown(code=0):
            """
            Shut down locust by firing quitting event, printing/writing stats and exiting
            """
            self.logger.info("Shutting down (exit code %s), bye." % code)
            if stats_printer_greenlet is not None:
                stats_printer_greenlet.kill(block=False)
            self.logger.info("Cleaning up runner...")
            if runners.locust_runner is not None:
                runners.locust_runner.quit()
            self.logger.info("Running teardowns...")
            events.quitting.fire(reverse=True)
            print_stats(runners.locust_runner.stats, current=False)
            print_percentile_stats(runners.locust_runner.stats)
            if self.options.csvfilebase:
                write_stat_csvs(self.options.csvfilebase, self.options.stats_history_enabled)
            print_error_report()
            sys.exit(code)

        # install SIGTERM handler
        def sig_term_handler():
            self.logger.info("Got SIGTERM signal")
            shutdown(0)
        gevent.signal(signal.SIGTERM, sig_term_handler)

        try:
            self.logger.info("Starting Locust %s" % version)
            main_greenlet.join()
            code = 0
            lr = runners.locust_runner
            if len(lr.errors) or len(lr.exceptions) or lr.cpu_log_warning():
                code = self.options.exit_code_on_error
            shutdown(code=code)
        except KeyboardInterrupt as e:
            shutdown(0)


if __name__ == "__main__":
    parser, options = parse_options()
    pipelline = LocustPipeline(options)
    pipelline.run()


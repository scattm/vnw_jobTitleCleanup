import time
from datetime import datetime
import ConfigParser


class Timer:
    def __init__(self):
        self.start_time = time.time() * 1000

    def get_total_time(self):
        total_time = time.time() * 1000 - self.start_time
        if total_time >= 60000:
            return "%d minutes %d seconds" % (total_time / 60000, (total_time % 60000) / 1000)
        elif total_time >= 1000:
            return "%d seconds" % (total_time / 1000)
        else:
            return "%d milliseconds" % total_time

    def get_total_milliseconds(self):
        return time.time() * 1000 - self.start_time

    def sleep_if_not_enough(self, total_delay_time):
        delta = (total_delay_time - self.get_total_milliseconds()) / 1000
        if delta > 0:
            time.sleep(delta)
            return " Sleep for %f seconds" % delta
        else:
            return " No sleep"


def set_timer_to_configuration(configfile, seconds_before=0):
    cur_time = time.time()
    cur_time -= seconds_before
    config = ConfigParser.RawConfigParser()
    config.read(configfile)

    try:
        config.set('RuntimeSettings', 'last_run_time', cur_time)
    except ConfigParser.NoSectionError:
        config.add_section('RuntimeSettings')
        config.set('RuntimeSettings', 'last_run_time', cur_time)

    with open(configfile, 'wb') as cf:
        config.write(cf)


def get_timer_from_configuration(configfile):
    config = ConfigParser.RawConfigParser()
    config.read(configfile)
    try:
        dt = datetime.fromtimestamp(float(config.get('RuntimeSettings', 'last_run_time')))
        return dt.strftime('%Y-%m-%dT%H:%M:%S+07:00')
    except ConfigParser.NoSectionError:
        return None
    except ConfigParser.NoOptionError:
        return None

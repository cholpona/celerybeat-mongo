# Copyright 2018 Regents of the University of Michigan

# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at http://www.apache.org/licenses/LICENSE-2.0

from datetime import datetime, timedelta

from mongoengine import *
from celery import current_app
import celery.schedules
import pytz
from celery.schedules import crontab
from collections import namedtuple

schedstate = namedtuple('schedstate', ('is_due', 'next'))

def get_periodic_task_collection():
    if hasattr(current_app.conf, "mongodb_scheduler_collection"):
        return current_app.conf.get("mongodb_scheduler_collection")
    elif hasattr(current_app.conf, "CELERY_MONGODB_SCHEDULER_COLLECTION"):
        return current_app.conf.CELERY_MONGODB_SCHEDULER_COLLECTION
    return "schedules"


#: Authorized values for PeriodicTask.Interval.period
PERIODS = ('days', 'hours', 'minutes', 'seconds', 'microseconds')


class TzAwareCrontab(crontab):
    def __init__(
            self, minute='*', hour='*', day_of_week='*',
            day_of_month='*', month_of_year='*', tz='Etc/UTC', app=None
    ):
        """Overwrite Crontab constructor to include a timezone argument."""
        self.tz = tz

        nowfun = self.nowfun
        super(TzAwareCrontab, self).__init__(
            minute=minute, hour=hour, day_of_week=day_of_week,
            day_of_month=day_of_month,
            month_of_year=month_of_year, nowfun=nowfun, app=app
        )

    def nowfun(self):
        return pytz.timezone(self.tz).normalize(
            pytz.utc.localize(datetime.utcnow())
        )

    def is_due(self, last_run_at):
        """Calculate when the next run will take place.
        Return tuple of (is_due, next_time_to_check).
        The last_run_at argument needs to be timezone aware.
        """
        # convert last_run_at to the schedule timezone
        print('last_run at',last_run_at)
        last_run_at = last_run_at.astimezone(pytz.timezone(self.tz))

        rem_delta = self.remaining_estimate(last_run_at)
        rem = max(rem_delta.total_seconds(), 0)
        due = rem == 0
        if due:
            rem_delta = self.remaining_estimate(self.utcnow())
            rem = max(rem_delta.total_seconds(), 0)
        return schedstate(due, rem)

    # Needed to support pickling
    def __repr__(self):
        return """<crontab: {0._orig_minute} {0._orig_hour}
         {0._orig_day_of_week} {0._orig_day_of_month}
          {0._orig_month_of_year} (m/h/d/dM/MY), {0.tz}>
        """.format(self)

    def __reduce__(self):
        return (self.__class__, (self._orig_minute,
                                 self._orig_hour,
                                 self._orig_day_of_week,
                                 self._orig_day_of_month,
                                 self._orig_month_of_year,
                                 self.tz), None)

    def __eq__(self, other):
        if isinstance(other, crontab):
            return (other.month_of_year == self.month_of_year
                    and other.day_of_month == self.day_of_month
                    and other.day_of_week == self.day_of_week
                    and other.hour == self.hour
                    and other.minute == self.minute
                    and other.tz == self.tz)
        return NotImplemented

class PeriodicTask(DynamicDocument):
    """MongoDB model that represents a periodic task"""

    meta = {'collection': get_periodic_task_collection(),
            'allow_inheritance': True}

    class Interval(EmbeddedDocument):
        """Schedule executing on a regular interval.

        Example: execute every 4 days
        every=4, period="days"
        """
        every = IntField(min_value=0, default=0, required=True)
        period = StringField(choices=PERIODS)

        meta = {'allow_inheritance': True}

        @property
        def schedule(self):
            return celery.schedules.schedule(timedelta(**{self.period: self.every}))

        @property
        def period_singular(self):
            return self.period[:-1]

        def __unicode__(self):
            if self.every == 1:
                return 'every {0.period_singular}'.format(self)
            return 'every {0.every} {0.period}'.format(self)



    class Crontab(EmbeddedDocument):
        """Crontab-like schedule.

        Example:  Run every hour at 0 minutes for days of month 10-15
        minute="0", hour="*", day_of_week="*", day_of_month="10-15", month_of_year="*"
        """
        minute = StringField(default='*', required=True)
        hour = StringField(default='*', required=True)
        day_of_week = StringField(default='*', required=True)
        day_of_month = StringField(default='*', required=True)
        month_of_year = StringField(default='*', required=True)
        tz = StringField(default='UTC')



        meta = {'allow_inheritance': True}

        @property
        def schedule(self):
            crontab = TzAwareCrontab(
                minute=self.minute,
                hour=self.hour,
                day_of_week=self.day_of_week,
                day_of_month=self.day_of_month,
                month_of_year=self.month_of_year,
                tz=self.tz
            )
            return crontab

        def __unicode__(self):
            rfield = lambda f: f and str(f).replace(' ', '') or '*'
            return '{0} {1} {2} {3} {4} (m/h/d/dM/MY)'.format(
                rfield(self.minute), rfield(self.hour), rfield(self.day_of_week),
                rfield(self.day_of_month), rfield(self.month_of_year),
            )

    name = StringField(unique=True)
    task = StringField(required=True)

    interval = EmbeddedDocumentField(Interval)
    crontab = EmbeddedDocumentField(Crontab)

    args = ListField()
    kwargs = DictField()

    queue = StringField()
    exchange = StringField()
    routing_key = StringField()
    soft_time_limit = IntField()

    expires = DateTimeField()
    start_after = DateTimeField()
    enabled = BooleanField(default=True)

    last_run_at = DateTimeField()

    total_run_count = IntField(min_value=0, default=0)
    max_run_count = IntField(min_value=0, default=0)

    date_changed = DateTimeField()
    date_creation = DateTimeField()
    description = StringField()

    run_immediately = BooleanField()
    no_changes = False

    def save(self, force_insert=False, validate=True, clean=True,
             write_concern=None, cascade=None, cascade_kwargs=None,
             _refs=None, save_condition=None, signal_kwargs=None, **kwargs):
        if not self.date_creation:
            self.date_creation = pytz.utc.localize(datetime.utcnow())
        if not self.date_creation.tzinfo:
            self.date_creation = pytz.utc.localize(self.date_creation)
        self.date_changed = pytz.utc.localize(datetime.utcnow())
        super(PeriodicTask, self).save(force_insert, validate, clean,
                                       write_concern, cascade, cascade_kwargs, _refs,
                                       save_condition, signal_kwargs, **kwargs)

    def clean(self):
        """validation by mongoengine to ensure that you only have
        an interval or crontab schedule, but not both simultaneously"""
        if self.interval and self.crontab:
            msg = 'Cannot define both interval and crontab schedule.'
            raise ValidationError(msg)
        if not (self.interval or self.crontab):
            msg = 'Must defined either interval or crontab schedule.'
            raise ValidationError(msg)

    @property
    def schedule(self):
        if self.interval:
            return self.interval.schedule
        elif self.crontab:
            return self.crontab.schedule
        else:
            raise Exception("must define interval or crontab schedule")

    def __unicode__(self):
        fmt = '{0.name}: {{no schedule}}'
        if self.interval:
            fmt = '{0.name}: {0.interval}'
        elif self.crontab:
            fmt = '{0.name}: {0.crontab}'
        else:
            raise Exception("must define interval or crontab schedule")
        return fmt.format(self)

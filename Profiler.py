"""
Lightweight-ish profiling of a Python program

We sometimes need to profile a long running Python program (i.e. work out how much time it spends in different states).
This arose out of problems doing a large ETL job which wasn't performing as desired and it was necessary to flush
out a whole host of bottlenecks.

Actinium offers a Profiler class, which then records various (user defined) state changes in as lightweight a manner
of possible and then periodically commits them to a database. (As it's designed to operate in long-running programs,
we have to do this to avoid memory leaks).
"""

import datetime
import time
import logging
import psycopg2


class Event:

    def __init__(self, state, comment=None, records=None):
        self.state = state
        self.comment = comment
        self.records = records
        self.start = time.perf_counter()
        self.end = None

    def set_end(self, counter):
        self.end = counter

class Profiler:

    def __init__(self, name, **kwargs):

        self.name = name
        self.db = self.connect_to_db(**kwargs)
        self.pid = self.create_new_profile(name, kwargs.get("comment"))
        self.events = []
        self.start = datetime.datetime.now()
        self.offset = time.perf_counter()
        self.created = datetime.datetime.now()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
        logging.debug(f"Profiler ID {self.pid} ({self.name}) closing")
        cursor = self.db.cursor()
        sql = "UPDATE profiler.profiles SET ended=current_timestamp WHERE id=%s"
        cursor.execute(sql, [self.pid])
        self.db.commit()
        cursor.close()
        self.db.close()

    def connect_to_db(self, **kwargs):
        """
        Connects to a Postgres DB
        :param kwargs:
        :return:
        """
        user = kwargs['user']
        password = kwargs['password']
        dbname = kwargs.get('dbname', 'quanta')
        host = kwargs.get('host', 'localhost')
        return psycopg2.connect(user=user, dbname=dbname, password=password, host=host)

    def create_new_profile(self, name, comment=None):
        """
        Inserts a new Profile in a Postgres DB and returns the inserted row number
        :param name:
        :param comment:
        :return:
        """
        sql = "INSERT INTO profiler.profiles (name, comment) VALUES (%s, %s) RETURNING id"
        cursor = self.db.cursor()
        cursor.execute(sql, [name, comment])
        new_row = cursor.fetchone()[0]
        self.db.commit()
        cursor.close()
        return new_row

    def flush(self):
        """
        Flushes the events in the queue out to the DB.
        :return:
        """
        if self.events:
            prev_state = self.events[-1].state
            prev_comment = self.events[-1].comment
            prev_records = self.events[-1].records
            self.append("Profiler Housekeeping")
            sql = "INSERT INTO profiler.events (profile_id, state, started, ended, records, comment) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor = self.db.cursor()
            for i in range(len(self.events)-1):
                self.events[i].set_end(self.events[i+1].start)
                start = (self.created + datetime.timedelta(seconds=self.events[i].start-self.offset)).strftime('%Y-%d-%m %H:%M:%S.%f')
                end = (self.created + datetime.timedelta(seconds=self.events[i].end-self.offset)).strftime('%Y-%d-%m %H:%M:%S.%f')
                cursor.execute(sql, [self.pid, self.events[i].state, start, end, self.events[i].records, self.events[i].comment])
            logging.debug(f"Profiler id {self.pid} ({self.name}) flushed {len(self.events)-1} events to DB")
            self.events = self.events[-1:]
            self.db.commit()
            cursor.close()
            self.append(prev_state, comment=prev_comment, records=prev_records)

    def append(self, state, comment=None, records=None):
        self.events.append(Event(state, comment=comment, records=records))
        if len(self.events) > 1:
            self.events[-1].set_end(self.events[-1].start)


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)
    with Profiler("Test", comment="Sunday afternoon", user="tim", dbname="quanta", host="crowley", password="swordfish123") as p:
        for i in range(10):
            for j in [1, 2, 5, 10]:
                logging.debug(f"Run #{i} will now sleep for {j}s")
                p.append(f"Run #{i}", comment=f"Will now sleep for {j}s", records=100*j)
                time.sleep(j)
            p.flush()

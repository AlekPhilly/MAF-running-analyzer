import psycopg2 as pg2
from sqlalchemy import create_engine
from pandas import read_sql, DataFrame
from modules.utilities import get_activities_list
from modules.tcx_processing import (
    parse_garmin_tcx,
    clean_garmin_tcx_data,
    extract_running_intervals,
)
from modules.utilities import timer
import datetime as dt
from numpy import nan, inf


class GarminDB:
    """Database with Garmin running activities
    and analytics"""

    def __init__(self, db_name, username, password):
        self._db_name = db_name
        self._username = username
        self._password = password

    @property
    def processed_files(self):
        self._cur.execute("SELECT filename FROM activities ORDER BY act_id")
        files = [x[0] for x in self._cur.fetchall()]
        return files

    @property
    def processed_activities(self):
        self._cur.execute("SELECT act_id FROM activities ORDER BY act_id")
        files = [x[0] for x in self._cur.fetchall()]
        return files

    # Methods for connection managing
    def connect(self):
        self._con = pg2.connect(
            database=self._db_name, user=self._username, password=self._password
        )
        self._cur = self._con.cursor()
        engine_name = f"postgresql+psycopg2://{self._username}:{self._password}@localhost:5432/{self._db_name}"
        self._engine = create_engine(engine_name)

    def disconnect(self):
        self._con.close()

    # Methods for working with tables
    def create_tables(self):
        """Initialize empty postgresql database with 3 tables:
        activities - contains all processed activities
        trackpoints - contains raw data from garmin tcx (only pace column is added)
        intervals - contains data for running/walking intervals
        summary - contains summary for activity
        """
        query1 = """
            CREATE TABLE IF NOT EXISTS activities(
                id serial,
                act_id timestamp PRIMARY KEY,
                filename varchar(30) UNIQUE
            );        
            """

        query2 = """
            CREATE TABLE IF NOT EXISTS trackpoints(
                act_id timestamp REFERENCES activities(act_id),
                time real,
                distance real,
                HR int,
                speed real,
                cadence int,
                latitude real,
                longitude real,
                altitude real,
                pace real
            );
            """

        query3 = """
            CREATE TABLE IF NOT EXISTS intervals(
                act_id timestamp REFERENCES activities(act_id),
                start_time real,
                stop_time real,
                start_dist real,
                stop_dist real,
                dHR int,
                type varchar(4),
                duration real,
                distance real,
                HRrate real
            );
            """

        query4 = """
            CREATE TABLE IF NOT EXISTS summary(
                act_id timestamp REFERENCES activities(act_id),
                duration varchar(8),
                distance real,
                pace real,
                avg_hr real,
                run_pct real
            );
            """
        for q in [query1, query2, query3, query4]:
            with self._con as con:
                with con.cursor() as cur:
                    cur.execute(q)

    def truncate_tables(self):
        query1 = """
                TRUNCATE TABLE activities, trackpoints, intervals, summary;
            """
        with self._con as con:
            with con.cursor() as cur:
                cur.execute(query1)

    def drop_tables(self):
        query1 = """
                DROP TABLE activities, trackpoints, intervals, summary;
            """
        with self._con as con:
            with con.cursor() as cur:
                cur.execute(query1)

    @timer
    def update_db(self, activities_dir):
        """update db from directory with tcx files"""

        activities = get_activities_list(activities_dir)
        processed_activities = self.processed_files

        for activity in activities:

            if activity.name in processed_activities:
                print("Entry already exists")
                continue

            try:
                id, data = parse_garmin_tcx(activity)
                data = clean_garmin_tcx_data(data)
                intervals = extract_running_intervals(data)
            except:
                continue

            intervals["act_id"] = id
            data["act_id"] = id
            data.columns = [x.lower() for x in data.columns]
            intervals.columns = [x.lower() for x in intervals.columns]

            run_duration = intervals[intervals["type"] == "run"]["duration"].sum()
            walk_duration = intervals[intervals["type"] == "walk"]["duration"].sum()
            run_pct = round(run_duration / (run_duration + walk_duration) * 100, 1)

            activity_stats = {
                "act_id": id,
                "duration": str(dt.timedelta(seconds=data["time"].values[-1])),
                "distance": round(data["distance"].values[-1] / 1000, 1),
                "pace": round(data["pace"].mean(), 1),
                "avg_hr": round(data["hr"].mean()),
                "run_pct": run_pct,
            }
            summary = DataFrame([activity_stats])

            try:
                self.add_activity_id(id, activity.name)
            except:
                # print('Entry already exists')
                continue

            for table, dta in zip(
                ("trackpoints", "intervals", "summary"), (data, intervals, summary)
            ):
                self.add_data(table, dta)

    @timer
    def actualize_db(self, activities_dir):
        """update db from directory with tcx files if any
        file isn't yet in db"""

        files_indb = set(self.processed_files)
        files_indir = get_activities_list(activities_dir)
        new_activities = {x.name for x in files_indir} - files_indb

        if new_activities:
            self.update_db(activities_dir)

    # Methods for working with activities
    def fetchone(self, act_id: str):
        trackpoints = read_sql(
            f"""SELECT * FROM trackpoints 
                                    WHERE act_id = '{act_id}'
                                    """,
            con=self._engine,
        )
        intervals = read_sql(
            f"""SELECT * FROM intervals
                                    WHERE act_id = '{act_id}'
                                    """,
            con=self._engine,
        )
        summary = read_sql(
            f"""SELECT * FROM summary AS s
                                WHERE act_id = '{act_id}'
                                """,
            con=self._engine,
        )
        return trackpoints, intervals, summary

    def fetchmany(self, act_ids: tuple):
        trackpoints = read_sql(
            f"""SELECT * FROM trackpoints 
                                    WHERE act_id IN {act_ids}
                                    """,
            con=self._engine,
        )
        intervals = read_sql(
            f"""SELECT * FROM intervals
                                WHERE act_id IN {act_ids}
                                """,
            con=self._engine,
        )
        summary = read_sql(
            f"""SELECT * FROM summary 
                                WHERE act_id IN {act_ids}
                                ORDER BY act_id DESC
                                """,
            con=self._engine,
        )
        return trackpoints, intervals, summary

    def fetchall(self):
        trackpoints = read_sql(
            f"""SELECT * FROM trackpoints 
                                    """,
            con=self._engine,
        )
        intervals = read_sql(
            f"""SELECT * FROM intervals
                                """,
            con=self._engine,
        )
        summary = read_sql(
            f"""SELECT * FROM summary
                                ORDER BY act_id DESC 
                                """,
            con=self._engine,
        )
        return trackpoints, intervals, summary

    def add_activity_id(self, id, filename):
        query1 = """
            INSERT INTO activities(act_id, filename)
                VALUES (%s, %s);
            """
        with self._con as con:
            with con.cursor() as cur:
                cur.execute(query1, (id, filename))

    def add_data(self, table, data):
        """Add data to one of the db's tables"""
        if table not in ["trackpoints", "intervals", "summary"]:
            raise ValueError(
                "No such table. Available tables: \
                            trackpoints, intervals, summary"
            )
        data.to_sql(table, con=self._engine, index=False, if_exists="append")

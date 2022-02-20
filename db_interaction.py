import psycopg2 as pg2
from sqlalchemy import create_engine



def init_db(db_name, username, password):

    con = pg2.connect(database=db_name, user=username, 
                        password=password)
    cur = con.cursor()

    query1 = '''
        CREATE TABLE IF NOT EXISTS activities(
            id serial,
            act_id timestamp PRIMARY KEY,
            filename varchar(30) UNIQUE
        );        
        '''

    query2 = '''
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
        '''
    
    query3 = '''
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
        '''

    query4 = '''
        CREATE TABLE IF NOT EXISTS summary(
            act_id timestamp REFERENCES activities(act_id),
            duration varchar(8),
            distance real,
            pace real,
            avg_hr real,
            run_pct real
        );
        '''
    for q in [query1, query2, query3, query4]:
        cur.execute(q)
    
    con.commit()
    con.close()

    return


def add_activity_id(db_name, username, password, id, filename):

    con = pg2.connect(database=db_name, user=username, 
                        password=password)
    cur = con.cursor()

    query1 = '''
        INSERT INTO activities(act_id, filename)
            VALUES (%s, %s);
        '''
    cur.execute(query1, (id, filename))
    con.commit()
    con.close()

    return


def add_data(db_name, username, password, table, data):

    if table not in ['trackpoints', 'intervals', 'summary']:
        raise ValueError('No such table. Available tables: \
                        trackpoints, intervals, summary')
    engine_name = f'postgresql+psycopg2://{username}:{password}@localhost:5432/{db_name}'
    engine = create_engine(engine_name)
    data.to_sql(table, con=engine, index=False, if_exists='append')

    return

def truncate_db(db_name, username, password):

    con = pg2.connect(database=db_name, user=username, 
                        password=password)
    cur = con.cursor()

    query1 = '''
            TRUNCATE TABLE activities, trackpoints, intervals, summary;
        '''
    
    cur.execute(query1)
    con.commit()
    con.close()

    return


def drop_all(db_name, username, password):

    con = pg2.connect(database=db_name, user=username, 
                        password=password)
    cur = con.cursor()

    query1 = '''
            DROP TABLE activities, trackpoints, intervals, summary;
        '''
    
    cur.execute(query1)
    con.commit()
    con.close()

    return

def processed_files(db_name, username, password):

    con = pg2.connect(database=db_name, user=username, 
                        password=password)
    cur = con.cursor()
    
    cur.execute('SELECT filename FROM activities')
    files = [x[0] for x in cur.fetchall()]

    con.close()

    return files
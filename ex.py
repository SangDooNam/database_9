import psycopg2
import os
import dotenv

dotenv.load_dotenv()

def setup():
    conn = None
    command_create_table = """CREATE TABLE IF NOT EXISTS site_user(id SERIAL PRIMARY KEY,
                                                        name VARCHAR(100));"""

    add_column_command = """ALTER TABLE site_user 
                            ADD COLUMN uuid UUID DEFAULT uuid_generate_v4(),
                            ADD COLUMN avatar BYTEA,
                            ADD COLUMN role ROLES,
                            ADD COLUMN birthdate DATE,
                            ADD COLUMN siblings TEXT[],
                            ADD COLUMN availability TIME_RANGE[],
                            ADD COLUMN site_setting JSON,
                            ADD COLUMN created_on TIMESTAMPTZ
                            ;"""
                            
    command_install_uuid = """CREATE EXTENSION IF NOT EXISTS "uuid-ossp";"""
    command_create_enum = """CREATE TYPE IF NOT EXISTS roles AS ENUM(
                            'Anonymous', 'Guest', 'User');"""
    commnad_create_time_range = """CREATE TYPE time_range AS (
                                    start_time TIME,
                                    end_time TIME
                                    );"""
    commnad_set_time_zone = """SET timezone = 'Europe/Berlin';"""
    
    
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        
        cursor = conn.cursor()
        cursor.execute(command_create_table)
        cursor.execute(command_install_uuid)
        # cursor.execute(command_create_enum)
        # cursor.execute(commnad_create_time_range)
        cursor.execute(commnad_set_time_zone)
        cursor.execute(add_column_command)
        
        conn.commit()
        cursor.close()
        
    except(Exception, psycopg2.DatabaseError) as  error:
        raise error
        
    finally:
        if conn is not None:
            conn.close()


def insert(data):
    conn = None
    command = """
            INSERT INTO site_user(
                name, role, birthdate, siblings, availability, site_setting, created_on)
            VALUES (
                %s, %s, %s, %s, ARRAY[%s]::time_range[] ,%s, %s
            );
            """
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('PASSWORD'),
            port=os.getenv('DB_PORT')
        )

        cursor = conn.cursor()
        
        def exists(id):
            cursor.execute('SELECT id FROM site_user WHERE id = %s;', (id,))
            return cursor.fetchone() is not None
        
        for record in data:
            if not exists(record['id']):
                values = (
                    record['name'],
                    record['role'],
                    record['birthdate'],
                    record['siblings'],
                    record['availability'],
                    record['site_setting'],
                    record['created_on']
                )
                cursor.execute(command, values)
        
        conn.commit()
        cursor.close()
    
    except(Exception, psycopg2.DatabaseError) as  error:
        raise error
        
    finally:
        if conn is not None:
            conn.close()


data = [
    {'id': '1',
    'name': 'Miriam Valira', 
    'role': 'Admin', 
    'birthdate': '1995-08-29', 
    "siblings": ['Dani', 'Louis'], 
    "availability": [('12:00:00','15:00:00')], 
    'site_setting':'{"background": "red", "notifications":false}', 
    'created_on':'09/23/15 08:56 AM'},
    
    {'id': '2',
    'name': 'Johann Müller', 
    'role': 'User', 
    'birthdate': '2002-05-09', 
    'siblings': [], 
    'availability': [('09:00:00','14:00:00'), ('18:00:00','20:00:00')], 
    'site_setting':'{"notifications":true}', 
    'created_on':'05/01/17 01:03 PM'},
    
    {'id': '3',
    'name': 'Louise Clark', 
    'role': 'Moderator', 
    'birthdate': '1992-05-03', 
    'siblings': ['Monique'], 
    'availability': [('09:00:00','12:00:00'), ('13:00:00','17:00:00')], 
    'site_setting':'{"notifications":true}', 
    'created_on':'03/21/07 10:31 AM'}
    ]


def alter_table():
    conn = None
    command = """
            ALTER TABLE site_user
            ADD COLUMN active_for INTERVAL;
            """
    command_column_check = """
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s AND
                        column_name = %s"""
    display_command = """
                    SELECT name, now() - created_on
                    AS active_for FROM site_user;
                    """
    # display_command = """
    #                 SELECT 
    #                 name, 
    #                 (EXTRACT(year FROM age(now(), created_on)) * 365) +
    #                 (EXTRACT(month FROM age(now(), created_on)) * 30) +
    #                 EXTRACT(day FROM age(now(), created_on)) AS active_for
    #                 FROM 
    #                 site_user;
    #                 """
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('PASSWORD'),
            port=os.getenv('DB_PORT')
        )

        cursor = conn.cursor()
        def exists(table_name, column_name):
            cursor.execute(command_column_check, (table_name, column_name))
            return cursor.fetchone() is not None
        if not exists('site_user', 'active_for'):
            cursor.execute(command)
        
        cursor.execute(display_command)
        rows = cursor.fetchall()
        
        print("{:<20} | {:<20}".format("Name", "Active_for"))
        print('-' * 45)
        for row in rows:
            print("{:<20} | {:<20}".format(*(str(value) if value is not None else '' for value in row)))
        
        conn.commit()
        cursor.close()
    
    except(Exception, psycopg2.DatabaseError) as  error:
        raise error
        
    finally:
        if conn is not None:
            conn.close()


find_command_1 = """SELECT name FROM site_user WHERE birthdate < '2000-01-01';"""
find_command_2 = """SELECT name FROM site_user WHERE array_length(siblings, 1) > 0;"""
find_command_3 = """SELECT name FROM site_user WHERE site_setting ->>'notifications' = 'true';"""
find_command_4 = """SELECT name FROM site_user WHERE cardinality(availability) > 1;"""



if __name__=='__main__':
    pass
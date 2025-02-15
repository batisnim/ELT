import subprocess
import time

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    retries =0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output =True, Text =True)
            if "accepting connections" in result.stdout:
                return True
        except subprocess.CalledProcessError as e:
            print(f"error connecting ot postgres:{e}")
            retries += 1
            print(f"Retrying in {delay_seconds} seconds...(Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    print("max reries reached exiting")
    return False
            
if not wait_for_postgres(host="source_postgres"):
    exit(1)
print("starting ELT Script...")


source_config={
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres'
}

dest_config={
    'dbname':'dest_db',
    'user':'postgres',
    'password': 'secret',
    'host': 'dest_postgres'
}

dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'
]

subprocess_env = dict(PGPASSWORD= source_config['password'])

subprocess.run(dump_command, env= subprocess_env, check=True)

load_command=[
    'psql',
    '-h', dest_config['host'],
    '-U', dest_config['user'],
    '-d', dest_config['dbname'],
    '-a','-f', 'data_dump.sql',
]

subprocess_env = dict(PGPASSWORD= dest_config['password'])

subprocess.run(load_command, env=subprocess_env, check=True )

print("ending ELT Script ...")
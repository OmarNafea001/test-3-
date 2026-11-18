import os
import re
import oracledb
import subprocess
from pathlib import Path

##Exit code	Meaning
##0	All rows loaded successfully
##1	Command line or unrecoverable error
##2	Warning (to handle headers exception)
##3	Fatal OS error



def prepare_ctl(flow_name, run_date,
                load_dir="/project/PLATFORM_INTEGRITY/load",
                export_dir="/project/PLATFORM_INTEGRITY/exports",
                ctl_dir="/project/PLATFORM_INTEGRITY/load/ctl",
                templates_dir="/project/PLATFORM_INTEGRITY/load/templates"
                ):

    template = Path(templates_dir) / f"{flow_name}.ctl"

    with open(template) as f:
        ctl = f.read()

    input_file = f"{export_dir}/{flow_name}_{run_date}.csv"

    ctl = ctl.replace("__INPUT_FILE__", input_file)

    temp_ctl = Path(ctl_dir) / f"{flow_name}_{run_date}.ctl"

    with open(temp_ctl, "w") as f:
        f.write(ctl)

    return str(temp_ctl)

def get_table_name_from_ctl(ctl_file):
    with open(ctl_file) as f:
        ctl = f.read()

    m = re.search(
        r'INTO\s+TABLE\s+("?[\w$#.]+"?)',
        ctl,
        re.IGNORECASE
    )

    if not m:
        raise ValueError(f"Could not find INTO TABLE in {ctl_file}")

    return m.group(1)


def drop_partition_if_exists(db_conn, table_name, run_date):
   

    conn = oracledb.connect(db_conn)
    cur = conn.cursor()

    try:
        if "." in table_name:
            owner, table = table_name.split(".", 1)
        else:
            owner = conn.username.upper()
            table = table_name

        owner = owner.upper()
        table = table.upper()

        sql = """
        SELECT partition_name
        FROM (
            SELECT
                partition_name,
                TO_DATE(
                    REGEXP_SUBSTR(
                        EXTRACTVALUE(
                            DBMS_XMLGEN.GETXMLTYPE(
                                'SELECT high_value
                                   FROM all_tab_partitions
                                  WHERE table_owner = ''' || table_owner || '''
                                    AND table_name = ''' || table_name || '''
                                    AND partition_name = ''' || partition_name || ''''
                            ),
                            '/ROWSET/ROW/HIGH_VALUE'
                        ),
                        '[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
                    ),
                    'YYYY-MM-DD HH24:MI:SS'
                ) - 1 partition_date
            FROM all_tab_partitions
            WHERE table_owner = :p_owner
              AND table_name = :p_table
              AND partition_name <> 'P_INIT'
        )
        WHERE partition_date = TO_DATE(:p_run_date,'YYYYMMDD')
        """

   

        cur.execute(
                sql,
                p_owner=owner,
                p_table=table,
                p_run_date=run_date
        )

        row = cur.fetchone()

        if row is None:
            print(f"No partition found for {table_name} on {run_date}")
            return

        partition_name = row[0]

        print(f"Dropping partition {partition_name}")

        cur.execute(
            f"ALTER TABLE {owner}.{table} DROP PARTITION {partition_name}"
        )

        conn.commit()

    finally:
        cur.close()
        conn.close()

def run_sqlldr(flow_name, run_date,
               db_conn=None,
               ctl_file=None,
               load_dir="/project/PLATFORM_INTEGRITY/load",
               rows=100000,
               recovery=True):

    if db_conn is None:
        db_conn = "PI/pipass123#@10.230.181.78:1521/RAIDDB"

    if ctl_file is None:
        ctl_file = os.path.join(load_dir, f"{flow_name}.ctl")
    table_name = get_table_name_from_ctl(ctl_file)

    if recovery:
        drop_partition_if_exists(
            db_conn=db_conn,
            table_name=table_name,
            run_date=run_date
        )

    cmd = [
        "sqlldr",
        db_conn,
        f"control={ctl_file}",
        "direct=true",
        f"rows={rows}",
        f"log={load_dir}/log/{flow_name}_{run_date}.log",
        f"bad={load_dir}/bad/{flow_name}_{run_date}.bad"
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True)

    if result.returncode not in (0, 2):
        raise RuntimeError(
            f"SQLLoader failed. Exit code={result.returncode}, stdout={result.stdout}, stderr={result.stderr}")


##ctl_file = prepare_ctl("charging", "20260607") ##it puts the run_date filename into the ctl file.
##run_sqlldr("charging", "20260607", ctl_file=ctl_file)

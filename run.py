#!/usr/bin/env python3
import MySQLdb
import argparse
from time import sleep


def main():
    args = define_parsers()
    db = "INFORMATION_SCHEMA"
    conn = create_db_connection(args.host, args.user, args.passwd, db)

    dic = {}

    for i in range(1):
        for p in get_process_list(conn):
            print(p)

        sleep(1)

    print(sorted(dic))
    conn.close


def get_process_list(conn):
    ret = []
    sql = "select * from INFORMATION_SCHEMA.PROCESSLIST"
    cursor = conn.cursor()
    cursor.execute(sql)

    for row in cursor.fetchall():

        host = row[2]
        port = ""

        if len(row[2].split(":")) == 2:
            host = row[2].split(":")[0]
            port = row[2].split(":")[1]

        ret.append(
            {
                "ID": row[0],
                "USER": row[1],
                "HOST": host,
                "PORT": port
            }
        )

    cursor.close

    return ret


def create_db_connection(host, user, password, db):

    connector = MySQLdb.connect(
        host=host,
        user=user,
        passwd=password,
        db=db
    )

    return connector


def define_parsers():
    parser = argparse.ArgumentParser(description='MySQL desc to BQ schema',
                                     add_help=False)
    parser.add_argument('--help', action='help', help='help')

    parser.add_argument('-u', '--user', type=str, default="root",
                        help='mysql user')
    parser.add_argument('-h', '--host', type=str, default="localhost",
                        help='mysql host')
    parser.add_argument('-p', '--passwd', type=str, default="",
                        help='mysql password')
    parser.add_argument('-d', type=str, default="1",
                        help='sleep range')

    return parser.parse_args()


if __name__ == "__main__":
    main()

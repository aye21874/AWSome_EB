import pymysql
from pymysql.constants import CLIENT

import os


class ColumbiaStudentResource:

    def __int__(self):
        pass

    @staticmethod
    def _get_connection():

        usr = os.environ.get("DBUSER")
        pw = os.environ.get("DBPW")
        h = os.environ.get("DBHOST")

        conn = pymysql.connect(
            user=usr,
            password=pw,
            host=h,
            cursorclass=pymysql.cursors.DictCursor,
            port=3306,
            autocommit=True
        )
        return conn

    @staticmethod
    def get_by_key(key):

        sql = 'SELECT s.course_name FROM courses.sections s NATURAL JOIN courses.enrollments e WHERE e.uni =%s'

        #update this

        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()

        # print(conn)

        res = cur.execute(sql, args=key)
        result = cur.fetchall()

        # print(result)

        return result

    @staticmethod
    def get_by_project_id(key):
        sql = 'select uni from courses.projects a, courses.enrollments b where a.project_id = b.project_id and a.project_id = %s'

        # update this

        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()

        # print(conn)

        res = cur.execute(sql, args=key)

        result = cur.fetchall()

        # print(result)

        return result



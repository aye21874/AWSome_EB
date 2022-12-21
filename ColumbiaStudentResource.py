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

    @staticmethod
    def get_by_call_no(key):
        sql = 'select uni from courses.sections Natural Join courses.enrollments where call_no = %s'
        # update this

        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()

        # print(conn)

        res = cur.execute(sql, args=key)

        result = cur.fetchall()

        # print(result)

        return result

    @staticmethod
    def get_by_instructor_name(key):
        sql = 'select * from courses.sections where instructor_name = %s'
        # update this

        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()

        # print(conn)

        res = cur.execute(sql, args=key)

        result = cur.fetchall()

        # print(result)

        return result

    @staticmethod
    def get_demo():
        sql = 'select * from courses.sections'
        # update this

        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()

        # print(conn)

        res = cur.execute(sql)

        result = cur.fetchall()

        # print(result)

        return result

    @staticmethod
    def get_all(uni):

        # sql = 'select * from courses.sections where call_no = %s'
        sql = 'select a.course_name, c.project_name, sum(b.credits) as total_credits from courses.sections a natural join courses.enrollments b join courses.projects as c on a.call_no = c.call_no where b.uni = %s group by b.uni'

        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()

        # print(conn)

        res = cur.execute(sql, args=uni)
        result = cur.fetchone()

        return result

    @staticmethod
    def update(data):
        # sql = 'select * from courses.sections where call_no = %s'
        sql = "insert into courses.enrollments (call_no, uni, project_id, credits) values ('%s', '%s', '%s', 3);"

        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()

        # print(conn)

        try:
            affected_count = cur.execute(sql, args=(data.uni,data.courses,data.projects))
            conn.commit()
        except pymysql.err.IntegrityError:
            pass
        finally:
            cur.close()
        # result = cur.fetchone()

    def delete(uni):

        sql = "DELETE FROM courses.enrollments WHERE uni = %s;"

        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()

        # print(conn)

        try:
            affected_count = cur.execute(sql, args=(uni))
            conn.commit()
        except pymysql.err.IntegrityError:
            pass
        finally:
            cur.close()




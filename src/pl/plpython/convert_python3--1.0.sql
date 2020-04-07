/* convert_python3--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION convert_python3" to load this file. \quit

-- This module provides two procedures, one to convert all python2
-- functions and one to do just one.  They're nearly identical, and
-- in principle convert_python3_all() could be written as a loop
-- around convert_python3_one().  It's not done that way since
-- creating a temp directory for each function in a bulk conversion
-- could get expensive.

-- For some benighted reason, lib2to3 has exactly no documented API,
-- so we must use the command-line API "2to3" instead.  User may pass
-- in the name of that program (in case it's not in the server's PATH)
-- as well as any desired options for it (perhaps some -f switches).

create procedure convert_python3_all(tool text default '2to3',
                                     options text default '')
language plpython3u as $$
import re, subprocess, tempfile

# pattern to extract just the function header from pg_get_functiondef result
aspat = re.compile("^(.*?\nAS )", re.DOTALL)
# pattern for replacing LANGUAGE portion
langpat = re.compile("\n LANGUAGE plpython2?u\n")

# collect info about functions to update
rv = plpy.execute("""
select p.oid::pg_catalog.regprocedure as funcid,
       pg_catalog.pg_get_functiondef(p.oid) as fd,
       prosrc as body
from pg_catalog.pg_proc p join pg_catalog.pg_language l on p.prolang = l.oid
where lanname in ('plpythonu', 'plpython2u')
order by proname
""")

# Make a temp directory to hold the file for 2to3 to work on.
with tempfile.TemporaryDirectory() as tmpdirname:

    # process each function
    for r in rv:
        # emit notices so user can tell which function failed, if one does
        plpy.notice("converting function " + r["funcid"])

        # extract everything but the body from pg_get_functiondef result
        m = aspat.match(r["fd"])
        if not m:
            raise ValueError('unexpected match failure')
        fheader = m.group(1)

        # replace the language clause
        fheader = langpat.sub("\n LANGUAGE plpython3u\n", fheader, 1)

        # put body in a temp file so we can apply 2to3
        f = open(tmpdirname + "/temp.py", mode = 'w')
        f.write(r["body"])
        f.close()

        # apply 2to3 to body
        subprocess.check_call(tool + " " + options + " --no-diffs -w " + tmpdirname + "/temp.py", shell=True)
        f = open(tmpdirname + "/temp.py", mode = 'r')
        fbody = f.read()
        f.close()

        # ensure check_function_bodies is enabled
        plpy.execute("set local check_function_bodies = true")

        # construct and execute SQL command to replace the function
        newstmt = fheader + plpy.quote_literal(fbody)
        # uncomment this for debugging purposes:
        # plpy.info(newstmt)
        plpy.execute(newstmt)

        # commit after each successful replacement, in case a later one fails
        plpy.commit()
$$;

-- The above procedure has to be superuser-only since it trivially allows
-- executing random programs.  But you'd have to be superuser anyway
-- to replace the definitions of plpython functions.

revoke all on procedure convert_python3_all(text, text) from public;


-- Here's the one-function version.

create procedure convert_python3_one(funcid regprocedure,
                                     tool text default '2to3',
                                     options text default '')
language plpython3u as $$
import re, subprocess, tempfile

# pattern to extract just the function header from pg_get_functiondef result
aspat = re.compile("^(.*?\nAS )", re.DOTALL)
# pattern for replacing LANGUAGE portion
langpat = re.compile("\n LANGUAGE plpython2?u\n")

# collect info about function to update, making sure it's the right language
plan = plpy.prepare("""
select p.oid::pg_catalog.regprocedure as funcid,
       pg_catalog.pg_get_functiondef(p.oid) as fd,
       prosrc as body
from pg_catalog.pg_proc p join pg_catalog.pg_language l on p.prolang = l.oid
where p.oid = $1 and lanname in ('plpythonu', 'plpython2u')
""", ["pg_catalog.regprocedure"])

rv = plpy.execute(plan, [funcid])

# Make a temp directory to hold the file for 2to3 to work on.
with tempfile.TemporaryDirectory() as tmpdirname:

    # process each function (we only expect one, but it's easy to loop)
    for r in rv:
        # extract everything but the body from pg_get_functiondef result
        m = aspat.match(r["fd"])
        if not m:
            raise ValueError('unexpected match failure')
        fheader = m.group(1)

        # replace the language clause
        fheader = langpat.sub("\n LANGUAGE plpython3u\n", fheader, 1)

        # put body in a temp file so we can apply 2to3
        f = open(tmpdirname + "/temp.py", mode = 'w')
        f.write(r["body"])
        f.close()

        # apply 2to3 to body
        subprocess.check_call(tool + " " + options + " --no-diffs -w " + tmpdirname + "/temp.py", shell=True)
        f = open(tmpdirname + "/temp.py", mode = 'r')
        fbody = f.read()
        f.close()

        # ensure check_function_bodies is enabled
        plpy.execute("set local check_function_bodies = true")

        # construct and execute SQL command to replace the function
        newstmt = fheader + plpy.quote_literal(fbody)
        # uncomment this for debugging purposes:
        # plpy.info(newstmt)
        plpy.execute(newstmt)
$$;

-- The above procedure has to be superuser-only since it trivially allows
-- executing random programs.  But you'd have to be superuser anyway
-- to replace the definitions of plpython functions.

revoke all on procedure convert_python3_one(regprocedure, text, text) from public;

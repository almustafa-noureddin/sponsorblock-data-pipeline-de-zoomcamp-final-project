import os
DBT_RAW_DATA=os.getenv('DBT_RAW_DATA')
# DROP TABLES

categoryVotes_table_drop = "DROP TABLE IF EXISTS categoryVotes"
lockCategories_table_drop = "DROP TABLE IF EXISTS lockCategories"
ratings_table_drop = "DROP TABLE IF EXISTS ratings"
sponsorTimes_table_drop = "DROP TABLE IF EXISTS sponsorTimes"
unlistedVideos_table_drop = "DROP TABLE IF EXISTS unlistedVideos"
userNames_table_drop = "DROP TABLE IF EXISTS userNames"
videoInfo_table_drop = "DROP TABLE IF EXISTS videoInfo"
vipUsers_table_drop = "DROP TABLE IF EXISTS vipUsers"
warnings_table_drop = "DROP TABLE IF EXISTS warnings"

# CREATE TABLES

categoryVotes_table_create = (f"""
    CREATE TABLE IF NOT EXISTS {DBT_RAW_DATA}.categoryVotes
    (
        uuid text,
        category text, 
        votes int 
    )
""")

lockCategories_table_create = (f"""
    CREATE TABLE IF NOT EXISTS {DBT_RAW_DATA}.lockCategories
    (
        videoid text,
        userid text,
        actiontype text,
        category text,
        hashedvideoid text,
        reason text,
        service text
    )
""")

ratings_table_create = (f"""
    CREATE TABLE IF NOT EXISTS {DBT_RAW_DATA}.ratings
    (
        videoid text,
        sevice text,
        type int,
        count int,
        hashedvideoid text
    )
""")

sponsorTimes_table_create = (f"""
    CREATE TABLE IF NOT EXISTS {DBT_RAW_DATA}.sponsorTimes
    (
        videoid text,
        starttime float4,
        endtime float4,
        votes int,
        locked int,
        incorrectvotes int,
        uuid text,
        userid text,
        timesubmitted int8,
        views int4,
        category text,
        actiontype text,
        service text,
        videoduration int4,
        hidden int4,
        reputation float4,
        shadowhidden int,
        hashedvideoid text,
        useragent text,
        description text
    )
""")

unlistedVideos_table_create = (f"""
    CREATE TABLE IF NOT EXISTS {DBT_RAW_DATA}.unlistedVideos
    (
        videoid text,
        year int,
        views int8,
        channelid text,
        timesubmitted int8,
        service text
    )
""")

userNames_table_create = (f"""
    CREATE TABLE IF NOT EXISTS {DBT_RAW_DATA}.userNames
    (
        userid text,
        username text,
        locked int
    )
""")

videoInfo_table_create = (f"""
    CREATE TABLE IF NOT EXISTS {DBT_RAW_DATA}.videoInfo
    (
        videoid text,
        channelid text,
        title text,
        published int8,
        genreurl text
    )
""")

vipUsers_table_create = (f"""
    CREATE TABLE IF NOT EXISTS {DBT_RAW_DATA}.vipUsers
    (
        userid text
    )
""")

warnings_table_create = (f"""
    CREATE TABLE IF NOT EXISTS {DBT_RAW_DATA}.warnings
    (
        userid text,
        issuetime int8,
        issuerid text,
        enabled int,
        reason text
    )
""")


# insert queries
categoryVotes_table_insert = (f"""
    INSERT INTO {DBT_RAW_DATA}.categoryVotes
    (uuid, category, votes)
    VALUES (%s, %s, %s)
    ;
""")

lockCategories_table_insert = (f"""
    INSERT INTO {DBT_RAW_DATA}.lockCategories
    (videoid, userid, actiontype, category, hashedvideoid, reason, service)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ;
""")

ratings_table_insert = (f"""
    INSERT INTO {DBT_RAW_DATA}.ratings
    (videoid ,sevice ,type ,count ,hashedvideoid)
    VALUES (%s, %s, %s, %s, %s)
    ;
""")

sponsorTimes_table_insert = (f"""
    INSERT INTO {DBT_RAW_DATA}.sponsorTimes
    (videoid, starttime, endtime, votes, locked, incorrectvotes, uuid, userid, timesubmitted, views, category, actiontype, service, videoduration, hidden, reputation, shadowhidden, hashedvideoid, useragent, description)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ;
""")

unlistedVideos_table_insert = (f"""
    INSERT INTO {DBT_RAW_DATA}.unlistedVideos
    (videoid, year, views, channelid, timesubmitted, service)
    VALUES (%s, %s, %s, %s, %s, %s)
    ;
""")

userNames_table_insert = (f"""
    INSERT INTO {DBT_RAW_DATA}.userNames
    (userid, username, locked)
    VALUES (%s, %s, %s)
    ;
""")

videoInfo_table_insert = (f"""
    INSERT INTO {DBT_RAW_DATA}.videoInfo
    (videoid, channelid, title, published, genreurl)
    VALUES (%s, %s, %s, %s, %s)
    ;
""")


vipUsers_table_insert = (f"""
    INSERT INTO {DBT_RAW_DATA}.vipUsers
    (userid)
    VALUES (%s)
    ;
""")

warnings_table_insert = (f"""
    INSERT INTO {DBT_RAW_DATA}.warnings
    (userid, issuetime, issuerid, enabled, reason)
    VALUES (%s, %s, %s, %s, %s)
    ;
""")


# QUERY LISTS

create_table_queries = [
                    categoryVotes_table_create, 
                    lockCategories_table_create, 
                    ratings_table_create, 
                    sponsorTimes_table_create, 
                    unlistedVideos_table_create, 
                    userNames_table_create, 
                    videoInfo_table_create, 
                    vipUsers_table_create, 
                    warnings_table_create
                    ]
drop_table_queries = [
                    categoryVotes_table_drop, 
                    lockCategories_table_drop, 
                    ratings_table_drop, 
                    sponsorTimes_table_drop, 
                    unlistedVideos_table_drop, 
                    userNames_table_drop, 
                    videoInfo_table_drop, 
                    vipUsers_table_drop, 
                    warnings_table_drop
                    ]
insert_table_queries = [
                    categoryVotes_table_insert, 
                    lockCategories_table_insert, 
                    ratings_table_insert, 
                    sponsorTimes_table_insert, 
                    unlistedVideos_table_insert, 
                    userNames_table_insert, 
                    videoInfo_table_insert, 
                    vipUsers_table_insert, 
                    warnings_table_insert
                    ]
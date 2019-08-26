"""Download NCAA football game data and store in a SQL database"""

import logging
import re
import requests
import sqlite3

import bs4
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from . import dbfile, now


def initialize_database(conn):
    """
    Initialize the SQL database and create the games table.
    """
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS games(
        date TEXT,
        season INTEGER,
        week INTEGER,
        team_home TEXT,
        score_home INTEGER,
        team_away TEXT,
        score_away INTEGER,
        UNIQUE(date, team_home, team_away));
    """)

    conn.commit()


def pullTable(url, tableID, header=True):
    """
    Pulls a table (indicated by tableID) from the specified url.

    """
    res = requests.get(url)
    comm = re.compile("<!--|-->")
    soup = bs4.BeautifulSoup(comm.sub("", res.text), 'lxml')
    tables = soup.findAll('table', id=tableID)
    data_rows = tables[0].findAll('tr')

    game_data = [
        [td.getText() for td in data_rows[i].findAll(['th', 'td'])]
        for i in range(len(data_rows))
    ]

    data = pd.DataFrame(game_data)

    if header is True:
        data_header = tables[0].findAll('thead')
        data_header = data_header[0].findAll("tr")
        data_header = data_header[0].findAll("th")

        header = []
        for i in range(len(data.columns)):
            header.append(data_header[i].getText())

        data.columns = header
        data = data.loc[data[header[0]] != header[0]]

    data = data.reset_index(drop=True)

    return data


def pullSeason(year):
    """
    Pull all college football games for the specified year.

    """
    baseurl = 'https://www.sports-reference.com/'
    url = baseurl + f"cfb/years/{year}-schedule.html"
    df = pullTable(url, "schedule")

    # drop extraneous columns
    drop_cols = [col for col in df.columns
                 if col in ['Rk', 'Time', 'Day', 'TV', 'Notes']]
    df.drop(labels=drop_cols, axis=1, inplace=True)

    # rename remaining columns
    df.columns = [
        'week',
        'date',
        'winner',
        'winner_pts',
        'location',
        'loser',
        'loser_pts',
    ]

    assert (len(df.columns) == 7)

    # drop rankings and replace empty games with NaN
    for team, team_pts in [('winner', 'winner_pts'), ('loser', 'loser_pts')]:
        df[team] = df[team].str.replace(r'\(\d+\)', '').str.strip()
        df[team_pts].replace('', np.nan, inplace=True)

    # drop games with no score
    df.dropna(inplace=True)

    # create home and away label and point columns
    away = df.location.str.contains('@')
    df['home'] = np.where(~away, df.winner, df.loser)
    df['home_pts'] = np.where(~away, df.winner_pts, df.loser_pts).astype(int)
    df['away'] = np.where(away, df.winner, df.loser)
    df['away_pts'] = np.where(away, df.winner_pts, df.loser_pts).astype(int)

    # drop winner and loser label and points columns
    drop_cols = ['location', 'winner', 'winner_pts', 'loser', 'loser_pts']
    df.drop(labels=drop_cols, axis=1, inplace=True)

    # insert season, specify datatypes
    df.insert(0, 'season', year)
    df.week = df.week.astype(int)
    df.date = pd.to_datetime(df.date).dt.strftime('%Y-%m-%d')

    # drop duplicates
    df.drop_duplicates(inplace=True)

    # change column order
    columns = ['date', 'season', 'week', 'home', 'home_pts', 'away', 'away_pts']
    return df.reindex(columns=columns)


def update_database(conn, refresh=False):
    """
    Save games to the SQL database.

    """
    c = conn.cursor()
    c.execute("SELECT season FROM games ORDER BY date DESC LIMIT 1")
    last_update = c.fetchone()

    start_season = (
        2000 if (last_update is None) or
        (refresh is True) else last_update[0]
    )

    end_season = now.year

    # loop over season years 2000-present
    logging.info("updating college football database")

    for season in range(start_season, end_season + 1):

        # print progress to stdout
        logging.info(f'season {season}')

        # scrape games from sports-reference
        for values in pullSeason(season).values.tolist():

            try:
                c.execute("""
                    INSERT INTO games(
                        date,
                        season,
                        week,
                        team_home,
                        score_home,
                        team_away,
                        score_away)
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                """, values)
            except sqlite3.IntegrityError:
                continue

    conn.commit()


def load_games(refresh=False):
    """
    Establish connection, then initialize and update database

    """
    engine = create_engine(r"sqlite:///{}".format(dbfile))

    if not refresh and dbfile.exists():
        return pd.read_sql_table('games', engine)

    conn = sqlite3.connect(str(dbfile))
    initialize_database(conn)
    update_database(conn, refresh=refresh)
    conn.close()

    return pd.read_sql_table('games', engine)

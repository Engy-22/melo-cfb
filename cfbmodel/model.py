"""Trains model and exposes predictor class objects"""

import logging
import operator
import pickle

from hyperopt import fmin, hp, tpe, Trials
from joblib import dump, load
import matplotlib.pyplot as plt
from melo import Melo
import numpy as np
import pandas as pd

from .data import load_games
from . import cachedir


class MeloCFB(Melo):
    """
    Generate college football point-spread or point-total predictions
    using the Margin-dependent Elo (MELO) model.

    """
    def __init__(self, mode, kfactor, home_field, halflife, fatigue):

        # hyperparameters
        self.mode = mode
        self.kfactor = kfactor
        self.home_field = home_field
        self.halflife = halflife
        self.fatigue = fatigue

        # model operation mode: 'spread' or 'total'
        if self.mode not in ['spread', 'total']:
            raise ValueError(
                "Unknown mode; valid options are 'spread' and 'total'")

        # mode-specific training hyperparameters
        self.commutes, self.compare, self.lines = {
            'total': (True, operator.add, np.arange(-0.5, 146.5)),
            'spread': (False, operator.sub, np.arange(-85.5, 86.5)),
        }[mode]

        # connect to college football stats database
        games_ = load_games(refresh=False)
        self.games = self.format_gamedata(games_)

        # instantiate the Melo base class
        super(MeloCFB, self).__init__(
            self.kfactor, lines=self.lines, sigma=1.0,
            regress=self.regress, regress_unit='year',
            commutes=self.commutes)

        # calibrate the model using the game data
        self.fit(
            self.games.date,
            self.games.team_home,
            self.games.team_away,
            self.compare(
                self.games.score_home,
                self.games.score_away,
            ),
            self.games.home_bias
        )

        # compute mean absolute error for calibration
        burnin = 2183
        residuals = self.residuals()
        self.loss = np.abs(residuals[burnin:]).mean()

    def format_gamedata(self, games):
        """
        Generator that yields a tuple of attributes for each game.

        """
        # sort games by date
        games.date = pd.to_datetime(games.date)
        games.sort_values('date', inplace=True)

        # game dates for every team
        game_dates = pd.concat([
            games[['date', 'team_home']].rename(
                columns={'team_home': 'team'}
            ),
            games[['date', 'team_away']].rename(
                columns={'team_away': 'team'}
            ),
        ]).sort_values('date')

        # calculate rest days
        for team in ['home', 'away']:
            games_prev = game_dates.rename(
                columns={'team': 'team_{}'.format(team)}
            )
            games_prev['date_{}_prev'.format(team)] = games.date

            games = pd.merge_asof(
                games, games_prev,
                on='date', by='team_{}'.format(team),
                allow_exact_matches=False
            )

        # add rest days columns
        rested = pd.Timedelta('255 days')
        games['home_rest'] = (games.date - games.date_home_prev).fillna(rested)
        games['away_rest'] = (games.date - games.date_away_prev).fillna(rested)

        # add bias factors
        home_fatigue = self.fatigue * np.exp(-games.home_rest.dt.days / 7.)
        away_fatigue = self.fatigue * np.exp(-games.away_rest.dt.days / 7.)
        relative_fatigue = self.compare(home_fatigue, away_fatigue)
        games['home_bias'] = self.home_field - relative_fatigue

        # drop unwanted columns
        games.drop(columns=['date_home_prev', 'date_away_prev'], inplace=True)

        return games

    def regress(self, years):
        """
        Regresses future ratings to the mean.

        """
        with np.errstate(divide='ignore'):
            return 1 - .5**np.divide(years, self.halflife)

    def bias(self, home_rest_days, away_rest_days):
        """
        Computes circumstantial bias factor given each team's rest.
        Accounts for home field advantage and rest.

        """
        home_fatigue = self.fatigue * np.exp(-home_rest_days / 7.)
        away_fatigue = self.fatigue * np.exp(-away_rest_days / 7.)

        return self.home_field - self.compare(home_fatigue, away_fatigue)

    def probability(self, times, labels1, labels2, bias=None, lines=0):
        """
        Survival function probability distribution

        """
        bias = self.home_field if bias is None else bias
        return super(MeloCFB, self).probability(
            times, labels1, labels2, bias=bias, lines=lines
        )

    def percentile(self, times, labels1, labels2, bias=None, p=50):
        """
        Distribution percentiles

        """
        bias = self.home_field if bias is None else bias
        return super(MeloCFB, self).percentile(
            times, labels1, labels2, bias=bias, p=p
        )

    def quantile(self, times, labels1, labels2, bias=None, q=.5):
        """
        Distribution quantiles

        """
        bias = self.home_field if bias is None else bias
        return super(MeloCFB, self).quantile(
            times, labels1, labels2, bias=bias, q=q
        )

    def mean(self, times, labels1, labels2, bias=None):
        """
        Distribution mean

        """
        bias = self.home_field if bias is None else bias
        return super(MeloCFB, self).mean(
            times, labels1, labels2, bias=bias
        )

    def median(self, times, labels1, labels2, bias=None):
        """
        Distribution median

        """
        bias = self.home_field if bias is None else bias
        return super(MeloCFB, self).median(
            times, labels1, labels2, bias=bias
        )

    def sample(self, times, labels1, labels2, bias=None, size=100):
        """
        Sample the distribution

        """
        bias = self.home_field if bias is None else bias
        return super(MeloCFB, self).sample(
            times, labels1, labels2, bias=bias, size=size
        )

    @classmethod
    def from_cache(cls, mode, steps=200, retrain=False):
        """
        Optimizes the MeloCFB model hyper parameters. Returns cached values
        if retrain is False and the parameters are cached, otherwise it
        optimizes the parameters and saves them to the cache.

        """
        cachefile = cachedir / '{}.pkl'.format(mode)

        if not retrain and cachefile.exists():
            logging.debug('loading {} model from cache', mode)
            model = load(cachefile)
            return model

        def objective(params):
            return cls(mode, *params).loss

        space = (
            hp.uniform('kfactor', 0.0, 1.0),
            hp.uniform('home_field', 0.0, 0.5),
            hp.uniform('halflife', 0.0, 40.0),
            hp.uniform('fatigue', 0.0, 1.0),
        )

        trials = Trials()

        logging.info(f'optimizing {mode} hyperparameters')

        parameters = fmin(objective, space, algo=tpe.suggest,
                          max_evals=steps, trials=trials,
                          show_progressbar=False)

        plotdir = cachedir / 'plots'
        plotdir.mkdir(exist_ok=True)

        fig, axes = plt.subplots(
            ncols=4, figsize=(12, 3), sharey=True)

        losses = trials.losses()

        for ax, (label, vals) in zip(axes.flat, trials.vals.items()):
            c = plt.cm.coolwarm(np.linspace(0, 1, len(vals)))
            ax.scatter(vals, losses, c=c)
            ax.axvline(parameters[label], color='k')
            ax.set_xlabel(label)
            if ax.is_first_col():
                ax.set_ylabel('Mean absolute error')

        plotfile = plotdir / '{}_params.pdf'.format(mode)
        plt.tight_layout()
        plt.savefig(str(plotfile))

        model = cls(mode, **parameters)

        logging.info('writing cache file %s', cachefile)

        cachefile.parent.mkdir(exist_ok=True)
        dump(model, cachefile, protocol=pickle.HIGHEST_PROTOCOL)

        return model

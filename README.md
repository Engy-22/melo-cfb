cfb-model
=========

*NCAA football ratings and predictions*

This module trains the margin-dependent Elo (melo) model on NFL game data. It creates two trained melo class objects, cfb_spreads and cfb_totals, which may be used to predict NCAA football point spreads and point totals. It also provides an interface to optimize the hyperparameters of the model.

Usage
-----
```
from datetime import datetime
from melo_cfb import cfb_spreads

ranked_teams = nfl_spreads.rank(datetime.today(), statistic='median')
```

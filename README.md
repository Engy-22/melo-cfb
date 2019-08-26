College football model
======================

*NCAA college football ratings and predictions*

This module trains the margin-dependent Elo (melo) model on college football game data.

Installation
------------

```
git clone git@github.com:morelandjs/melo-cfb.git && cd melo-cfb
pip install .
```

Quick Start
-----------
First, populate the database
```
cfbmodel update
```
Then train the model on the dataset
```
cfbmodel train --steps 200
```
Finally, compute point spread and point total predictions
```
cfbmodel predict 2018-12-30 "Ohio State" Michigan
cfbmodel predict 2019-09-05 Clemson Alabama
```
The model also ranks teams by their mean expected point spread (and point total) against a league average opponent.
```
cfbmodel rank
```

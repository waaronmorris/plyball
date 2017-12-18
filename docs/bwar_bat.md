# Bwar Batting

__bwar_bat(return_all=False)__

Get Baseball Reference's WAR stats from its war_daily_bat table, along with some other data that's not included in the batting_stats_bref() function. 

## Arguments
__return_all__ Bool. Returns all fields from war_daily_bat table if True, returns only a subset of columns if False. Defaults to False because most fields aren't needed for standard use cases. 

## Examples of valid queries

~~~~
from pybaseball import bwar_bat

# get war stats from baseball reference 
data = bwar_bat()

# get war stats plus additional fields from this table 
data = bwar_bat(return_all=True)
~~~~
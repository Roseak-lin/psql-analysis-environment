# psql-analysis-environment

This project is the setup for the postgresql environment. 

## Obtaining data sets and dependencies

To start, download all 7 .tsv.gz files from [here](https://datasets.imdbws.com/) and add them all to the empty `DB` directory

Next, this project requires the `psycopg2` dependency, which can be installed by running 
```
pip install psycopg2-binary
```

Finally, before running the python setup script, open up the powershell (or some other command line program), and run the following
```
sed -i 's/\r$//' decompress.sh
```
This fixes the line endings in the decompression script so that it can be executed by bash (on Windows)
___

## Running Program
There is a file called `credentials.json` which contains 2 fields: `user` and `password`. Fill these out to match those of your local postgresql environment.

Next, run the `initialize.py` file. The 7 data files should be decompressed and loaded into the local postgres environment.

The new database should be called `imdb`, and can be connected to via the postgresql command line with `\c imdb`. Once connected, queries can be run on any of the 7 tables:
```
name_basics
title_akas
title_basics
title_crew
title_episode
title_principals
title_ratings
```
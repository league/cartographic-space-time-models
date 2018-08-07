# cartographic-space-time-models

For the maximum daily temperature models from CDIAC data, the Python
script `temperature-tube.py` can be loaded and run in Blender. The
constants `tempsFile` and `dataDir` specify from where it reads the
data.

For MTA ridership models, the Haskell script `mta-analysis.hs` was
used to create a PostgreSQL database into which raw CSV data can be
imported -- see the comments near the top of the script. Then the
provided Haskell functions can be used to aggregate, de-accumulate,
and analyze the data from the PostgreSQL database. Preprocessed
station data are then pasted from Haskell output into the
`mta-tube.py` script.

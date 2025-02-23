# lstore/config.py

PAGE_SIZE = 4096              # bytes per page
NUM_COLS_LIMIT = 16           # max columns, as an example
FRAME_CAPACITY = 8            # how many pages can be in memory at once
BACKGROUND_MERGE = True       # toggles whether we spawn a background merge thread

# For the bufferpool's replacement policy:
REPLACEMENT_POLICY = 'LRU'    # or 'MRU', etc.

# Filenames or paths
DATA_PATH = "./data"          # directory to store table files

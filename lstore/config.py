PAGE_SIZE = 4096              # bytes per page
NUM_COLS_LIMIT = 16           # just an example
FRAME_CAPACITY = 8            # how many pages can be in memory at once
BACKGROUND_MERGE = True       # toggles background merge
REPLACEMENT_POLICY = 'LRU'    # for the bufferpool
DATA_PATH = "./data"          # directory to store table files

# For concurrency
ENABLE_CONCURRENCY = True

#
# Author:
#   Kian Ho <hui.kian.ho@gmail.com>
#
# Description:
# 	This Makefile controls the data wrangling tasks associated with the BX
# 	dataset.
#
# Usage:
#
#   Make the primary BX dataset:
#
#   $ make data/bx.db
# 
export SHELL
SHELL:=/bin/bash

BASE_DIR=$(shell pwd)
BIN=$(BASE_DIR)/bin
DATA=$(BASE_DIR)/data
CSV=$(DATA)/csv
LOG=$(BASE_DIR)/log

CSV_ZIP=$(DATA)/BX-CSV-Dump.zip
CSV_FILES=$(addprefix $(DATA)/BX, -Book-Ratings.csv -Books.csv -Users.csv)

ETL_PY=$(BIN)/etl.py
BX_DB=$(DATA)/bx.db


# Inspect the bx.db sqlite tables.


# ETL the csv files into a single sqlite db.
$(BX_DB): $(DATA)/.ds
	time $(ETL_PY) etl \
	   	$(CSV)/BX-Users.csv $(CSV)/BX-Books.csv $(CSV)/BX-Book-Ratings.csv --db $@

# Inspect the csv files, this doesn't clean the csv files, it merely writes the
# suspect rows to stdout for the developer's reference. "Suspect" rows are
# assumed to be those that don't conform to a pre-determined format.
$(LOG)/BX.log: $(LOG)/.ds
	( time $(ETL_PY) check \
	   	$(CSV)/BX-Users.csv $(CSV)/BX-Books.csv $(CSV)/BX-Book-Ratings.csv ) > $@

# Generate/unzip the raw csv files.
$(DATA)/csv/.ds: $(CSV_ZIP) $(DATA)/.ds
	mkdir -p $(@D) && touch $@
	unzip -d $(@D) $<

#
# Directory stamp targets.
#
$(LOG)/.ds: 
	mkdir -p $(@D) && touch $@

$(DATA)/.ds:
	mkdir -p $(@D) && touch $@

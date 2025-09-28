# Makefile
# Handy shortcuts. Requires Python 3.10+ and bs4 installed.

VENV?=.venv
PY?=python3
PIP?=$(VENV)/bin/pip
PYBIN?=$(VENV)/bin/python

SEEDS?=seeds.txt
LOG?=logs/run1.tsv
UA?=NYU-CS6913-HW1/1.0\ (Your\ Name;\ your_email@nyu.edu)

.PHONY: venv install run analyze clean

venv:
	$(PY) -m venv $(VENV)

install: venv
	$(PIP) install -U pip
	$(PIP) install -r requirements.txt

run:
	mkdir -p $(dir $(LOG))
	$(PYBIN) main.py --seeds $(SEEDS) --user-agent "$(UA)" --threads 32 --max-pages 5000 --log $(LOG)

analyze:
	$(PYBIN) tools/analyze_log.py $(LOG)

clean:
	rm -f logs/*.tsv

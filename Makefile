.PHONY: build clean rebuild

build:
	pyinstaller build.spec

clean:
	rm -rf build dist __pycache__ *.pyc

rebuild: clean build

buildgui:
	mkdir -p dist
	pyside6-deploy -c pysidedeploy.linux.spec

cleangui:
	rm -rf dist src/merge_scores/deployment

rebuildgui: cleangui buildgui

install:
	pip install .
	ln -sfn $(HOME)/.local/state/mergemusescores logs

dev:
	pip install -e . --group dev
	ln -sfn $(HOME)/.local/state/mergemusescores logs

lint:
	ruff check .

format:
	ruff format .

fix:
	ruff check . --fix
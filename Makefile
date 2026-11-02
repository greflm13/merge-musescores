.PHONY: build clean rebuild

build:
	pyinstaller build.spec

clean:
	rm -rf build dist __pycache__ *.pyc

rebuild: clean build

gui:
	mkdir -p dist
	pyside6-deploy -c pysidedeploy.linux.spec

install:
	pip install .
	ln -s $(HOME)/.local/state/mergemusescores logs

dev:
	pip install -e . --group dev
	ln -s $(HOME)/.local/state/mergemusescores logs

lint:
	ruff check .

format:
	ruff format .

fix:
	ruff check . --fix
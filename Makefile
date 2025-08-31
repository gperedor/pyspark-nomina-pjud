.PHONY: clean fmt install install-deps lint test

clean:
	rm -rf build/
	rm -rf dist/

fmt:
	python3 -m black pyspark_applications test
	python3 -m isort pyspark_applications test

install:
	python3 -m pip install -r requirements.txt
	python3 -m poetry build

install-deps:
	python3 -m pip install -r requirements.txt -r requirements-test.txt

lint:
	python3 -m black --check pyspark_applications test
	python3 -m isort --check-only pyspark_applications test

test:
	python3 -m pytest test

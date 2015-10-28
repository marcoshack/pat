.PHONY: all test doctest test_modules clean

all: test

test: doctest test_modules

doctest: clean
	cd pat && for j in $$(find ./nginx -name '*.py' |egrep -v ^.*[_]?test.py); do DOCTEST=1 python $$j; done

test_modules: clean
	python -m unittest test.nginx

clean:
	find ./pat -name *.pyc -exec rm -rf {} \;

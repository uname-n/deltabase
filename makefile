
project_dir = /Users/dm/Documents/github/uname-n/

pytest:
	poetry run pytest --durations=0 -v \
		tests/test_delta.py \
		tests/test_delta_workflow.py

pytest-compare:
	poetry run pytest --durations=0 -vv tests/test_comparision.py --width 1000 --height 1000 \
		| sed 's|$(project_dir)||g' > tests/logs/compare.sqlite.01-01.txt

	poetry run pytest --durations=0 -vv tests/test_comparision.py --width 2000 --height 2000 \
		| sed 's|$(project_dir)||g' > tests/logs/compare.sqlite.02-02.txt

	poetry run pytest --durations=0 -vv tests/test_comparision.py --width 1000 --height 10000 \
		| sed 's|$(project_dir)||g' > tests/logs/compare.sqlite.01-10.txt

	poetry run pytest --durations=0 -vv tests/test_comparision.py --width 10000 --height 10000 \
		| sed 's|$(project_dir)||g' > tests/logs/compare.sqlite.10-10.txt
	
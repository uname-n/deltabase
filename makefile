
docs_build_delete:
	rm -rf docs/build/

docs_build: docs_build_delete
	sphinx-build -M html docs/source/ docs/build/

docs: docs_build
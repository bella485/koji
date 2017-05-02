PYTHON=python
BUILDERPLUGINDIR = /usr/lib/koji-builder-plugins
BUILDERFILES = $(wildcard builder/*.py)
BUILDERCONFDIR = /etc/kojid/plugins
BUILDERCONFFILES = $(wildcard builder/*.conf)

_default:
	@echo "nothing to make.  try make install"

clean:
	rm -f *.o *.so *.pyc *.pyo *~

install:
	@if [ "$(DESTDIR)" = "" ]; then \
		echo " "; \
		echo "ERROR: A destdir is required"; \
		exit 1; \
	fi

	mkdir -p $(DESTDIR)/$(BUILDERPLUGINDIR)
	install -p -m 644 $(BUILDERFILES) $(DESTDIR)/$(BUILDERPLUGINDIR)
	$(PYTHON) -c "import compileall; compileall.compile_dir('$(DESTDIR)/$(BUILDERPLUGINDIR)', 1, '$(BUILDERPLUGINDIR)', 1)"
	mkdir -p $(DESTDIR)/$(BUILDERCONFDIR)
	install -p -m 644 $(BUILDERCONFFILES) $(DESTDIR)/$(BUILDERCONFDIR)

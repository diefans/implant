# this is part of the core bootstrap
# use only double quotes!
if __name__ == "__main__":
    import os, sys, site, pkg_resources
    venv_path = os.path.expanduser("{venv}")
    entry = site.getsitepackages([venv_path])[0]

    # create venv if missing
    if not os.path.isdir(entry):
        import venv
        venv.create(venv_path, system_site_packages=False, clear=True, symlinks=False, with_pip=True)

    # insert venv at first position
    # pkg_resources is not adding site-packages if there is no distribution
    sys.prefix = venv_path
    sys.path.insert(0, entry)
    site.addsitedir(entry)
    pkg_resources.working_set.add_entry(entry)

    # pip should come from venv now
    try:
        import msgpack
    except ImportError:
        # try to install msgpack
        import pip
        # TODO use ssh port forwarding to install via master
        pip.main(["install", "--prefix", venv_path, "-q", "msgpack-python"])

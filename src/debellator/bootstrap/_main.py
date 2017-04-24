# this is part of the core bootstrap
# use only double quotes!
if __name__ == "__main__":
    import sys, imp, base64, zlib
    # just a msgpack fallback if no venv is used or msgpack somehow failed to install
    try:
        import msgpack
    except ImportError:
        sys.modules["msgpack"] = msgpack = imp.new_module("msgpack")
        c = compile(zlib.decompress(base64.b64decode(b"{msgpack_code}")), "{msgpack_code_path}", "exec")
        exec(c, msgpack.__dict__)

    sys.modules["debellator"] = debellator = imp.new_module("debellator")
    setattr(debellator, "__path__", [])
    sys.modules["debellator.core"] = core = imp.new_module("debellator.core")
    debellator.__dict__["core"] = core

    c = compile(zlib.decompress(base64.b64decode(b"{code}")), "{code_path}", "exec", dont_inherit=True)
    exec(c, core.__dict__)
    core.main(**core.decode(base64.b64decode(b"{options}")))

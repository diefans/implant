"""

---

hosts: !group
    - !remote
        hostname: foobar
        sudo: true
        user: dbltr

evolve:
    - !for
        iterate: !ref hosts
        name: host
        do: !spawn


"""
import pkg_resources
import logging
from . import definitions, specs

log = logging.getLogger(__name__)


def main(root=None):
    """Run the evolution."""

    log.info("Evolving...")

    eps = pkg_resources.iter_entry_points('debellator.specs')
    spx = specs.Specs(*eps)

    if root:
        log.info("Lookup specs in %s", root)
        spx.add_root(root)

    for ns, resources in spx.items():
        log.info("Found %s in namespace %s", ', '.join(resources.keys()) or 'nothing', ns)


        for spec in resources.values():
            if 'evolve' in spec:
                log.info("evolving %s", spec)

    from pdb import set_trace; set_trace()       # XXX BREAKPOINT
    #
